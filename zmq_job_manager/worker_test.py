import sys
import traceback
import threading
import signal
import functools
from multiprocessing import Pipe
from datetime import datetime, timedelta
from threading import Thread
import logging
from collections import OrderedDict
from uuid import uuid4
import time
import platform
import os
try:
    import cPickle as pickle
except ImportError:
    import pickle

import psutil
import netifaces
import eventlet
import zmq
# The following durus imports cause the worker process to hang
# (see #4, https://github.com/cfobel/zmq_job_manager/issues/4)
# For now, the worker process throws a `SystemError` exception when terminating
# to force termination.
from durus.persistent_list import PersistentList
from persistent_helpers.storage import DurusStorage
from zmq.utils import jsonapi
from zmq.eventloop import zmqstream
from zmq.eventloop.ioloop import IOLoop, PeriodicCallback
from zmq_helpers.socket_configs import get_run_context
from zmq_helpers.rpc import ZmqRpcTask, ZmqRpcProxy, RpcHandlerMixin
from zmq_helpers.utils import log_label, unique_ipc_uri, cleanup_ipc_uris
from cpu_info.cpu_info import cpu_info, cpu_summary

from .manager import get_seconds_since_epoch
from .process import PopenPipeReactor
from .constants import SERIALIZE__PICKLE, SERIALIZE__JSON
from .worker_limits import extract_memory_limit, extract_runtime_limit


class ProxyPopen(PopenPipeReactor):
    def communicate(self, proxy):
        self.proxy = proxy
        super(ProxyPopen, self).communicate()

    def on_stdout(self, value):
        self.proxy.stdout(value)

    def on_stderr(self, value):
        self.proxy.stderr(value)


def worker_info():
    interfaces = [i for i in netifaces.interfaces()
                  if netifaces.ifaddresses(i).get(netifaces.AF_INET,
                                                  [{}])[0].get('addr')]
    p = psutil.Process(os.getpid())
    worker_memory = p.get_memory_info()._asdict()

    d = OrderedDict([
        ('cpu_summary', cpu_summary()),
        ('cpu_info', cpu_info()),
        ('hostname', platform.node()),
        ('physical_memory', psutil.phymem_usage()._asdict()),
        ('virtual_memory', psutil.virtual_memory()._asdict()),
        ('worker_memory', worker_memory),
        ('swap_memory', psutil.swap_memory()._asdict()),
        ('interfaces', OrderedDict([
            (i, netifaces.ifaddresses(i)[netifaces.AF_INET][0])
                                    for i in interfaces])),
    ])
    return d


class TaskMonitor(object):
    def __init__(self, worker, supervisor, task_count=-1):
        self.worker = worker
        self.supervisor = supervisor
        self._task_count = task_count
        self._completed_task_uuids = []
        self._task = None
        self._thread = None
        self._uuid = None
        self._deferred = None

    def update_state(self):
        if self._thread is not None:
            logging.getLogger(log_label(self)).debug('self._thread %s',
                                                     self._thread)
            if not self._thread.isAlive():
                # The task thread has completed, so perform finalization.
                self._finalize()
            else:
                # The task thread is still running, so try to join, but timeout
                # if the task thread is still not ready.
                self._thread.join(0.01)
        elif self._deferred is None:
            # There is no task thread currently running, so begin an
            # asynchronous request for a task from the supervisor.  Note that
            # `request_task.spawn()` is used to make the call asynchronous.
            logging.getLogger(log_label(self)).debug('self._deferred %s',
                                                     self._deferred)
            try:
                self.supervisor.register_worker(worker_info())
                self._deferred = self.supervisor.request_task.spawn()
            except RuntimeError:
                logging.getLogger(log_label(self)).error('could not request task')
        elif self._deferred.ready():
            # The response from a previous asynchronous task request from the
            # supervisor is ready.
            result = pickle.loads(str(self._deferred.wait()))
            if result:
                uuid, p = result
                # The supervisor provided a task to run.
                logging.getLogger(log_label(self)).info('self._deferred is '
                                                        'ready: %s', (uuid, p._args))
                task_uuid, task = result
                self._task = task
                self._uuid = task_uuid
                self._thread = self.start()
            else:
                # The supervisor did not have any task to run, so reset the
                # task request state, preparing to send a new request on the
                # next task monitor timer callback.
                self._reset()
        else:
            eventlet.sleep(0.1)

    def start(self):
        # Run a task in a subprocess, forwarding any `stdout` or `stderr`
        # output to supervisor.
        logging.getLogger(log_label(self)).info(self._uuid)
        # Run an IO-loop here, to allow useful work while the subprocess is
        # run in the background thread, `t`.
        self._env = os.environ.copy()
        self._env.update({'ZMQ_JOB_MANAGER__SUPERVISOR_URI':
                           self.worker.uris['supervisor'],
                          'ZMQ_JOB_MANAGER__WORKER_UUID': self.worker.uuid,
                          'ZMQ_JOB_MANAGER__TASK_UUID': self._uuid,
                          'ZMQ_JOB_MANAGER__WORKER_URI':
                           self.worker.uris['pull'], })

        p = self._task.make(popen_class=ProxyPopen, env=self._env)
        task_thread = Thread(target=p.communicate, args=(self.supervisor, ))
        task_thread.daemon = True
        task_thread.start()
        self._begin()
        return task_thread

    def _store(self, key, value, **kwargs):
        self.worker.command_queue.push(self._uuid, ('store', (key, value),
                                                    {'task_uuid': self._uuid}))

    def _begin(self):
        self._store('__task__', pickle.dumps(self._task),
                    serialization=SERIALIZE__PICKLE)
        self._store('__env__', pickle.dumps(self._env),
                    serialization=SERIALIZE__PICKLE)
        args = (self._uuid, )
        kwargs = {}
        self.worker.command_queue.push(self._uuid, ('begin_task', args,
                                                    kwargs))

    def _finalize(self):
        logging.getLogger(log_label(self)).info(self._uuid)
        self._complete()
        self._thread.join()
        self._reset()

    def _complete(self):
        self._store('done', pickle.dumps(datetime.now()),
                    serialization=SERIALIZE__PICKLE)
        args = (self._uuid, )
        kwargs = {}
        self.worker.command_queue.push(self._uuid, ('complete_task', args,
                                                    kwargs))
        self._completed_task_uuids.append(self._uuid)

    def handle_sigterm(self):
        #import pudb; pudb.set_trace()
        if self._thread is not None:
            self.supervisor.store('__sigterm_caught__',
                        jsonapi.dumps(get_seconds_since_epoch()),
                        serialization=SERIALIZE__JSON)

    def _reset(self):
        for name in ('_thread', '_deferred', '_task', '_uuid'):
            if hasattr(self, name):
                delattr(self, name)
            setattr(self, name, None)


class QueueState(object):
    def __init__(self, deferred):
        self.deferred = deferred
        self.submit_timestamp = datetime.now()

    def ready(self):
        if (datetime.now() - self.submit_timestamp) > timedelta(minutes=5):
            raise RuntimeError, 'Request timed out'
        return self.deferred.ready()

    def wait(self):
        if self.deferred.ready():
            return self.deferred.wait()
        raise RuntimeError, 'Deferred is not ready'


class CommandQueue(object):
    def __init__(self, storage, supervisor, on_empty_queues=None):
        self.supervisor = supervisor
        self.storage = storage
        self.queue_states = OrderedDict()
        self.on_empty_queues = on_empty_queues
        #print 'Using storage: %s' % self.storage.host
        #self.storage.root['command_queue'] = PersistentDict()
        #self.storage.commit()

    def push(self, task_uuid, command_tuple):
        queue = self.storage.root.setdefault(task_uuid, PersistentList())
        queue.append(command_tuple)
        self.storage.commit()
        command, args, kwargs = command_tuple
        logging.getLogger(log_label(self)).info('[%s] %s', command, args[0])

    def queue_command(self, _task_uuid, command, *args, **kwargs):
        rpc_method = getattr(self.supervisor, command)
        logging.getLogger(log_label(self)).info('[%s] %s', command, args[0])
        self.queue_states[_task_uuid] = QueueState(
                rpc_method.spawn(*args, **kwargs))

    def process_pending(self):
        for task_uuid, queue in self.storage.root.iteritems():
            if task_uuid not in self.queue_states and len(queue) > 0:
                command, args, kwargs = queue[0]
                self.queue_command(task_uuid, command, *args, **kwargs)
        eventlet.sleep(0.01)
        empty_queues = []
        for task_uuid, queue_state in self.queue_states.iteritems():
            try:
                if queue_state.ready():
                    # The deferred request has completed, so clear the queue
                    # state instance and remove the corresponding command from
                    # the relevant task's command queue.
                    result = queue_state.wait()
                    del self.queue_states[task_uuid]
                    if task_uuid in self.storage.root:
                        del self.storage.root[task_uuid][0]
                        if len(self.storage.root[task_uuid]) <= 0:
                            # Command queue for current task is empty, so delete it
                            # (it will be recreated, if required, through the
                            # `push` method).
                            del self.storage.root[task_uuid]
                            empty_queues.append(task_uuid)
                    self.storage.commit()
            except RuntimeError:
                logging.getLogger(log_label(self)).error('')
                # The deferred request has timed out, so resubmit.
                command, args, kwargs = self.storage.root[task_uuid][0]
                self.queue_command(task_uuid, command, *args, **kwargs)
        self.storage.commit()
        if empty_queues:
            logging.getLogger(log_label(self)).info('pack storage')
            self.storage.pack()
            if self.on_empty_queues:
                self.on_empty_queues(empty_queues)
        eventlet.sleep(0.01)


class WorkerTest(ZmqRpcTask):
    def __init__(self, rpc_uri, supervisor_uri, uuid=None):
        self.uris = OrderedDict(rpc=rpc_uri)
        super(WorkerTest, self).__init__()
        self.uris['supervisor'] = supervisor_uri

        if uuid is None:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid
        self.supervisor = ZmqRpcProxy(self.uris['supervisor'], uuid=self.uuid)
        self.storage = DurusStorage(host='%s.durus.dat' % self.uuid,
                                    port=False)
        self.command_queue = []
        self._deferred_command = None

    def get_uris(self):
        return self.uris

    def rpc__command(self, env, client_uuid, command, *args, **kwargs):
        #try:
            #return getattr(self.supervisor, command)(*args, **kwargs)
        #except:
            #traceback.print_exc()
        self.command_queue.append((command, args, kwargs))

    def rpc__get_queue(self, env, client_uuid):
        return self.command_queue

    def rpc__process_queue_item(self, env, client_uuid):
        if not self.command_queue:
            return None
        elif self._deferred_command is not None:
            raise RuntimeError, ('Previous deferred request must be completed '
                                 'before processing the next deferred request.')

        command, args, kwargs = self.command_queue[0]
        f = getattr(self.supervisor, command)
        self._deferred_command = f.spawn(*args, **kwargs)
        return True

    def rpc__deferred_ready(self, env, client_uuid):
        return self._deferred_command.ready()

    def rpc__deferred_wait(self, env, client_uuid):
        if self._deferred_command is None:
            raise RuntimeError, 'No pending deferred command.'
        elif not self._deferred_command.ready():
            raise RuntimeError, 'Deferred must be ready before calling wait'

        result = self._deferred_command.wait()
        del self.command_queue[0]
        del self._deferred_command
        self._deferred_command = None
        return result

    def timer__queue_monitor(self, io_loop):
        pass

    def handle_sigterm(self, io_loop):
        io_loop.stop()

    def run(self):
        self.start_time = datetime.now()

        ctx, io_loop, socks, streams = get_run_context(self.sock_configs)

        callbacks = OrderedDict()

        # Periodically send a heartbeat signal to let the supervisor know we're
        # still running.
        callbacks['queue_monitor'] = PeriodicCallback(
            functools.partial(self.timer__queue_monitor, io_loop), 1000,
            io_loop=io_loop)

        callbacks['event_sleep'] = PeriodicCallback(lambda: eventlet.sleep(),
                                                    10, io_loop=io_loop)

        def _on_run():
            logging.getLogger(log_label()).info('')
            for c in callbacks.values():
                c.start()
                time.sleep(0.1)

        io_loop.add_callback(_on_run)

        signal.signal(signal.SIGTERM, functools.partial(self.handle_sigterm,
                                                        io_loop))
        signal.siginterrupt(signal.SIGTERM, False)

        try:
            io_loop.start()
        except KeyboardInterrupt:
            pass

        # The durus imports (see top of file) cause the worker process to hang.
        # For now, raise a `SystemError` exception to force termination.
        raise SystemError, ('Forcing process to exit because of: '
                'https://github.com/cfobel/zmq_job_manager/issues/4')


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Worker demo''')
    parser.add_argument(nargs=1, dest='supervisor_uri', type=str)
    parser.add_argument(nargs=1, dest='rpc_uri', type=str)
    parser.add_argument(nargs='?', dest='worker_uuid', type=str,
                        default=str(uuid4()))
    args = parser.parse_args()
    args.supervisor_uri = args.supervisor_uri[0]
    args.rpc_uri = args.rpc_uri[0]
    return args


if __name__ == '__main__':
    # if someone tried to log something before basicConfig is called, Python
    # creates a default handler that
    # goes to the console and will ignore further basicConfig calls. Remove the
    # handler if there is one.
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s-%(name)s-%(levelname)s:%(message)s")
    logging.info('test log')
    args = parse_args()
    w = WorkerTest(args.rpc_uri, args.supervisor_uri, uuid=args.worker_uuid)
    w.run()
