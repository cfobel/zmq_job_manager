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
from zmq_helpers.rpc import ZmqRpcProxy, RpcHandlerMixin
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
    def __init__(self, worker, supervisor):
        self.worker = worker
        self.supervisor = supervisor
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
    def __init__(self, storage, supervisor):
        self.supervisor = supervisor
        self.storage = storage
        self.queue_states = OrderedDict()
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
        do_pack = False
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
                            do_pack = True
                    self.storage.commit()
            except RuntimeError:
                logging.getLogger(log_label(self)).error('')
                # The deferred request has timed out, so resubmit.
                command, args, kwargs = self.storage.root[task_uuid][0]
                self.queue_command(task_uuid, command, *args, **kwargs)
        self.storage.commit()
        if do_pack:
            logging.getLogger(log_label(self)).info('pack storage')
            self.storage.pack()
        eventlet.sleep(0.01)


class Worker(RpcHandlerMixin):
    def __init__(self, supervisor_uri, uuid=None, labels=tuple(),
                 time_limit=None, memory_limit=None, n_procs=1, n_threads=1):
        self.uris = OrderedDict(supervisor=supervisor_uri)
        if time_limit is not None:
            time_limit = extract_runtime_limit(time_limit)
        if memory_limit is not None:
            memory_limit = extract_memory_limit(memory_limit)
        self.config = dict(labels=labels, time_limit=time_limit,
                           memory_limit=memory_limit, n_procs=n_procs,
                           n_threads=n_threads)
        self._task_monitor = None

        self.start_time = None
        self.end_time = None
        self.requested_end_time = None

        if uuid is None:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid
        self.refresh_handler_methods()

    def rpc__terminate(self, io_loop, supervisor, *args, **kwargs):
        #import pudb; pudb.set_trace()
        io_loop.stop()
        supervisor.terminate_worker()

    def timer__command_queue_monitor(self, io_loop, supervisor):
        '''
        Process any pending task commands.
        '''
        logging.getLogger(log_label(self)).info('')
        self.command_queue.process_pending()

    def timer__queue_monitor(self, io_loop, supervisor):
        '''
        Process any pending requests in our queue.
        '''
        logging.getLogger(log_label(self)).info('')
        try:
            deferred = supervisor.request_queue.spawn(_d_none_on_error=True)
        except RuntimeError:
            # Notify supervisor that we're alive and send our configuration
            # information.  This `worker_info` currently contains information
            # regarding the CPU and the network interfaces.
            supervisor.register_worker(worker_info())
            deferred = supervisor.request_queue.spawn(_d_none_on_error=True)
        eventlet.sleep(0.01)
        for i in range(10):
            if deferred.ready():
                requests = deferred.wait()
                logging.getLogger(log_label(self)).info('got request response: %s', requests)
                if requests is not None:
                    supervisor.register_worker(worker_info())
                    for command, args, kwargs in requests:
                        logging.getLogger(log_label(self)).info(command)
                        handler = self.get_handler(command)
                        if handler:
                            handler(io_loop, supervisor, *args, **kwargs)
                break
            else:
                eventlet.sleep(0.01)

    def timer__task_monitor(self, io_loop, supervisor):
        self._task_monitor.update_state()

    def timer__heartbeat(self, io_loop, supervisor):
        '''
        Send a heartbeat request to the supervisor to notify that we are
        still alive.
        '''
        logging.getLogger(log_label(self)).info('')
        supervisor.heartbeat.spawn()
        eventlet.sleep(0.01)

    def handle_sigterm(self, io_loop, supervisor, *args, **kwargs):
        logging.getLogger(log_label(self)).info('%s, %s', args, kwargs)
        self._task_monitor.handle_sigterm()
        self.rpc__terminate(io_loop, supervisor)

    def pull__message_received(self, supervisor, multipart_message):
        task_uuid, command = multipart_message[:2]
        args, kwargs = map(pickle.loads, multipart_message[2:])
        if command == 'store':
            logging.getLogger(log_label(self)).info('store: %s', args[0])
        self.command_queue.push(task_uuid, (command, args, kwargs))

    def task_completed(self, task_uuid):
        logging.getLogger(log_label(self)).info('pack storage')
        self.storage.pack()

    def run(self):
        #import pudb; pudb.set_trace()
        supervisor = ZmqRpcProxy(self.uris['supervisor'], uuid=self.uuid)
        self.storage = DurusStorage(host='%s.durus.dat' % self.uuid, port=False)
        self.command_queue = CommandQueue(self.storage, supervisor)
        self._task_monitor = TaskMonitor(self, supervisor)

        ctx = zmq.Context()
        socket_pull = zmq.Socket(ctx, zmq.PULL)
        self.uris['pull'] = unique_ipc_uri()
        #print 'uris:', self.uris.items()
        socket_pull.bind(self.uris['pull'])
        self.start_time = datetime.now()
        #print self.config
        if self.config['time_limit']:
            delta = max(timedelta(), self.config['time_limit'] -
                        timedelta(minutes=5))
            self.end_time = self.start_time + delta

        # Notify supervisor that we're alive and send our configuration
        # information.  This `worker_info` currently contains information
        # regarding the CPU and the network interfaces.
        supervisor.register_worker(worker_info())

        io_loop = IOLoop()
        stream_pull = zmqstream.ZMQStream(socket_pull, io_loop)
        stream_pull.on_recv(functools.partial(self.pull__message_received,
                                              supervisor))
        parent, child = Pipe()

        callbacks = OrderedDict()

        # Periodically send a heartbeat signal to let the supervisor know we're
        # still running.
        callbacks['heartbeat'] = PeriodicCallback(
            functools.partial(self.timer__heartbeat, io_loop, supervisor), 4000,
            io_loop=io_loop)

        # Periodically check and update task state.  When no task is running,
        # this means requesting a new task from the supervisor.  When a task is
        # running, wait until the task finishes, then finalize the task and
        # prepare to request a new task from the supervisor.
        callbacks['task_monitor'] = PeriodicCallback(
            functools.partial(self.timer__task_monitor, io_loop, supervisor),
            2000, io_loop=io_loop)

        # Periodically check for
        callbacks['command_queue_monitor'] = PeriodicCallback(
            functools.partial(self.timer__command_queue_monitor, io_loop,
                              supervisor), 10, io_loop=io_loop)

        # Periodically check with the supervisor to see if there are any
        # outstanding worker commands queued, processing any queued commands.
        callbacks['queue_monitor'] = PeriodicCallback(
            functools.partial(self.timer__queue_monitor, io_loop, supervisor),
            2000, io_loop=io_loop)

        callbacks['event_sleep'] = PeriodicCallback(
            lambda: eventlet.sleep(0.001), 10, io_loop=io_loop)

        def _on_run():
            logging.getLogger(log_label()).info('')
            for c in callbacks.values():
                c.start()
                time.sleep(0.1)

        io_loop.add_callback(_on_run)

        signal.signal(signal.SIGTERM, functools.partial(self.handle_sigterm,
                                                        io_loop, supervisor))
        signal.siginterrupt(signal.SIGTERM, False)

        try:
            io_loop.start()
        except KeyboardInterrupt:
            pass
        cleanup_ipc_uris([self.uris['pull']])
        logging.getLogger(log_label(self)).info('cleaned up IPC uris')
        del self._task_monitor
        logging.getLogger(log_label(self)).info('threads: %s', threading.enumerate())
        io_loop.stop()
        del io_loop
        self.storage.connection.storage.close()
        del self.storage.connection
        del self.storage

        # The durus imports (see top of file) cause the worker process to hang.
        # For now, raise a `SystemError` exception to force termination.
        raise SystemError, ('Forcing process to exit because of: '
                'https://github.com/cfobel/zmq_job_manager/issues/4')


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Worker demo''')
    parser.add_argument(nargs=1, dest='supervisor_uri', type=str)
    parser.add_argument(nargs='?', dest='worker_uuid', type=str,
                        default=str(uuid4()))
    parser.add_argument(nargs='?', dest='time_limit', default='5m')
    parser.add_argument(nargs='?', dest='memory_limit', default='1G')
    parser.add_argument(nargs='?', dest='n_procs', type=int, default=1)
    parser.add_argument(nargs='?', dest='n_threads', type=int, default=1)
    args = parser.parse_args()
    args.supervisor_uri = args.supervisor_uri[0]
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
    w = Worker(args.supervisor_uri, uuid=args.worker_uuid,
               time_limit=args.time_limit,
               memory_limit=args.memory_limit, n_procs=args.n_procs,
               n_threads=args.n_threads)
    w.run()
