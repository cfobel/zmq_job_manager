import platform
from threading import Thread
import os
import signal
import functools
import logging
from collections import OrderedDict
from uuid import uuid4
import time
from datetime import datetime
try:
    import cPickle as pickle
except ImportError:
    import pickle

import eventlet
import zmq
from zmq.eventloop.ioloop import PeriodicCallback
from zmq.utils import jsonapi
# The following durus imports cause the worker process to hang
# (see #4, https://github.com/cfobel/zmq_job_manager/issues/4)
# For now, the worker process throws a `SystemError` exception when terminating
# to force termination.
from zmq_helpers.socket_configs import DeferredSocket, get_run_context
from zmq_helpers.rpc import ZmqRpcTask
from zmq_helpers.utils import log_label, unique_ipc_uri, cleanup_ipc_uris
import psutil
import netifaces
from cpu_info.cpu_info import cpu_info, cpu_summary

from zmq_job_manager.rpc import DeferredZmqRpcQueue
from zmq_job_manager.process import PopenPipeReactor
from zmq_job_manager.constants import SERIALIZE__PICKLE, SERIALIZE__JSON
from zmq_job_manager.manager import get_seconds_since_epoch


class PushPopenReactor(PopenPipeReactor):
    def communicate(self, push_uri):
        self.ctx = zmq.Context.instance()
        self.push_uri = push_uri
        self.push_socket = zmq.Socket(self.ctx, zmq.PUSH)
        self.push_socket.connect(push_uri)
        super(PushPopenReactor, self).communicate()

    def on_stdout(self, value):
        self.push_socket.send_pyobj(('stdout', value))

    def on_stderr(self, value):
        self.push_socket.send_pyobj(('stderr', value))

    def __del__(self):
        del self.push_socket
        del self.ctx


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
    def __init__(self, worker):
        self.worker = worker
        self._task = None
        self._thread = None
        self._uuid = None
        self._deferred = None
        self._request_task_pending = False
        self.ctx = zmq.Context.instance()
        self.pull_uri = 'inproc://task_monitor'
        self.pull_socket = zmq.Socket(self.ctx, zmq.PULL)
        self.pull_socket.bind(self.pull_uri)
        self._task_count = 0

    def __del__(self):
        del self.pull_socket
        del self.ctx

    def _store_std_output(self):
        try:
            while True:
                output_type, value = (
                        self.pull_socket.recv_pyobj(flags=zmq.NOBLOCK))
                self.worker.queue_request(output_type, value)
                logging.getLogger(log_label(self)).info('[%s] %s', output_type,
                                                        value)
        except zmq.Again:
            # No more data in output queue.
            pass

    def update_state(self):
        self._store_std_output()
        if self._thread is not None:
            logging.getLogger(log_label(self)).debug('self._thread %s',
                                                     self._thread)
            if not self._thread.isAlive() and self.worker.deferred_queue.queue_length <= 0:
                # The task thread has completed, so perform finalization.
                self._finalize()
            else:
                # The task thread is still running, so try to join, but timeout
                # if the task thread is still not ready.
                self._thread.join(0.01)
        elif not self._request_task_pending:
            # There is no task thread currently running and there is no task
            # request pending, so request a task from the supervisor, using a
            # callback to process the result.
            self._request_task_pending = True
            self.worker.queue_request('request_task',
                                      callback=self.callback__request_task)

    def callback__complete_task(self, request_uuid, result):
        # The response from a previous asynchronous task completed request from
        # the supervisor is ready.  Reset state to accept new task.
        self._reset()
        self._task_count += 1
        logging.getLogger(log_label(self)).info('Task completed: %s (%d total)',
                                                request_uuid, self._task_count)

    def callback__request_task(self, request_uuid, result):
        # The response from a previous asynchronous task request from the
        # supervisor is ready.
        result = pickle.loads(str(result))
        if result:
            uuid, p = result
            # The supervisor provided a task to run.
            logging.getLogger(log_label(self)).info('task received: %s',
                                                    (uuid, p._args))
            task_uuid, task = result
            self._task = task
            self._uuid = task_uuid
            self._thread = self.start()
            self._request_task_pending = False
        else:
            # The supervisor did not have any task to run, so reset the
            # task request state, preparing to send a new request on the
            # next task monitor timer callback.
            self._reset()

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

        p = self._task.make(popen_class=PushPopenReactor, env=self._env)
        task_thread = Thread(target=p.communicate, args=(self.pull_uri, ))
        task_thread.daemon = True
        task_thread.start()
        self._begin()
        return task_thread

    def _store(self, key, value, **kwargs):
        kwargs_ = {'task_uuid': self._uuid}
        kwargs_.update(kwargs)
        self.worker.queue_request('store', key, value, **kwargs_)

    def _begin(self):
        self._store('__task__', pickle.dumps(self._task),
                    serialization=SERIALIZE__PICKLE)
        self._store('__env__', pickle.dumps(self._env),
                    serialization=SERIALIZE__PICKLE)
        self.worker.queue_request('begin_task', self._uuid)

    def _finalize(self):
        logging.getLogger(log_label(self)).info(self._uuid)
        self._complete()

    def _complete(self):
        self._store('done', pickle.dumps(datetime.now()),
                    serialization=SERIALIZE__PICKLE)
        self.worker.queue_request('complete_task', self._uuid,
                                  callback=self.callback__complete_task)
        self._thread.join()

    def handle_sigterm(self):
        logging.getLogger(log_label(self)).error(str(datetime.now()))
        if self._thread is not None:
            self.worker.queue_request('store', '__sigterm_caught__',
                                      jsonapi.dumps(get_seconds_since_epoch()),
                                      serialization=SERIALIZE__JSON)

    def _reset(self):
        for name in ('_thread', '_task', '_uuid'):
            if hasattr(self, name):
                delattr(self, name)
            setattr(self, name, None)
        self._request_task_pending = False


class WorkerMonitorMixin(object):
    '''
    This task's run loop monitors and manages a FIFO `DeferredZmqRpcQueue` of
    requests to be processed asynchronously by a supervisor process.

    Periodically, the worker attempts to send the least recently queued request
    to the supervisor.  If no response is received from the supervisor within
    20 seconds, the request is aborted and restarted.  Otherwise, the response
    from the completed request is printed to stdout.

    Also, see the doctring for `DeferredZmqRpcQueue`.
    '''
    queue_class = None

    def make_queue(self, supervisor_uri, queue_storage, uuid):
        if self.queue_class is None:
            raise ValueError, 'Default queue class must be set'
        return self.queue_class(supervisor_uri, queue_storage=queue_storage,
                                uuid=uuid)

    def get_uris(self):
        return self.uris

    def get_sock_configs(self):
        sock_configs = super(WorkerMonitorMixin, self).get_sock_configs()
        sock_configs['pull'] = (DeferredSocket(zmq.PULL)
                                .stream_callback('on_recv',
                                                 self.callback__pull_on_recv))
        return sock_configs

    def callback__pull_on_recv(self, env, multipart_message):
        task_uuid, command = multipart_message[:2]
        args, kwargs = map(pickle.loads, multipart_message[2:])
        logging.getLogger(log_label(self)).debug('store: %s', args[0])
        self.queue_request(command, *args, **kwargs)

    def _on_result_received(self, io_loop, request_uuid, result):
        logging.getLogger(log_label(self)).debug('result: %s', result)

    def timer__supervisor_queue_monitor(self, io_loop):
        if self.deferred_queue.queue_length > 0:
            if not self.deferred_queue.request_pending:
                self.deferred_queue.process_queue_item()
            elif self.deferred_queue.ready():
                request_uuid = self.deferred_queue.ready()
                result = self.deferred_queue.wait()
                self._on_result_received(io_loop, request_uuid, result)
            elif (self.deferred_queue._deferred_start and (datetime.now() -
                  self.deferred_queue._deferred_start).total_seconds() > 20):
                # Timeout after no RPC response
                logging.getLogger(log_label(self)).info('Timeout after no RPC response')
                self.deferred_queue.abort()
                self.register()

    def register(self):
        if not getattr(self, '_registration_pending', False):
            self._registration_pending = True
            self.insert_request('register_worker', worker_info(),
                                callback=self.callback__register_worker)

    def callback__register_worker(self, *args, **kwargs):
        self._registration_pending = False
        logging.getLogger(log_label(self)).info('')

    def handle_sigterm(self, io_loop, signum, frame):
        self._task_monitor.handle_sigterm()
        io_loop.stop()

    def timer__heartbeat(self, io_loop):
        '''
        Send a heartbeat request to the supervisor to notify that we are
        still alive.
        '''
        if self.deferred_queue.queue_length <= 0:
            logging.getLogger(log_label(self)).info('')
            self.queue_request('heartbeat')
            eventlet.sleep()

    def timer__task_monitor(self, io_loop):
        task_count_limit = getattr(self, 'task_count_limit', 1)
        completed_task_count = self._task_monitor._task_count
        if task_count_limit > 0 and completed_task_count >= task_count_limit:
            io_loop.stop()
        else:
            self._task_monitor.update_state()

    def timer__pack(self, io_loop):
        if hasattr(self.deferred_queue, 'pack'):
            self.deferred_queue.pack()
            logging.getLogger(log_label(self)).info(
                'request queue length: %s', self.deferred_queue.queue_length)

    def run(self):
        self.start_time = datetime.now()
        self._task_monitor = TaskMonitor(self)

        ctx, io_loop, socks, streams = get_run_context(self.sock_configs)

        callbacks = OrderedDict()

        # Periodically process the next queued request (if available) ready for
        # the supervisor.
        callbacks['supervisor_queue_monitor'] = PeriodicCallback(
            functools.partial(self.timer__supervisor_queue_monitor, io_loop), 10,
            io_loop=io_loop)

        callbacks['event_sleep'] = PeriodicCallback(eventlet.sleep, 10,
                                                    io_loop=io_loop)

        # Periodically send a heartbeat signal to let the supervisor know we're
        # still running.
        callbacks['heartbeat'] = PeriodicCallback(
            functools.partial(self.timer__heartbeat, io_loop), 4000,
            io_loop=io_loop)

        # Periodically check and update task state.  When no task is running,
        # this means requesting a new task from the supervisor.  When a task is
        # running, wait until the task finishes, then finalize the task and
        # prepare to request a new task from the supervisor.
        callbacks['task_monitor'] = PeriodicCallback(
            functools.partial(self.timer__task_monitor, io_loop), 500,
            io_loop=io_loop)

        # Periodically pack the queue storage (if applicable).
        callbacks['pack'] = PeriodicCallback(
            functools.partial(self.timer__pack, io_loop), 120000,
            io_loop=io_loop)
        def _on_run():
            logging.getLogger(log_label()).info('')
            for c in callbacks.values():
                c.start()
                time.sleep(0.1)
            self.register()

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

    def _add_request(self, add_method_name, *args, **kwargs):
        '''
        Wrap the insert/queue methods of the underlying deferred request queue
        to provide callback capabilities (based on request UUID).
        '''
        callback = kwargs.pop('callback', None)
        request_uuid = getattr(self.deferred_queue, add_method_name)(*args,
                                                                     **kwargs)
        if callback:
            self.request_callbacks[request_uuid] = callback
        return request_uuid

    def insert_request(self, *args, **kwargs):
        '''
        See the _add_request method.
        '''
        logging.getLogger(log_label(self)).info(args[0])
        return self._add_request('insert_request', *args, **kwargs)

    def queue_request(self, *args, **kwargs):
        '''
        See the _add_request method.
        '''
        logging.getLogger(log_label(self)).info(args[0])
        return self._add_request('queue_request', *args, **kwargs)


class DeferredWorkerTask(WorkerMonitorMixin, ZmqRpcTask):
    '''
    This task provides a ZeroMQ RPC API to interface with a FIFO
    `DeferredZmqRpcQueue` for managing a set of asynchronous requests to a
    supervisor process.
    '''
    queue_class = DeferredZmqRpcQueue

    def __init__(self, rpc_uri, supervisor_uri, queue_storage=None, uuid=None):
        self.uris = OrderedDict(rpc=rpc_uri, pull=unique_ipc_uri())
        super(DeferredWorkerTask, self).__init__()
        self.uris['supervisor'] = supervisor_uri

        if uuid is None:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid
        self.deferred_queue = self.make_queue(supervisor_uri,
                                              queue_storage=queue_storage,
                                              uuid=uuid)
        self.request_callbacks = OrderedDict()

    def rpc__insert_request(self, env, client_uuid, *args, **kwargs):
        return self.insert_request(*args, **kwargs)

    def rpc__queue_request(self, env, client_uuid, *args, **kwargs):
        return self.queue_request(*args, **kwargs)

    def rpc__get_queue_length(self, env, client_uuid):
        return self.deferred_queue.queue_length

    def rpc__get_queue(self, env, client_uuid):
        return self.deferred_queue.queue

    def rpc__process_queue_item(self, env, client_uuid):
        return self.deferred_queue.process_queue_item()

    def rpc__next_result(self, env, client_uuid, timeout_seconds=None):
        return self.deferred_queue.next_result(timeout_seconds)

    def rpc__deferred_ready(self, env, client_uuid):
        return self.deferred_queue.ready()

    def rpc__deferred_wait(self, env, client_uuid):
        return self.deferred_queue.wait()

    def __del__(self):
        super(DeferredWorkerTask, self).__del__()
        cleanup_ipc_uris([self.uris['pull']])
