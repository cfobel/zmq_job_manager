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
from zmq.eventloop.ioloop import PeriodicCallback
# The following durus imports cause the worker process to hang
# (see #4, https://github.com/cfobel/zmq_job_manager/issues/4)
# For now, the worker process throws a `SystemError` exception when terminating
# to force termination.
from zmq_helpers.socket_configs import get_run_context
from zmq_helpers.rpc import ZmqRpcTask
from zmq_helpers.utils import log_label

from zmq_job_manager.rpc import DeferredZmqRpcQueue


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

    def _on_result_received(self, io_loop, request_uuid, result):
        print result

    def timer__queue_monitor(self, io_loop):
        if self.deferred_queue.queue_length > 0:
            if not self.deferred_queue.request_pending:
                self.deferred_queue.process_queue_item()
            elif self.deferred_queue.ready():
                print 'Result is ready for request:', (
                        self.deferred_queue.ready())
                request_uuid = self.deferred_queue.ready()
                result = self.deferred_queue.wait()
                self._on_result_received(io_loop, request_uuid, result)
            elif (self.deferred_queue._deferred_start and (datetime.now() -
                  self.deferred_queue._deferred_start).total_seconds() > 20):
                # Timeout after no RPC response
                logging.getLogger(log_label(self)).info('Timeout after no RPC response')
                self.deferred_queue.abort()

    def handle_sigterm(self, io_loop):
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

    def run(self):
        self.start_time = datetime.now()

        ctx, io_loop, socks, streams = get_run_context(self.sock_configs)

        callbacks = OrderedDict()

        # Periodically send a heartbeat signal to let the supervisor know we're
        # still running.
        callbacks['queue_monitor'] = PeriodicCallback(
            functools.partial(self.timer__queue_monitor, io_loop), 1000,
            io_loop=io_loop)

        callbacks['event_sleep'] = PeriodicCallback(eventlet.sleep, 10,
                                                    io_loop=io_loop)

        # Periodically send a heartbeat signal to let the supervisor know we're
        # still running.
        callbacks['heartbeat'] = PeriodicCallback(
            functools.partial(self.timer__heartbeat, io_loop), 4000,
            io_loop=io_loop)

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


class DeferredWorkerTask(WorkerMonitorMixin, ZmqRpcTask):
    '''
    This task provides a ZeroMQ RPC API to interface with a FIFO
    `DeferredZmqRpcQueue` for managing a set of asynchronous requests to a
    supervisor process.
    '''
    queue_class = DeferredZmqRpcQueue

    def __init__(self, rpc_uri, supervisor_uri, queue_storage=None, uuid=None):
        self.uris = OrderedDict(rpc=rpc_uri)
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

    def rpc__queue_request(self, env, client_uuid, *args, **kwargs):
        return self.deferred_queue.queue_request(*args, **kwargs)

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
