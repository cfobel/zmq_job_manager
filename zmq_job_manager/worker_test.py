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


class WorkerTest(ZmqRpcTask):
    def __init__(self, rpc_uri, supervisor_uri, queue_storage=None, uuid=None):
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
        if queue_storage is None:
            queue_storage = []
        self.request_queue = queue_storage
        self._deferred_request = None

    def get_uris(self):
        return self.uris

    def rpc__queue_request(self, env, client_uuid, command, *args, **kwargs):
        '''
        Queue request to be executed once all prior pending requests have been
        processed.
        '''
        self.request_queue.append((command, args, kwargs))

    def rpc__get_queue_length(self, env, client_uuid):
        '''
        Return the length of the current request queue.
        '''
        return len(self.request_queue)

    def rpc__get_queue(self, env, client_uuid):
        '''
        Return the current request queue contents.
        '''
        return self.request_queue

    def rpc__process_queue_item(self, env, client_uuid):
        '''
        Process the least-recently queued request by spawning a new deferred
        request instance and assigning it to the `_deferred_request` attribute.
        If there are no queued requests, return `None`.  Otherwise, return
        `True`.

        Raise an exception if there is already an active/pending spawned
        request.
        '''
        if not self.request_queue:
            return None
        elif self._deferred_request is not None:
            raise RuntimeError, ('Previous deferred request must be completed '
                                 'before processing the next deferred request.')

        command, args, kwargs = self.request_queue[0]
        f = getattr(self.supervisor, command)
        self._deferred_request = f.spawn(*args, **kwargs)
        return True

    def rpc__next_result(self, env, client_uuid):
        '''
        Process next item in queue and block until result is ready.  Return
        result.
        '''
        if (self._deferred_request is not None or
                self.rpc__process_queue_item(env, client_uuid)):
            while not self.rpc__deferred_ready(env, client_uuid):
                eventlet.sleep(0.001)
            return self.rpc__deferred_wait(env, client_uuid)
        raise ValueError, 'There are no pending requests.'

    def rpc__deferred_ready(self, env, client_uuid):
        '''
        Return the status of the current active/pending spawned request.
        '''
        return self._deferred_request.ready()

    def rpc__deferred_wait(self, env, client_uuid):
        '''
        Return the result from the current spawned request.

        Raise an exception if the spawned request is not ready, or there is no
        pending spawned request.
        '''
        if self._deferred_request is None:
            raise RuntimeError, 'No pending deferred request.'
        elif not self._deferred_request.ready():
            raise RuntimeError, 'Deferred must be ready before calling wait'

        result = self._deferred_request.wait()
        del self.request_queue[0]
        del self._deferred_request
        self._deferred_request = None
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
