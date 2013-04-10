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
from zmq.eventloop.ioloop import IOLoop, PeriodicCallback
from zmq_helpers.rpc import ZmqJsonRpcProxy
from zmq_helpers.utils import log_label
from cpu_info.cpu_info import cpu_info, cpu_summary

from .process import PopenPipeReactor
from .constants import SERIALIZE__PICKLE
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


class Worker(object):
    def __init__(self, manager_uri, uuid=None, labels=tuple(), time_limit=None,
                 memory_limit=None, n_procs=1, n_threads=1):
        self.uris = OrderedDict(manager=manager_uri)
        if time_limit is not None:
            time_limit = extract_runtime_limit(time_limit)
        if memory_limit is not None:
            memory_limit = extract_memory_limit(memory_limit)
        self.config = dict(labels=labels, time_limit=time_limit,
                           memory_limit=memory_limit, n_procs=n_procs,
                           n_threads=n_threads)

        self.start_time = None
        self.end_time = None
        self.requested_end_time = None

        if uuid is None:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid

    def run(self):
        manager = ZmqJsonRpcProxy(self.uris['manager'], uuid=self.uuid)
        self.start_time = datetime.now()
        if self.config['time_limit']:
            delta = max(timedelta(), self.config['time_limit'] - timedelta(minutes=5))
            self.end_time = self.start_time + delta
        # Notify broker that we're alive and send our configuration
        # information.  This `worker_info` currently contains information
        # regarding the CPU and the network interfaces.
        manager.register_worker(worker_info())
        logging.getLogger(log_label(self)).info(
            'available handlers: %s' % (manager.available_handlers(), ))
        logging.getLogger(log_label(self)).info(
            'uris: %s' % (manager.get_uris(), ))
        shell_command = 'python -m zmq_job_manager.test_task'
        logging.getLogger(log_label(self)).info(
            'register task: %s' % (manager.register_task(shell_command), ))

        io_loop = IOLoop()
        parent, child = Pipe()
        self._task_thread = None
        self._task_uuid = None
        self._task = None
        self._deferred = None

        def timer__task_monitor():
            '''
            If the subprocess has finished,
            gracefully.
            '''
            if self._task_thread is not None:
                logging.getLogger(log_label()).debug('self._task_thread %s', self._task_thread)
                if not self._task_thread.isAlive():
                    self._complete_task(manager, self._task_uuid, self._task)
                    self._task_thread.join()
                    del self._task_thread
                    self._task_thread = None
                    self._task_uuid = None
                    self._task = None
                    self._deferred = None
                else:
                    self._task_thread.join(0.01)
            elif self._deferred is None:
                logging.getLogger(log_label()).debug('self._deferred %s', self._deferred)
                self._deferred = manager.request_task.spawn()
            elif self._deferred.ready():
                result = pickle.loads(str(self._deferred.wait()))
                if result:
                    logging.getLogger(log_label()).info('self._deferred is ready: %s', result)
                    task_uuid, task = result
                    self._task_thread = self.start_task(manager, task_uuid,
                                                        task)
                    self._task_uuid = task_uuid
                    self._task = task
                else:
                    del self._deferred
                    del self._task

                    self._task_thread = None
                    self._task_uuid = None
                    self._task = None
                    self._deferred = None
            else:
                eventlet.sleep(0.1)

        def timer__heartbeat():
            '''
            Send a heartbeat request to the broker to notify that we are
            still alive.
            '''
            logging.getLogger(log_label()).debug('')
            manager.heartbeat()

        callbacks = OrderedDict()
        callbacks['heartbeat'] = PeriodicCallback(timer__heartbeat, 4000,
                                                    io_loop=io_loop)
        callbacks['task_monitor'] = PeriodicCallback(timer__task_monitor, 2000,
                                                    io_loop=io_loop)

        def _on_run():
            logging.getLogger(log_label()).info('')
            for c in callbacks.values():
                c.start()
                time.sleep(0.1)

        io_loop.add_callback(_on_run)

        try:
            io_loop.start()
        except KeyboardInterrupt:
            pass

    def start_task(self, manager, task_uuid, task):
        # Run a task in a subprocess, forwarding any `stdout` or `stderr`
        # output to manager.
        logging.getLogger(log_label(self)).info(task_uuid)
        # Run an IO-loop here, to allow useful work while the subprocess is
        # run in the background thread, `t`.
        self._env = os.environ.copy()
        self._env.update({'ZMQ_JOB_MANAGER__BROKER_URI': self.uris['manager'],
                          'ZMQ_JOB_MANAGER__WORKER_UUID': self.uuid,
                          'ZMQ_JOB_MANAGER__TASK_UUID': task_uuid})

        p = task.make(popen_class=ProxyPopen, env=self._env)
        task_thread = Thread(target=p.communicate, args=(manager, ))
        task_thread.daemon = True
        task_thread.start()
        self._begin_task(manager, task_uuid, task)
        return task_thread

    def _begin_task(self, manager, task_uuid, task):
        manager.store('__task__', pickle.dumps(task),
                        serialization=SERIALIZE__PICKLE)
        manager.store('__env__', pickle.dumps(self._env),
                        serialization=SERIALIZE__PICKLE)
        manager.begin_task(task_uuid)

    def _complete_task(self, manager, task_uuid, task):
        manager.store('done', pickle.dumps(datetime.now()),
                        serialization=SERIALIZE__PICKLE)
        manager.complete_task(task_uuid)


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Worker demo''')
    parser.add_argument(nargs=1, dest='manager_uri', type=str)
    parser.add_argument(nargs='?', dest='worker_uuid', type=str,
                        default=str(uuid4()))
    parser.add_argument(nargs='?', dest='time_limit', default='5m')
    parser.add_argument(nargs='?', dest='memory_limit', default='1G')
    parser.add_argument(nargs='?', dest='n_procs', type=int, default=1)
    parser.add_argument(nargs='?', dest='n_threads', type=int, default=1)
    args = parser.parse_args()
    args.manager_uri = args.manager_uri[0]
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s-%(name)s-%(levelname)s:%(message)s")
    args = parse_args()
    w = Worker(args.manager_uri, uuid=args.worker_uuid,
               time_limit=args.time_limit,
               memory_limit=args.memory_limit, n_procs=args.n_procs,
               n_threads=args.n_threads)
    w.run()
