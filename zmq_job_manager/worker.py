from multiprocessing import Pipe
from datetime import datetime
from threading import Thread
import logging
from collections import OrderedDict
from uuid import uuid4
import time
try:
    import cPickle as pickle
except ImportError:
    import pickle

from zmq.eventloop.ioloop import IOLoop, PeriodicCallback

from zmq_helpers.rpc import ZmqJsonRpcProxy
from zmq_helpers.utils import log_label
from cpu_info.cpu_info import cpu_info, cpu_summary

from process import PopenPipeReactor
from constants import SERIALIZE__PICKLE


class ProxyPopen(PopenPipeReactor):
    def communicate(self, proxy):
        self.proxy = proxy
        super(ProxyPopen, self).communicate()

    def on_stdout(self, value):
        self.proxy.stdout(value)

    def on_stderr(self, value):
        self.proxy.stderr(value)


class Worker(object):
    def __init__(self, master_uri, uuid=None, time_limit='5m',
                 memory_limit='1G', n_procs=1, n_threads=1):
        self.uris = OrderedDict(master=master_uri)
        self.config = dict(time_limit=time_limit, memory_limit=memory_limit,
                           n_procs=n_procs, n_threads=n_threads)
        if uuid is None:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid

    def run(self):
        master = ZmqJsonRpcProxy(self.uris['master'], uuid=self.uuid)
        master.store('__cpu_summary__', cpu_summary())
        master.store('__cpu_info__', pickle.dumps(cpu_info()),
                        serialization=SERIALIZE__PICKLE)
        logging.getLogger(log_label(self)).info(
            'available handlers: %s' % (master.available_handlers(), ))
        logging.getLogger(log_label(self)).info(
            'uris: %s' % (master.get_uris(), ))
        logging.getLogger(log_label(self)).info(
            'broker hello world: %s' % (master.broker_hello_world(), ))
        shell_command = 'echo "[start] $(date)"; sleep 5; '\
                'echo "[mid] $(date)"; sleep 5; echo "[end] $(date)";'
        logging.getLogger(log_label(self)).info(
            'register task: %s' % (master.register_task(shell_command), ))
        while master.pending_task_ids():
            self.run_task(master)
        logging.getLogger(log_label(self)).info(
            'pending tasks: %s' % (master.pending_task_ids(), ))
        logging.getLogger(log_label(self)).info(
            'running tasks: %s' % (master.running_task_ids(), ))
        logging.getLogger(log_label(self)).info(
            'completed tasks: %s' % (master.completed_task_ids(), ))

    def run_task(self, master):
        # Request a task from the master and run it in a subprocess, forwarding
        # any `stdout` or `stderr` output to master.
        d = pickle.loads(str(master.request_task()))
        logging.getLogger(log_label(self)).info('request_task: %s' % (d, ))
        if d:
            # Run an IO-loop here, to allow useful work while the subprocess is
            # run in the background thread, `t`.
            task_uuid, d = d
            p = d.make(popen_class=ProxyPopen)
            t = Thread(target=p.communicate, args=(master, ))
            t.daemon = True
            t.start()
            io_loop = IOLoop()
            parent, child = Pipe()

            def timer__completed():
                '''
                If the subprocess has finished, stop the IO-loop to exit
                gracefully.
                '''
                if not t.isAlive():
                    child.send('STOP')
                else:
                    t.join(0.01)

            def timer__watchdog():
                '''
                If there is a request to stop the IO-loop, do so.
                '''
                if parent.poll():
                    io_loop.stop()

            def timer__heartbeat():
                '''
                Send a heartbeat request to the broker to notify that we are
                still alive.
                '''
                master.heartbeat()

            callbacks = OrderedDict()
            callbacks['heartbeat'] = PeriodicCallback(timer__heartbeat, 4000,
                                                      io_loop=io_loop)
            callbacks['watchdog'] = PeriodicCallback(timer__watchdog, 500,
                                                     io_loop=io_loop)
            callbacks['completed'] = PeriodicCallback(timer__completed, 500,
                                                      io_loop=io_loop)
            def _on_run():
                for c in callbacks.values():
                    c.start()
                    time.sleep(0.1)
                master.begin_task(task_uuid)

            io_loop.add_callback(_on_run)

            try:
                io_loop.start()
            except KeyboardInterrupt:
                pass

            master.store('done', pickle.dumps(datetime.now()),
                         serialization=SERIALIZE__PICKLE)
            master.complete_task(task_uuid)


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Worker demo''')
    parser.add_argument(nargs=1, dest='master_uri', type=str)
    parser.add_argument(nargs='?', dest='time_limit', default='5m')
    parser.add_argument(nargs='?', dest='memory_limit', default='1G')
    parser.add_argument(nargs='?', dest='n_procs', type=int, default=1)
    parser.add_argument(nargs='?', dest='n_threads', type=int, default=1)
    args = parser.parse_args()
    args.master_uri = args.master_uri[0]
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    w = Worker(args.master_uri, time_limit=args.time_limit,
               memory_limit=args.memory_limit, n_procs=args.n_procs,
               n_threads=args.n_threads)
    w.run()
