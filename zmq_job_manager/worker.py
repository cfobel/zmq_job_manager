import logging
from collections import OrderedDict
from uuid import uuid4

from zmq_helpers.rpc import ZmqJsonRpcProxy
from zmq_helpers.utils import log_label


class Worker(object):
    def __init__(self, master_uri, uuid=None, time_limit='5m',
                 memory_limit='1G', n_procs=1, n_threads=1, control_pipe=None):
        self.uris = OrderedDict(master=master_uri)
        self.config = dict(time_limit=time_limit, memory_limit=memory_limit,
                           n_procs=n_procs, n_threads=n_threads)
        if uuid is None:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid

    def run(self):
        master = ZmqJsonRpcProxy(self.uris['master'], uuid=self.uuid)
        #print datetime.now(), master.get_job(**self.config)
        logging.getLogger(log_label(self)).info(
            'available handlers: %s' % (master.available_handlers(), ))
        logging.getLogger(log_label(self)).info(
            'hello world: %s' % (master.hello_world(), ))
        logging.getLogger(log_label(self)).info(
            'broker hello world: %s' % (master.broker_hello_world(), ))


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
