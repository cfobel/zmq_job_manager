from collections import OrderedDict
from datetime import datetime

import zmq
from zmq.utils import jsonapi


class Worker(object):
    def __init__(self, master_uri, master_bind=False, time_limit='5m',
                 memory_limit='1G', n_procs=1, n_threads=1, control_pipe=None):
        self.uris = OrderedDict(master=master_uri)
        self.binds = OrderedDict(master=master_bind)
        self.config = dict(time_limit=time_limit, memory_limit=memory_limit,
                           n_procs=n_procs, n_threads=n_threads)

    def run(self):
        ctx = zmq.Context.instance()
        sock = zmq.Socket(ctx, zmq.REQ)
        if self.binds['master']:
            sock.bind(self.uris['master'])
        else:
            sock.connect(self.uris['master'])
        json_data = map(jsonapi.dumps, ['%s' % datetime.now(), 'get_job', None, self.config])
        sock.send_multipart(json_data)
        print datetime.now(), map(jsonapi.loads, sock.recv_multipart())


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
    args = parse_args()
    w = Worker(args.master_uri, time_limit=args.time_limit,
               memory_limit=args.memory_limit, n_procs=args.n_procs,
               n_threads=args.n_threads)
    w.run()
