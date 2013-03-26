from collections import OrderedDict
from datetime import datetime

import zmq


class Worker(object):
    def __init__(self, master_uri, master_bind=False, control_pipe=None):
        self.uris = OrderedDict(master=master_uri)
        self.binds = OrderedDict(master=master_bind)

    def run(self):
        ctx = zmq.Context.instance()
        sock = zmq.Socket(ctx, zmq.DEALER)
        if self.binds['master']:
            sock.bind(self.uris['master'])
        else:
            sock.connect(self.uris['master'])
        for i in range(5): sock.send_multipart(['', '[%s] hello world' % datetime.now()])
        for i in range(5): print sock.recv_multipart()[1:]


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Worker demo''')
    parser.add_argument(nargs=1, dest='master_uri', type=str)
    args = parser.parse_args()
    args.master_uri = args.master_uri[0]
    return args


if __name__ == '__main__':
    args = parse_args()
    w = Worker(args.master_uri)
    w.run()
