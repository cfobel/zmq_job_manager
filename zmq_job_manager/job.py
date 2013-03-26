from collections import OrderedDict
from datetime import datetime

import zmq


class Job(object):
    def __init__(self, manager_uri, manager_bind=False, control_pipe=None):
        self.uris = OrderedDict(manager=manager_uri)
        self.binds = OrderedDict(manager=manager_bind)

    def run(self):
        ctx = zmq.Context.instance()
        sock = zmq.Socket(ctx, zmq.DEALER)
        if self.binds['manager']:
            sock.bind(self.uris['manager'])
        else:
            sock.connect(self.uris['manager'])
        for i in range(5): sock.send_multipart(['', '[%s] hello world' % datetime.now()])
        for i in range(5): print sock.recv_multipart()[1:]


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Job demo''')
    parser.add_argument(nargs=1, dest='manager_uri', type=str)
    args = parser.parse_args()
    args.manager_uri = args.manager_uri[0]
    return args


if __name__ == '__main__':
    args = parse_args()
    j = Job(args.manager_uri)
    j.run()
