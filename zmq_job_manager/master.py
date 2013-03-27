from collections import OrderedDict

import zmq
from zmq.utils import jsonapi
from zmq_helpers.socket_configs import SockConfigsTask, DeferredSocket


class Master(SockConfigsTask):
    def __init__(self, rep_uri, control_pipe=None, **kwargs):
        self.uris = OrderedDict([
            ('rep', rep_uri),
        ])
        self.sock_configs = OrderedDict([
                ('rep', DeferredSocket(zmq.REP)
                            .stream_callback('on_recv', self.process_request)),
        ])
        for k in self.sock_configs:
            if kwargs.get(k + '_bind'):
                self.sock_configs[k].bind(self.uris[k])
            else:
                self.sock_configs[k].connect(self.uris[k])
        super(Master, self).__init__(self.sock_configs, control_pipe=control_pipe)

    def process_request(self, env, multipart_message):
        timestamp, command, args, kwargs = map(jsonapi.loads, multipart_message)
        env['socks']['rep'].send_multipart(multipart_message)


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Job manager master''')
    parser.add_argument(nargs=1, dest='rep_uri', type=str)
    args = parser.parse_args()
    args.rep_uri = args.rep_uri[0]
    return args


if __name__ == '__main__':
    args = parse_args()
    m = Master(args.rep_uri)
    m.run()
