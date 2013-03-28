import logging
from datetime import datetime
from collections import OrderedDict

import zmq
from zmq_helpers.socket_configs import DeferredSocket
from zmq_helpers.rpc import ZmqJsonRpcTask
from zmq_helpers.utils import log_label, get_public_ip


class Master(ZmqJsonRpcTask):
    def __init__(self, pub_uri, rpc_uri, hostname=None, **kwargs):
        if hostname is None:
            self.hostname = get_public_ip()
        else:
            self.hostname = hostname
        self._uris = OrderedDict(pub=pub_uri, rpc=rpc_uri)
        super(Master, self).__init__(**kwargs)
        self.sock_configs['pub'] = DeferredSocket(zmq.PUB).bind(pub_uri)

    def get_uris(self):
        return self._uris

    def on__get_uris(self, *args, **kwargs):
        return OrderedDict([(k, u.replace(r'tcp://*', r'tcp://%s' %
                                          self.hostname))
                            for k, u in self.get_uris().items()])

    def on__hello_world(self, env, uuid):
        message = '[%s] hello world %s' % (datetime.now(), uuid)
        logging.getLogger(log_label(self)).info(message)
        env['socks']['pub'].send_multipart([message])
        return message


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Job manager master''')
    parser.add_argument(nargs=1, dest='pub_uri', type=str)
    parser.add_argument(nargs=1, dest='rep_uri', type=str)
    args = parser.parse_args()
    args.pub_uri = args.pub_uri[0]
    args.rep_uri = args.rep_uri[0]
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    m = Master(args.pub_uri, args.rep_uri)
    m.run()
