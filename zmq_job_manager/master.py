from datetime import datetime

import zmq
from zmq_helpers.socket_configs import DeferredSocket
from zmq_helpers.rpc import ZmqJsonRpcTask


class Master(ZmqJsonRpcTask):
    def __init__(self, pub_uri, *args, **kwargs):
        super(Master, self).__init__(*args, **kwargs)
        self.uris['pub'] = pub_uri
        self.sock_configs['pub'] = DeferredSocket(zmq.PUB).bind(pub_uri)

    def on__hello_world(self, env, uuid):
        message = '[%s] hello world %s' % (datetime.now(), uuid)
        print message
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
    args = parse_args()
    m = Master(args.pub_uri, args.rep_uri)
    m.run()
