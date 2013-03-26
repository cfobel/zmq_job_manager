from collections import OrderedDict

import zmq
from zmq_helpers.socket_configs import SockConfigsTask, DeferredSocket


class Broker(SockConfigsTask):
    def __init__(self, dealer_uri, router_uri, dealer_bind=False,
                 router_bind=True, control_pipe=None):
        self.uris = OrderedDict([
            ('dealer', dealer_uri),
            ('router', router_uri),
        ])
        self.sock_configs = OrderedDict([
                ('dealer', DeferredSocket(zmq.DEALER)
                            .stream_callback('on_recv', self.process_dealer_response)),
                ('router', DeferredSocket(zmq.ROUTER)
                            .stream_callback('on_recv', self.process_router_request))
        ])
        if router_bind:
            self.sock_configs['router'].bind(router_uri)
        else:
            self.sock_configs['router'].connect(router_uri)
        if dealer_bind:
            self.sock_configs['dealer'].bind(dealer_uri)
        else:
            self.sock_configs['dealer'].connect(dealer_uri)
        super(Broker, self).__init__(self.sock_configs, control_pipe=control_pipe)

    def process_dealer_response(self, env, multipart_message):
        # We received a response to the dealer socket, so we need to forward
        # the message back to the job that requested through the router socket.
        env['socks']['router'].send_multipart(multipart_message)

    def process_router_request(self, env, multipart_message):
        # We received a request on the router socket, so we need to forward
        # the message to the master through the dealer socket.
        env['socks']['dealer'].send_multipart(multipart_message)


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Router-to dealer broker''')
    parser.add_argument(nargs=1, dest='router_uri', type=str)
    parser.add_argument(nargs=1, dest='dealer_uri', type=str)
    args = parser.parse_args()
    args.router_uri = args.router_uri[0]
    args.dealer_uri = args.dealer_uri[0]
    return args


if __name__ == '__main__':
    args = parse_args()
    b = Broker(args.dealer_uri, args.router_uri)
    b.run()
