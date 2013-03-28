from datetime import datetime
from collections import OrderedDict

import zmq
from zmq_helpers.socket_configs import DeferredSocket
from zmq_helpers.rpc import ZmqJsonRpcTask, ZmqJsonRpcProxy


class Broker(ZmqJsonRpcTask):
    def __init__(self, dealer_uri, router_uri, dealer_bind=False, **kwargs):
        self._uris = OrderedDict()
        self._uris['rpc'] = router_uri
        self._uris['dealer'] = dealer_uri
        self._remote_handlers = set()
        self._pending_requests = {}
        super(Broker, self).__init__(on_run=self.on_run, **kwargs)

    def on_run(self, ctx, io_loop, socks, streams):
        z = ZmqJsonRpcProxy(self._uris['dealer'])
        self._remote_handlers = set(z.available_handlers())

    def on__available_handlers(self, env, *args, **kwargs):
        return sorted(set(self.handler_methods.keys()).union(self._remote_handlers))

    def get_handler(self, command):
        '''
        Check to see if a handler exists for a given command.  If the command
        is not found in the dictionary of handler methods, refresh the
        dictionary to be sure the handler is still not available.
        '''
        if not command in self._handler_methods:
            if command in self._remote_handlers:
                def _do_command(env, multipart_message, uuid, *args, **kwargs):
                    z = ZmqJsonRpcProxy(self._uris['dealer'], uuid=uuid)
                    return getattr(z, command)(*args, **kwargs)
                return _do_command
            self.refresh_handler_methods()
        return self._handler_methods.get(command)

    def call_handler(self, handler, env, multipart_message, request):
        # Add `multipart_message` argument to handler call.
        return handler(env, multipart_message, request['sender_uuid'],
                    *request['args'], **request['kwargs'])

    def unpack_request(self, multipart_message):
        # We need to manually extract the first two frames from the
        # multipart-message since we are using a `ROUTER` socket for the RPC
        # socket.  This means that the incoming message has two frames
        # prepended to the front of the message (compared to when using a `REP`
        # socket).  Note that these two frames must NOT be mangled in any way,
        # including de-serializing.
        #
        # The first frame contains the ZMQ address of the sender and the second
        # frame is empty.  These frames are used to keep track of where to send
        # the message back to when we receive a response on the `DEALER`
        # socket.
        message = multipart_message[2:]
        return super(Broker, self).unpack_request(message)

    def get_uris(self):
        return self._uris

    def get_sock_configs(self):
        return OrderedDict([
                ('rpc', DeferredSocket(zmq.ROUTER)
                            .stream_callback('on_recv',
                                             self.process_rpc_request)),
        ])

    def process_dealer_response(self, env, multipart_message):
        # We received a response to the dealer socket, so we need to forward
        # the message back to the job that requested through the router socket.
        sender_uuid = self.deserialize_frame(multipart_message[2])
        pending_requests = self.deserialize_frame(sender_uuid)
        request_info = pending_requests[0]
        del pending_requests[0]
        request = request_info['request']
        response = request_info['response']

        # Ignore first element (sender uuid)
        data = map(self.serialize_frame, request.values()[1:])
        # Ignore first element (sender uuid), and last element (error).
        data += map(self.serialize_frame, response.values()[1:-1])
        try:
            error = self.serialize_frame(response.values()[-1])
        except:
            error = self.serialize_frame(None)
        data.insert(0, self.serialize_frame(response['timestamp']))
        data.append(error)
        data = multipart_message[:2] + data
        print '[send_response]', data, request, response
        env['socks'][self.rpc_sock_name].send_multipart(data)

    #def on__hello_world(self, env, multipart_message, uuid):
        ## We received a request on the router socket, so we need to forward
        ## the message to the master through the dealer socket.
        #print datetime.now(), 'hello world', uuid

    def send_response(self, socks, multipart_message, request, response):
        # Ignore first element (sender uuid)
        data = map(self.serialize_frame, request.values()[1:])
        # Ignore first element (timestamp), and last element (error).
        data += map(self.serialize_frame, response.values()[1:-1])
        try:
            error = self.serialize_frame(response.values()[-1])
        except:
            error = self.serialize_frame(None)
        data.insert(0, self.serialize_frame(response['timestamp']))
        data.append(error)
        data = multipart_message[:2] + data
        print '[send_response]', data, request, response
        socks[self.rpc_sock_name].send_multipart(data)


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
