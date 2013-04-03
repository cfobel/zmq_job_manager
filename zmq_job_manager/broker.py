import logging
from collections import OrderedDict
from datetime import datetime
import functools

import zmq
from zmq.eventloop.ioloop import PeriodicCallback
from zmq_helpers.socket_configs import DeferredSocket
from zmq_helpers.rpc import ZmqJsonRpcTask, ZmqJsonRpcProxy
from zmq_helpers.utils import log_label


class WorkerState(object):
    def __init__(self, uuid, task_uuid=None, heartbeat_count=None):
        self._uuid = uuid
        self._request_queue = []
        self._task_uuid = task_uuid
        self._starting_heartbeat_count = heartbeat_count
        self._heartbeat_count = None

    def reset_heartbeat(self, value=None):
        old_value = getattr(self, '_starting_heartbeat_count', None)
        if value is None:
            if old_value is None:
                raise ValueError, 'No initial value provided for heartbeat count.'
            value = old_value
        logging.getLogger(log_label(self)).debug('reset_heartbeat %s', value)
        self._heartbeat_count = value
        self._starting_heartbeat_count = value


class Broker(ZmqJsonRpcTask):
    def __init__(self, dealer_uri, router_uri, dealer_bind=False, **kwargs):
        self._uris = OrderedDict()
        self._uris['rpc'] = router_uri
        self._uris['dealer'] = dealer_uri
        self._remote_handlers = set()
        self._data = None
        super(Broker, self).__init__(on_run=self.on_run, **kwargs)

    def _initial_data(self):
        '''
        By initializing data using a method, the broker may be sub-classed to,
        for example, provide persistent storage, allowing the process to be
        stopped and restarted while maintaining state.
        '''
        data = {
            # Worker mappings based on state.  Note that a worker may be
            # present in more than one mapping.  All mappings are indexed by
            # worker uuid.
            'workers': {
                # Worker
                'pending_create': set(),
                'pending_terminate': OrderedDict(),
                'running': OrderedDict(),
                'completed': OrderedDict(),
                'terminated': OrderedDict(),
                'flatlined': OrderedDict(),
            },
            # The state of each worker, with respect to the worker's uuid, the
            # assigned task's uuid, the worker's request queue, and the
            # worker's heartbeat count.  The `worker_states` mapping is indexed
            # by worker uuid.
            'worker_states': OrderedDict(),
        }
        return data

    def on_run(self, ctx, io_loop, socks, streams):
        z = ZmqJsonRpcProxy(self._uris['dealer'])
        self._data = self._initial_data()
        self._remote_handlers = set(z.available_handlers())
        env = OrderedDict([
            ('ctx', ctx),
            ('io_loop', io_loop),
            ('socks', socks),
            ('streams', streams),
        ])
        f = functools.partial(self.timer__monitor_heartbeats, env)
        callback = PeriodicCallback(f, 1000, io_loop=io_loop)
        callback.start()
        f = functools.partial(self.timer__grim_reaper, env)
        callback = PeriodicCallback(f, 5000, io_loop=io_loop)
        callback.start()

    def on__available_handlers(self, env, *args, **kwargs):
        return sorted(
            set(self.handler_methods.keys()).union(self._remote_handlers)
        )

    def on__begin_task(self, env, multipart_message, uuid, task_uuid,
                       **kwargs):
        self._data['worker_states'] = OrderedDict()
        self._data['worker_states'][uuid] = WorkerState(uuid, task_uuid, 5)
        z = ZmqJsonRpcProxy(self._uris['dealer'], uuid=uuid)
        result = z.begin_task(task_uuid, **kwargs)
        return result

    def on__broker_hello_world(self, env, multipart_message, uuid):
        # We received a request on the router socket, so we need to forward
        # the message to the master through the dealer socket.
        message = '[%s] hello world (%s)' % (datetime.now(), uuid)
        logging.getLogger(log_label(self)).info(message)
        return message

    def on__heartbeat(self, env, multipart_message, uuid):
        '''
        Do nothing here, since heart-beat count is reset in the `call_handler`
        method.
        '''
        return True

    def call_handler(self, handler, env, multipart_message, request):
        # Add `multipart_message` argument to handler call.
        sender_uuid = request['sender_uuid']
        if sender_uuid in self._data['worker_states']:
            self._data['worker_states'][sender_uuid].reset_heartbeat()
        return handler(env, multipart_message, request['sender_uuid'],
                    *request['args'], **request['kwargs'])

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

    def get_sock_configs(self):
        return OrderedDict([
                ('rpc', DeferredSocket(zmq.ROUTER)
                            .stream_callback('on_recv',
                                             self.process_rpc_request)),
        ])

    def get_uris(self):
        return self._uris

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
        logging.getLogger(log_label(self)).info(
                'request: uuid=%(sender_uuid)s command=%(command)s' % request)
        env['socks'][self.rpc_sock_name].send_multipart(data)

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
        logging.getLogger(log_label(self)).info(
                'request: uuid=%(sender_uuid)s command=%(command)s' % request)
        socks[self.rpc_sock_name].send_multipart(data)

    def terminate_worker(self, env, worker_uuid):
        '''
        Terminate worker externally (i.e., workers that are no longer
        responding), since we cannot worker would not respond to a request to
        shutdown gracefully.

        By default, nothing is done except removing the worker from the
        `pending_terminate` set, but the sub-classes may implement
        `terminate_worker(self, env, uuid)` accordingly.
        '''
        z = ZmqJsonRpcProxy(self._uris['dealer'], uuid=worker_uuid)
        if worker_uuid in self._data['workers']['pending_terminate']:
            del self._data['workers']['pending_terminate'][worker_uuid]
            z.terminated_worker()
        if worker_uuid in self._data['workers']['flatlined']:
            z.flatlined_worker()

    def timer__grim_reaper(self, env, *args, **kwargs):
        for workers_set in ('pending_terminate', 'flatlined'):
            for uuid, worker in self._data['workers'][workers_set].iteritems():
                self.terminate_worker(env, uuid)

    def timer__monitor_heartbeats(self, *args, **kwargs):
        for uuid, worker in self._data['worker_states'].iteritems():
            heartbeat_count = worker._heartbeat_count
            if heartbeat_count is not None:
                # If `heartbeat_count` is `None`, the heart-beat has not been
                # started.  We only process the heart-beat after it has been
                # started.
                heartbeat_count -= 1
                if heartbeat_count <= 0:
                    # This process has missed the maximum number of expected
                    # heartbeats, so add to list of flatlined workers.
                    self._data['workers']['flatlined'][uuid] = worker
                    if heartbeat_count == 0:
                        logging.getLogger(log_label(self)).info(
                            'worker has flatlined (i.e., heartbeat_count=%d):'
                            ' %s' % (heartbeat_count, uuid)
                        )
                worker._heartbeat_count = heartbeat_count

        for uuid, worker in self._data['workers']['flatlined'].iteritems():
            if worker._heartbeat_count is not None\
                    and worker._heartbeat_count > 0:
                # A worker has come back to life!  Update the worker mappings
                # accordingly.
                self._data['workers']['running'][uuid] = worker
                del self._data['workers']['flatlined'][uuid]
                logging.getLogger(log_label(self)).info(
                    'worker %s has revived - heartbeat_count=%d'
                    % (uuid, heartbeat_count)
                )
                z = ZmqJsonRpcProxy(self._uris['dealer'], uuid=uuid)
                z.revived_worker()

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
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    b = Broker(args.dealer_uri, args.router_uri)
    b.run()
