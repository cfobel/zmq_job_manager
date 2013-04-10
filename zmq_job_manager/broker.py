import logging
from collections import OrderedDict
from datetime import datetime
import functools
from uuid import uuid1

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
        self._heartbeat_count = heartbeat_count

    def reset_heartbeat(self, value=None):
        old_value = getattr(self, '_starting_heartbeat_count', None)
        if value is None:
            if old_value is None:
                raise ValueError, 'No initial value provided for heartbeat count.'
            value = old_value
        logging.getLogger(log_label(self)).debug('reset_heartbeat %s', value)
        self._heartbeat_count = value
        self._starting_heartbeat_count = value


class BrokerBase(ZmqJsonRpcTask):
    def __init__(self, dealer_uri, router_uri, dealer_bind=False, **kwargs):
        self._uris = OrderedDict()
        self._uris['rpc'] = router_uri
        self._uris['dealer'] = dealer_uri
        self._remote_handlers = set()
        self._data = None
        super(BrokerBase, self).__init__(on_run=self.on_run, **kwargs)

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
                # Worker creation requested (but not completed/verified)
                'pending_create': set(),
                # Worker termination requested (but not completed/verified)
                'pending_terminate': set(),
                # Worker running (i.e., we've received a request from the
                # worker)
                'running': set(),
                'terminated': set(),
                # We haven't received any request (including heart-beat) from
                # the worker in 5 heart-beat interval counts.
                'flatlined': set(),
                'flatlined_latch': set(),
            },
            # The state of each worker, with respect to the worker's uuid, the
            # assigned task's uuid, the worker's request queue, and the
            # worker's heartbeat count.  The `worker_states` mapping is indexed
            # by worker uuid.
            'worker_states': OrderedDict(),
            # The information regarding the worker provided by the worker during
            # registration.
            'worker_infos': OrderedDict(),
            'worker_attrs': OrderedDict(),
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
        self._data['worker_states'][uuid]._task_uuid = task_uuid
        z = ZmqJsonRpcProxy(self._uris['dealer'], uuid=uuid)
        result = z.begin_task(task_uuid, **kwargs)
        return result

    def on__broker_hello_world(self, env, multipart_message, uuid):
        message = '[%s] hello world (%s)' % (datetime.now(), uuid)
        logging.getLogger(log_label(self)).info(message)
        return message

    def on__create_worker(self, env, multipart_message, uuid, *args, **kwargs):
        '''
        Create a new worker and return the worker's uuid.
        '''
        new_worker_uuid = kwargs.pop('worker_uuid', None)
        if new_worker_uuid is None:
            new_worker_uuid = str(uuid1())
        message = '[%s] create worker (%s)' % (datetime.now(), new_worker_uuid)
        result = self.create_worker(env, new_worker_uuid, *args, **kwargs)
        if result:
            logging.getLogger(log_label(self)).info(message)
            self._data['workers']['pending_create'].add(result)
        return result

    def on__heartbeat(self, env, multipart_message, uuid):
        '''
        Do nothing here, since heart-beat count is reset in the `call_handler`
        method.
        '''
        return True

    def on__register_worker(self, env, multipart_message, uuid, worker_info,
                            affinity_labels=tuple(), end_time=None,
                            memory_limit=None):
        if uuid in self._data['workers']['pending_create']:
            self._data['workers']['pending_create'].remove(uuid)
        # Default the heart-beat timeout to 5 cycles
        #self._data['worker_states'][uuid] = WorkerState(uuid, task_uuid, 5)
        self._data['worker_states'][uuid] = WorkerState(uuid, None, 5)
        self._data['workers']['running'].add(uuid)
        self._data['worker_infos'][uuid] = worker_info
        self._data['worker_attrs'][uuid] = OrderedDict([
            ('affinity_labels', affinity_labels),
            ('end_time', end_time),
            ('memory_limit', memory_limit),
        ])

    def on__request_task(self, env, multipart_message, worker_uuid):
        z = ZmqJsonRpcProxy(self._uris['dealer'], uuid=worker_uuid)
        result = z.request_task(self._data['worker_infos'][worker_uuid])
        return result

    def create_worker(self, env, worker_uuid, *args, **kwargs):
        raise NotImplementedError

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
        '''
        Isolate handler call in this method to allow subclasses to perform
        special-handling, if necessary.

        Note that the multipart-message is ignored by default.
        '''
        if request['sender_uuid'] in self._data['worker_states']:
            self._data['worker_states'][request['sender_uuid']].reset_heartbeat()
        if request['command'] not in ('heartbeat', ):
            logging.getLogger(log_label(self)).info(request['command'])
        return handler(env, multipart_message, request['sender_uuid'],
                       *request['args'], **request['kwargs'])

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
        if request['command'] not in ('heartbeat', ):
            logging.getLogger(log_label(self)).info('request: '
                    'uuid=%(sender_uuid)s command=%(command)s' % request)
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
        def get_z():
            return ZmqJsonRpcProxy(self._uris['dealer'], uuid=worker_uuid)

        z = None

        if worker_uuid in self._data['workers']['pending_terminate']:
            if z is None: z = get_z()
            self._data['workers']['pending_terminate'].remove(worker_uuid)
            self._data['workers']['terminated'].add(worker_uuid)
            z.terminated_worker()
        if worker_uuid in self._data['workers']['flatlined']\
                and worker_uuid in self._data['workers']['flatlined_latch']:
            if z is None: z = get_z()
            z.flatlined_worker()
            self._data['workers']['flatlined_latch'].remove(worker_uuid)

        if worker_uuid in self._data['worker_states']:
            self._data['worker_states'][worker_uuid]._task_uuid = None

    def timer__grim_reaper(self, env, *args, **kwargs):
        for workers_set in ('pending_terminate', 'flatlined'):
            for uuid in self._data['workers'][workers_set].copy():
                self.terminate_worker(env, uuid)

    def timer__monitor_heartbeats(self, *args, **kwargs):
        for uuid in self._data['workers']['running'].copy():
            worker = self._data['worker_states'].get(uuid)
            if worker:
                heartbeat_count = worker._heartbeat_count
                if uuid in self._data['workers']['running']\
                        and heartbeat_count is not None:
                    # If `heartbeat_count` is `None`, the heart-beat has not
                    # been started.  We only process the heart-beat after it
                    # has been started.
                    heartbeat_count -= 1
                    if heartbeat_count <= 0:
                        # This process has missed the maximum number of
                        # expected heartbeats, so add to list of flatlined
                        # workers.
                        self._data['workers']['flatlined'].add(uuid)
                        # Set a flag to mark worker as newly flat-lined.
                        self._data['workers']['flatlined_latch'].add(uuid)
                        self._data['workers']['running'].remove(uuid)
                        if heartbeat_count == 0:
                            logging.getLogger(log_label(self)).info('worker '
                                    'has flatlined (i.e., heartbeat_count=%s):'
                                    ' %s' % (heartbeat_count, uuid))
                    worker._heartbeat_count = heartbeat_count

        for uuid in self._data['workers']['flatlined'].copy():
            worker = self._data['worker_states'][uuid]
            if worker._heartbeat_count is not None\
                    and worker._heartbeat_count > 0:
                # A worker has come back to life!  Update the worker mappings
                # accordingly.
                self._data['workers']['running'].add(uuid)
                self._data['workers']['flatlined'].remove(uuid)
                logging.getLogger(log_label(self)).info(
                    'worker %s has revived - heartbeat_count=%s'
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
        return super(BrokerBase, self).unpack_request(message)


class Broker(BrokerBase):
    def create_worker(self, env, worker_uuid, *args, **kwargs):
        '''
        Create worker subprocess.
        '''
        import os
        from subprocess import Popen, PIPE
        import platform

        if not hasattr(self, '_worker_subprocesses'):
            self._worker_subprocesses = OrderedDict()
        if worker_uuid in self._worker_subprocesses:
            p = self._worker_subprocesses[worker_uuid]
            if p.poll() is not None:
                # Worker with same name existed, but has finished.
                try:
                    p.terminate()
                except OSError:
                    pass
                finally:
                    del self._worker_subprocesses[worker_uuid]
            else:
                # Worker already exists, so return `None`
                logging.getLogger(log_label(self)).info(('worker already exists:', worker_uuid))
                return None
        logging.getLogger(log_label(self)).info((worker_uuid, args, kwargs))

        env = os.environ
        env.update(kwargs.pop('env', {}))
        broker_connect_uri = self.uris['rpc'].replace(r'tcp://*', r'tcp://%s' %
                                                      (platform.node()))
        command = ('bash -c "'
            '. /usr/local/bin/virtualenvwrapper.sh &&'
            'workon zmq_job_manager && '
            'cdvirtualenv && '
            'mkdir -p worker_envs/%(uuid)s && '
            'cd worker_envs/%(uuid)s && '
            'python -m zmq_job_manager.worker %(uri)s %(uuid)s"'
            % {'uuid': worker_uuid, 'uri': broker_connect_uri}
        )
        self._worker_subprocesses[worker_uuid] = Popen(command, shell=True,
                                                       env=env, stdout=PIPE,
                                                       stderr=PIPE)
        logging.getLogger(log_label(self)).info('started worker subprocess for:'
                                                ' %s\n%s', worker_uuid, command)
        return worker_uuid

    def terminate_worker(self, env, worker_uuid):
        '''
        If a worker subprocess has been launched by the broker for the
        specified worker uuid, terminate the subprocess.
        '''
        super(Broker, self).terminate_worker(env, worker_uuid)
        if hasattr(self, '_worker_subprocesses'):
            if worker_uuid in self._worker_subprocesses:
                logging.getLogger(log_label(self)).info('terminate_worker: %s',
                                                        worker_uuid)
                p = self._worker_subprocesses[worker_uuid]
                try:
                    p.terminate()
                except OSError:
                    pass
                finally:
                    del self._worker_subprocesses[worker_uuid]
            for workers_set in ('pending_terminate', 'flatlined'):
                if worker_uuid in self._data['workers'][workers_set]:
                    self._data['workers'][workers_set].remove(worker_uuid)
                self._data['workers']['terminated'].add(worker_uuid)

    def __del__(self):
        self.close()

    def close(self):
        '''
        Terminate any subprocesses that are still running.
        '''
        super(Broker, self).__del__()
        if hasattr(self, '_worker_subprocesses'):
            for uuid, p in self._worker_subprocesses:
                try:
                    p.terminate()
                    p.close()
                except:
                    pass



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
    #logging.basicConfig(level=logging.CRITICAL)
    args = parse_args()
    b = Broker(args.dealer_uri, args.router_uri)
    b.run()
