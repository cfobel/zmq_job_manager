import logging
from collections import OrderedDict
from datetime import datetime
try:
    import cPickle as pickle
except ImportError:
    import pickle

import transaction
import zmq
from zmq_helpers.socket_configs import DeferredSocket
from zmq_helpers.rpc import ZmqJsonRpcTask
from zmq_helpers.utils import log_label
from path import path
from persistent_helpers.ordered_dict import PersistentOrderedDict
from ZODB import DB
from ZEO import ClientStorage


class SubMessage(object):
    def __init__(self, worker_uuid, task_uuid, timestamp, command, *args):
        self.worker_uuid = worker_uuid
        self.task_uuid = task_uuid
        self.timestamp = timestamp
        self.command = command
        self.args = args

    @classmethod
    def from_multipart_message(cls, multipart_message):
        result = cls(multipart_message[0], multipart_message[1],
                     datetime.utcfromtimestamp(float(multipart_message[2])),
                     multipart_message[3], *multipart_message[4:])
        return result

    def __repr__(self):
        args = ['%s=%s' % (k, self.__dict__[k])
                for k in 'worker_uuid', 'task_uuid', 'timestamp', 'command',
                'args']
        return u'SubMessage(%s)' % ', '.join(args)


class WorkersSink(ZmqJsonRpcTask):
    default_data_dir = path('~/.workers_sink').expand()
    default_db_name = 'zeo.socket'

    def __init__(self, rpc_uri, sub_uri, data_dir=None, db_name=None,
                 init_subcribe=None, **kwargs):
        self._uris = OrderedDict()
        self._uris['rpc'] = rpc_uri
        self._uris['sub'] = sub_uri
        self._registered_workers = set()
        self._init_subscribe = init_subcribe
        if db_name is None:
            self._db_name = self.default_db_name
        else:
            self._db_name = db_name
        if data_dir is None:
            self._data_dir = self.default_data_dir
        else:
            self._data_dir = path(data_dir).abspath()
        if not self._data_dir.isdir():
            self._data_dir.makedirs_p()
        super(WorkersSink, self).__init__(on_run=self.on_run, sub_bind=False,
                                          **kwargs)

    def on_run(self, ctx, io_loop, socks, streams):
        self._db_path = self._data_dir.joinpath(self._db_name)
        logging.getLogger(log_label(self)).info('using db path: %s',
                                                self._db_path)
        self._reset_db()

    def _reset_db(self):
        self._storage = ClientStorage.ClientStorage(self._db_path)
        self._db = DB(self._storage)
        self._connection = self._db.open()
        self._root = self._connection.root()

    def cleanup(self):
        for k in ('_connection', '_db', '_storage', ):
            v = getattr(self, k, None)
            if v:
                v.close()

    def __del__(self):
        self.cleanup()

    def _get_db_node(self, db_path):
        nodes = db_path.split('/')
        assert(nodes[0] == '')
        node_names = nodes[1:]
        for i in range(3):
            try:
                node = self._root
                for i in range(len(node_names)):
                    if node_names[i] not in node:
                        node[node_names[i]] = PersistentOrderedDict()
                    node = node[node_names[i]]
                return node
            except:
                self._reset_db()
        raise

    def _save_to_db(self, db_path, value):
        for i in range(3):
            try:
                db_path = path(db_path)
                node = self._get_db_node(db_path.parent)
                node[db_path.name] = value
                transaction.commit()
                return
            except:
                self._reset_db()
        raise

    def get_uris(self):
        return self._uris

    def get_sock_configs(self):
        sock_configs = super(WorkersSink, self).get_sock_configs()
        sock_configs['sub'] = (DeferredSocket(zmq.SUB)
                               .stream_callback('on_recv',
                                                self.process_sub_message))
        if self._init_subscribe is not None:
            sock_configs['sub'].setsockopt(zmq.SUBSCRIBE, self._init_subscribe)
        return sock_configs

    def _message_data_dir(self, message, make=True):
        return self._task_data_dir(message.worker_uuid, message.task_uuid, make)

    def _task_data_dir(self, worker_uuid, task_uuid, make=True):
        result = self._data_dir.joinpath('task_data', worker_uuid, task_uuid)
        if make and not result.isdir():
            result.makedirs_p()
        return result

    def _on__std_base(self, message, data, stream_name):
        std_path = self._message_data_dir(message).joinpath(stream_name)
        std = std_path.open('a')
        std.write(data)
        std.close()
        logging.getLogger(log_label(self)).info('append %s to : %s',
                                                stream_name, std_path)

    def sub__stdout(self, message, data):
        self._on__std_base(message, data, 'stdout')

    def sub__stderr(self, message, data):
        self._on__std_base(message, data, 'stderr')

    def sub__begin_task(self, message, data):
        db_path = '/data/%s/%s/__begin_task__' % (message.worker_uuid,
                                                  message.task_uuid)
        self._save_to_db(db_path, datetime.utcfromtimestamp(float(data)))
        logging.getLogger(log_label(self)).info(data)

    def sub__complete_task(self, message, data):
        db_path = '/data/%s/%s/__complete_task__' % (message.worker_uuid,
                                                     message.task_uuid)
        self._save_to_db(db_path, datetime.utcfromtimestamp(float(data)))
        logging.getLogger(log_label(self)).info(data)

    def sub__store(self, message, serialization, key, value):
        if serialization == 'SERIALIZE__PICKLE':
            value = pickle.loads(value)

        db_path = '/data/%s/%s/%s' % (message.worker_uuid, message.task_uuid,
                                      key)
        self._save_to_db(db_path, value)
        logging.getLogger(log_label(self)).info('key=%s value=%s', key, value)

    def process_sub_message(self, env, multipart_message):
        message = SubMessage.from_multipart_message(multipart_message)
        f = getattr(self, 'sub__' + message.command, None)
        if f and hasattr(f, '__call__'):
            f(message, *message.args)

    def on__unregister_worker(self, env, uuid, worker_uuid):
        if worker_uuid in self._registered_workers:
            env['socks']['sub'].setsockopt(zmq.UNSUBSCRIBE, worker_uuid)
            self._registered_workers.remove(worker_uuid)
        logging.getLogger(log_label(self)).info(worker_uuid)

    def on__register_worker(self, env, uuid, worker_uuid):
        if not self._registered_workers and self._init_subscribe is not None:
            env['socks']['sub'].setsockopt(zmq.UNSUBSCRIBE, self._init_subscribe)
        env['socks']['sub'].setsockopt(zmq.SUBSCRIBE, worker_uuid)
        self._registered_workers.add(worker_uuid)
        logging.getLogger(log_label(self)).info(worker_uuid)


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Router-to sub broker''')
    parser.add_argument(nargs=1, dest='rpc_uri', type=str)
    parser.add_argument(nargs=1, dest='sub_uri', type=str)
    args = parser.parse_args()
    args.rpc_uri = args.rpc_uri[0]
    args.sub_uri = args.sub_uri[0]
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    b = WorkersSink(args.rpc_uri, args.sub_uri, init_subcribe='')
    b.run()
