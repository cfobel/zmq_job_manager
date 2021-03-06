import logging
from collections import OrderedDict
from datetime import datetime
try:
    import cPickle as pickle
except ImportError:
    import pickle

import zmq
from zmq.utils import jsonapi
from zmq_helpers.socket_configs import DeferredSocket
from zmq_helpers.rpc import ZmqRpcTask
from zmq_helpers.utils import log_label
from persistent_helpers.storage import DurusStorage
from persistent_helpers.ordered_dict import PersistentOrderedDict
from durus.persistent_set import PersistentSet
import durus.btree
from path import path


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


class WorkersSink(ZmqRpcTask):
    default_data_dir = path('~/.workers_sink').expand()

    def __init__(self, rpc_uri, sub_uri, storage, data_dir=None,
                 init_subcribe=None, **kwargs):
        self._uris = OrderedDict()
        self._uris['rpc'] = rpc_uri
        self._uris['sub'] = sub_uri
        self._registered_workers = set()
        self._init_subscribe = init_subcribe
        self.storage = storage
        if data_dir is None:
            self._data_dir = self.default_data_dir
        else:
            self._data_dir = path(data_dir).abspath()
        if not self._data_dir.isdir():
            self._data_dir.makedirs_p()
        super(WorkersSink, self).__init__(sub_bind=False, **kwargs)

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

    def _rpc__std_base(self, message, data, stream_name):
        std_path = self._message_data_dir(message).joinpath(stream_name)
        std = std_path.open('a')
        std.write(data)
        std.close()
        logging.getLogger(log_label(self)).info('append %s to : %s',
                                                stream_name, std_path)

    def sub__stdout(self, message, data):
        self._rpc__std_base(message, data, 'stdout')

    def sub__stderr(self, message, data):
        self._rpc__std_base(message, data, 'stderr')

    def sub__begin_task(self, message, seconds_since_epoch_str, worker_info,
                        serialization):
        if serialization == 'SERIALIZE__PICKLE':
            worker_info = pickle.loads(worker_info)
        elif serialization == 'SERIALIZE__JSON':
            worker_info = jsonapi.loads(worker_info)
        elif serialization != 'SERIALIZE__NONE':
            worker_info = {'data': worker_info, 'serialization': serialization}
        begin_time = datetime.utcfromtimestamp(float(seconds_since_epoch_str))
        self.save(message.worker_uuid, message.task_uuid, '__begin_task__',
                  begin_time)
        self.save(message.worker_uuid, message.task_uuid, '__worker_info__',
                  worker_info)
        logging.getLogger(log_label(self)).info(worker_info)

    def sub__complete_task(self, message, data):
        self.save(message.worker_uuid, message.task_uuid, '__complete_task__',
                  datetime.utcfromtimestamp(float(data)))
        logging.getLogger(log_label(self)).info(data)

    def save(self, worker_uuid, task_uuid, key, value):
        logging.getLogger(log_label(self)).info(self.storage.root.keys())
        for i in range(3):
            self.storage.abort()
            try:
                db_path = '/worker_uuid_by_task_uuid/%s' % task_uuid
                self.storage[db_path] = value
                db_path = '/task_by_uuid/%s/%s' % (task_uuid, key)
                self.storage[db_path] = value
                if task_uuid not in self.storage.root.setdefault(
                        'datetime_by_task_uuid', PersistentOrderedDict()):
                    db_path = '/datetime_by_task_uuid/%s' % task_uuid
                    now = datetime.now()
                    self.storage[db_path] = now
                    task_uuid_by_datetime = self.storage.root.setdefault(
                            'task_uuid_by_datetime', durus.btree.BTree())
                    task_uuid_by_datetime[now] = task_uuid
                task_uuids_by_label = self.storage.root.setdefault(
                        'task_uuids_by_label', PersistentOrderedDict())
                if key == '__label__':
                    logging.getLogger(log_label(self)).warning('key=%s value=%s', key, value)
                    label = value
                    uuids = task_uuids_by_label.setdefault(label, PersistentSet())
                    if task_uuid not in uuids:
                        logging.getLogger(log_label(self)).warning(
                                'adding %s to task_uuids_by_label["%s"]',
                                task_uuid, label)
                        uuids.add(task_uuid)
                        self.storage.root._p_note_change()
                self.storage.commit()
                return
            except:
                pass
        raise

    def sub__store(self, message, serialization, key, value):
        if serialization == 'SERIALIZE__PICKLE':
            value = pickle.loads(value)
        elif serialization == 'SERIALIZE__JSON':
            value = jsonapi.loads(value)
        self.save(message.worker_uuid, message.task_uuid, key, value)
        logging.getLogger(log_label(self)).debug('key=%s value=%s', key, value)

    def process_sub_message(self, env, multipart_message):
        message = SubMessage.from_multipart_message(multipart_message)
        f = getattr(self, 'sub__' + message.command, None)
        if f and hasattr(f, '__call__'):
            f(message, *message.args)

    def rpc__unregister_worker(self, env, uuid, worker_uuid):
        if worker_uuid in self._registered_workers:
            env['socks']['sub'].setsockopt(zmq.UNSUBSCRIBE, worker_uuid)
            self._registered_workers.remove(worker_uuid)
        logging.getLogger(log_label(self)).info(worker_uuid)

    def rpc__register_worker(self, env, uuid, worker_uuid):
        if not self._registered_workers and self._init_subscribe is not None:
            env['socks']['sub'].setsockopt(zmq.UNSUBSCRIBE, self._init_subscribe)
        env['socks']['sub'].setsockopt(zmq.SUBSCRIBE, worker_uuid)
        self._registered_workers.add(worker_uuid)
        logging.getLogger(log_label(self)).info(worker_uuid)


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser, RawDescriptionHelpFormatter

    parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter,
                            description='''
Worker data sink
----------------

The data-sink process listens for messages from a `manager` process containing
updates from worker processes.  The data-sink saves any relevant information to
a ZODB database, where the results are organized by worker UUID and task UUID.
Several indexes also exist to look up a task based on other criteria, including
by the date and time the first message was received from each worker.
    '''.strip())
    parser.add_argument(nargs=1, dest='rpc_uri', type=str)
    parser.add_argument(nargs=1, dest='sub_uri', type=str)
    args = parser.parse_args()
    args.rpc_uri = args.rpc_uri[0]
    args.sub_uri = args.sub_uri[0]
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    b = WorkersSink(args.rpc_uri, args.sub_uri, storage=DurusStorage(),
                    init_subcribe='')
    b.run()
