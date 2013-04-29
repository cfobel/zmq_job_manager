from subprocess import PIPE
import logging
from datetime import datetime
from collections import OrderedDict
from uuid import uuid4
try:
    import cPickle as pickle
except ImportError:
    import pickle

import zmq
from zmq.utils import jsonapi
from zmq_helpers.socket_configs import DeferredSocket
from zmq_helpers.rpc import ZmqRpcTask
from zmq_helpers.utils import log_label, get_public_ip

from .process import DeferredPopen
from .constants import SERIALIZE__NONE, SERIALIZE__JSON


def get_seconds_since_epoch():
    return (datetime.now() - datetime(1970,1,1)).total_seconds()


class Manager(ZmqRpcTask):
    def __init__(self, pub_uri, rpc_uri, hostname=None, **kwargs):
        if hostname is None:
            self.hostname = get_public_ip()
        else:
            self.hostname = hostname
        self._uris = OrderedDict(pub=pub_uri, rpc=rpc_uri)
        super(Manager, self).__init__(**kwargs)
        self.sock_configs['pub'] = DeferredSocket(zmq.PUB).bind(pub_uri)
        self.pending_tasks = OrderedDict()
        self.running_tasks = OrderedDict()
        self.completed_tasks = OrderedDict()
        # Live worker uuids
        self.workers = set()
        self.worker_by_task = OrderedDict()
        self.task_by_worker = OrderedDict()
        # Pending worker requests
        self.worker_requests = OrderedDict()

    def get_uris(self):
        return self._uris

    def get_task(self, env, worker_uuid, worker_info):
        return self.pending_tasks.popitem(0)

    def rpc__get_uris(self, env, uuid, *args, **kwargs):
        return OrderedDict([(k, u.replace(r'tcp://*', r'tcp://%s' %
                                          self.hostname))
                            for k, u in self.get_uris().items()])

    def publish(self, env, uuid, task_uuid, command, *args):
        data = [uuid, task_uuid, '%.6f' % get_seconds_since_epoch(), command] + list(args)
        env['socks']['pub'].send_multipart(data)

    def rpc__store(self, env, uuid, key, value, task_uuid=None,
                  serialization=SERIALIZE__NONE):
        if task_uuid is None and uuid in self.task_by_worker:
            task_uuid = self.task_by_worker[uuid]
        if task_uuid is not None:
            message = '[%s] uuid=%s, %s=%s' % (log_label(self), uuid, key, value)
            logging.getLogger(log_label(self)).info(message)
            self.publish(env, uuid, task_uuid, 'store', serialization, key, value)
            return message

    def rpc__stdout(self, env, uuid, value):
        message = self._rpc__std_base(env, uuid, 'stdout', value)
        logging.getLogger(log_label(self)).info(message)
        return message

    def rpc__stderr(self, env, uuid, value):
        message = self._rpc__std_base(env, uuid, 'stderr', value)
        logging.getLogger(log_label(self)).info(message)
        return message

    def _rpc__std_base(self, env, uuid, stream_name, value):
        if uuid in self.task_by_worker:
            task_uuid = self.task_by_worker[uuid]
            message = '[%s] uuid=%s\n%s' % (log_label(self), uuid, value)
            self.publish(env, uuid, task_uuid, stream_name, value)
            return message

    def rpc__running_task_ids(self, env, uuid):
        return self.running_tasks.keys()

    def rpc__pending_task_ids(self, env, uuid):
        return self.pending_tasks.keys()

    def rpc__completed_task_ids(self, env, uuid):
        return self.completed_tasks.keys()

    def assign_task(self, worker_uuid, task_uuid, task):
        if worker_uuid not in self.workers:
            self.workers.add(worker_uuid)
        self.worker_by_task[task_uuid] = worker_uuid
        self.task_by_worker[worker_uuid] = task_uuid

    def rpc__get_worker_task_uuid(self, env, uuid, worker_uuid):
        return self.task_by_worker.get(worker_uuid)

    def rpc__get_task_worker_uuid(self, env, uuid, task_uuid):
        return self.worker_by_task.get(task_uuid)

    def rpc__get_task_command(self, env, uuid, task_uuid):
        tasks = getattr(self, '%s_tasks' % self.rpc__get_task_state(env, uuid,
                                                                   task_uuid))
        return tasks[task_uuid]._args[0]

    def rpc__get_task_state(self, env, uuid, task_uuid):
        if task_uuid in self.pending_tasks:
            return 'pending'
        elif task_uuid in self.running_tasks:
            return 'running'
        elif task_uuid in self.completed_tasks:
            return 'completed'
        else:
            raise KeyError, 'No task found for uuid: %s' % task_uuid

    def rpc__request_task(self, env, uuid, worker_info):
        if not hasattr(self, '_worker_infos'):
            self._worker_infos = OrderedDict()
        self._worker_infos[uuid] = worker_info
        if self.pending_tasks:
            result = self.get_task(env, uuid, worker_info)
            self.running_tasks[result[0]] = result[1]
            self.assign_task(uuid, *result)
        else:
            result = None
        return pickle.dumps(result)

    def rpc__begin_task(self, env, uuid, task_uuid, seconds_since_epoch=None):
        if task_uuid in self.running_tasks:
            if seconds_since_epoch is None:
                seconds_since_epoch = get_seconds_since_epoch()
            self.publish(env, uuid, task_uuid, 'begin_task', '%.6f' %
                         seconds_since_epoch,
                         jsonapi.dumps(self._worker_infos.get(uuid)),
                         SERIALIZE__JSON)

    def rpc__terminate_worker(self, env, uuid, seconds_since_epoch=None):
        self._publish_with_time_float(env, uuid, '', 'terminated_worker',
                                      seconds_since_epoch)

    def rpc__flatlined_worker(self, env, uuid, seconds_since_epoch=None):
        self._publish_with_time_float(env, uuid, '', 'flatlined_worker',
                                      seconds_since_epoch)

    def rpc__get_worker_info(self, env, uuid, worker_uuid):
        if hasattr(self, '_worker_infos'):
            return self._worker_infos.get(worker_uuid)

    def rpc__revived_worker(self, env, uuid, seconds_since_epoch=None):
        self._publish_with_time_float(env, uuid, '', 'revived_worker',
                                      seconds_since_epoch)

    def _publish_with_time_float(self, env, worker_uuid, task_uuid, command,
                                 seconds_since_epoch=None):
        if seconds_since_epoch is None:
            seconds_since_epoch = get_seconds_since_epoch()
        self.publish(env, worker_uuid, task_uuid, command, '%.6f' %
                     seconds_since_epoch)

    def rpc__uncomplete_task(self, env, uuid, task_uuid):
        if task_uuid in self.completed_tasks:
            t = self.completed_tasks[task_uuid]
            del self.completed_tasks[task_uuid]
            self.running_tasks[task_uuid] = t

    def rpc__complete_task(self, env, uuid, task_uuid, seconds_since_epoch=None):
        if task_uuid in self.running_tasks:
            t = self.running_tasks[task_uuid]
            del self.running_tasks[task_uuid]
            self.completed_tasks[task_uuid] = t
            if seconds_since_epoch is None:
                seconds_since_epoch = get_seconds_since_epoch()
            self.publish(env, uuid, task_uuid, 'complete_task',
                         '%.6f' % seconds_since_epoch)

    def rpc__register_task(self, env, uuid, shell_command):
        task_uuid = str(uuid4())
        self.pending_tasks[task_uuid] = DeferredPopen([shell_command],
                                                      shell=True, stdout=PIPE,
                                                      stderr=PIPE)
        return task_uuid

    def rpc__queue_worker_request(self, env, uuid, worker_uuid, command, *args,
                                 **kwargs):
        pass


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Job manager manager''')
    parser.add_argument(nargs=1, dest='pub_uri', type=str)
    parser.add_argument(nargs=1, dest='rep_uri', type=str)
    args = parser.parse_args()
    args.pub_uri = args.pub_uri[0]
    args.rep_uri = args.rep_uri[0]
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
    args = parse_args()
    m = Manager(args.pub_uri, args.rep_uri)
    m.run()
