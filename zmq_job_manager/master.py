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
from zmq_helpers.socket_configs import DeferredSocket
from zmq_helpers.rpc import ZmqJsonRpcTask
from zmq_helpers.utils import log_label, get_public_ip

from process import DeferredPopen


class Master(ZmqJsonRpcTask):
    def __init__(self, pub_uri, rpc_uri, hostname=None, **kwargs):
        if hostname is None:
            self.hostname = get_public_ip()
        else:
            self.hostname = hostname
        self._uris = OrderedDict(pub=pub_uri, rpc=rpc_uri)
        super(Master, self).__init__(**kwargs)
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

    def on__get_uris(self, *args, **kwargs):
        return OrderedDict([(k, u.replace(r'tcp://*', r'tcp://%s' %
                                          self.hostname))
                            for k, u in self.get_uris().items()])

    def on__store(self, env, uuid, key, value):
        message = '[%s] uuid=%s, %s=%s' % (log_label(self), uuid, key, value)
        logging.getLogger(log_label(self)).info(message)
        return message

    def on__stdout(self, env, uuid, value):
        message = self._on__std_base(env, uuid, 'stdout', value)
        logging.getLogger(log_label(self)).info(message)
        return message

    def on__stderr(self, env, uuid, value):
        message = self._on__std_base(env, uuid, 'stderr', value)
        logging.getLogger(log_label(self)).info(message)
        return message

    def _on__std_base(self, env, uuid, stream_name, value):
        if uuid in self.task_by_worker:
            task_uuid = self.task_by_worker[uuid]
            message = '[%s] uuid=%s\n%s' % (log_label(self), uuid, value)
            data = [uuid, task_uuid, stream_name, value]
            env['socks']['pub'].send_multipart(data)
            return message

    def on__running_task_ids(self, env, uuid):
        return self.running_tasks.keys()

    def on__pending_task_ids(self, env, uuid):
        return self.pending_tasks.keys()

    def on__completed_task_ids(self, env, uuid):
        return self.completed_tasks.keys()

    def assign_task(self, worker_uuid, task_uuid, task):
        if worker_uuid not in self.workers:
            self.workers.add(worker_uuid)
        self.worker_by_task[task_uuid] = worker_uuid
        self.task_by_worker[worker_uuid] = task_uuid

    def on__get_worker_task_uuid(self, env, uuid, worker_uuid):
        return self.task_by_worker.get(worker_uuid)

    def on__get_task_worker_uuid(self, env, uuid, task_uuid):
        return self.worker_by_task.get(task_uuid)

    def on__get_task(self, env, uuid):
        if self.pending_tasks:
            result = self.pending_tasks.popitem(0)
            self.running_tasks[result[0]] = result[1]
            self.assign_task(uuid, *result)
        else:
            result = None
        return pickle.dumps(result)

    def on__uncomplete_task(self, env, uuid, task_uuid):
        if task_uuid in self.completed_tasks:
            t = self.completed_tasks[task_uuid]
            del self.completed_tasks[task_uuid]
            self.running_tasks[task_uuid] = t

    def on__complete_task(self, env, uuid, task_uuid):
        if task_uuid in self.running_tasks:
            t = self.running_tasks[task_uuid]
            del self.running_tasks[task_uuid]
            self.completed_tasks[task_uuid] = t

    def on__register_task(self, env, uuid, shell_command):
        task_uuid = str(uuid4())
        self.pending_tasks[task_uuid] = DeferredPopen([shell_command],
                                                      shell=True, stdout=PIPE,
                                                      stderr=PIPE)
        return task_uuid

    def on__queue_worker_request(self, env, uuid, worker_uuid, command, *args,
                                 **kwargs):
        pass


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
