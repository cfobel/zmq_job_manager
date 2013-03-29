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
        message = '[%s] uuid=%s\n%s' % (log_label(self), uuid, value)
        env['socks']['pub'].send_multipart([uuid, stream_name, value])
        return message

    def on__running_task_ids(self, env, uuid):
        return self.running_tasks.keys()

    def on__pending_task_ids(self, env, uuid):
        return self.pending_tasks.keys()

    def on__completed_task_ids(self, env, uuid):
        return self.completed_tasks.keys()

    def on__get_task(self, env, uuid):
        if uuid not in self.workers:
            self.workers.add(uuid)
        if self.pending_tasks:
            result = self.pending_tasks.popitem(0)
            self.running_tasks[result[0]] = result[1]
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

    def on__hello_world(self, env, uuid):
        message = '[%s] hello world %s' % (datetime.now(), uuid)
        logging.getLogger(log_label(self)).info(message)
        env['socks']['pub'].send_multipart([message])
        return message

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
