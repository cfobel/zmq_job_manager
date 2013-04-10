from datetime import datetime, timedelta
import os
import cPickle as pickle

from zmq_helpers.rpc import ZmqJsonRpcProxy
from zmq_job_manager.constants import SERIALIZE__PICKLE


def main():
    supervisor_uri = os.environ['ZMQ_JOB_MANAGER__SUPERVISOR_URI']
    worker_uuid = os.environ['ZMQ_JOB_MANAGER__WORKER_UUID']
    task_uuid = os.environ['ZMQ_JOB_MANAGER__TASK_UUID']

    z = ZmqJsonRpcProxy(supervisor_uri, uuid=worker_uuid)
    z.store('test_datetime', pickle.dumps(datetime.now()), serialization=SERIALIZE__PICKLE)
    z.store('__description__', 'This field may contain a markdown description of the task.')
    print datetime.now(), 'Test task completed', supervisor_uri, worker_uuid, task_uuid


if __name__ == '__main__':
    main()
