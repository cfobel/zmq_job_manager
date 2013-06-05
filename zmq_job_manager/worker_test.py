from collections import OrderedDict
from uuid import uuid4
import logging

from path import path
from persistent_helpers.storage import DurusStorage
from zmq_helpers.utils import log_label

from .deferred import DeferredWorkerTask
from .rpc import DeferredTransactionalZmqRpcQueue
from . import configure_logger


class TestWorkerTask(DeferredWorkerTask):
    '''
    Test class to demonstrate callback functionality.
    '''
    queue_class = DeferredTransactionalZmqRpcQueue

    def __init__(self, rpc_uri, supervisor_uri, queue_storage=None, uuid=None):
        super(TestWorkerTask, self).__init__(rpc_uri, supervisor_uri,
                                             queue_storage, uuid)
        self.request_callbacks = OrderedDict()

    def rpc__test_callback(self, env, client_uuid, *args, **kwargs):
        kwargs['callback'] = self._handle_request_callback
        return self.queue_request(*args, **kwargs)

    def _handle_request_callback(self, request_uuid, result):
        logging.getLogger(log_label(self)).info('Processing callback for %s',
                                                request_uuid)

    def _on_result_received(self, io_loop, request_uuid, result):
        if request_uuid in self.request_callbacks:
            self.request_callbacks[request_uuid](request_uuid, result)


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Worker demo''')
    parser.add_argument('--log_level', choices=('info', 'debug', 'warning',
                                                'error', 'critical'),
                        default='warning')
    parser.add_argument(nargs=1, dest='supervisor_uri', type=str)
    parser.add_argument(nargs=1, dest='rpc_uri', type=str)
    parser.add_argument(nargs='?', dest='worker_uuid', type=str,
                        default=str(uuid4()))
    args = parser.parse_args()
    args.supervisor_uri = args.supervisor_uri[0]
    args.rpc_uri = args.rpc_uri[0]
    return args


if __name__ == '__main__':
    args = parse_args()
    configure_logger(eval('logging.%s' % args.log_level.upper()))
    storage = DurusStorage(host='%s.durus.dat' % args.worker_uuid, port=False)
    w = TestWorkerTask(args.rpc_uri, args.supervisor_uri,
                       queue_storage=storage, uuid=args.worker_uuid)
    try:
        w.run()
    except:
        raise
    finally:
        # Delete persistent queue file if the queue is empty
        queue = storage.root.get('queue', {})
        storage.connection.storage.close()
        if len(queue.keys()) <= 0:
            logging.getLogger(log_label()).info(
                'Deleting queue file(s), since queue is empty')
            storage_path = path(storage.host)
            if storage_path.isfile():
                storage_path.remove()
                logging.getLogger(log_label()).info(
                    'Deleted: %s' % storage_path)
            prepack_path = path('%s.prepack' % storage_path)
            if prepack_path.isfile():
                prepack_path.remove()
                logging.getLogger(log_label()).info(
                    'Deleted: %s' % prepack_path)
