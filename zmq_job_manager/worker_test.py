from collections import OrderedDict
from uuid import uuid4
import logging

from persistent_helpers.storage import DurusStorage

from .deferred import DeferredWorkerTask
from .rpc import DeferredTransactionalZmqRpcQueue


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
        request_uuid = self.deferred_queue.queue_request(*args, **kwargs)
        self.request_callbacks[request_uuid] = self._handle_request_callback

    def _handle_request_callback(self, request_uuid, result):
        print 'Processing callback for %s' % (request_uuid, )
        print result

    def _on_result_received(self, io_loop, request_uuid, result):
        if request_uuid in self.request_callbacks:
            self.request_callbacks[request_uuid](request_uuid, result)
        else:
            print result


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Worker demo''')
    parser.add_argument(nargs=1, dest='supervisor_uri', type=str)
    parser.add_argument(nargs=1, dest='rpc_uri', type=str)
    parser.add_argument(nargs='?', dest='worker_uuid', type=str,
                        default=str(uuid4()))
    args = parser.parse_args()
    args.supervisor_uri = args.supervisor_uri[0]
    args.rpc_uri = args.rpc_uri[0]
    return args


if __name__ == '__main__':
    # if someone tried to log something before basicConfig is called, Python
    # creates a default handler that
    # goes to the console and will ignore further basicConfig calls. Remove the
    # handler if there is one.
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s-%(name)s-%(levelname)s:%(message)s")
    logging.info('test log')
    args = parse_args()
    storage = DurusStorage(host='worker-test.durus.dat', port=False)
    w = TestWorkerTask(args.rpc_uri, args.supervisor_uri,
                       queue_storage=storage, uuid=args.worker_uuid)
    w.run()
