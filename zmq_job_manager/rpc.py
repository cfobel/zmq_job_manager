from datetime import datetime
from collections import OrderedDict
from uuid import uuid4

from zmq_helpers.rpc import ZmqRpcProxy
import eventlet


class DeferredZmqRpcQueue(object):
    def __init__(self, rpc_uri, queue_storage=None, uuid=None):
        self.uris = OrderedDict(rpc=rpc_uri)

        if uuid is None:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid
        self.proxy = ZmqRpcProxy(self.uris['rpc'], uuid=self.uuid)
        if queue_storage is None:
            queue_storage = []
        self.request_queue = queue_storage
        self.abort()

    def queue_request(self, command, *args, **kwargs):
        '''
        Queue request to be executed once all prior pending requests have been
        processed.
        '''
        self.request_queue.append((command, args, kwargs))

    @property
    def request_pending(self):
        '''
        Return whether or not there is a spawned request pending.
        '''
        return (self._deferred_request is not None)

    @property
    def queue_length(self):
        '''
        Return the length of the current request queue.
        '''
        return len(self.request_queue)

    @property
    def queue(self):
        '''
        Return the current request queue contents.
        '''
        return self.request_queue

    def process_queue_item(self):
        '''
        Process the least-recently queued request by spawning a new deferred
        request instance and assigning it to the `_deferred_request` attribute.
        If there are no queued requests, return `None`.  Otherwise, return
        `True`.

        Raise an exception if there is already an active/pending spawned
        request.
        '''
        if not self.request_queue:
            return None
        elif self._deferred_request is not None:
            raise RuntimeError, ('Previous deferred request must be completed '
                                 'before processing the next deferred request.')

        command, args, kwargs = self.request_queue[0]
        f = getattr(self.proxy, command)
        self._deferred_request = f.spawn(*args, **kwargs)
        self._deferred_start = datetime.now()
        return True

    def __iter__(self):
        while self.queue_length > 0:
            yield self.next_result()

    def next_result(self):
        '''
        Process next item in queue and block until result is ready.  Return
        result.
        '''
        if (self._deferred_request is not None or
                self.process_queue_item()):
            while not self.ready():
                eventlet.sleep(0.001)
            return self.wait()
        raise ValueError, 'There are no pending requests.'

    def ready(self):
        '''
        Return the status of the current active/pending spawned request.
        '''
        return self._deferred_request.ready()

    def wait(self):
        '''
        Return the result from the current spawned request.

        Raise an exception if the spawned request is not ready, or there is no
        pending spawned request.
        '''
        if self._deferred_request is None:
            raise RuntimeError, 'No pending deferred request.'
        elif not self._deferred_request.ready():
            raise RuntimeError, 'Deferred must be ready before calling wait'

        result = self._deferred_request.wait()
        del self.request_queue[0]
        self.abort()
        return result

    def abort(self):
        if hasattr(self, '_deferred_request'):
            del self._deferred_request
        self._deferred_request = None
        self._deferred_start = None
