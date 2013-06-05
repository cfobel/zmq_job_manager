from datetime import datetime
from collections import OrderedDict
from uuid import uuid4

from zmq_helpers.rpc import ZmqRpcProxy
from persistent_helpers.durus_types import PersistentOrderedDict
import eventlet


class TimeoutError(Exception):
    pass


class DeferredZmqRpcQueue(object):
    '''
    This class maintains a queue for managing a set of asynchronous requests.

    For example, let's start ZmqRpcDemoTask instance running at a random tcp
    URI (i.e., `rpc_uri`) in a separate process, providing a control pipe to
    gracefully close the process when we're finished:

    >>> import eventlet
    >>> from zmq_job_manager.deferred import DeferredZmqRpcQueue
    >>> from zmq_helpers.rpc import ZmqRpcProxy, ZmqRpcDemoTask
    >>> from multiprocessing import Process, Pipe
    >>> from zmq_helpers.utils import get_random_tcp_uri
    >>> rpc_uri = get_random_tcp_uri('*')
    >>> open('rpc_uri.log', 'wb').write(rpc_uri)
    >>> parent, child = Pipe()
    >>> t = ZmqRpcDemoTask(rpc_uri, control_pipe=child)
    >>> p = Process(target=t.run)
    >>> p.start()

    Here we create a new deferred RPC queue and connect it to the RPC task
    process.

    >>> q = DeferredZmqRpcQueue(rpc_uri.replace('*', 'localhost'))

    Now, let's queue up two requests:

    >>> q.queue_request('available_handlers')
    '...'
    >>> q.queue_request('available_handlers')
    '...'
    >>> q.queue_length
    2
    >>> q.queue.items()
    [('...', ('available_handlers', (), {})), ('...', ('available_handlers', (), {}))]

    Note that no request has actually been sent at this point.  The requests
    have simply been added to the queue, for later processing.  To begin
    processing the least recently queued task, we use the
    `process_queue_item()` method, which submits the request to the RPC task
    process and returns immediately without waiting for a response.  As shown
    below, the method returns `True` since there is at least one item in the
    queue to process.

    >>> q.process_queue_item()
    True

    At this point, we can query to see if a result is ready for the request
    currently being processed:

    >>> q.ready()
    False

    Here we see that the result is not yet ready.  Note that the deferred queue
    uses eventlets to provide non-blocking request processing, so we must make
    calls to `eventlet.sleep()` to allow the deferred request eventlet to have
    a chance to update the request's state.  The following line queries
    continuously until a result is ready:

    >>> print 'got here'
    got here
    >>> while not q.ready(): eventlet.sleep(0.01)

    When a result is ready, the `ready()` method returns the UUID of the
    request that has completed, at which point the `wait()` method will:

        1) Return the result of the request (i.e., the return value of the
           deferred request's command).
        2) Remove the completed request from the queue.

    >>> q.ready()
    '...'
    >>> q.wait()
    ['available_handlers']
    >>> q.queue_length
    1
    >>> q.queue.items()
    [('...', ('available_handlers', (), {}))]

    Since calling `process_queue_item()` and making repeated calls to `ready()`
    results in verbose code, the `next_result()` provides a shortcut to perform
    the same operations in one call.  For example:

    >>> q.next_result()
    ['available_handlers']
    >>> q.queue_length
    0
    >>> q.queue.items()
    []

    As we can see, now that the two queued requests have completed, the queue
    is empty.

    Now we send a stop signal to the demo process and join the process.

    >>> parent.send('stop')
    >>> p.join()
    '''
    def __init__(self, rpc_uri, queue_storage=None, uuid=None):
        self.uris = OrderedDict(rpc=rpc_uri)

        if uuid is None:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid
        self.proxy = ZmqRpcProxy(self.uris['rpc'], uuid=self.uuid)
        if queue_storage is None:
            queue_storage = OrderedDict()
        self.request_queue = queue_storage
        self.abort()

    def queue_request(self, command, *args, **kwargs):
        '''
        Queue request to be executed once all prior pending requests have been
        processed.
        '''
        request_uuid, request = self._make_request(command, *args, **kwargs)
        self._store_request(request_uuid, request)
        return request_uuid

    def insert_request(self, command, *args, **kwargs):
        '''
        Insert request in queue before all prior pending requests.
        '''
        request_uuid, request = self._make_request(command, *args, **kwargs)
        self._insert_request(request_uuid, request)
        return request_uuid

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

        self._active_uuid, (command, args, kwargs) = self._next_request()
        f = getattr(self.proxy, command)
        self._deferred_request = f.spawn(*args, **kwargs)
        self._deferred_start = datetime.now()
        return True

    def __iter__(self):
        while self.queue_length > 0:
            yield self.next_result()

    def next_result(self, timeout_seconds=None):
        '''
        Process next item in queue and block until result is ready.  Return
        result.
        '''
        if (self._deferred_request is not None or
                self.process_queue_item()):
            start_time = datetime.now()
            while not self.ready():
                if timeout_seconds and ((datetime.now() -
                                         start_time).total_seconds() >
                                        timeout_seconds):
                    raise TimeoutError, 'No response after %s seconds' % timeout_seconds
                eventlet.sleep(0.001)
            return self.wait()
        raise ValueError, 'There are no pending requests.'

    def ready(self):
        '''
        Return the status of the current active/pending spawned request.
        '''
        if self._deferred_request and self._deferred_request.ready():
            return self._active_uuid
        return False

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
        self._clear_request(self._active_uuid)
        self.abort()
        return result

    def abort(self):
        if hasattr(self, '_deferred_request'):
            del self._deferred_request
        self._deferred_request = None
        self._deferred_start = None
        self._active_uuid = None

    ############################################################################
    # Request handling methods
    # ------------------------
    #
    # The following methods are related to creating, retrieving, and clearing
    # requests to/in/from the request queue.  By overriding these methods, it
    # is straight-forward to customize queue storage behaviour.
    #
    def _make_request(self, command, *args, **kwargs):
        '''
        Create a request, along with a corresponding UUID used for all future
        handling for the new request.
        '''
        request_uuid = str(uuid4())
        request = (command, args, kwargs)
        return request_uuid, request

    def _clear_request(self, request_uuid):
        '''
        Remove a request from the queue by the request's UUID.
        '''
        del self.request_queue[request_uuid]

    def _store_request(self, request_uuid, request):
        '''
        Store a request in the queue, referenced by the request's UUID.
        '''
        self.request_queue[request_uuid] = request

    def _insert_request(self, request_uuid, request):
        '''
        Insert a request at the beginning of the queue, referenced by the
        request's UUID.
        '''
        new_queue = OrderedDict()
        new_queue[request_uuid] = request
        for u, r in self.request_queue.iteritems():
            new_queue[u] = r
        self.request_queue = new_queue

    def _next_request(self):
        '''
        Retrieve the next request from the queue, along with the corresponding
        request UUID.
        '''
        request_uuid, request = self.request_queue.items()[0]
        return request_uuid, request
    # [END] Request handling methods
    #---------------------------------------------------------------------------

class DeferredTransactionalZmqRpcQueue(DeferredZmqRpcQueue):
    @property
    def queue_length(self):
        '''
        Return the length of the current request queue.
        '''
        return ('queue' in self.request_queue.root and
                len(self.request_queue.root['queue']))

    @property
    def queue(self):
        '''
        Return the current request queue contents.
        '''
        return self.request_queue.root['queue']

    def _clear_request(self, request_uuid):
        '''
        Remove a request from the queue by the request's UUID.
        '''
        self.request_queue.abort()
        del self.request_queue.root['queue'][request_uuid]
        self.request_queue.commit()

    def _store_request(self, request_uuid, request):
        '''
        Store a request in the queue, referenced by the request's UUID.
        '''
        self.request_queue.abort()
        queue = self.request_queue.root.setdefault('queue',
                                                   PersistentOrderedDict())
        queue[request_uuid] = request
        self.request_queue.commit()

    def _insert_request(self, request_uuid, request):
        '''
        Insert a request at the beginning of the queue, referenced by the
        request's UUID.
        '''
        self.request_queue.abort()
        queue = self.request_queue.root.setdefault('queue',
                                                   PersistentOrderedDict())
        queue[request_uuid] = request
        # Move request to beginning of queue
        queue.move_to_end(request_uuid, last=False)
        self.request_queue.commit()

    def _next_request(self):
        '''
        Retrieve the next request from the queue, along with the corresponding
        request UUID.
        '''
        request_uuid, request = self.request_queue.root['queue'].items()[0]
        return request_uuid, request

    def pack(self):
        self.request_queue.pack()
