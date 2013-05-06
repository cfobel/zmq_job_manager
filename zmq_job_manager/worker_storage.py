try:
    import cPickle as pickle
except ImportError:
    import pickle

import zmq


class WorkerStorage(object):
    def __init__(self, worker_uri, ctx=None):
        self.uri = worker_uri
        if ctx is None:
            self.ctx = zmq.Context()
            self._ctx_created = True
        else:
            self.ctx = ctx
            self._ctx_created = False
        self.push = zmq.Socket(self.ctx, zmq.PUSH)
        self.push.connect(worker_uri)

    def store(self, key, value, **kwargs):
        data = map(pickle.dumps, [(key, value), kwargs])
        self.push.send_multipart(['store'] + data)

    def cleanup(self):
        self.push.close()
        del self.push
        if self._ctx_created:
            del self.ctx

    def __del__(self):
        self.cleanup()
