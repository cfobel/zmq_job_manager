import sys
import subprocess
import time
import threading
import Queue

import zmq


class AsynchronousFileReader(threading.Thread):
    '''
    Helper class to implement asynchronous reading of a file
    in a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''

    def __init__(self, fd, queue):
        assert isinstance(queue, Queue.Queue)
        assert callable(fd.readline)
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue

    def run(self):
        '''The body of the tread: read lines and put them on the queue.'''
        for line in iter(self._fd.readline, ''):
            self._queue.put(line)

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return not self.is_alive() and self._queue.empty()


class AsynchronousFileReaderIterator(threading.Thread):
    def __init__(self, fd, queue=None):
        if queue is None:
            queue = Queue.Queue()
        assert isinstance(queue, Queue.Queue)
        assert callable(fd.readline)
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue
        self.start()

    def run(self):
        '''The body of the tread: read lines and put them on the queue.'''
        for line in iter(self._fd.readline, ''):
            self._queue.put(line)

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return not self.is_alive() and self._queue.empty()

    def iter(self):
        # Show what we received from standard output.
        while not self._queue.empty():
            line = self._queue.get()
            yield line


class DeferredPopen(object):
    default_popen_class = subprocess.Popen

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        if 'popen_class' in kwargs:
            self._popen_class = self._kwargs.pop('popen_class')
        else:
            self._popen_class = self.default_popen_class

    def make(self, *args, **kwargs):
        _args = self._args + args
        _kwargs = self._kwargs.copy()
        _kwargs.update(kwargs)
        if 'popen_class' in _kwargs:
            popen_class = _kwargs.pop('popen_class')
        else:
            popen_class = self._popen_class
        return popen_class(*_args, **_kwargs)


class PopenPipeReactor(subprocess.Popen):
    def __init__(self, *args, **kwargs):
        super(PopenPipeReactor, self).__init__(*args, **kwargs)

        # Launch the asynchronous readers of the process' stdout and stderr.
        self.stdout_reader = AsynchronousFileReaderIterator(self.stdout)
        self.stderr_reader = AsynchronousFileReaderIterator(self.stderr)

    def on_stdout(self, stdout):
        print stdout.strip()

    def on_stderr(self, stderr):
        print >> sys.stderr, stderr.strip()

    def communicate(self):
        # Check the queues if we received some output (until there is nothing more to get).
        while not self.stdout_reader.eof() or not self.stderr_reader.eof():
            message = ''.join(list(self.stdout_reader.iter()))
            if message:
                self.on_stdout(message)

            message = ''.join(list(self.stderr_reader.iter()))
            if message:
                self.on_stderr(message)

            # Sleep a bit before asking the readers again.
            time.sleep(.1)

        # Let's be tidy and join the threads we've started.
        self.stdout_reader.join()
        self.stderr_reader.join()
        # Close subprocess' file descriptors.
        self.stdout.close()
        self.stderr.close()


class PopenZmqOutput(PopenPipeReactor):
    def communicate(self, pub_uri):
        self.ctx = zmq.Context.instance()
        self.sock = zmq.Socket(self.ctx, zmq.PUB)
        self.sock.connect(pub_uri)

        super(PopenZmqOutput, self).communicate()

    def on_stderr(self, stderr):
        self.sock.send_multipart(['stderr', stderr])

    def on_stdout(self, stdout):
        self.sock.send_multipart(['stdout', stdout])
