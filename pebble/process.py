# This file is part of Pebble.

# Pebble is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License
# as published by the Free Software Foundation,
# either version 3 of the License, or (at your option) any later version.

# Pebble is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with Pebble.  If not, see <http://www.gnu.org/licenses/>.


from uuid import uuid4
from traceback import format_exc
from itertools import count
from threading import Thread, current_thread
from collections import Callable
from functools import update_wrapper
from multiprocessing import Process, Pipe
try:  # Python 2
    from cPickle import PicklingError
except:  # Python 3
    from pickle import PicklingError

from .pebble import PebbleError, TimeoutError, TaskCancelled


def worker(function, writer, *args, **kwargs):
    try:
        writer.send(function(*args, **kwargs))
    except (IOError, OSError):  # pipe was closed
        return
    except Exception as error:
        error.traceback = format_exc()
        try:
            writer.send(error)
        except PicklingError:
            writer.send(SerializingError(str(error), type(error)))


def process(*args, **kwargs):
    """Turns a *function* into a Process and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.

    """
    def wrapper(function):
        return Wrapper(function, timeout, callback)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return Wrapper(args[0], None, None)
    elif not len(args) and len(kwargs):
        timeout = kwargs.get('timeout', None)
        callback = kwargs.get('callback', None)

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


class SerializingError(PebbleError):
    """Raised if unable to serialize an Exception."""
    def __init__(self, msg, value):
        super(SerializingError, self).__init__(msg, value)
        self.msg = msg
        self.value = value

    def __repr__(self):
        return "%s %s: %s\n%s" % (self.__class__, self.value, self.msg)

    def __str__(self):
        return "Unable to serialize %s. Message: %s" % (self.value, self.msg)


class Task(object):
    def __init__(self, task_nr, worker, reader, callback, timeout):
        self.id = uuid4()
        self.number = task_nr
        self._ready = False
        self._cancelled = False
        self._timeout = timeout
        self._results = None
        self._reader = reader
        self._worker = worker
        self._callback = callback
        self._worker_listener = Thread(target=self._set)
        self._worker_listener.daemon = True
        self._worker_listener.start()

    @property
    def ready(self):
        return self._ready

    @property
    def cancelled(self):
        return self._cancelled

    def get(self, timeout=None):
        """Retrieves the produced results.

        If the executed code raised an error it will be re-raised.

        """
        if self._worker_listener is not current_thread():
            self._worker_listener.join(timeout)
            if self._worker_listener.is_alive():
                raise TimeoutError("Task is still running")
        if (isinstance(self._results, BaseException)):
            raise self._results
        else:
            return self._results

    def cancel(self):
        """Cancels the Task terminating the running process
        and dropping the results."""
        self._cancelled = True
        self._worker.terminate()

    def _set(self):
        try:
            if self._reader.poll(self._timeout):
                if not self._cancelled:
                    self._results = self._reader.recv()
                    if self._callback is not None:
                        self._callback(self)
                else:
                    self._results = TaskCancelled("Task has been cancelled")
            elif self._worker.is_alive():
                self._worker.terminate()
                self._results = TimeoutError("Task timeout expired")
        except (IOError, OSError) as error:  # pipe was closed
            if not self._cancelled:
                self._results = error
        finally:
            self._ready = True
            self._worker.join()


class Wrapper(object):
    def __init__(self, function, timeout, callback):
        self._function = function
        self._counter = count()
        self.timeout = timeout
        self.callback = callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        reader, writer = Pipe(duplex=False)
        args = list(args)
        args.insert(0, self._function)
        args.insert(1, writer)
        p = Process(target=worker, args=(args), kwargs=(kwargs))
        p.daemon = True
        p.start()
        writer.close()
        return Task(next(self._counter), p, reader,
                    self.callback, self.timeout)
