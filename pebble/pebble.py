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

from threading import Thread
try:  # Python 2
    from cPickle import PicklingError
except:  # Python 3
    from pickle import PicklingError


def thread_worker(function, task, *args, **kwargs):
    try:
        results = function(*args, **kwargs)
        task._set(results)
    except Exception as error:
        error.traceback = format_exc()
        task._set(error)


def process_worker(function, writer, *args, **kwargs):
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


class PebbleError(Exception):
    """Pebble base exception."""
    pass


class TimeoutError(PebbleError):
    """Raised when Task.get() timeout expires."""
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return "%s %s" % (self.__class__, self.msg)

    def __str__(self):
        return str(self.msg)


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


class ThreadTask(object):
    def __init__(self, task_nr, callback, error_callback):
        self.id = uuid4()
        self.number = task_nr
        self._results = None
        self._worker = None  # set by Asynchronous._wrapper
        self._callback = callback
        self._error_callback = error_callback

    def get(self, timeout=None):
        self._worker.join(timeout)
        if not self._worker.is_alive():
            if (isinstance(self._results, Exception)):
                raise self._results
            else:
                return self._results
        else:
            raise TimeoutError("Task is still running")

    def _set(self, results):
        self._results = results
        if (isinstance(self._results, Exception) and
                self._error_callback is not None):
            self._error_callback(self.id, self._results)
        elif self._callback is not None:
            self._callback(self.id, self._results)


class ProcessTask(object):
    def __init__(self, task_nr, worker, reader,
                 callback, error_callback, timeout):
        self.id = uuid4()
        self.number = task_nr
        self._timeout = timeout
        self._results = None
        self._reader = reader
        self._worker = worker
        self._callback = callback
        self._error_callback = error_callback
        self._worker_listener = Thread(target=self._set)
        self._worker_listener.daemon = True
        self._worker_listener.start()

    def get(self, timeout=None):
        self._worker_listener.join(timeout)
        if not self._worker_listener.is_alive():
            if (isinstance(self._results, Exception)):
                raise self._results
            else:
                return self._results
        else:
            raise TimeoutError("Task is still running")

    def _set(self):
        try:
            if self._reader.poll(self._timeout):
                self._results = self._reader.recv()
                if (isinstance(self._results, Exception) and
                        self._error_callback is not None):
                    self._error_callback(self.id, self._results)
                elif self._callback is not None:
                    self._callback(self.id, self._results)
            elif self._worker.is_alive():
                self._worker.terminate()
        except (IOError, OSError) as error:  # pipe was closed
            self._results = error
        finally:
            self._worker.join()
