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

from threading import Thread


def thread_worker(function, task, *args, **kwargs):
    try:
        results = function(*args, **kwargs)
        task._set(results)
    except Exception as error:
        task._set(error)


class ThreadTask(object):
    def __init__(self, callback=None, error_callback=None):
        self.id = uuid4()
        self._results = None
        self._worker = None  # set by Asynchronous._wrapper
        self._callback = callback
        self._error_callback = error_callback

    def get(self, block=True, timeout=None):
        self._worker.join(timeout)
        if (isinstance(self._results, Exception)):
            raise self._results
        elif self._results is not None:
            return self._results

    def _set(self, results):
        self._results = results
        if (isinstance(self._results, Exception) and
                self._error_callback is not None):
            self._error_callback(self.id, self._results)
        elif self._callback is not None:
            self._callback(self.id, self._results)


def process_worker(function, outqueue, *args, **kwargs):
    try:
        outqueue._reader.close()
        outqueue.put(function(*args, **kwargs))
    except (IOError, OSError):  # pipe was closed
        return
    except Exception as error:
        outqueue.put(error)


class ProcessTask(object):
    def __init__(self, worker, inqueue, callback=None, error_callback=None):
        self.id = uuid4()
        self._results = None
        self._inqueue = inqueue
        self._worker = worker
        self._callback = callback
        self._error_callback = error_callback
        self._worker_listener = Thread(target=self._set)
        self._worker_listener.daemon = True
        self._worker_listener.start()

    def get(self, block=True, timeout=None):
        self._worker_listener.join(timeout)
        if (isinstance(self._results, Exception)):
            raise self._results
        elif self._results is not None:
            return self._results

    def _set(self):
        try:
            self._results = self._inqueue.get()
        except (IOError, OSError) as error:  # pipe was closed
            self._results = error
        if (isinstance(self._results, Exception) and
                self._error_callback is not None):
            self._error_callback(self.id, self._results)
        elif self._callback is not None:
            self._callback(self.id, self._results)
        self._worker.join()
