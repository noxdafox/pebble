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

# Pebble's generic objects

import threading

from traceback import print_exc

from .exceptions import TimeoutError, TaskCancelled


class Task(object):
    """Handler to the ongoing task."""
    def __init__(self, task_nr, callback=None, timeout=None, identifier=None,
                 metadata=None):
        self.id = identifier
        self._timeout = timeout
        self._number = task_nr
        self._ready = False
        self._cancelled = False
        self._results = None
        self._task_ready = threading.Condition(threading.Lock())
        self._timestamp = 0
        self._callback = callback
        self._metadata = metadata

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "%s (Task-%d, %s)" % (self.__class__, self.number, self.id)

    @property
    def number(self):
        return self._number

    @property
    def ready(self):
        return self._ready

    @property
    def cancelled(self):
        return self._cancelled

    @property
    def started(self):
        return self._timestamp > 0 and True or False

    @property
    def timeout(self):
        return self._timeout

    @property
    def success(self):
        return (self._ready and not
                isinstance(self._results, BaseException) or False)

    def get(self, timeout=None, cancel=False):
        """Retrieves the produced results, blocks until results are ready.

        If the executed code raised an error it will be re-raised.

        If *timeout* is set the call will block until the timeout expires
        raising *TimeoutError" if results are not yet available.
        If *cancel* is True while *timeout* is set *Task* will be cancelled
        once the timeout expires.

        """
        if self.wait(timeout=timeout):
            if (isinstance(self._results, BaseException)):
                raise self._results
            else:
                return self._results
        else:
            if cancel:
                self.cancel()
            raise TimeoutError("Task is still running")

    def wait(self, timeout=None):
        """Waits until results are ready.

        If *timeout* is set the call will block until the timeout expires.

        Returns *True* if results are ready, *False* if timeout expired.

        """
        with self._task_ready:
            if not self._ready:
                self._task_ready.wait(timeout)
            return self._ready

    def cancel(self):
        """Cancels the Task."""
        self._cancelled = True
        self.set_results(TaskCancelled("Task cancelled"))

    def set_results(self, results):
        """Sets the results within the task and run the installed callback.

        This function is meant for testing and internal use.

        """
        self._set(results)
        self._run_callback()

    def _set(self, results):
        with self._task_ready:
            if not self._ready:
                self._ready = True
                self._results = results
                self._task_ready.notify_all()

    def _run_callback(self):
        if self._callback is not None:
            try:
                self._callback(self)
            except Exception:
                print_exc()
