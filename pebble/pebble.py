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

"""Container for generic objects."""


from uuid import uuid4
from threading import Condition, Lock


# --------------------------------------------------------------------------- #
#                                 Exceptions                                  #
# --------------------------------------------------------------------------- #
class PebbleError(Exception):
    """Pebble base exception."""
    pass


class TaskCancelled(PebbleError):
    """Raised if get is called on a cancelled task."""
    pass


class TimeoutError(PebbleError):
    """Raised when a timeout expires."""
    def __init__(self, msg, value=0):
        self.msg = msg
        self.value = value


# --------------------------------------------------------------------------- #
#                               Common Objects                                #
# --------------------------------------------------------------------------- #
class Task(object):
    """Handler to the ongoing task."""
    def __init__(self, task_nr, function=None, args=None, kwargs=None,
                 callback=None, timeout=0, identifier=None):
        self.id = identifier is not None and identifier or uuid4()
        self.timeout = timeout
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._number = task_nr
        self._ready = False
        self._cancelled = False
        self._results = None
        self._task_ready = Condition(Lock())
        self._timestamp = 0
        self._callback = callback

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
    def success(self):
        return (self._ready and not
                isinstance(self._results, BaseException) or False)

    def wait(self, timeout=None):
        """Waits until results are ready.

        If *timeout* is set the call will block until the timeout expires.

        Returns *True* if results are ready, *False* if timeout expired.

        """
        with self._task_ready:
            if not self._ready:
                self._task_ready.wait(timeout)
            return self._ready

    def get(self, timeout=None, cancel=False):
        """Retrieves the produced results, blocks until results are ready.

        If the executed code raised an error it will be re-raised.

        If *timeout* is set the call will block until the timeout expires
        raising *TimeoutError" if results are not yet available.
        If *cancel* is True while *timeout* is set *Task* will be cancelled
        once the timeout expires.

        """
        with self._task_ready:
            if not self._ready:  # block if not ready
                self._task_ready.wait(timeout)
            if self._ready:  # return results
                if (isinstance(self._results, BaseException)):
                    raise self._results
                else:
                    return self._results
            else:  # get timeout
                if cancel:
                    self._cancel()
                raise TimeoutError("Task is still running")

    def cancel(self):
        """Cancels the Task."""
        with self._task_ready:
            self._cancel()

    def _cancel(self):
        """Cancels the Task."""
        self._results = TaskCancelled("Task cancelled")
        self._ready = self._cancelled = True
        self._task_ready.notify_all()

    def _set(self, results):
        """Sets the results within the task."""
        with self._task_ready:
            if not self._ready:
                self._ready = True
                self._results = results
                self._task_ready.notify_all()
