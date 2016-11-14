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

from functools import wraps
from threading import Thread
from traceback import format_exc
from concurrent.futures import Future


def thread(function):
    """Runs the decorated function in a concurrent thread,
    taking care of the result and error management.

    Decorated functions will return a concurrent.futures.Future object
    once called.

    It is possible to attach callables to the returned future
    via the add_done_callback function.

    """
    callbacks = []

    @wraps(function)
    def wrapper(*args, **kwargs):
        future = Future()
        for callback in callbacks:
            future.add_done_callback(callback)

        worker = Thread(target=function_handler,
                        args=(function, args, kwargs, future))
        worker.daemon = True
        worker.start()

        future.set_running_or_notify_cancel()

        return future

    def add_done_callback(value):
        if not callable(value):
            raise TypeError("Callback must be callable")

        callbacks.append(value)

    return wrapper


def function_handler(function, args, kwargs, future):
    """Runs the actual function in separate thread and returns its result."""
    try:
        result = function(*args, **kwargs)
    except BaseException as error:
        error.traceback = format_exc()
        return error

    if isinstance(result, BaseException):
        future.set_exception(result)
    else:
        future.set_result(result)
