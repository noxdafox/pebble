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
from traceback import format_exc
from concurrent.futures import Future

from pebble.common import launch_thread


def thread(function):
    """Runs the decorated function within a concurrent thread,
    taking care of the result and error management.

    Decorated functions will return a concurrent.futures.Future object
    once called.

    """
    @wraps(function)
    def wrapper(*args, **kwargs):
        future = Future()

        launch_thread(_function_handler, function, args, kwargs, future)

        return future

    return wrapper


def _function_handler(function, args, kwargs, future):
    """Runs the actual function in separate thread and returns its result."""
    future.set_running_or_notify_cancel()

    try:
        result = function(*args, **kwargs)
    except BaseException as error:
        error.traceback = format_exc()
        future.set_exception(error)
    else:
        future.set_result(result)
