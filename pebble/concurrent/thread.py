# This file is part of Pebble.
# Copyright (c) 2013-2022, Matteo Cafasso

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


def thread(*args, **kwargs):
    """Runs the decorated function within a concurrent thread,
    taking care of the result and error management.

    Decorated functions will return a concurrent.futures.Future object
    once called.

    The name parameter will set the thread name.

    The daemon parameter controls the underlying thread daemon flag.
    Default is True.

    """
    name = kwargs.get('name')
    daemon = kwargs.get('daemon', True)

    # decorator without parameters
    if not kwargs and len(args) == 1 and callable(args[0]):
        return _thread_wrapper(args[0], name, daemon)

    # decorator with parameters
    _validate_parameters(name, daemon)

    # without @pie syntax
    if len(args) == 1 and callable(args[0]):
        return _thread_wrapper(args[0], name, daemon)

    # with @pie syntax
    def decorating_function(function):
        return _thread_wrapper(function, name, daemon)

    return decorating_function


def _thread_wrapper(function, name, daemon):
    @wraps(function)
    def wrapper(*args, **kwargs):
        future = Future()

        launch_thread(name, _function_handler, daemon, function, args, kwargs, future)

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


def _validate_parameters(name, daemon):
    if name is not None and not isinstance(name, str):
        raise TypeError('Name expected to be None or string')
    if daemon is not None and not isinstance(daemon, bool):
        raise TypeError('Daemon expected to be None or bool')
