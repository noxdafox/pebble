# This file is part of Pebble.
# Copyright (c) 2013-2024, Matteo Cafasso

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

from typing import Callable
from functools import wraps
from traceback import format_exc
from concurrent.futures import Future

from pebble.common import execute, launch_thread, SUCCESS


def thread(*args, **kwargs) -> Callable:
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
    def decorating_function(function: Callable) -> Callable:
        return _thread_wrapper(function, name, daemon)

    return decorating_function


def _thread_wrapper(function: Callable, name: str, daemon: bool) -> Callable:
    @wraps(function)
    def wrapper(*args, **kwargs) -> Future:
        future = Future()

        launch_thread(name, _function_handler, daemon, function, args, kwargs, future)

        return future

    return wrapper


def _function_handler(
        function: Callable,
        args: list,
        kwargs: dict,
        future: Future
):
    """Runs the actual function in separate thread and returns its result."""
    future.set_running_or_notify_cancel()

    result = execute(function, *args, **kwargs)

    if result.status == SUCCESS:
        future.set_result(result.value)
    else:
        future.set_exception(result.value)


def _validate_parameters(name: str, daemon: bool):
    if name is not None and not isinstance(name, str):
        raise TypeError('Name expected to be None or string')
    if daemon is not None and not isinstance(daemon, bool):
        raise TypeError('Daemon expected to be None or bool')
