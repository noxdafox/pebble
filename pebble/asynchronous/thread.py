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

import asyncio

from typing import Callable
from functools import wraps

from pebble.common import decorate_function, execute, launch_thread, SUCCESS


def thread(*args, **kwargs) -> Callable:
    """Runs the decorated function within a concurrent thread,
    taking care of the result and error management.

    Decorated functions will return an asyncio.Future object
    once called.

    The name parameter will set the thread name.

    The daemon parameter controls the underlying thread daemon flag.
    Default is True.

    """
    return decorate_function(_thread_wrapper, *args, **kwargs)


def _thread_wrapper(function: Callable, name: str, daemon: bool, *_) -> Callable:
    @wraps(function)
    def wrapper(*args, **kwargs) -> asyncio.Future:
        loop = _get_asyncio_loop()
        future = loop.create_future()

        launch_thread(name, _function_handler, daemon, function, args, kwargs, future)

        return future

    return wrapper


def _function_handler(
        function: Callable,
        args: list,
        kwargs: dict,
        future: asyncio.Future
):
    """Runs the actual function in separate thread and returns its result."""
    loop = future.get_loop()

    result = execute(function, *args, **kwargs)

    if result.status == SUCCESS:
        loop.call_soon_threadsafe(future.set_result, result.value)
    else:
        loop.call_soon_threadsafe(future.set_exception, result.value)


def _get_asyncio_loop() -> asyncio.BaseEventLoop:
    """Backwards compatible loop getter."""
    try:
        return asyncio.get_running_loop()
    except AttributeError:
        return asyncio.get_event_loop()
