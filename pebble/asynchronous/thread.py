# This file is part of Pebble.
# Copyright (c) 2013-2026, Matteo Cafasso

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

from functools import wraps
from typing import Any, Callable, Mapping, Iterable, overload

from pebble import common
from pebble.common.types import P, T
from pebble.pool.thread import ThreadPool


@overload
def thread(func: Callable[P, T]) -> Callable[P, asyncio.Future[T]]: ...
@overload
def thread(
    name: str | None = None, daemon: bool = True, pool: ThreadPool | None = None
) -> Callable[[Callable[P, T]], Callable[P, asyncio.Future[T]]]: ...
def thread(*args, **kwargs):
    """Runs the decorated function within a concurrent thread,
    taking care of the result and error management.

    Decorated functions will return an asyncio.Future object
    once called.

    The name parameter will set the thread name.

    The daemon parameter controls the underlying thread daemon flag.
    Default is True.

    The pool parameter accepts a pebble.ThreadPool instance to be used
    instead of running the function in a new process.

    """
    return common.decorate_function(_thread_wrapper, *args, **kwargs)


def _thread_wrapper(
    function: Callable[P, T],
    name: str,
    daemon: bool,
    _timeout: float,
    _unused_mp_context: None,
    pool: ThreadPool | None,
) -> Callable:
    if pool is not None:
        if not isinstance(pool, ThreadPool):
            raise TypeError("Pool expected to be ThreadPool")

    @wraps(function)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> asyncio.Future[T]:
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

        if pool is not None:
            future = loop.run_in_executor(
                pool,  # type: ignore[arg-type] - Executor compatible API
                function,
                *args,  # type: ignore[arg-type] - Unknown error
            )
        else:
            future = loop.create_future()

            common.launch_thread(
                name, _function_handler, daemon, function, args, kwargs, future
            )

        return future

    return wrapper


def _function_handler(
    function: Callable[..., T],
    args: Iterable[Any],
    kwargs: Mapping[str, Any],
    future: asyncio.Future,
):
    """Runs the actual function in separate thread and returns its result."""
    loop = future.get_loop()

    result = common.execute(function, *args, **kwargs)

    if result.status == common.ResultStatus.SUCCESS:
        loop.call_soon_threadsafe(future.set_result, result.value)
    else:
        loop.call_soon_threadsafe(future.set_exception, result.value)
