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

from functools import wraps
from concurrent.futures import Future
from typing import Callable, Optional, overload

from pebble import common
from pebble.pool.thread import ThreadPool

@overload
def thread(func: common.CallableType) -> common.ThreadDecoratorReturnType:
    ...
@overload
def thread(
        name: Optional[str] = None,
        daemon: bool = True,
        pool: Optional[ThreadPool] = None
) -> common.ThreadDecoratorParamsReturnType:
    ...
def thread(*args, **kwargs):
    """Runs the decorated function within a concurrent thread,
    taking care of the result and error management.

    Decorated functions will return a concurrent.futures.Future object
    once called.

    The name parameter will set the thread name.

    The daemon parameter controls the underlying thread daemon flag.
    Default is True.

    The pool parameter accepts a pebble.ThreadPool instance to be used
    instead of running the function in a new process.

    """
    return common.decorate_function(_thread_wrapper, *args, **kwargs)


def _thread_wrapper(
        function: Callable,
        name: str,
        daemon: bool,
        _timeout: float,
        _mp_context,
        pool: ThreadPool
) -> Callable:
    if pool is not None:
        if not isinstance(pool, ThreadPool):
            raise TypeError('Pool expected to be ThreadPool')

    @wraps(function)
    def wrapper(*args, **kwargs) -> Future:
        if pool is not None:
            future = pool.schedule(function, args=args, kwargs=kwargs)
        else:
            future = Future()

            common.launch_thread(
                name, _function_handler, daemon,
                function, args, kwargs, future)

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

    result = common.execute(function, *args, **kwargs)

    if result.status == common.ResultStatus.SUCCESS:
        future.set_result(result.value)
    else:
        future.set_exception(result.value)
