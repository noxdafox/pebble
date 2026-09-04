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

from __future__ import annotations

import os

from itertools import count
from functools import wraps
from typing import Callable, overload
from multiprocessing import connection
from multiprocessing.process import BaseProcess
from types import FunctionType
from concurrent.futures import CancelledError, TimeoutError

from pebble import common
from pebble.common.types import MultiprocessingContext, P, T
from pebble.pool.process import ProcessPool


@overload
def process(func: Callable[P, T]) -> Callable[P, common.ProcessFuture[T]]: ...
@overload
def process(
    name: str | None = None,
    daemon: bool = True,
    timeout: float | None = None,
    mp_context: MultiprocessingContext | None = None,
    pool: ProcessPool | None = None,
) -> Callable[[Callable[P, T]], Callable[P, common.ProcessFuture[T]]]: ...
def process(*args, **kwargs):
    """Runs the decorated function in a concurrent process,
    taking care of the result and error management.

    Decorated functions will return a concurrent.futures.Future object
    once called.

    The timeout parameter will set a maximum execution time
    for the decorated function. If the execution exceeds the timeout,
    the process will be stopped and the Future will raise TimeoutError.

    The name parameter will set the process name.

    The daemon parameter controls the underlying process daemon flag.
    Default is True.

    The context parameter allows to provide the multiprocessing.context
    object used for starting the process.

    The pool parameter accepts a pebble.ProcessPool instance to be used
    instead of running the function in a new process.

    """
    return common.decorate_function(_process_wrapper, *args, **kwargs)


def _process_wrapper(
    function: Callable[P, T],
    name: str,
    daemon: bool,
    timeout: float,
    mp_context: MultiprocessingContext,
    pool: ProcessPool | None,
) -> Callable:
    if isinstance(function, FunctionType):
        common.register_function(function)

    if hasattr(mp_context, "get_start_method"):
        start_method: str = mp_context.get_start_method()
    else:
        start_method = "spawn" if os.name == "nt" else "fork"

    if pool is not None:
        if not isinstance(pool, ProcessPool):
            raise TypeError("Pool expected to be ProcessPool")
        start_method = "pool"

    @wraps(function)  # type: ignore[arg-type] - FunctionType confuses pyright
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> common.ProcessFuture[T]:
        target, args = common.maybe_install_trampoline(function, args, start_method)

        if pool is not None:
            future: common.ProcessFuture = pool.schedule(
                target, args=args, kwargs=kwargs, timeout=timeout
            )
        else:
            future: common.ProcessFuture = common.ProcessFuture()
            reader, writer = mp_context.Pipe(duplex=False)
            worker: BaseProcess = common.launch_process(
                name,
                common.function_handler,
                daemon,
                mp_context,
                target,
                args,
                kwargs,
                writer,
            )

            writer.close()
            future.set_running_or_notify_cancel()

            common.launch_thread(
                name, _worker_handler, True, future, worker, reader, timeout
            )

        return future

    return wrapper


def _worker_handler(
    future: common.ProcessFuture,
    worker: BaseProcess,
    pipe: connection.Connection,
    timeout: float,
):
    """Worker lifecycle manager.

    Waits for the worker to be perform its task,
    collects result, runs the callback and cleans up the process.

    """
    result: common.Result = _get_result(future, pipe, timeout)

    if worker.is_alive():
        common.stop_process(worker)

    if result.status == common.ResultStatus.SUCCESS:
        future.set_result(result.value)
    else:
        if result.status == common.ResultStatus.ERROR:
            result.value.exitcode = worker.exitcode
            result.value.pid = worker.pid
        if not isinstance(result.value, CancelledError):
            future.set_exception(result.value)


def _get_result(
    future: common.ProcessFuture, pipe: connection.Connection, timeout: float
) -> common.Result:
    """Waits for result and handles communication errors."""
    error: BaseException | None = None
    counter: count = count(step=common.CONSTS.sleep_unit)

    try:
        while not pipe.poll(common.CONSTS.sleep_unit):
            if timeout is not None and next(counter) >= timeout:
                error = TimeoutError("Task Timeout", timeout)
                return common.Result(common.ResultStatus.FAILURE, error)
            if future.cancelled():
                error = CancelledError()
                return common.Result(common.ResultStatus.FAILURE, error)

        return pipe.recv()
    except (EOFError, OSError):
        error = common.ProcessExpired("Abnormal termination")
        return common.Result(common.ResultStatus.ERROR, error)
    except Exception as error:
        return common.Result(common.ResultStatus.ERROR, error)
