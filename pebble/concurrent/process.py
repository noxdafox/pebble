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

import os
import types
import multiprocessing

from itertools import count
from functools import wraps
from typing import Any, Callable
from concurrent.futures import CancelledError, TimeoutError

from pebble import common


def process(*args, **kwargs) -> Callable:
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

    """
    return common.decorate_function(_process_wrapper, *args, **kwargs)


def _process_wrapper(
        function: Callable,
        name: str,
        daemon: bool,
        timeout: float,
        mp_context: multiprocessing.context.BaseContext
) -> Callable:
    if isinstance(function, types.FunctionType):
        common.register_function(function)

    if hasattr(mp_context, 'get_start_method'):
        start_method = mp_context.get_start_method()
    else:
        start_method = 'spawn' if os.name == 'nt' else 'fork'

    @wraps(function)
    def wrapper(*args, **kwargs) -> common.ProcessFuture:
        future = common.ProcessFuture()
        reader, writer = mp_context.Pipe(duplex=False)
        target, args = common.maybe_install_trampoline(function, args, start_method)

        worker = common.launch_process(
            name, common.function_handler, daemon, mp_context,
            target, args, kwargs, (reader, writer))

        writer.close()

        future.set_running_or_notify_cancel()

        common.launch_thread(
            name, _worker_handler, True, future, worker, reader, timeout)

        return future

    return wrapper


def _worker_handler(
        future: common.ProcessFuture,
        worker: multiprocessing.Process,
        pipe: multiprocessing.Pipe,
        timeout: float
):
    """Worker lifecycle manager.

    Waits for the worker to be perform its task,
    collects result, runs the callback and cleans up the process.

    """
    result = _get_result(future, pipe, timeout)

    if worker.is_alive():
        common.stop_process(worker)

    if result.status == common.ResultStatus.SUCCESS:
        future.set_result(result.value)
    else:
        if result.status == common.ResultStatus.ERROR:
            result.value.exitcode = worker.exitcode
        if not isinstance(result.value, CancelledError):
            future.set_exception(result.value)


def _get_result(
        future: common.ProcessFuture,
        pipe: multiprocessing.Pipe,
        timeout: float
) -> Any:
    """Waits for result and handles communication errors."""
    counter = count(step=common.SLEEP_UNIT)

    try:
        while not pipe.poll(common.SLEEP_UNIT):
            if timeout is not None and next(counter) >= timeout:
                error = TimeoutError('Task Timeout', timeout)
                return common.Result(common.ResultStatus.FAILURE, error)
            if future.cancelled():
                error = CancelledError()
                return common.Result(common.ResultStatus.FAILURE, error)

        return pipe.recv()
    except (EOFError, OSError):
        error = common.ProcessExpired('Abnormal termination')
        return common.Result(common.ResultStatus.ERROR, error)
    except Exception as error:
        return common.Result(common.ResultStatus.ERROR, error)
