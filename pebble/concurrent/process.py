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
import signal
import multiprocessing

from itertools import count
from functools import wraps
from typing import Any, Callable
from concurrent.futures import CancelledError, TimeoutError

from pebble.common import ProcessExpired, ProcessFuture
from pebble.common import Result, SUCCESS, FAILURE, ERROR, SLEEP_UNIT
from pebble.common import register_function, trampoline
from pebble.common import process_execute, launch_thread, send_result
from pebble.common import decorate_function, launch_process, stop_process


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
    return decorate_function(_process_wrapper, *args, **kwargs)


def _process_wrapper(
        function: Callable,
        name: str,
        daemon: bool,
        timeout: float,
        mp_context: multiprocessing.context.BaseContext
) -> Callable:
    if isinstance(function, types.FunctionType):
        register_function(function)

    if hasattr(mp_context, 'get_start_method'):
        start_method = mp_context.get_start_method()
    else:
        start_method = 'spawn' if os.name == 'nt' else 'fork'

    @wraps(function)
    def wrapper(*args, **kwargs) -> ProcessFuture:
        future = ProcessFuture()
        reader, writer = mp_context.Pipe(duplex=False)

        if isinstance(function, types.FunctionType) and start_method != 'fork':
            target = trampoline
            args = [function.__qualname__, function.__module__] + list(args)
        else:
            target = function

        worker = launch_process(
            name, _function_handler, daemon, mp_context,
            target, args, kwargs, (reader, writer))

        writer.close()

        future.set_running_or_notify_cancel()

        launch_thread(name, _worker_handler, True, future, worker, reader, timeout)

        return future

    return wrapper


def _worker_handler(
        future: ProcessFuture,
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
        stop_process(worker)

    if result.status == SUCCESS:
        future.set_result(result.value)
    else:
        if result.status == ERROR:
            result.value.exitcode = worker.exitcode
        if not isinstance(result.value, CancelledError):
            future.set_exception(result.value)


def _function_handler(
        function: Callable,
        args: list,
        kwargs: dict,
        pipe: multiprocessing.Pipe
):
    """Runs the actual function in separate process and returns its result."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    reader, writer = pipe
    reader.close()

    result = process_execute(function, *args, **kwargs)

    send_result(writer, result)


def _get_result(
        future: ProcessFuture,
        pipe: multiprocessing.Pipe,
        timeout: float
) -> Any:
    """Waits for result and handles communication errors."""
    counter = count(step=SLEEP_UNIT)

    try:
        while not pipe.poll(SLEEP_UNIT):
            if timeout is not None and next(counter) >= timeout:
                return Result(FAILURE, TimeoutError('Task Timeout', timeout))
            if future.cancelled():
                return Result(FAILURE, CancelledError())

        return pipe.recv()
    except (EOFError, OSError):
        return Result(ERROR, ProcessExpired('Abnormal termination'))
    except Exception as error:
        return Result(ERROR, error)
