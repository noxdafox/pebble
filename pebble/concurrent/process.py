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


import os
import sys
import types
import signal
import multiprocessing

from itertools import count
from functools import wraps
from concurrent.futures import CancelledError, TimeoutError

from pebble.common import ProcessExpired, ProcessFuture
from pebble.common import launch_process, stop_process, SLEEP_UNIT
from pebble.common import process_execute, launch_thread, send_result


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

    """
    timeout = kwargs.get('timeout')
    name = kwargs.get('name')
    daemon = kwargs.get('daemon', True)
    mp_context = kwargs.get('context')

    # decorator without parameters
    if not kwargs and len(args) == 1 and callable(args[0]):
        return _process_wrapper(args[0], timeout, name, daemon, multiprocessing)

    # decorator with parameters
    _validate_parameters(timeout, name, daemon, mp_context)
    mp_context = mp_context if mp_context is not None else multiprocessing

    # without @pie syntax
    if len(args) == 1 and callable(args[0]):
        return _process_wrapper(args[0], timeout, name, daemon, multiprocessing)

    # with @pie syntax
    def decorating_function(function):
        return _process_wrapper(function, timeout, name, daemon, mp_context)

    return decorating_function


def _process_wrapper(function, timeout, name, daemon, mp_context):
    if isinstance(function, types.FunctionType):
        _register_function(function)

    if hasattr(mp_context, 'get_start_method'):
        start_method = mp_context.get_start_method()
    else:
        start_method = 'spawn' if os.name == 'nt' else 'fork'

    @wraps(function)
    def wrapper(*args, **kwargs):
        future = ProcessFuture()
        reader, writer = mp_context.Pipe(duplex=False)

        if isinstance(function, types.FunctionType) and start_method != 'fork':
            target = _trampoline
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


def _worker_handler(future, worker, pipe, timeout):
    """Worker lifecycle manager.

    Waits for the worker to be perform its task,
    collects result, runs the callback and cleans up the process.

    """
    result = _get_result(future, pipe, timeout)

    if worker.is_alive():
        stop_process(worker)

    if isinstance(result, BaseException):
        if isinstance(result, ProcessExpired):
            result.exitcode = worker.exitcode
        if not isinstance(result, CancelledError):
            future.set_exception(result)
    else:
        future.set_result(result)


def _function_handler(function, args, kwargs, pipe):
    """Runs the actual function in separate process and returns its result."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    reader, writer = pipe
    reader.close()

    result = process_execute(function, *args, **kwargs)

    send_result(writer, result)


def _get_result(future, pipe, timeout):
    """Waits for result and handles communication errors."""
    counter = count(step=SLEEP_UNIT)

    try:
        while not pipe.poll(SLEEP_UNIT):
            if timeout is not None and next(counter) >= timeout:
                return TimeoutError('Task Timeout', timeout)
            elif future.cancelled():
                return CancelledError()

        return pipe.recv()
    except (EOFError, OSError):
        return ProcessExpired('Abnormal termination')
    except Exception as error:
        return error


def _validate_parameters(timeout, name, daemon, mp_context):
    if timeout is not None and not isinstance(timeout, (int, float)):
        raise TypeError('Timeout expected to be None or integer or float')
    if name is not None and not isinstance(name, str):
        raise TypeError('Name expected to be None or string')
    if daemon is not None and not isinstance(daemon, bool):
        raise TypeError('Daemon expected to be None or bool')


################################################################################
# Spawn process start method handling logic
################################################################################

_registered_functions = {}


def _register_function(function):
    _registered_functions[function.__qualname__] = function

    return function


def _trampoline(name, module, *args, **kwargs):
    """Trampoline function for decorators.

    Lookups the function between the registered ones;
    if not found, forces its registering and then executes it.

    """
    function = _function_lookup(name, module)

    return function(*args, **kwargs)


def _function_lookup(name, module):
    """Searches the function between the registered ones.
    If not found, it imports the module forcing its registration.

    """
    try:
        return _registered_functions[name]
    except KeyError:  # force function registering
        __import__(module)
        mod = sys.modules[module]
        function = getattr(mod, name)

        try:
            return _registered_functions[name]
        except KeyError:  # decorator without @pie syntax
            return _register_function(function)
