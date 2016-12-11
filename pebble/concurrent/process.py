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

import os
import sys
import signal

from functools import wraps
from multiprocessing import Pipe
from concurrent.futures import Future, TimeoutError
try:
    from multiprocessing import get_start_method
except ImportError:
    def get_start_method():
        return 'spawn' if os.name == 'nt' else 'fork'

from pebble.common import execute, launch_thread, send_result
from pebble.common import ProcessExpired, launch_process, stop_process


def process(*args, **kwargs):
    """Runs the decorated function in a concurrent process,
    taking care of the result and error management.

    Decorated functions will return a concurrent.futures.Future object
    once called.

    The timeout parameter will set a maximum execution time
    for the decorated function. If the execution exceeds the timeout,
    the process will be stopped and the Future will raise TimeoutError.

    """
    timeout = kwargs.get('timeout')

    # decorator without parameters
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return _process_wrapper(args[0], timeout)
    else:
        # decorator with parameters
        if timeout is not None and not isinstance(timeout, (int, float)):
            raise TypeError('Timeout expected to be None or integer or float')

        def decorating_function(function):
            return _process_wrapper(function, timeout)

        return decorating_function


def _process_wrapper(function, timeout):
    _register_function(function)

    @wraps(function)
    def wrapper(*args, **kwargs):
        future = Future()
        reader, writer = Pipe(duplex=False)

        if get_start_method() != 'fork':
            target = _trampoline
            args = [function.__name__, function.__module__] + list(args)
        else:
            target = function

        worker = launch_process(
            _function_handler, target, args, kwargs, writer)

        writer.close()

        future.set_running_or_notify_cancel()

        launch_thread(_worker_handler, future, worker, reader, timeout)

        return future

    return wrapper


def _worker_handler(future, worker, pipe, timeout):
    """Worker lifecycle manager.

    Waits for the worker to be perform its task,
    collects result, runs the callback and cleans up the process.

    """
    result = _get_result(pipe, timeout)

    if isinstance(result, BaseException):
        if isinstance(result, ProcessExpired):
            result.exitcode = worker.exitcode

        future.set_exception(result)
    else:
        future.set_result(result)

    if worker.is_alive():
        stop_process(worker)


def _function_handler(function, args, kwargs, pipe):
    """Runs the actual function in separate process and returns its result."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    result = execute(function, *args, **kwargs)

    send_result(pipe, result)


def _get_result(pipe, timeout):
    """Waits for result and handles communication errors."""
    try:
        if pipe.poll(timeout):
            return pipe.recv()
        else:
            return TimeoutError('Task Timeout', timeout)
    except (EOFError, OSError):
        return ProcessExpired('Abnormal termination')
    except Exception as error:
        return error


################################################################################
### Spawn process start method handling logic
################################################################################
_registered_functions = {}


def _register_function(function):
    global _registered_functions

    _registered_functions[function.__name__] = function


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
        getattr(mod, name)

        return _registered_functions[name]
