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
import pickle
import signal
import multiprocessing

from threading import Thread
from traceback import format_exc
from typing import Any, Callable

from pebble.common.types import Result, RemoteException, SUCCESS, FAILURE, ERROR


def launch_thread(name, function, daemon, *args, **kwargs):
    thread = Thread(target=function, name=name, args=args, kwargs=kwargs)
    thread.daemon = daemon
    thread.start()

    return thread


def launch_process(name, function, daemon, mp_context, *args, **kwargs):
    process = mp_context.Process(
        target=function, name=name, args=args, kwargs=kwargs)
    process.daemon = daemon
    process.start()

    return process


def stop_process(process):
    """Does its best to stop the process."""
    process.terminate()
    process.join(3)

    if process.is_alive() and os.name != 'nt':
        try:
            os.kill(process.pid, signal.SIGKILL)
            process.join()
        except OSError:
            return

    if process.is_alive():
        raise RuntimeError("Unable to terminate PID %d" % os.getpid())


def execute(function, *args, **kwargs):
    """Runs the given function returning its results or exception."""
    try:
        return Result(SUCCESS, function(*args, **kwargs))
    except BaseException as error:
        try:
            error.traceback = format_exc()
        except AttributeError:  # Frozen exception
            pass

        return Result(FAILURE, error)


def process_execute(function, *args, **kwargs):
    """Runs the given function returning its results or exception."""
    try:
        return Result(SUCCESS, function(*args, **kwargs))
    except BaseException as error:
        return Result(FAILURE, RemoteException(error, format_exc()))


def send_result(pipe, data):
    """Send result handling pickling and communication errors."""
    try:
        pipe.send(data)
    except (pickle.PicklingError, TypeError) as error:
        pipe.send(Result(ERROR, RemoteException(error, format_exc())))


# concurrent/asyncrhonous decorators

def decorate_function(wrapper, *args, **kwargs) -> Callable:
    name = kwargs.get('name')
    daemon = kwargs.get('daemon', True)
    timeout = kwargs.get('timeout')
    mp_context = kwargs.get('context')

    # decorator without parameters
    if not kwargs and len(args) == 1 and callable(args[0]):
        return wrapper(args[0], name, daemon, timeout, multiprocessing)

    # decorator with parameters
    _validate_parameters(timeout, name, daemon)
    mp_context = mp_context if mp_context is not None else multiprocessing

    # without @pie syntax
    if len(args) == 1 and callable(args[0]):
        return wrapper(args[0], name, daemon, timeout, multiprocessing)

    # with @pie syntax
    def decorating_function(function: Callable) -> Callable:
        return wrapper(function, name, daemon, timeout, mp_context)

    return decorating_function


def _validate_parameters(timeout: float, name: str, daemon: bool):
    if timeout is not None and not isinstance(timeout, (int, float)):
        raise TypeError('Timeout expected to be None or integer or float')
    if name is not None and not isinstance(name, str):
        raise TypeError('Name expected to be None or string')
    if daemon is not None and not isinstance(daemon, bool):
        raise TypeError('Daemon expected to be None or bool')
