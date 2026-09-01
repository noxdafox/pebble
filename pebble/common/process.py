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
import sys
import pickle
import signal
import multiprocessing

from traceback import format_exc
from multiprocessing import connection
from types import FunctionType, ModuleType
from typing import Any, Callable, Iterable, Dict, Mapping

from pebble.common.types import Result, ResultStatus, RemoteException, CONSTS, T, P


def launch_process(
    name: str,
    function: Callable[P, T],
    daemon: bool,
    mp_context: ModuleType,
    *args: P.args,
    **kwargs: P.kwargs,
) -> multiprocessing.Process:
    process: multiprocessing.Process = mp_context.Process(
        target=function, name=name, args=args, kwargs=kwargs
    )
    process.daemon = daemon
    process.start()

    return process


def stop_process(process: multiprocessing.Process):
    """Does its best to stop the process."""
    process.terminate()
    process.join(CONSTS.term_timeout)

    if process.is_alive() and os.name != "nt" and process.pid is not None:
        try:
            os.kill(process.pid, signal.SIGKILL)
            process.join()
        except OSError:
            return

    if process.is_alive():
        raise RuntimeError("Unable to terminate PID %d" % os.getpid())


def process_execute(
    function: Callable[P, T], *args: P.args, **kwargs: P.kwargs
) -> Result[T]:
    """Runs the given function returning its results or exception."""
    try:
        return Result(ResultStatus.SUCCESS, function(*args, **kwargs))
    except BaseException as error:
        return Result(ResultStatus.FAILURE, RemoteException(error, format_exc()))


def send_result(pipe: connection.Connection, data: Any):
    """Send result handling pickling and communication errors."""
    try:
        pipe.send(data)
    except (pickle.PicklingError, TypeError) as error:
        pipe.send(Result(ResultStatus.ERROR, RemoteException(error, format_exc())))


def function_handler(
    function: Callable[..., T],
    args: Iterable[Any],
    kwargs: Mapping[str, Any],
    writer: connection.Connection,
):
    """Runs the actual function in separate process and returns its result."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, process_exit)

    result: Result = process_execute(function, *args, **kwargs)

    send_result(writer, result)


def process_exit(exitcode: int, *_):
    """Ensure mltiprocessing cleanup is performed to avoid resources leak."""
    multiprocessing.util._exit_function()  # type: ignore[attr-defined]
    os._exit(exitcode)


################################################################################
# Spawn process start method handling logic.                                   #
#                                                                              #
# Processes created via Spawn will load the modules anew. As a consequence,    #
# @concurrent/@asynchronous decorated functions will be decorated again        #
# making the child process unable to execute them.                             #
################################################################################

_registered_functions: Dict[str, FunctionType] = {}


def register_function(function: FunctionType) -> FunctionType:
    """Registers the function to be used within the trampoline."""
    _registered_functions[function.__qualname__] = function

    return function


def maybe_install_trampoline(
    function: FunctionType, args: Iterable[Any], start_method: str
) -> tuple:
    """Install the trampoline on the right process start methods."""
    if isinstance(function, FunctionType) and start_method != "fork":
        target = _trampoline
        args = [function.__qualname__, function.__module__] + list(args)
    else:
        target = function

    return target, args


def _trampoline(name: str, module: ModuleType, *args, **kwargs) -> Any:
    """Trampoline function for decorators.

    Lookups the function between the registered ones;
    if not found, forces its registering and then executes it.

    """
    function: FunctionType = _function_lookup(name, module)

    return function(*args, **kwargs)


def _function_lookup(name: str, module: Any) -> FunctionType:
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
            return register_function(function)
