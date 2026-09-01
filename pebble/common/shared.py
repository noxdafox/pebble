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
import multiprocessing

from typing import Callable
from threading import Thread
from traceback import format_exc

from pebble.common.types import Result, ResultStatus, T, P


def launch_thread(
    name: str | None,
    function: Callable[P, T],
    daemon: bool,
    *args: P.args,
    **kwargs: P.kwargs,
) -> Thread:
    thread: Thread = Thread(target=function, name=name, args=args, kwargs=kwargs)
    thread.daemon = daemon
    thread.start()

    return thread


def execute(function: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> Result:
    """Runs the given function returning its results or exception."""
    try:
        return Result(ResultStatus.SUCCESS, function(*args, **kwargs))
    except BaseException as error:
        try:
            error.traceback = format_exc()  # type: ignore[attr-defined]
        except AttributeError:  # Frozen exception
            pass

        return Result(ResultStatus.FAILURE, error)


################################################################################
# @concurrent/@asyncrhonous decorators.                                        #
################################################################################


def decorate_function(wrapper: Callable, *args, **kwargs) -> Callable:
    """Decorate the function taking care of all the possible uses."""
    name = kwargs.get("name")
    pool = kwargs.get("pool")
    daemon = kwargs.get("daemon", True)
    timeout = kwargs.get("timeout")
    mp_context = kwargs.get("context")

    # decorator without parameters: @process/process(function)
    if not kwargs and len(args) == 1 and callable(args[0]):
        return wrapper(args[0], name, daemon, timeout, multiprocessing, pool)

    # decorator with parameters
    _validate_parameters(name, daemon, timeout)
    mp_context = mp_context if mp_context is not None else multiprocessing

    ## without @pie syntax: process(function, timeout=12)
    if len(args) == 1 and callable(args[0]):
        return wrapper(args[0], name, daemon, timeout, multiprocessing, pool)

    ## with @pie syntax: @process(timeout=12)
    def decorating_function(function: Callable) -> Callable:
        return wrapper(function, name, daemon, timeout, mp_context, pool)

    return decorating_function


def _validate_parameters(name: str | None, daemon: bool, timeout: float | None):
    if name is not None and not isinstance(name, str):
        raise TypeError("Name expected to be None or string")
    if daemon is not None and not isinstance(daemon, bool):
        raise TypeError("Daemon expected to be None or bool")
    if timeout is not None and not isinstance(timeout, (int, float)):
        raise TypeError("Timeout expected to be None or integer or float")
