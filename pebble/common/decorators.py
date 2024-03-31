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
import sys
import types
import signal
import multiprocessing

from itertools import count
from functools import wraps
from typing import Any, Callable
from concurrent.futures import CancelledError, TimeoutError

from pebble.common import ProcessExpired, ProcessFuture
from pebble.common import Result, SUCCESS, FAILURE, ERROR
from pebble.common import launch_process, stop_process, SLEEP_UNIT
from pebble.common import process_execute, launch_thread, send_result


def decorate_function(wrapper: Callable, *args, **kwargs) -> Callable:
    name = kwargs.get('name')
    daemon = kwargs.get('daemon', True)
    timeout = kwargs.get('timeout')
    mp_context = kwargs.get('context')

    # decorator without parameters
    if not kwargs and len(args) == 1 and callable(args[0]):
        return wrapper(args[0], timeout, name, daemon, multiprocessing)

    # decorator with parameters
    _validate_parameters(timeout, name, daemon)
    mp_context = mp_context if mp_context is not None else multiprocessing

    # without @pie syntax
    if len(args) == 1 and callable(args[0]):
        return wrapper(args[0], timeout, name, daemon, multiprocessing)

    # with @pie syntax
    def decorating_function(function: Callable) -> Callable:
        return wrapper(function, timeout, name, daemon, mp_context)

    return decorating_function


def _validate_parameters(timeout: float, name: str, daemon: bool):
    if timeout is not None and not isinstance(timeout, (int, float)):
        raise TypeError('Timeout expected to be None or integer or float')
    if name is not None and not isinstance(name, str):
        raise TypeError('Name expected to be None or string')
    if daemon is not None and not isinstance(daemon, bool):
        raise TypeError('Daemon expected to be None or bool')
