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


import sys

from typing import Any, Callable


################################################################################
# Spawn process start method handling logic.                                   #
#                                                                              #
# Processes created via Spawn will load the modules anew. As a consequence,    #
# @concurrent/@asynchronous decorated functions will be decorated again        #
# making the child process unable to execute them.                             #
################################################################################

_registered_functions = {}


def register_function(function: Callable) -> Callable:
    """Registers the function to be used within the trampoline."""
    _registered_functions[function.__qualname__] = function

    return function


def trampoline(name: str, module: Any, *args, **kwargs) -> Any:
    """Trampoline function for decorators.

    Lookups the function between the registered ones;
    if not found, forces its registering and then executes it.

    """
    function = _function_lookup(name, module)

    return function(*args, **kwargs)


def _function_lookup(name: str, module: Any) -> Callable:
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
