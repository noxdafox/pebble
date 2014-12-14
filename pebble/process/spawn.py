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

from functools import wraps
from multiprocessing import Process

from .generic import dump_function, register_function


def spawn(*args, **kwargs):
    """Spawns a new process and runs a function within it.

    The *spawn* function works as well as a decorator.

    *target* is the desired function to be run
    with the given *args* and *kwargs* parameters; if *daemon* is True,
    the process will be stopped if the parent exits (default False).
    *name* is a string, if assigned will be given to the process.

    The *spawn* function returns the Process object which is running
    the *target* or decorated one.

    .. note:
       The decorator accepts the keywords *daemon* and *name* only.
       If *target* keyword is not specified, the function will act as
       a decorator.

    """
    if args and not kwargs:  # decorator, no parameters
        return decorate(args[0])
    elif kwargs and not args:  # function or decorator with parameters
        name = kwargs.pop('name', None)
        daemon = kwargs.pop('daemon', False)
        target = kwargs.pop('target', None)
        args = kwargs.pop('args', [])
        kwargs = kwargs.pop('kwargs', {})

        if target is not None:
            return launch(target, name, daemon, args, kwargs)
        else:
            def wrap(function):
                return decorate(function, name, daemon)

            return wrap
    else:
        raise ValueError("Only keyword arguments are accepted.")


def decorate(function, name=None, daemon=False):
    """Decorates the given function
    taking care of Windows process decoration issues.

    """
    if os.name == 'nt':
        register_function(function)

    @wraps(function)
    def wrapper(*args, **kwargs):
        if os.name == 'nt':
            target, args = dump_function(function, args)
        else:
            target = function

        return launch(target, name, daemon, args, kwargs)

    return wrapper


def launch(target, name, daemon, args, kwargs):
    """Launches the target function within a process."""
    process = Process(target=target, name=name, args=args, kwargs=kwargs)
    process.daemon = daemon
    process.start()

    return process
