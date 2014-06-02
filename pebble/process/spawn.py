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


def launch(function, name, daemon, args, kwargs):
    """Launches the function within a process."""
    process = Process(target=function, name=name, args=args, kwargs=kwargs)
    process.daemon = daemon
    process.start()

    return process


def wrapped(function, name, daemon, args, kwargs):
    """Starts decorated function within a process."""
    if os.name == 'nt':
        function, args = dump_function(function, args)

    return launch(function, name, daemon, args, kwargs)


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
    name = None
    daemon = False

    if len(args) > 0 and len(kwargs) == 0:  # @concurrent
        function = args[0]
        if os.name == 'nt':
            register_function(function)

        @wraps(function)
        def wrapper(*args, **kwargs):
            return wrapped(function, name, daemon, args, kwargs)

        return wrapper
    elif len(kwargs) > 0 and len(args) == 0:  # concurrent() or @concurrent()
        name = kwargs.pop('name', None)
        daemon = kwargs.pop('daemon', False)
        target = kwargs.pop('target', None)
        args = kwargs.pop('args', [])
        kwargs = kwargs.pop('kwargs', {})

        if target is not None:
            return launch(target, name, daemon, args, kwargs)

        def wrap(function):
            if os.name == 'nt':
                register_function(function)

            @wraps(function)
            def wrapper(*args, **kwargs):
                return wrapped(function, name, daemon, args, kwargs)

            return wrapper

        return wrap
    else:
        raise ValueError("Decorator accepts only keyword arguments.")
