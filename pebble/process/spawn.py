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

from .common import dump_function, register_function


def spawn(*args, **kwargs):
    """Spawns a new process and runs a function within it.

    *target* is the desired function to be run
    with the given *args* and *kwargs* parameters; if *daemon* is True,
    the process will be stopped if the parent exits (default False).
    *name* is a string, if assigned will be given to the process.

    The *spawn* function works as well as a decorator.

    Returns the Process object which is running
    the *target* function or decorated one.

    .. note:
       The decorator accepts the keywords *daemon* and *name* only.
       If *target* keyword is not specified, the function will act as
       a decorator.

    """
    if args and not kwargs:  # decorator, no parameters
        return decorate(args[0])
    elif kwargs and not args:  # function or decorator with parameters
        if 'target' in kwargs:
            process = ProcessWorker(kwargs.pop('target', None), **kwargs)
            process.start()

            return process
        else:
            def wrap(function):
                name = kwargs.get('name')
                daemon = kwargs.get('daemon')

                return decorate(function, name=name, daemon=daemon)

            return wrap
    else:
        raise ValueError("Only keyword arguments are accepted.")


def decorate(function, name=None, daemon=None):
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

        process = ProcessWorker(target, name=name, daemon=daemon,
                                args=args, kwargs=kwargs)
        process.start()
        return process

    return wrapper


class ProcessWorker(Process):
    def __init__(self, target, name=None, daemon=False,
                 args=None, kwargs=None):
        super(ProcessWorker, self).__init__(target=target, name=name,
                                            args=args, kwargs=kwargs)
        self.daemon = daemon
