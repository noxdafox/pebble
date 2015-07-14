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

# Common utility functions

from time import sleep
from inspect import isclass
from itertools import count
from traceback import format_exc, print_exc

try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue

from .exceptions import PoolError, TimeoutError


# Pool states
STOPPED = 0
RUNNING = 1
CLOSED = 2
CREATED = 3
EXPIRED = 4
ERROR = 5


def function_handler(launcher, decorator, *args, **kwargs):
    """Distinguishes between function and decorator usage of spawn and
    concurrent functions.

    """
    if isfunction(args, kwargs):
        return launcher(kwargs.pop('target', None), **kwargs)
    elif issimpledecorator(args, kwargs):
        return decorator(args[0], launcher)
    elif isparametrizeddecorator(args, kwargs):
        def wrap(function):
            return decorator(function, launcher, **kwargs)

        return wrap
    else:
        raise ValueError("Only keyword arguments are accepted.")


def isfunction(args, kwargs):
    """spawn or concurrent used as regular function."""
    if not args and kwargs and 'target' in kwargs:
        return True
    else:
        return False


def issimpledecorator(args, kwargs):
    """spawn or concurrent used as decorator with no parameters."""
    if args and not kwargs:
        return True
    else:
        return False


def isparametrizeddecorator(args, kwargs):
    """spawn or concurrent used as decorator with parameters."""
    if not args and kwargs:
        return True
    else:
        return False


def execute(function, args=(), kwargs={}):
    """Runs the given function returning its results or exception."""
    try:
        return function(*args, **kwargs)
    except Exception as error:
        error.traceback = format_exc()
        return error
