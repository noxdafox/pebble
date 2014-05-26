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


from functools import wraps
from threading import Thread
from types import FunctionType, MethodType


def wrapped(function, name, daemon, *args, **kwargs):
    """Starts decorated function within a thread."""
    thread = Thread(target=function, name=name, args=args, kwargs=kwargs)
    thread.daemon = daemon
    thread.start()

    return thread


def concurrent(*args, **kwargs):
    """Runs the decorated *function* in a separate thread.

    Returns the *Thread* object.

    """
    name = None
    daemon = False

    # decorator without parameters
    if len(args) > 0 and isinstance(args[0], (FunctionType, MethodType)):
        function = args[0]

        @wraps(function)
        def wrapper(*args, **kwargs):
            return wrapped(function, name, daemon, *args, **kwargs)

        return wrapper

    # decorator with parameters
    elif len(kwargs) > 0:
        name = kwargs.get('name', None)
        daemon = kwargs.get('daemon', False)

        def wrap(function):

            @wraps(function)
            def wrapper(*args, **kwargs):
                return wrapped(function, name, daemon, *args, **kwargs)

            return wrapper

        return wrap

    else:
        raise ValueError("Decorator accepts only keyword arguments.")
