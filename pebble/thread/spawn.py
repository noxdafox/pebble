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


def launch(function, name, daemon, args, kwargs):
    """Launches the function within a thread."""
    thread = Thread(target=function, name=name, args=args, kwargs=kwargs)
    thread.daemon = daemon
    thread.start()

    return thread


def spawn(*args, **kwargs):
    """Spawns a new thread and runs a function within it.

    The concurrent function works as well as a decorator.

    *target* is the desired function to be run
    with the given *args* and *kwargs* parameters; if *daemon* is True,
    the thread will be stopped if the parent exits (default False).
    *name* is a string, if assigned will be given to the thread.

    The *spawn* function returns the Thread object which is running
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

        @wraps(function)
        def wrapper(*args, **kwargs):
            return launch(function, name, daemon, args, kwargs)

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

            @wraps(function)
            def wrapper(*args, **kwargs):
                return launch(function, name, daemon, args, kwargs)

            return wrapper

        return wrap
    else:
        raise ValueError("Decorator accepts only keyword arguments.")
