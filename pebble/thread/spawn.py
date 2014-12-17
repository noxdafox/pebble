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


def spawn(*args, **kwargs):
    """Spawns a new thread and runs a function within it.

    *target* is the desired function to be run
    with the given *args* and *kwargs* parameters; if *daemon* is True,
    the thread will be stopped if the parent exits (default False).
    *name* is a string, if assigned will be given to the thread.

    The *spawn* function works as well as a decorator.

    Returns the Thread object which is running
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
            thred = ThreadWorker(kwargs.pop('target', None), **kwargs)
            thred.start()

            return thred
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
    taking care of Windows thread decoration issues.

    """
    @wraps(function)
    def wrapper(*args, **kwargs):
        thred = ThreadWorker(function, name=name, daemon=daemon,
                             args=args, kwargs=kwargs)
        thred.start()

        return thred

    return wrapper


class ThreadWorker(Thread):
    def __init__(self, target, name=None, daemon=False,
                 args=None, kwargs=None):
        super(ThreadWorker, self).__init__(target=target, name=name,
                                           args=args, kwargs=kwargs)
        self.daemon = daemon
