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


def function_handler(launcher, decorator, *args, **kwargs):
    """"""
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
    if not args and kwargs and 'target' in kwargs:
        return True
    else:
        return False


def issimpledecorator(args, kwargs):
    if args and not kwargs:
        return True
    else:
        return False


def isparametrizeddecorator(args, kwargs):
    if not args and kwargs:
        return True
    else:
        return False
