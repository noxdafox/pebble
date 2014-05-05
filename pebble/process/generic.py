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

import sys

try:  # Python 2
    from cPickle import PicklingError, loads, dumps
except:  # Python 3
    from pickle import PicklingError, loads, dumps


# -------------------- Deal with decoration and pickling -------------------- #
def trampoline(state, *args, **kwargs):
    """Trampoline function for decorators."""
    if state['type'] == 'function':
        func = load_function(state)
    elif state['type'] == 'method':
        func = load_method(state)
    return func(*args, **kwargs)


def dump_function(function):
    """Dumps the decorated function for pickling."""
    try:
        name = function.__name__
        module = function.__module__
        __import__(module)
        mod = sys.modules[module]
        getattr(mod, name)
        return {'type': 'function', 'name': name, 'module': module}
    except (ImportError, KeyError, AttributeError):
        raise PicklingError(
            "Can't pickle %r: it's not found as %s.%s" %
            (function, module, name))


def dump_method(method, instance):
    """Dumps the decorated method for pickling."""
    name = method.__name__
    return {'type': 'method', 'name': name, 'object': dumps(instance)}


def load_function(state):
    """Loads the function and extracts it from its decorator."""
    name = state.get('name')
    module = state.get('module')
    __import__(module)
    mod = sys.modules[module]
    decorated = getattr(mod, name)
    return decorated._function


def load_method(state):
    """Loads the method and extracts it from its decorator."""
    name = state.get('name')
    instance = state.get('object')
    obj = loads(instance)
    decorated = getattr(obj, name)
    return decorated._function
