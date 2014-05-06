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


_registered_functions = {}


# -------------------- Deal with decoration and pickling -------------------- #
def trampoline(identifier, *args, **kwargs):
    """Trampoline function for decorators."""
    function = _registered_functions[identifier]

    return function(*args, **kwargs)


def dump_function(function, args):
    global _registered_functions

    identifier = id(function)
    if identifier not in _registered_functions:
        _registered_functions[identifier] = function
    args = [identifier] + list(args)

    return trampoline, args
