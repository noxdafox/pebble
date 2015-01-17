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


def decorate(function, launcher, **properties):
    """Decorates the given function.

    *function* represent the target function to be decorated,
    *launcher* takes care of executing the function with the
    given decoration *properties*.

    """
    @wraps(function)
    def wrapper(*args, **kwargs):
        return launcher(function, args=args, kwargs=kwargs, **properties)

    return wrapper
