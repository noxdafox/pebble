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


class PebbleError(Exception):
    """Pebble base exception."""
    pass


class TimeoutError(PebbleError):
    """Raised when Task.get() timeout expires."""
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return "%s %s" % (self.__class__, self.msg)

    def __str__(self):
        return str(self.msg)


class TaskCancelled(PebbleError):
    """Raised if get is called on a cancelled task."""
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return "%s %s" % (self.__class__, self.msg)

    def __str__(self):
        return str(self.msg)
