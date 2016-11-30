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


class PoolError(PebbleError):
    """Raised if an error occurred within the Pool."""
    pass


class TaskCancelled(PebbleError):
    """Raised if get is called on a cancelled task."""
    pass


class ChannelError(OSError):
    """Error occurring within the process channel."""
    pass


class TimeoutError(PebbleError):
    """Raised when a timeout expires."""
    def __init__(self, msg, value=0):
        super(TimeoutError, self).__init__(msg)
        self.timeout = value


class ProcessExpired(PebbleError):
    """Raised when process dies unexpectedly."""
    def __init__(self, msg, code=0):
        super(ProcessExpired, self).__init__(msg)
        self.exitcode = code
