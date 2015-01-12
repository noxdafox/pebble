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

from multiprocessing import Process

from .common import decorate
from ..utils import function_handler


def spawn(*args, **kwargs):
    """Spawns a new process and runs a function within it.

    *target* is the desired function to be run
    with the given *args* and *kwargs* parameters; if *daemon* is True,
    the process will be stopped if the parent exits (default False).
    *name* is a string, if assigned will be given to the process.

    The *spawn* function works as well as a decorator.

    Returns the Process object which is running
    the *target* function or decorated one.

    .. note:
       The decorator accepts the keywords *daemon* and *name* only.
       If *target* keyword is not specified, the function will act as
       a decorator.

    """
    return function_handler(launch, decorate, *args, **kwargs)


def launch(target, name=None, daemon=False, args=(), kwargs={}):
    """Launches the target function within a process."""
    process = Process(target=target, name=name, args=args, kwargs=kwargs)
    process.daemon = daemon
    process.start()

    return process
