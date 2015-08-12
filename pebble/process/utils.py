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

import os
import sys

from select import select
from functools import wraps
from traceback import format_exc
try:  # Python 2
    from cPickle import PicklingError
except:  # Python 3
    from pickle import PicklingError
if os.name in ('posix', 'os2'):
    from signal import SIGKILL

from pebble.exceptions import TimeoutError, ProcessExpired


_registered_functions = {}


def stop(worker):
    """Does its best to stop the worker."""
    worker.terminate()
    worker.join(3)

    if worker.is_alive() and os.name != 'nt':
        try:
            os.kill(worker.pid, SIGKILL)
            worker.join()
        except OSError:
            return

    if worker.is_alive():
        raise RuntimeError("Unable to terminate PID %d" % os.getpid())


def get_results(pipe, timeout):
    """Waits for results and handles communication errors."""
    try:
        if poll(pipe, timeout):
            return pipe.recv()
        else:
            return TimeoutError('Task Timeout', timeout)
    except (EnvironmentError, EOFError):
        return ProcessExpired('Abnormal termination')
    except Exception as error:
        return error


def poll(pipe, timeout):
    """Python's Pipe.poll blocks undefinitely if data is too big."""
    if os.name != 'nt':
        return select([pipe], [], [], timeout)[0] and True or False
    else:
        return pipe.poll(timeout)


def send_results(pipe, data):
    """Send results and handles communication errors."""
    try:
        pipe.send(data)
    except PicklingError as error:
        error.traceback = format_exc()
        pipe.send(error)


def decorate(function, launcher, **properties):
    """Decorates the given function
    taking care of Windows process decoration issues.

    *function* represent the target function to be decorated,
    *launcher* takes care of executing the function with the
    given decoration *properties*.

    """
    if os.name == 'nt':
        register_function(function)

    @wraps(function)
    def wrapper(*args, **kwargs):
        if os.name == 'nt':
            target, args = dump_function(function, args)
        else:
            target = function

        return launcher(target, args=args, kwargs=kwargs, **properties)

    return wrapper


def register_function(function):
    global _registered_functions

    _registered_functions[function.__name__] = function


def dump_function(function, args):
    """Dumps a decorated function."""
    args = [function.__name__, function.__module__] + list(args)

    return trampoline, args


def trampoline(name, module, *args, **kwargs):
    """Trampoline function for decorators.

    Lookups the function between the registered ones;
    if not found, forces its registering and then executes it.

    """
    function = function_lookup(name, module)
    return function(*args, **kwargs)


def function_lookup(name, module):
    """Searches the function between the registered ones.
    If not found, it imports the module forcing its registration.

    """
    try:
        return _registered_functions[name]
    except KeyError:  # force function registering
        __import__(module)
        mod = sys.modules[module]
        getattr(mod, name)

        return _registered_functions[name]
