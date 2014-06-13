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

from itertools import count
from functools import wraps
from traceback import print_exc, format_exc

from .spawn import spawn
from ..pebble import Task


_task_counter = count()


def launch(function, callback, identifier, args, kwargs):
    """Launches the function within a process."""
    task = Task(next(_task_counter), callback=callback,
                function=function, args=args, kwargs=kwargs,
                identifier=identifier)
    task_worker(task)

    return task


def concurrent(*args, **kwargs):
    """Runs the given function in a concurrent thread,
    taking care of the results and error management.

    The *concurrent* function works as well as a decorator.

    *target* is the desired function to be run
    with the given *args* and *kwargs* parameters.
    If a *callback* is passed, it will be run after the job has finished with
    the returned *Task* as parameter.

    The *concurrent* function returns a *Task* object.

    .. note:
       The decorator accepts the *callback* keyword only.
       If *target* keyword is not specified, the function will act as
       a decorator.

    """
    callback = None
    identifier = None

    if len(args) > 0 and len(kwargs) == 0:  # @task
        function = args[0]

        @wraps(function)
        def wrapper(*args, **kwargs):
            return launch(function, callback, identifier, args, kwargs)

        return wrapper
    elif len(kwargs) > 0 and len(args) == 0:  # task() or @task()
        callback = kwargs.pop('callback', None)
        identifier = kwargs.pop('identifier', None)
        target = kwargs.pop('target', None)
        args = kwargs.pop('args', [])
        kwargs = kwargs.pop('kwargs', {})

        if target is not None:
            return launch(target, callback, identifier, args, kwargs)

        def wrap(function):

            @wraps(function)
            def wrapper(*args, **kwargs):
                return launch(function, callback, identifier, args, kwargs)

            return wrapper

        return wrap
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


@spawn(daemon=True)
def task_worker(task):
    """Runs the actual function in separate thread."""
    error = None
    results = None
    function = task._function
    args = task._args
    kwargs = task._kwargs

    try:
        results = function(*args, **kwargs)
    except Exception as err:
        error = err
        error.traceback = format_exc()
    finally:
        task._set(error is not None and error or results)
        if task._callback is not None:
            try:
                task._callback(task)
            except Exception:
                print_exc()
