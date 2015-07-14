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
from threading import Thread

from pebble.task import Task
from pebble.thread.utils import decorate
from pebble.utils import execute, function_handler


_task_counter = count()


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
    return function_handler(launch_thread, decorate, *args, **kwargs)


def launch_thread(target, name=None, daemon=False, args=(), kwargs={}):
    """Launches the target function within a thread."""
    thread = Thread(target=target, name=name, args=args, kwargs=kwargs)
    thread.daemon = daemon
    thread.start()

    return thread


def concurrent(*args, **kwargs):
    """Runs the given function in a concurrent thread,
    taking care of the results and error management.

    *target* is the desired function to be run
    with the given *args* and *kwargs* parameters; if *timeout* is set,
    the thread will be stopped once expired returning TimeoutError as results.
    If a *callback* is passed, it will be run after the job has finished with
    the returned *Task* as parameter.

    The *concurrent* function works as well as a decorator.

    Returns a *Task* object.

    .. note:
       The decorator accepts the keywords *timeout* and *callback* only.
       If *target* keyword is not specified, the function will act as
       a decorator.
    """
    return function_handler(launch_task, decorate, *args, **kwargs)


def launch_task(function, callback=None, identifier=None,
                args=(), kwargs={}):
    """Wraps the target function within a Task
    and executes it in a separate thread.
    """
    metadata = {'function': function, 'args': args, 'kwargs':  kwargs}
    task = Task(next(_task_counter), callback=callback,
                metadata=metadata, identifier=identifier)
    task_worker(task)

    return task


@spawn(daemon=True)
def task_worker(task):
    """Runs the actual function in separate thread."""
    function = task._metadata['function']
    args = task._metadata['args']
    kwargs = task._metadata['kwargs']

    results = execute(function, args, kwargs)
    task.set_results(results)
