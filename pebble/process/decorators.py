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

from time import time
from itertools import count
from multiprocessing import Pipe, Process

from pebble import thread
from pebble.task import Task
from pebble.exceptions import ProcessExpired
from pebble.utils import execute, function_handler
from pebble.process.utils import stop, send_results, get_results, decorate


_task_counter = count()


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
    return function_handler(launch_process, decorate, *args, **kwargs)


def launch_process(target, name=None, daemon=False, args=(), kwargs={}):
    """Launches the target function within a process."""
    process = Process(target=target, name=name, args=args, kwargs=kwargs)
    process.daemon = daemon
    process.start()

    return process


def concurrent(*args, **kwargs):
    """Runs the given function in a concurrent process,
    taking care of the results and error management.

    *target* is the desired function to be run
    with the given *args* and *kwargs* parameters; if *timeout* is set,
    the process will be stopped once expired returning TimeoutError as results.
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


def launch_task(target, timeout=None, callback=None, identifier=None,
                args=(), kwargs={}):
    """Wraps the target function within a Task
    and executes it in a separate process.
    """
    reader, writer = Pipe(duplex=False)
    worker = task_worker(writer, target, args, kwargs)
    writer.close()

    task = ProcessTask(next(_task_counter), worker, callback=callback,
                       timeout=timeout, identifier=identifier)
    task_manager(task, reader)

    return task


@spawn(daemon=True)
def task_worker(pipe, function, args, kwargs):
    """Runs the actual function in separate process."""
    results = execute(function, args, kwargs)
    send_results(pipe, results)


@thread.spawn(daemon=True)
def task_manager(task, pipe):
    """Task's lifecycle manager.

    Waits for the *Task* to be performed,
    collects results, runs the callback and cleans up the process.
    """
    worker = task._worker

    results = get_results(pipe, task.timeout)

    if isinstance(results, ProcessExpired):
        results.exitcode = worker.exitcode

    task.set_results(results)

    if worker.is_alive():
        stop(worker)


class ProcessTask(Task):
    """Extends the *Task* object to support *process* decorator."""
    def __init__(self, task_nr, worker, **kwargs):
        super(ProcessTask, self).__init__(task_nr, **kwargs)
        self._worker = worker
        self._timestamp = time()

    def cancel(self):
        """Overrides the *Task* cancel method in order to signal it
        to the *process* decorator handler."""
        super(ProcessTask, self).cancel()
        stop(self._worker)
