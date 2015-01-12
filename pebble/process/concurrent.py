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
from multiprocessing import Pipe

from .spawn import spawn as process_spawn
from .common import decorate, stop, send_results, get_results
from ..thread import spawn as thread_spawn
from ..pebble import execute, Task
from ..utils import function_handler
from ..exceptions import ProcessExpired


_task_counter = count()


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
    return function_handler(launch, decorate, *args, **kwargs)


def launch(target, timeout=None, callback=None, identifier=None,
           args=None, kwargs=None):
    """Wraps the target function within a Task
    and executes it in a separate process.

    """
    reader, writer = Pipe(duplex=False)
    worker = task_worker(writer, target, args, kwargs)
    writer.close()

    task = ProcessTask(next(_task_counter), worker, time(),
                       callback=callback, timeout=timeout,
                       function=target, args=args, kwargs=kwargs,
                       identifier=identifier)
    task_manager(task, reader)

    return task


@process_spawn(daemon=True)
def task_worker(pipe, function, args, kwargs):
    """Runs the actual function in separate process."""
    results = execute(function, args, kwargs)
    send_results(pipe, results)


@thread_spawn(daemon=True)
def task_manager(task, pipe):
    """Task's lifecycle manager.

    Starts a new worker, waits for the *Task* to be performed,
    collects results, runs the callback and cleans up the process.

    """
    worker = task._worker

    results = get_results(pipe, task.timeout)

    if worker.is_alive():
        stop(worker)

    if isinstance(results, ProcessExpired):
        results.exitcode = worker.exitcode

    task.set_results(results)


class ProcessTask(Task):
    """Extends the *Task* object to support *process* decorator."""
    def __init__(self, task_nr, worker, timestamp, **kwargs):
        super(ProcessTask, self).__init__(task_nr, **kwargs)
        self._worker = worker
        self._timestamp = timestamp

    def cancel(self):
        """Overrides the *Task* cancel method in order to signal it
        to the *process* decorator handler."""
        super(ProcessTask, self).cancel()
        stop(self._worker)
