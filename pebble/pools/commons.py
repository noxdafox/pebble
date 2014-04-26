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


from inspect import isclass
from itertools import count
from threading import Thread, Event
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue


# Pool and Workers states
STOPPED = 0
RUNNING = 1
CLOSED = 2
CREATED = 3
EXPIRED = 4
ERROR = 5


class PoolContext(object):
    """Container for the Pool state.

    Wraps all the variables needed to represent a Pool.

    """
    def __init__(self, workers, limit, queue, queueargs,
                 initializer, initargs):
        self.state = CREATED
        self.workers = workers
        self.pool = []
        self.limit = limit
        self.task_counter = count()
        self.workers_event = Event()
        self.initializer = initializer
        self.initargs = initargs
        if queue is not None:
            if isclass(queue):
                self.queue = queue(*queueargs)
            else:
                raise ValueError("Queue must be Class")
        else:
            self.queue = Queue()

    @property
    def counter(self):
        """Tasks counter."""
        return next(self.task_counter)


class PoolManager(Thread):
    """Pool management routine.

    Respawns missing workers.
    Collects expired ones and cleans them up.

    """
    def __init__(self, context):
        Thread.__init__(self)
        self.daemon = True
        self.context = context

    def wait_for_worker(self, timeout):
        """Waits for expired workers and restarts them."""
        self.context.workers_event.wait(timeout=timeout)
        self.context.workers_event.clear()

        return [w for w in self.context.pool if w.state == EXPIRED]

    def cleanup_workers(self, expired):
        raise NotImplementedError('Not implemented')

    def spawn_workers(self):
        """Spawns missing Workers."""
        raise NotImplementedError('Not implemented')

    def run(self):
        while self.context.state != STOPPED:
            self.spawn_workers()
            expired_workers = self.wait_for_worker(0.8)
            self.cleanup_workers(expired_workers)
