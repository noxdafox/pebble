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


from Queue import Queue
from threading import Thread
from multiprocessing import Process
from multiprocessing.queues import SimpleQueue

from pebble import Task, thread_task, process_task


class Asynchronous(object):
    def __init__(self, *args, **kwargs):
        self._function = None
        self.callback = None
        self.error_callback = None

        if len(args) == 1 and not len(kwargs) and callable(args[0]):
            self._function = args[0]
        elif not len(args) and len(kwargs):
            self.callback = kwargs.get('callback', None)
            self.error_callback = kwargs.get('error_callback', None)
        else:
            raise ValueError("Decorator accepts only keyword arguments.")

    def wrapper(self, *args, **kwargs):
        inqueue = Queue()
        args = list(args)
        args.insert(0, self._function)
        args.insert(1, inqueue)
        t = Thread(target=thread_task, args=(args), kwargs=(kwargs))
        t.daemon = True
        t.start()
        return Task(t, inqueue, self.callback, self.error_callback)

    def __call__(self, *args, **kwargs):
        if self._function is None:
            self._function = args[0]
            return self.wrapper
        else:
            return self.wrapper(*args, **kwargs)


class Concurrent(object):
    def __init__(self, *args, **kwargs):
        self._function = None
        self.callback = None
        self.error_callback = None

        if len(args) == 1 and not len(kwargs) and callable(args[0]):
            self._function = args[0]
        elif not len(args) and len(kwargs):
            self.callback = kwargs.get('callback', None)
            self.error_callback = kwargs.get('error_callback', None)
        else:
            raise ValueError("Decorator accepts only keyword arguments.")

    def wrapper(self, *args, **kwargs):
        inqueue = SimpleQueue()
        args = list(args)
        args.insert(0, self._function)
        args.insert(1, inqueue)
        p = Process(target=process_task, args=(args), kwargs=(kwargs))
        p.daemon = True
        p.start()
        return Task(p, inqueue, self.callback, self.error_callback)

    def __call__(self, *args, **kwargs):
        if self._function is None:
            self._function = args[0]
            return self.wrapper
        else:
            return self.wrapper(*args, **kwargs)
