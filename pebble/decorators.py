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


from threading import Thread
from multiprocessing import Process
from multiprocessing.queues import SimpleQueue

from pebble import ThreadTask, ProcessTask, thread_worker, process_worker


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
        t = ThreadTask(self.callback, self.error_callback)
        args = list(args)
        args.insert(0, self._function)
        args.insert(1, t)
        t._worker = Thread(target=thread_worker, args=(args), kwargs=(kwargs))
        t._worker.daemon = True
        t._worker.start()
        return t

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
        p = Process(target=process_worker, args=(args), kwargs=(kwargs))
        p.daemon = True
        p.start()
        return ProcessTask(p, inqueue, self.callback, self.error_callback)

    def __call__(self, *args, **kwargs):
        if self._function is None:
            self._function = args[0]
            return self.wrapper
        else:
            return self.wrapper(*args, **kwargs)
