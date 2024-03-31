# This file is part of Pebble.
# Copyright (c) 2013-2024, Matteo Cafasso

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
import pickle
import signal

from threading import Thread
from traceback import format_exc

from pebble.common.types import Result, RemoteException, SUCCESS, FAILURE, ERROR


def launch_thread(name, function, daemon, *args, **kwargs):
    thread = Thread(target=function, name=name, args=args, kwargs=kwargs)
    thread.daemon = daemon
    thread.start()

    return thread


def launch_process(name, function, daemon, mp_context, *args, **kwargs):
    process = mp_context.Process(
        target=function, name=name, args=args, kwargs=kwargs)
    process.daemon = daemon
    process.start()

    return process


def stop_process(process):
    """Does its best to stop the process."""
    process.terminate()
    process.join(3)

    if process.is_alive() and os.name != 'nt':
        try:
            os.kill(process.pid, signal.SIGKILL)
            process.join()
        except OSError:
            return

    if process.is_alive():
        raise RuntimeError("Unable to terminate PID %d" % os.getpid())


def execute(function, *args, **kwargs):
    """Runs the given function returning its results or exception."""
    try:
        return Result(SUCCESS, function(*args, **kwargs))
    except BaseException as error:
        try:
            error.traceback = format_exc()
        except AttributeError:  # Frozen exception
            pass

        return Result(FAILURE, error)


def process_execute(function, *args, **kwargs):
    """Runs the given function returning its results or exception."""
    try:
        return Result(SUCCESS, function(*args, **kwargs))
    except BaseException as error:
        return Result(FAILURE, RemoteException(error, format_exc()))


def send_result(pipe, data):
    """Send result handling pickling and communication errors."""
    try:
        pipe.send(data)
    except (pickle.PicklingError, TypeError) as error:
        pipe.send(Result(ERROR, RemoteException(error, format_exc())))
