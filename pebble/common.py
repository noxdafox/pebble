import os
import signal

from threading import Thread
from traceback import format_exc
from multiprocessing import Process


def launch_thread(function, *args, **kwargs):
    thread = Thread(target=function, args=args, kwargs=kwargs)
    thread.daemon = True
    thread.start()

    return thread


def launch_process(function, *args, **kwargs):
    process = Process(target=function, args=args, kwargs=kwargs)
    process.daemon = True
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
        return function(*args, **kwargs)
    except Exception as error:
        error.traceback = format_exc()
        return error


def send_result(pipe, data):
    """Send result handling communication errors."""
    try:
        pipe.send(data)
    except TypeError as error:
        error.traceback = format_exc()
        pipe.send(error)


class ProcessExpired(OSError):
    """Raised when process dies unexpectedly."""
    def __init__(self, msg, code=0):
        super(ProcessExpired, self).__init__(msg)
        self.exitcode = code
