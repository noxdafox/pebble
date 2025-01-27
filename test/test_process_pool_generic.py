from concurrent.futures import FIRST_COMPLETED, wait
from dataclasses import dataclass
import time
import unittest

from pebble import ProcessPool
from pebble.common.types import CONSTS, FutureStatus

def function(argument, sleep_interval):
    time.sleep(sleep_interval)
    return argument

g_pool = None

@dataclass
class SpamMessage:
    pass

def flooding_function():
    workers_channel = g_pool._pool_manager.worker_manager.workers_channel
    while True:
        workers_channel.send(SpamMessage())

class TestProcessPoolGeneric(unittest.TestCase):
    def test_big_values_and_cancellation(self):
        """Test that the pool handles workers with big tasks and can cancel them."""
        # Ideally this should be bigger than the multiprocessing pipe's internal
        # buffer.
        BIG_VALUE = [0] * 10 * 1000 * 1000
        # The bigger number of workers is, the higher is the chance of catching
        # bugs.
        CNT = 50
        # Let the worker events cluster around the sleep unit granularity to
        # increase the chance of catching bugs.
        INITIAL_SLEEP = CONSTS.sleep_unit * 10
        EPS = CONSTS.sleep_unit / 10

        futures = []
        with ProcessPool(max_workers=CNT) as pool:
            for i in range(CNT):
                futures.append(pool.schedule(function, args=[BIG_VALUE, INITIAL_SLEEP + i * EPS]))
            wait(futures, return_when=FIRST_COMPLETED)
            for f in futures:
                f.cancel()
            time.sleep(EPS * CNT / 2)
            pool.stop()
            pool.join()

    def test_message_flood_from_worker(self):
        """Test that the pool stops despite the worker spamming the message pipe."""
        with ProcessPool() as pool:
            # Use a global variable to pass channels to the worker.
            global g_pool
            g_pool = pool

            future = pool.schedule(flooding_function)

            # Wait until the worker starts running the (spammy) task.
            while future._state == FutureStatus.PENDING:
                time.sleep(0.1)
            assert future._state == FutureStatus.RUNNING

            pool.stop()
            pool.join()
            g_pool = None
