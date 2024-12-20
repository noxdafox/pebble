from concurrent.futures import FIRST_COMPLETED, wait
import time
import unittest

from pebble import ProcessPool
from pebble.common.types import CONSTS
from pebble.pool.base_pool import PoolStatus

def function(argument, sleep_interval):
    time.sleep(sleep_interval)
    return argument

class TestProcessPoolGeneric(unittest.TestCase):
    def test_big_values_and_cancellation(self):
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
