

from pebble import thread
from pebble.task import Task
from pebble.process.utils import stop
from pebble.process.decorators import spawn


@thread.spawn
def task_scheduler_loop(pool):
    scheduler = task_scheduler(pool._workers)
    next(scheduler)  # prime the coroutine

    while pool.alive:
        for task in task_fetcher(pool._task_queue):
            scheduler.send(task)


def task_fetcher(queue):
    task = queue.get()
    if isinstance(task, Task):
        yield task


def task_scheduler(workers):
    task = None

    while 1:
        for worker in workers:
            task = task or (yield)

            try:
                worker.schedule_task(task)
            except EnvironmentError:
                continue
            else:
                task = None


@thread.spawn
def pool_manager_loop(pool):
    while pool.alive:
        reset_expired_workers(pool)
        manage_tasks(pool)


def reset_expired_workers(pool):
    for worker in pool._workers:
        if not worker.alive:
            worker.reset()


def manage_tasks(pool):
    for worker in pool._workers:
        worker.handle_result()
        worker.handle_timeout()
        worker.handle_cancel()


class Worker(object):
    def __init__(self):
        self.process_worker = None
        self.task_manager = WorkerTaskManager()

    @property
    def alive(self):
        pass

    @property
    def receiver(self):
        pass

    def schedule_task(self, task):
        self.task_manager.schedule_task(task)
        try:
            self.process_worker.send(task)
        except EnvironmentError:
            self.task_manager.retract_task()
            raise

    def handle_result(self):
        try:
            result = self._get_result()
            self.task_manager.set_result(result)
        except EOFError:
            pass

    def _get_result(self):
        try:
            return self.process_worker.receive()
        except (EOFError, EnvironmentError):
            return self._handle_result_error()

    def _handle_result_error(self):
        if self.process_worker.exitcode != 0:
            result = ProcessExpired('Abnormal termination')
            result.exitcode = self.process_worker.exitcode
            return result
        else:
            raise

    def handle_timeout(self):
        if self.task_manager.task_timeout:
            self.process_worker.stop()
            self.task_manager.set_result(TimeoutError('Task timeout'))

    def handle_cancel(self):
        if self.task_manager.task_cancelled:
            self.process_worker.stop()
            self.task_manager.set_result(TaskCancelled('Task cancelled'))

    def reset(self):
        self.process_worker = WorkerProcess()
        self._reschedule_tasks()

    def _reschedule_tasks(self):
        for task in self.task_manager.tasks:
            self.process_worker.send(task)


class WorkerTaskManager(object):
    def __init__(self):
        self.task_buffer = deque()

    @property
    def tasks(self):
        for task in self.task_buffer:
            yield task

    @property
    def task_timeout(self):
        if self.task_buffer and has_timeout(self.task_buffer[0]):
            return True
        else:
            return False

    @property
    def task_cancelled(self):
        if self.task_buffer and self.task_buffer[0].cancelled:
            return True
        else:
            return False

    def schedule_task(self, task):
        self.task_buffer.append(task)

    def retract_task(self):
        task = self.task_buffer.pop()
        task._timestamp = 0

    def set_result(self, result):
        task = self.task_buffer.popleft()
        task.set_result(result)
        if self.task_buffer:
            self.task_buffer[0]._timestamp = time()


class WorkerProcess(object):
    def __init__(self):
        pool_side, worker_side = create_channels()
        self.alive = True
        self.channel = pool_side
        self.process = worker_process(worker_side)

    def send(self, task):
        function = task._metadata['function']
        args = task._metadata['args']
        kwargs = task._metadata['kwargs']
        self.channel.send((function, args, kwargs))

    def receive(self):
        try:
            self.channel.recv()
        except (EOFError, EnvironmentError):
            self.alive = False

    def stop(self):
        stop(self.process)
        self.alive = False


@spawn(name='pool_worker', daemon=True)
def pool_worker(channel, initializer, initargs, limit):
    """Runs the actual function in separate process."""
    signal(SIGINT, SIG_IGN)
    counter = count()

    if initializer is not None:
        if not run_initializer(initializer, initargs):
            return os.EX_SOFTWARE

    while not limit or next(counter) < limit:
        try:
            execute_next_task(channel)
        except (EOFError, EnvironmentError) as error:
            # TODO: return correct ERRNO
            return error.errno

    # if deinitializer is not None:
        # if not run_initializer(deinitializer, deinitargs):
        #     return os.EX_SOFTWARE

    return os.EX_OK


def execute_next_task(channel):
    function, args, kwargs = channel.recv()
    results = execute(function, args, kwargs)
    send_results(channel, results)


def run_initializer(initializer, initargs):
    try:
        initializer(*initargs)
        return True
    except Exception as error:
        print_exc()
        return False
