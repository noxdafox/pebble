Pebble
======


.. note::

    Since Pebble 1.0.0 decorators are functions and not classes anymore.


Description
-----------

Pebble provides a neat API to manage threads and processes within an application.


Examples
--------

Launch a task in a thread and wait for its results::

    from pebble import asynchronous


    @asynchronous
    def do_job(foo, bar=0):
    	return foo + bar


    if __name__ == "__main__":
        task = do_job(1, bar=2)
	print task.get()  # it will block until do_job has completed

Launch five tasks in separate processes and handle their results in a callback::

    from pebble import concurrent


    def task_done(task_id, results):
    	print "Task %s has returned %d" % (task_id, results)


    @concurrent(callback=task_done)
    def do_job(foo, bar=0):
    	return foo + bar


    if __name__ == "__main__":
        for i in range(0, 5):
            do_job(i)

	raw_input("Press any key to exit.")

Callbacks can be dynamically (re)assigned, useful to set instance methods as callback::

    from pebble import concurrent


    class Foo(object):
	def __init__(self):
	    self.counter = 0
	    self.errors = 0
	    self.do_job.callback = self.task_done
	    self.do_wrong_job.error_callback = self.task_error

	def task_done(self, task_id, results):
	    self.counter += results

	def task_error(self, task_id, exception):
	    self.errors += 1

	@concurrent
	def do_job():
	    return 1

	@concurrent
	def do_wrong_job():
	    raise Exception("Ops!")


    if __name__ == "__main__":
	foo = Foo()
	tasks = []

	for i in range(0, 5):
	    task = foo.do_job()
	    tasks.append(task)
	    task = foo.do_wrong_job()
	    tasks.append(task)

	for task in tasks:
	    print "Waiting for %s" % task.id
	    try:
		task.get()  # wait for task to finish
	    except Exception:
		pass  # exception are re-raised by the get() method

	print foo.counter
	print foo.errors

TODO
----

A roadmap::

 * return traceback in error callbacks
 * implement pebbles (pools of workers) and make it possible to bind decorators to them
 * memoization as decorator parameter
