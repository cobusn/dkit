# Copyright (c) 2020 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Multiprocessing abstractions
"""
import multiprocessing
import queue
from typing import Iterable, MutableMapping, Dict
from .utilities import instrumentation
from .utilities.identifier import uuid
from datetime import datetime
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)
SENTINEL = StopIteration


class Message(object):
    """
    Envelope for moving data
    """
    def __init__(self, payload):
        self._id = uuid()
        self.payload = payload
        self.initiated = datetime.now()

    def __hash__(self):
        return self._id


class Journal(object):
    """
    Journal class for accounting messages
    """
    def __init__(self, database=None):
        self.db = database or {}

    def push(self, message: Message):
        self.db[message._id] = datetime.now()

    def pop(self, message: Message):
        del self.db[message._id]

    def __len__(self):
        return len(self.db)


class Pipeline(object):
    """
    multiprocessing framework
    """
    def __init__(self,  workers: Dict, log_trigger: int = 10000,
                 queue_size: int = 1000, journal: Journal = None):
        self.workers = workers
        self.queue_size: int = queue_size
        self.log_trigger: int = log_trigger
        self.shared_lock: multiprocessing.Lock = multiprocessing.Lock()
        self.queue_log = multiprocessing.Queue(self.queue_size)
        self.queues = []
        self.instances = defaultdict(lambda: [])
        self.counter_in = instrumentation.CounterLogger(self.__class__.__name__)
        self.counter_out = instrumentation.CounterLogger(self.__class__.__name__)
        self.journal = journal or Journal()
        self.done = multiprocessing.Event()

    def __call__(Iterable: input):
        pass

    def __create_workers(self):
        """
        Create worker processes
        """
        logger.info("Instantiating {} worker processes.".format(self.process_count))
        self.q_inbound = q_in = multiprocessing.Queue(self.queue_size)
        for Worker, instances in self.workers.items():
            q_out = multiprocessing.Queue(self.queue_size)
            for _ in range(self.process_count):
                new_worker = Worker(
                    q_in,
                    q_out,
                    self.queue_log,
                    self.shared_lock,
                    self.done
                )
                self.instances[Worker].append(new_worker)
                self.queues.append(q_out)
                q_in = q_out
                new_worker.start()
        self.q_outbound = q_out

        self.counter_in.start()
        self.counter_out.start()

    def _log_progress(self):
        """
        Log queue and progress status
        """
        iter_in = self.counter_in.value
        iter_out = self.counter_out.value
        q_in = self.queue_in.qsize()
        q_out = self.queue_out.qsize()
        msg = "ITER_IN: {}, ITER_OUT: {}, Q_IN: {}, Q_OUT: {}".format(
            iter_in, iter_out, q_in, q_out
        )
        logger.info(msg)

    def __kill_processes(self):
        """
        Kill worker processes
        """
        logger.info("Killing {} worker processes".format(self.process_count))
        for i in range(len(self.process_list)):
            self.queue_in.put(SENTINEL)

    def __iter__(self) -> Iterable:
        """
        main iteration loop
        """
        self.__instantiate_workers()

        # Feed data
        for row in self.the_iterator:
            self.queue_in.put(row)
            self.counter_in.increment()

            while self.queue_out.qsize() > 0:
                if self.counter_out.value % self.log_trigger == 0:
                    self._log_progress()
                try:
                    row = self.queue_out.get(False)
                    yield row
                    self.counter_out.increment()
                except queue.Empty:
                    pass

        # Get the rest out as well..
        # while self.counter_out.value < self.counter_in.value:
        while (self.queue_in.qsize() > 0 or self.queue_out.qsize() > 0):
            try:
                if self.counter_out.value % self.log_trigger == 0:
                    self._log_progress()
                row = self.queue_out.get(True, 1)
                yield row
                self.counter_out.increment()
            except queue.Empty:
                pass

        self.__kill_processes()
        logger.debug("Joining queues.")
        self.queue_in.join()


class Worker(multiprocessing.Process):
    """
    implements multiprocessing worker

    Inherit from this class and implement the run method.

    Interface:
        * self.pull()
        * self.push()
        * self.lock: global lock
        * properties: shared data
    """
    class Bootstrap:
        """
        Decorator to bootstrap parameters for the Worker class
        """
        def __call__(self, other_init):

            def wrap(init_self, in_queue, out_queue, log_queue, lock, *args, **kwargs):
                super(init_self.__class__, init_self).__init__(
                    in_queue,
                    out_queue,
                    log_queue,
                    lock
                )
                other_init(init_self, *args, **kwargs)

            return wrap

    def __init__(self, in_queue, out_queue, log_queue, lock):
        super().__init__()
        self.log_queue = log_queue
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.lock = lock

    def run(self):
        """
        implement logic in this method
        """
        raise NotImplementedError

    def pull(self) -> Iterable:
        """Iterator for inbound data"""
        row = self.in_queue.get()
        while row != SENTINEL:
            yield row
            self.in_queue.task_done()
            row = self.in_queue.get()

        # received poison pill. Stop
        self.in_queue.task_done()  # For the poison pill

    def push(self, row: MutableMapping):
        """push data back"""
        self.out_queue.put(row)
