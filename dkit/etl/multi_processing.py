#
# Copyright (C) 2016  Cobus Nel
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
"""
Multiprocessing abstractions
"""
import multiprocessing
import queue
from typing import Iterable, MutableMapping
from . import source
from ..utilities import instrumentation
import logging

SENTINEL = StopIteration


logger = logging.getLogger(__name__)


class Coordinator(source.AbstractSource):
    """
    multiprocessing framework
    """

    def __init__(self,  worker_class: "Worker", process_count: int = None,
                 log_trigger: int = 10000, queue_size: int = 1000, worker_args=(),
                 worker_kwargs={}):
        super().__init__(log_trigger)
        self.worker_class: "Worker" = worker_class
        self.process_count: int = process_count if process_count else multiprocessing.cpu_count()
        self.queue_size: int = queue_size
        self.shared_lock: multiprocessing.Lock = multiprocessing.Lock()
        self.worker_args = worker_args
        self.worker_kwargs = worker_kwargs

        self.queue_in = multiprocessing.JoinableQueue(self.queue_size)
        self.queue_out = multiprocessing.Queue(self.queue_size)
        self.queue_log = multiprocessing.Queue(self.queue_size)
        self.process_list = []
        self.counter_in = instrumentation.CounterLogger(__name__)
        self.counter_out = instrumentation.CounterLogger(__name__)

    def __call__(Iterable: input):
        pass

    def __instantiate_workers(self):
        """
        Create worker processes
        """
        logger.info("Instantiating {} worker processes.".format(self.process_count))
        for _ in range(self.process_count):
            new_worker = self.worker_class(
                self.queue_in,
                self.queue_out,
                self.queue_log,
                self.shared_lock,
                *self.worker_args,
                **self.worker_kwargs,
            )
            self.process_list.append(new_worker)

        # Fire up the processes
        for process in self.process_list:
            process.start()

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
