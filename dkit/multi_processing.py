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
import threading
import queue
from typing import Iterable, Dict
from .utilities import log_helper as lh
from .utilities import instrumentation
from .utilities.identifier import uid
from .utilities.iter_helper import chunker
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class Message(object):
    """
    Envelope for moving data
    """
    def __init__(self, payload, args=None):
        self._id = uid()
        self.payload = payload
        self.args = args or {}
        self.initiated = datetime.now()

    def clear(self):
        """set payload to empty list"""
        self.payload = []

    def __iter__(self):
        yield from self.payload


class Journal(object):
    """
    Journal class for accounting messages

    all operations protected with a multi threading lock
    """
    def __init__(self, database=None):
        self.db = database or {}
        self.lock = threading.Lock()

    def push(self, message: Message):
        """add journal entry"""
        with self.lock:
            self.db[message._id] = datetime.now()

    def pop(self, message: Message):
        """remove item from journal"""
        with self.lock:
            del self.db[message._id]

    def empty(self):
        """return True when all items have been accounted for"""
        return self.__len__() == 0

    def __len__(self):
        with self.lock:
            return len(self.db)


class Worker(multiprocessing.Process):
    """
    implements multiprocessing worker

    Inherit from this class and implement the run method.

    Interface:
        * self.pull()
        * self.push()
        * self.lock: global lock
        * args: shared data Dict
    """
    def __init__(self, args, queue_in, queue_out, queue_log, lock, stop_event):
        super().__init__()
        self.args = args
        self.log_queue = queue_log
        self.in_queue = queue_in
        self.out_queue = queue_out
        self.lock = lock
        self.stop = stop_event
        self.timeout = 0.5  # seconds
        self.logger = lh.init_queue_logger(queue_log, self.name)

    def start(self):
        super().start()
        self.logger.debug(f"Started process id {self.pid}")

    def run(self):
        """
        implement logic in this method
        """
        for data in self.pull():
            self.push(data)

    def __get(self):
        """retrieve next item from queue

        return None in case of timeout
        """
        try:
            return self.in_queue.get(timeout=self.timeout)
        except queue.Empty:
            return None

    def pull(self) -> Iterable[Message]:
        """Iterator for inbound data"""
        data = self.__get()
        while not self.stop.is_set():
            if data:
                yield data
            data = self.__get()

    def push(self, data: Message):
        """push data back"""
        self.out_queue.put(data)


class Pipeline(object):
    """
    Multiprocessing Pipeline

    workers: Dict with class as key and #processes as value
    worker_args: Dict for static parameters passed to workers as argument
    queue_size: Queue size
    journal: Journal instance, default to in memory if not provided
    chunk_size: group input in a list of this size
    queue_timeout: timeout on queue
    log_trigger: trigger for loggin
    """
    def __init__(self,  workers: Dict[Worker, int], worker_args: Dict = None,
                 queue_size: int = 100, journal: Journal = None,
                 chunk_size=100, queue_timeout=0.5, log_trigger=10_000):
        self.workers = workers
        self.args = worker_args or {}
        self.queue_size: int = queue_size
        self.journal = journal or Journal()
        self.chunk_size = chunk_size
        self.queue_timeout = queue_timeout
        self.log_trigger: int = log_trigger

        self.shared_lock: multiprocessing.Lock = multiprocessing.Lock()
        self.queue_log = multiprocessing.Queue(self.queue_size)
        self.queues = []
        self.instances = []
        self.counter_in = instrumentation.CounterLogger(self.__class__.__name__)
        self.counter_out = instrumentation.CounterLogger(self.__class__.__name__)
        self.evt_stop = multiprocessing.Event()
        self.evt_input_completed = multiprocessing.Event()
        self.q_inbound = None
        self.q_outbound = None

    def _create_workers(self):
        """
        Create worker processes
        """
        logger.info("Instantiating worker processes.")
        self.q_inbound = q_in = multiprocessing.Queue(self.queue_size)
        self.queues.append(self.q_inbound)
        for Worker, instances in self.workers.items():
            q_out = multiprocessing.Queue(self.queue_size)
            for _ in range(instances):
                new_worker = Worker(
                    self.args,
                    q_in,
                    q_out,
                    self.queue_log,
                    self.shared_lock,
                    self.evt_stop
                )
                self.instances.append(new_worker)
                self.queues.append(q_out)
                new_worker.start()
            q_in = q_out
        self.q_outbound = q_out

        self.counter_in.start()
        self.counter_out.start()

    def _log_progress(self):
        """
        Log queue and progress status
        """
        iter_in = self.counter_in.value
        iter_out = self.counter_out.value
        q_in = self.q_inbound.qsize()
        q_out = self.q_outbound.qsize()
        msg = "ITER_IN: {}, ITER_OUT: {}, Q_IN: {}, Q_OUT: {}".format(
            iter_in, iter_out, q_in, q_out
        )
        logger.info(msg)

    def _feeder(self, data):
        """separate thread to feed data into queues."""
        for i, chunk in enumerate(chunker(data, size=self.chunk_size)):
            batch = Message(list(chunk))
            self.journal.push(batch)
            self.q_inbound.put(batch)
            # only one feeder thread so no need to lock this
            # line:
            self.counter_in.increment(len(batch.payload))
        logger.info("data feed comleted")
        self.evt_input_completed.set()

    def __call__(self, data: Iterable) -> Iterable:
        """
        main iteration loop
        """
        log_listener = lh.init_queue_listener(self.queue_log)
        self._create_workers()
        log_listener.start()

        # start feeding thread
        feeder = threading.Thread(target=self._feeder, args=(data,))
        feeder.start()

        # empty output
        while (not self.evt_input_completed.is_set()) or (not self.journal.empty()):
            while not self.q_outbound.empty():
                batch = self.q_outbound.get(True, self.queue_timeout)
                self.journal.pop(batch)
                yield from batch.payload
                self.counter_out.increment(len(batch.payload))
                if self.counter_out.value % self.log_trigger == 0:
                    self._log_progress()

        # shut down
        self.evt_stop.set()   # signal processes to stop
        logger.info("joining feeder thread")
        feeder.join()   # should join immediately
        for i, instance in enumerate(self.instances):
            logger.info(f"joining worker process {instance.pid}: {i+1}/{len(self.instances)}")
            instance.join()
        log_listener.stop()
