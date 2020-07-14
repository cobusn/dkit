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
import shelve
from zlib import adler32
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Dict
from .utilities import log_helper as lh
from .utilities import instrumentation
from .utilities.identifier import uid
from .utilities.iter_helper import chunker
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class Message(ABC):
    """
    Envelope for moving data
    """

    @abstractmethod
    def _generate_id(self):
        pass


def adler_hash(obj):
    return adler32(repr(obj).encode())


class ImmutableMessage(Message):
    """
    Message that has a hashable payload (that can be
    used as a key to track progress)
    """

    def _generate_id(self):
        return str(adler_hash(self.args))

    def __init__(self, args):
        self.args = args
        self.result = None
        self.initiated = datetime.now()
        self._id = self._generate_id()


class ListMessage(Message):
    """
    Message that has a list of items as payload
    """
    def __init__(self, payload):
        self.payload = payload
        self.initiated = datetime.now()
        self._id = self._generate_id()

    def _generate_id(self):
        return uid()

    def clear(self):
        """set payload to empty list"""
        self.payload = []


class Journal(object):
    """
    Journal class for accounting messages

    thread safe

    args:
        database: Dict like object
        accounting:
    """
    @dataclass
    class Entry:
        __slots__ = ("_id", "created", "completed")
        _id: object
        created: datetime
        completed: datetime

    def __init__(self, database=None):
        if database is not None:
            self.db = database
        else:
            self.db = {}
        self.lock = threading.Lock()

    def __del__(self):
        if hasattr(self.db, "close"):
            self.db.close()

    def enter(self, message: Message):
        """add journal entry"""
        with self.lock:
            self.db[message._id] = self.Entry(message._id, datetime.now(), None)

    def complete(self, message: Message, accounting=False):
        """complete entry

        if accounting is disabled, the entry will be removed
        otherwise it will me marked as complete
        """
        with self.lock:
            if accounting:
                msg = self.db[message._id]
                msg.completed = datetime.now()
                self.db[message._id] = msg
                self.sync()
            else:
                del self.db[message._id]

    def is_completed(self, message):
        if message._id in self.db and self.db[message._id].completed:
            return True
        else:
            return False

    def empty(self):
        """return True when all items have been accounted for"""
        not_completed = [i for i in self.db.values() if not i.completed]
        return len(not_completed) == 0

    def sync(self):
        """sync to file (where applicable)"""
        if hasattr(self.db, "sync"):
            self.db.sync()

    @classmethod
    def from_shelve(cls, file_name):
        """constructor from shelve file"""
        db = shelve.open(file_name)
        return cls(db)

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


class AbstractPipeline(ABC):
    """
    Multiprocessing Pipeline

    workers: Dict with class as key and #processes as value
    worker_args: Dict for static parameters passed to workers as argument
    queue_size: Queue size
    journal: Journal instance, default to in memory if not provided
    chunk_size: group input in a list of this size
    queue_timeout: timeout on queue
    log_trigger: trigger for loggin
    unique_messge:
    """
    def __init__(self,  workers: Dict[Worker, int], worker_args: Dict = None,
                 queue_size: int = 100, journal: Journal = None,
                 chunk_size=100, queue_timeout=0.5, log_trigger=10_000,
                 unique_messages: bool = False):
        self.workers = workers
        self.args = worker_args or {}
        self.queue_size: int = queue_size
        if journal is not None:
            self.journal = journal
        else:
            self.journal = Journal()
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
        self.enable_accounting = False

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

    @abstractmethod
    def _yield_results(self, message):
        pass

    @abstractmethod
    def _feeder(self, data):
        pass

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
                message = self.q_outbound.get(True, self.queue_timeout)
                yield from self._yield_results(message)
                self.journal.complete(message, self.enable_accounting)
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


class ListPipeline(AbstractPipeline):

    def _feeder(self, data):
        """separate thread to feed data into queues."""
        for chunk in chunker(data, size=self.chunk_size):
            batch = ListMessage(list(chunk))
            self.journal.enter(batch)
            self.q_inbound.put(batch)
            # only one feeder thread so no need to lock this
            # line:
            self.counter_in.increment(len(batch.payload))
        logger.info("data feed comleted")
        self.evt_input_completed.set()

    def _yield_results(self, message):
        yield from message.payload
        self.counter_out.increment(len(message.payload))


class ImmutablePipeline(AbstractPipeline):

    def __init__(self,  workers: Dict[Worker, int], worker_args: Dict = None,
                 queue_size: int = 100, journal: Journal = None,
                 chunk_size=100, queue_timeout=0.5, log_trigger=10_000,
                 unique_messages: bool = False, accounting=False):
        super().__init__(workers, worker_args, queue_size, journal, chunk_size,
                         queue_timeout, log_trigger, unique_messages)
        self.enable_accounting = accounting


    def _yield_results(self, message):
        yield message.result
        self.counter_out.increment()

    def _feeder(self, data):
        """separate thread to feed data into queues."""
        if self.enable_accounting:
            for entry in data:
                msg = ImmutableMessage(entry)
                if not self.journal.is_completed(msg):
                    self.journal.enter(msg)
                    self.q_inbound.put(msg)
                    self.journal.sync()
                    self.counter_in.increment()
                else:
                    logger.warning(f"message with id {msg._id} already completed")
                # only one feeder thread so no need to lock this
                # line:
                self.counter_in.increment()
        else:
            for entry in data:
                self.q_inbound.put(entry)

        logger.info("data feed comleted")
        self.evt_input_completed.set()
