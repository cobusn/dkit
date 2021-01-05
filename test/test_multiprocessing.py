import logging
import random
import sys
import time
from pathlib import Path
import unittest
sys.path.insert(0, "..")  # noqa

from dkit.multi_processing import (
    Worker,
    ListPipeline,
    TaskPipeline,
    Journal,
    MD5TaskMessage,
)
from dkit.utilities.log_helper import init_stderr_logger

N = 1_00


class ListWorker(Worker):

    def run(self):
        for i, message in enumerate(self.pull()):
            self.logger.info(f"Processing batch {i}")
            for row in message:
                row["w1"] = self.args["value"]
            time.sleep(random.triangular(0, 0.001))
            self.push(message)


class TaskWorker(Worker):

    def run(self):
        for i, message in enumerate(self.pull()):
            self.logger.info(f"Processing batch {i}")
            message.result = self.args["value"] * message.args["a"]
            time.sleep(random.triangular(0, 0.01))
            self.push(message)


class TestMultiprocessing(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        # delete journal
        j = Path.cwd() / "data" / "journal.shelve"
        if j.exists():
            j.unlink()

    def test_list_pipeline(self):
        """list pipeline with two workers in series"""
        pipeline = ListPipeline(
            {
                ListWorker: 10,
                ListWorker: 10,
            },
            worker_args={"value": 10},
            queue_size=10
        )

        result = list(pipeline({"a": 10} for i in range(N)))
        self.assertEqual(
            result[0],
            {'a': 10, 'w1': 10}
        )
        self.assertEqual(
            len(result),
            N
        )

    def test_shelve_journal(self):
        """test using shelve as journal functionality"""
        pipeline = ListPipeline(
            {
                ListWorker: 10,
                ListWorker: 10,
            },
            worker_args={"value": 10},
            queue_size=10,
            journal=Journal.from_shelve("data/journal.shelve")
        )

        result = list(pipeline({"a": 10} for i in range(N)))
        self.assertEqual(
            result[0],
            {'a': 10, 'w1': 10}
        )
        self.assertEqual(
            len(result),
            N
        )

    def test_immutable_accounting(self):

        _input = [{"a": 1} for i in range(N)]
        pipeline = TaskPipeline(
            {
                TaskWorker: 10,
            },
            worker_args={"value": 10},
            queue_size=10,
            journal=Journal.from_shelve("data/journal.shelve"),
            message_type=MD5TaskMessage
        )

        result = list(pipeline(_input))
        print(result)
        self.assertEqual(
            sum(result), 10
        )
        self.assertEqual(
            len(result),
            1
        )

    def test_message_repr(self):
        msg = MD5TaskMessage(10)
        r = repr(msg)
        self.assertEqual(
            r,
            "MD5TaskMessage(args=10, _id=c1ecf43a95efdf7cc8b0ec6533492ca4)"
        )


if __name__ == '__main__':
    init_stderr_logger(level=logging.DEBUG)
    unittest.main()
