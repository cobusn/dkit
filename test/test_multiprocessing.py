import logging
import random
import sys
import time
from pathlib import Path
import unittest
sys.path.insert(0, "..")  # noqa
from boltons.dictutils import FrozenDict

from dkit.multi_processing import (
    Worker,
    ListPipeline,
    ImmutablePipeline,
    Journal,
)
from dkit.utilities.log_helper import init_stderr_logger


class ListWorker(Worker):

    def run(self):
        for i, message in enumerate(self.pull()):
            self.logger.info(f"Processing batch {i}")
            for row in message.payload:
                row["w1"] = self.args["value"]
            time.sleep(random.triangular(0, 0.01))
            self.push(message)


class ItemWorker(Worker):

    def run(self):
        for i, message in enumerate(self.pull()):
            self.logger.info(f"Processing batch {i}")
            message.result = self.args["value"]
            time.sleep(random.triangular(0, 0.01))
            self.push(message)


class TestMultiprocessing(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        # delete journal
        j = Path.cwd() / "data" / "journal.shelve"
        if j.exists():
            j.unlink()

    def test_pipeline(self):
        pipeline = ListPipeline(
            {
                ListWorker: 10,
                ListWorker: 10,
            },
            worker_args={"value": 10},
            queue_size=10
        )

        result = list(pipeline({"a": 10} for i in range(1_000)))
        self.assertEqual(
            result[0],
            {'a': 10, 'w1': 10}
        )
        self.assertEqual(
            len(result),
            1_000
        )

    def test_shelve_journal(self):
        pipeline = ListPipeline(
            {
                ListWorker: 10,
                ListWorker: 10,
            },
            worker_args={"value": 10},
            queue_size=10,
            journal=Journal.from_shelve("data/journal.shelve")
        )

        result = list(pipeline({"a": 10} for i in range(1_000)))
        self.assertEqual(
            result[0],
            {'a': 10, 'w1': 10}
        )
        self.assertEqual(
            len(result),
            1_000
        )

    def test_immutable_accounting(self):

        _input = [FrozenDict({"a": i}) for i in range(1_000)]
        pipeline = ImmutablePipeline(
            {
                ItemWorker: 10,
            },
            worker_args={"value": 10},
            queue_size=10,
            journal=Journal.from_shelve("data/journal.shelve"),
            accounting=True
        )

        result = list(pipeline(_input))
        self.assertEqual(
            result[0], 10
        )
        self.assertEqual(
            len(result),
            1_000
        )


if __name__ == '__main__':
    init_stderr_logger(level=logging.ERROR)
    unittest.main()
