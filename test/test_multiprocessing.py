import logging
import random
import sys
import time
import unittest
sys.path.insert(0, "..")  # noqa

from dkit.multi_processing import Worker, Pipeline
from dkit.utilities.log_helper import init_stderr_logger


class _Worker(Worker):

    def run(self):
        for i, message in enumerate(self.pull()):
            self.logger.info(f"Processing batch {i}")
            for row in message:
                row["w1"] = self.args["value"]
            time.sleep(random.triangular(0, 0.01))
            self.push(message)


class TestCase(unittest.TestCase):

    def setUp(self):
        self.pipeline = Pipeline(
            {
                _Worker: 10,
                _Worker: 10,
            },
            worker_args={"value": 10},
            queue_size=10
        )

    def test_pipeline(self):
        result = list(self.pipeline({"a": 10} for i in range(1_000)))
        self.assertEqual(
            result[0],
            {'a': 10, 'w1': 10}
        )
        self.assertEqual(
            len(result),
            1_000
        )


if __name__ == '__main__':
    init_stderr_logger(level=logging.ERROR)
    unittest.main()
