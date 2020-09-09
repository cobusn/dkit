import sys
sys.path.insert(0, "..")  # noqa
from dkit.multi_processing import Worker, Pipeline
from dkit.utilities.log_helper import init_stderr_logger
import time
import random
import logging


class Worker1(Worker):

    def run(self):
        for i, batch in enumerate(self.pull()):
            self.logger.info(f"Processing batch {i}")
            for row in batch:
                row["w1"] = self.args["value"]
            time.sleep(random.triangular(0, 0.3))
            self.push(batch)


class Worker2(Worker):

    def run(self):
        for batch in self.pull():
            for row in batch:
                row["w2"] = 2
            time.sleep(random.triangular(0, 0.4))
            self.push(batch)


if __name__ == "__main__":
    init_stderr_logger(level=logging.INFO)
    pipeline = Pipeline(
        {
            Worker1: 10,
            Worker2: 10,
        },
        worker_args={"value": 10},
        queue_size=100
    )

    result = list(pipeline({"a": 10} for i in range(5_000)))
    print(f"Processed {len(result)} rows")
