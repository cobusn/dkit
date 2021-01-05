import sys
sys.path.insert(0, "..")  # noqa
from dkit.multi_processing import Worker, TaskPipeline
from dkit.utilities.log_helper import init_stderr_logger
import time
import random
import logging


class Worker(Worker):

    def run(self):
        for i, msg in enumerate(self.pull()):
            msg.result = i * msg.args["a"] * self.args["value"]
            time.sleep(random.triangular(0, 0.3))
            self.push(msg)


if __name__ == "__main__":
    init_stderr_logger(level=logging.INFO)
    pipeline = TaskPipeline(
        {
            Worker: 10,
        },
        worker_args={"value": 10},
        queue_size=10
    )

    result = list(pipeline({"a": 10} for i in range(50)))
    print(f"Processed {len(result)} rows")
