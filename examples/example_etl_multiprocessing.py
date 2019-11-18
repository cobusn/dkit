"""
Multiprocessing example
"""
import socket
import time
from itertools import repeat, chain

from dkit.etl.multi_processing import Coordinator, Worker
from dkit.etl.sink import CsvDictSink
from dkit.etl.writer import FileWriter
from dkit.utilities import log_helper as log


class LookupWorker(Worker):

    @Worker.Bootstrap()
    def __init__(self, delay, *args, **kwargs):
        self.delay = delay

    def run(self):
        for row in self.pull():
            try:
                address = socket.gethostbyname(row["host"])
                time.sleep(self.delay)
            except Exception:
                address = ""
            row["address"] = address
            self.push(row)


if __name__ == "__main__":
    hosts = [
        {"host": "slashdot.org"},
        {"host": "google.com"},
        {"host": "microsoft.com"},
        {"host": "ibm.com"},
    ]

    iter_hosts = chain.from_iterable(repeat(hosts, 1000))
    logger = log.stderr_logger()
    lookups = Coordinator(
        iter_hosts,
        LookupWorker,
        process_count=20,
        logger=logger,
        log_trigger=5,
        worker_args=(0.05,)
    )

    CsvDictSink(
        FileWriter("data/hosts.csv"),
        logger=logger
    ).process(lookups)
