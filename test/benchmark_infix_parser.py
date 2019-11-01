'''
Transform CSV data according to template provided.
}'''
import sys
import csv
from datetime import datetime
sys.path.insert(0, "../")
from dkit.utilities.instrumentation import Timer
from dkit.parsers.infix_parser import InfixParser


class Benchmark(object):

    def __init__(self):
        self.benchmark = "benchmarks/infix_parser_benchmark.csv"
        self.iteratons = 100000
        self.parser = InfixParser()
        self.parser.parse("(10 + ${X}^3 / sin(${X}) + 20 + ${X})^3")
        self.parser.variables['X'] = 3

    def execute(self):
        """
        Execute the transformation.
        """
        timer = Timer()
        msg = "Processed %d iterations in %.2f seconds at %.2f iterations per second."
        records_processed = 0

        timer.start()
        while records_processed < self.iteratons:
            self.parser.eval()
            records_processed += 1
        timer.stop()

        sys.stderr.write(msg % (
            records_processed, timer.seconds_elapsed, records_processed/timer.seconds_elapsed)
        )

        with open(self.benchmark, "a") as recorderfile:
            recorder = csv.writer(recorderfile)
            recorder.writerow(
                [datetime.now(), records_processed, timer.seconds_elapsed,
                 records_processed/timer.seconds_elapsed]
            )
            sys.exit(0)


def main():
    """
    Main entry point
    """
    driver = Benchmark()
    driver.execute()


if __name__ == "__main__":
    main()
