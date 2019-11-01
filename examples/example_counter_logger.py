"""
Example use of Counterlogger class
"""
import time
import sys; sys.path.insert(0, "..")
from dkit.utilities.instrumentation import CounterLogger
from dkit.utilities.log_helper import stderr_logger

t = CounterLogger(stderr_logger(), trigger=8).start()
for i in range(100):
    time.sleep(0.01)
    t.increment()
t.stop()
print("Completed {} iterations after {} seconds".format(t, t.seconds_elapsed))
