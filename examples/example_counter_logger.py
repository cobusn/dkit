"""
Example use of Counterlogger class
"""
import time
import sys; sys.path.insert(0, "..") # noqa
from dkit.utilities.instrumentation import CounterLogger


t = CounterLogger(__name__, trigger=8).start()
for i in range(100):
    time.sleep(0.01)
    t.increment()
t.stop()

print("Completed {} iterations after {} seconds".format(t, t.seconds_elapsed))
