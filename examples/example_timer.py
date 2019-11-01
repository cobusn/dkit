"""
Example use of Timer class
"""
import time
import sys; sys.path.insert(0, "..")
from dkit.utilities.instrumentation import Timer

t = Timer().start()
time.sleep(1)
t.stop()
print(t.hms_elapsed)