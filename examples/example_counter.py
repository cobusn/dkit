"""
Example use of counter class
"""
import sys; sys.path.insert(0, "..")
from dkit.utilities.instrumentation import Counter

c = Counter()
for i in range(300):
    c.increment()

print(c.value)
print(c)