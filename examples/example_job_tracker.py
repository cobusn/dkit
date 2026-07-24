"""
Demonstrates JobTracker: a journal that records which entries in a
data stream have already been processed, so that a job can be
re-run and skip work it already completed.

- iter_not_completed() filters out entries already marked done
- track() marks an entry complete only if its block succeeds,
  leaving it unmarked (and eligible for retry) if it raises
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import tempfile

from dkit.data.fake_helper import sales_transactions
from dkit.utilities.job_tracker import JobTracker

tracker = JobTracker(
    key=lambda x: x["id"],
    file_name=tempfile.mktemp(),
)

data = list(sales_transactions(20))

# process only the first 5 rows in this run
for row in tracker.iter_not_completed(data[:5]):
    with tracker.track(row):
        pass  # do some heavy computing here

# a later run (or the rest of this one) skips what's done
remaining = list(tracker.iter_not_completed(data))
assert len(remaining) == 15  # five have been completed
