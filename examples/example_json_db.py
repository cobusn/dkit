"""
Example: use JSONDB to track completed work in an ETL job.

Each unit of work is identified by a key. The job can be restarted
safely: keys that were already processed are skipped. The per-key
lock protects the "check, then compute, then write" sequence against
concurrent workers sharing the same directory.
"""
import tempfile

from dkit.data.json_db import JSONDB


def process(key: int) -> dict:
    """pretend to do expensive work for a unit of work"""
    return {"key": key, "square": key * key}


with tempfile.TemporaryDirectory() as tmpdir:
    db = JSONDB(tmpdir)

    # first run: process keys 0..4
    for key in range(5):
        skey = str(key)
        with db.lock(skey):
            if skey not in db:
                db[skey] = process(key)

    print(f"entries after first run: {len(db)}")

    # simulate a restart: only the new key (5) is processed,
    # the rest are skipped
    for key in range(6):
        skey = str(key)
        with db.lock(skey):
            if skey not in db:
                print(f"processing key {skey}")
                db[skey] = process(key)
            else:
                print(f"skipping key {skey} (already done)")

    for skey, value in sorted(db.items(), key=lambda kv: int(kv[0])):
        print(skey, value)

    # remove lock files that are no longer held
    db.purge_locks()
