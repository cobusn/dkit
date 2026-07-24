from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from typing import Callable
import threading
import dbm.gnu
from collections.abc import Iterable, Iterator

import filelock


class _BaseJobTracker(ABC):
    """
    Shared logic for journal classes that account for messages
    using a function to compute a unique id.

    Args:
        key: function that return the key for an entry
    """
    def __init__(self, key: Callable):
        """
        Args:
            key: function that computes the unique id for an entry
        """
        self.get_key = key

    def _key_for(self, entry) -> str:
        """derive the gdbm-compatible string key for an entry"""
        return str(self.get_key(entry))

    @abstractmethod
    def complete(self, entry):
        """
        mark an entry as complete

        Args:
            entry: object passed to the key function to derive
                its id
        """

    @abstractmethod
    def is_completed(self, entry) -> bool:
        """
        test if an entry has been completed

        Args:
            entry: object passed to the key function to derive
                its id

        Returns:
            True if the entry has been completed
        """

    def iter_not_completed(self, items: Iterable) -> Iterator:
        for item in items:
            if not self.is_completed(item):
                yield item

    @contextmanager
    def track(self, entry):
        """
        context manager that marks an entry complete only if the
        wrapped block finishes without raising

        Args:
            entry: object passed to the key function to derive
                its id
        """
        yield
        self.complete(entry)

    def __contains__(self, message):
        return self.is_completed(message)

    @abstractmethod
    def __len__(self):
        pass


class JobTracker(_BaseJobTracker):
    """
    Journal class for a single process (or multiple threads within
    the same process).

    The gdbm file is opened once and kept open for the lifetime of
    this instance. Not safe for use from separate, independently
    launched processes sharing the same file_name - use
    MultiProcessJobTracker for that case.

    Args:
        key: function that return the key for an entry
        file_name: path to the gdbm database file
    """
    def __init__(self, key: Callable, file_name: str):
        """
        Args:
            key: function that computes the unique id for an entry
            file_name: path to the gdbm database file, opened
                immediately and kept open for this instance's
                lifetime
        """
        super().__init__(key)
        self.db = dbm.gnu.open(file_name, "c")
        self.lock = threading.Lock()

    def __del__(self):
        if hasattr(self, "db"):
            self.db.close()

    def complete(self, entry):
        key = self._key_for(entry)
        with self.lock:
            self.db[key] = datetime.now().isoformat()
            self.db.sync()

    def is_completed(self, entry) -> bool:
        with self.lock:
            return self._key_for(entry) in self.db

    def __len__(self):
        with self.lock:
            return len(self.db)


class MultiProcessJobTracker(_BaseJobTracker):
    """
    Journal class safe for use across threads, processes and
    independently launched programs sharing the same file_name.

    The gdbm file is opened only for the duration of each
    operation, while a file lock is held, since gdbm itself
    refuses a second concurrent open of the same file.

    Args:
        key: function that return the key for an entry
        file_name: path to the gdbm database file
    """
    def __init__(self, key: Callable, file_name: str):
        """
        Args:
            key: function that computes the unique id for an entry
            file_name: path to the gdbm database file, opened and
                closed for the duration of each operation
        """
        super().__init__(key)
        self.file_name = file_name
        self.lock = filelock.FileLock(f"{file_name}.lock")

    @contextmanager
    def _open_db(self):
        db = dbm.gnu.open(self.file_name, "c")
        try:
            yield db
        finally:
            db.close()

    def complete(self, entry):
        key = self._key_for(entry)
        with self.lock, self._open_db() as db:
            db[key] = datetime.now().isoformat()
            db.sync()

    def is_completed(self, entry) -> bool:
        with self.lock, self._open_db() as db:
            return self._key_for(entry) in db

    def __len__(self):
        with self.lock, self._open_db() as db:
            return len(db)
