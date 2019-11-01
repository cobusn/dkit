import enum
import sys
assert sys.version_info >= (3, 6)

DEFAULT_LOG_TRIGGER = 100_000
DEFAULT_READ_CHUNK_SIZE = 100_000

class MESSAGES(enum.Enum):
    LIST_REQUIRED = "List object required for parameter: ${parameter}"
