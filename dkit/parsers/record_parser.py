import datetime
from . import helpers

STATE_BEFORE_START = 0
STATE_START_RECORD = 1
STATE_END_RECORD = -1


class AbstractRecordParser(object):

    def __init__(self, file_obj, rules):
        self.scanner = helpers.MatchScanner(rules)
        self.file_obj = file_obj
        self.current_record = {}
        self.last_key = ""
        self.state = STATE_BEFORE_START

    def start_record(self, match):
        """start of record"""
        self.current_record = {}
        self.state = STATE_START_RECORD
        self.last_type = match.groups()[0]
        self.last_key = match.groups()[1]

    def end_record(self, match):
        """adjust state for end of record"""
        self.state = STATE_END_RECORD

    def process_bool(self, match):
        """process boolean value"""
        data = match.groups()
        if data[1] == "false":
            self.current_record[data[0]] = False
        else:
            self.current_record[data[0]] = True

    def process_float(self, match):
        """process float value"""
        data = match.groups()
        self.current_record[data[0]] = float(data[1])

    def process_int(self, match):
        """process integer value"""
        data = match.groups()
        self.current_record[data[0]] = int(data[1])

    def process_str(self, match):
        """process string"""
        data = match.groups()
        self.current_record[data[0]] = data[1]

    def process_date(self, match):
        """process date"""
        data = match.groups()
        self.current_record[data[0]] = datetime.date(
            int(data[1]),
            int(data[2]),
            int(data[3])
        )

    def process_datetime(self, match):
        """process datetime"""
        data = match.groups()
        self.current_record[data[0]] = datetime.datetime(
            int(data[1]),
            int(data[2]),
            int(data[3]),
            int(data[4]),
            int(data[5]),
            int(data[6]),
        )

    def process_time(self, match):
        """process time"""
        data = match.groups()
        self.current_record[data[0]] = datetime.time(
            int(data[1]),
            int(data[2]),
            int(data[3]),
        )

    def __iter__(self):
        """iter through values"""
        for line in self.file_obj:
            self.scanner.scan(line)
            if self.state == STATE_END_RECORD:
                # note need to create a new instance otherwise all will be the last
                # one
                yield dict(self.current_record)
                self.state = STATE_BEFORE_START
