#
# Copyright (C) 2016  Cobus Nel
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#

"""
Data matching classes and routines

=========== =========== =================================================
22 May 2017 Cobus Nel   Created
23 May 2017 Cobus Nel   Added supporting functions and tests.
=========== =========== =================================================
"""
import string

from Levenshtein import ratio
from dkit.data import manipulate as cm
from dkit.utilities import instrumentation as ci, log_helper as clog


class FieldSpec(object):
    """
    Field specification for data linker.

    Is used by DictLinker class

    Args:
        left: field name in left hand table
        right: field name in right hand table
        contribution: relative contribution to total score
        case_sensitive: should matching be case sensitive (default False)
        punctuation: punctuation characters to remove before matching
        whitespace: whitespace characters to remove before matching
        stop_words: list of words to remove before matching
        skip: list of objects to skip for matching (e.g None, NA)
    """
    common_punctuation = string.punctuation
    common_whitespace = string.whitespace

    def __init__(self, left: str, right: str, contribution=1, case_sensitive: bool=False,
                 punctuation: str=common_punctuation, whitespace=common_whitespace,
                 stop_words=None, skip=[None]):
        self.left = left
        self.right = right
        self.case_sensitive = False
        self.contribution = contribution
        self.punctuation = punctuation
        self.stop_words = set(stop_words) if stop_words else None
        self.whitespace = whitespace
        self.skip = set(skip)
        self.data = None
        if self.punctuation is not None:
            self.translate_table = str.maketrans({key: None for key in self.punctuation})
        else:
            self.translate_table = None

        # Index is set by controlling class
        self.index = None
        self.ratio = ratio

    def remove_stopwords(self, the_string):
        """
        remove stopwords and whitespace
        """
        words = the_string.split()
        return "".join([word for word in words if word not in self.stop_words])

    def remove_whitespace(self, the_string):
        """
        remove whitespace
        """
        return the_string.replace(self.whitespace, "")

    def prep_right(self, row):
        """
        return cleaned data for right hand row
        """
        return self.clean_string(row[self.right])

    def prep_left(self, row):
        """
        return cleaned data for left hand row
        """
        return self.clean_string(row[self.left])

    def clean_string(self, the_object):
        """
        clean string according to defined rules
        """
        if the_object not in self.skip:
            if not self.case_sensitive:
                data = str(the_object).lower()
            else:
                data = data
            # remove punctuation
            if self.punctuation is not None:
                data = data.translate(self.translate_table)
            # remove stopwords
            if self.stop_words is not None:
                data = self.remove_stopwords(data)
            # remove whitespace
            if self.whitespace is not None:
                data = self.remove_whitespace(data)
            return data
        else:
            return None

    def score(self, left_row, compare_values):
        """
        score one instance of left and right using
        specified scoring mechanism.
        """
        left_cleaned = self.prep_left(left_row)
        right_cleaned = compare_values[self.index]
        if left_cleaned is None or right_cleaned is None:
            return 0
        else:
            r = self.ratio(left_cleaned, right_cleaned)
            return r * self.contribution


class DictMatcher(object):
    """
    Link two datasets.

    Each dataset should be an iterable of type dictionaries.

    The right hand side are is assumed to be the smaller set and is
    loaded in memory.

    Args:
        left: left hand iterable
        right: right hand iterable
        left_key: key for left hand interable
        right_key: key for right hand iterable
        field_spec: list of FieldSpec instances
        threshold: only return matches above this value (0-100)
        logger: logger instance
        count_trigger: period for triggering a log entry

    Yields:
        dicts with fields "left", "right", "probability"
    """
    def __init__(self, left, right, left_key, right_key, field_spec,
                 threshold: float=92.0, logger=None, count_trigger: int=1000):
        self.left = left
        self.right = right
        self.left_key = left_key
        self.right_key = right_key
        self.field_spec = field_spec
        self.threshold = threshold
        self.right_data = {}
        self.logger = logger if logger else clog.null_logger()
        self.stats = ci.CounterLogger(logger=self.logger, trigger=count_trigger)
        self.__cached = None

    @property
    def matches(self):
        """
        cached matches

        This property will trigger a full and store in memory.
        """
        if self.__cached is None:
            self.__cached = list(self)
        return self.__cached

    def __initialize(self):
        """
        initialize data and prepare right hand data
        """
        self.logger.info("Preparing right hand data for matching..")
        # set index and contribution for each field spec:
        total_contribution = sum([i.contribution for i in self.field_spec])
        for i, field_spec in enumerate(self.field_spec):
            field_spec.index = i
            field_spec.contribution = float(field_spec.contribution) / total_contribution

        # build right hand data
        for row in self.right:
            self.right_data[row[self.right_key]] =\
                [i.prep_right(row) for i in self.field_spec]
        self.logger.info("Preparation complete")

    def __iter__(self):
        """iter"""
        self.__initialize()
        self.logger.info("Start matching")
        self.stats.start()
        for left_row in self.left:
            candidates = []
            for right_key, compare_values in self.right_data.items():
                score = 100.0 * sum(i.score(left_row, compare_values) for i in self.field_spec)
                if score > self.threshold:
                    retval = {
                        "m.left.key": left_row[self.left_key],
                        "m.right.key": right_key,
                        "m.score": score,
                    }
                    candidates.append(retval)
            no_candidates = len(candidates)
            for i, candidate in enumerate(candidates):
                candidate["m.rank"] = "{} of {}".format(i+1, no_candidates)
                yield candidate
            self.stats.increment()
        self.stats.stop()


def inner_join(matches_list: list, left, right, distinct_rows=False):
    """
    generator that yields inner join of matches

    Args:
        matches: matches iterable
        left: iterable of left hand data
        right: iterable of right hand data
        distinct_rows: only return one match per row (best match)
    """
    if not distinct_rows:
        combined = []
        for matches in matches_list:
            combined.extend(matches.matches)
        left_key = matches_list[0].left_key
        left_join = cm.merge(left, combined, [left_key], ["m.left.key"])
        yield from cm.merge(left_join, right, ["m.right.key"], [matches.right_key])
    else:
        raise NotImplementedError()


def unmatched_left(matches_list: list, left):
    """
    iterator unmatched left hand rows.

    Args:
        matches: instance of DictMatcher object
        left: iterable of left hand data

    Yields:
        iterator for unmatched rows
    """
    left_matched = set()
    for matches in matches_list:
        left_matched.update((i["m.left.key"] for i in matches.matches))
        left_key = matches.left_key

    for row in left:
        if row[left_key] not in left_matched:
            yield row


def unmatched_right(matches_list, right):
    """
    iterator for unmatched right hand rows.

    Args:
        matches: instance of DictMatcher opbject
        right: iterable of right hand data

    Yields:
        iterator for unmatched rows.
    """
    right_matched = set()
    for matches in matches_list:
        right_matched.update(set(i["m.right.key"] for i in matches.matches))
        right_key = matches.right_key
    for row in right:
        if row[right_key] not in right_matched:
            yield row
