from dkit.etl import source, sink
from dkit.data.containers import DictionaryEmulator
from ..parsers.infix_parser import ExpressionParser
from typing import Dict, List


class Dimension(DictionaryEmulator):
    """Represent a data dimension

    This class is used in conjunction with Normalizer do not
    instantiate directly.

    Args:
        - endpoint: sink endpoint
        - fact_keys: mapping of fact keys to row keys
        - fact_fields: mapping of fact_fields to row fields
        - load: attempt to load endpoint on startup for multiple runs
    """
    def __init__(self, endpoint, fact_keys, fact_fields, load=True):
        super().__init__({})
        self.endpoint = endpoint
        self.fact_keys = fact_keys
        self.fact_fields = fact_fields
        self.parsed_keys = {k: ExpressionParser(v) for k, v in self.fact_keys.items()}
        self.parsed_facts = {k: ExpressionParser(v) for k, v in self.fact_fields.items()}
        self.load = load
        self.fkeys = list(self.fact_keys.keys())
        self.ffields = list(self.fact_fields.keys())

    def load_file(self):
        """Called internally

        Load pre existing dimension data if it exists and load is
        specified
        """
        if self.load:
            try:
                if len(self) == 0:
                    with source.load(self.endpoint) as src:
                        for row in src:
                            k = tuple(row[i] for i in self.fact_keys.keys())
                            v = {k: row[k] for k in self.fact_fields.keys()}
                            self[k] = v
            except FileNotFoundError:
                pass

    def fetch(self, row):
        """return row by looking up keys in row provided"""
        return self[tuple(row[k] for k in self.fact_keys.values())]

    def update_row(self, row):
        """Update dimension and return new keys

        Update index with another row
        """
        key = tuple(self.parsed_keys[k](row) for k in self.fkeys)
        values = {k: self.parsed_facts[k](row) for k in self.ffields}
        if key not in self.store:
            self.store[key] = values
        return {k: self.parsed_keys[k](row) for k in self.fkeys}

    def iter_rows(self):
        """
        Iterates through keys and values while combining

        yields a dictionary
        """
        for k, v in self.items():
            r = dict(v)
            r.update(zip(self.fact_keys.keys(), k))
            yield r

    def sync(self):
        """Sync data with endpoint file"""
        with sink.load(self.endpoint) as snk:
            snk.process(self.iter_rows())

    def as_dict(self):
        """return configuraiton as dictionary"""
        return {
            "endpoint": self.endpoint,
            "fact_keys": self.fact_keys,
            "fact_fields": self.fact_fields,
            "load": self.load,
        }


class Normalizer(DictionaryEmulator):
    """Normalize data

    Normalize data by stripping recurring information out to
    supplementary fact files

    This class can also be used to recombine the fact data

    Args:
        fields: dict
    """
    def __init__(self, fields: Dict[str, str] = None,
                 dimensions: Dict[str, Dimension] = None):
        dimensions = dimensions if dimensions else dict()
        super().__init__(**dimensions)
        self.fields = fields if fields else {}

    def recombine(self, src, dimensions: List[str]):
        """recombine dimensions with master data"""
        dlist = []
        for d in dimensions:
            self[d].load()
            dlist.append(self[d])

        for row in src:
            for d in dlist:
                row.update(d.fetch(row))
                yield row

    def normalize(self, src):
        dimensions = list(self.values())
        exp = {k: ExpressionParser(v) for k, v in self.fields.items()}
        # load existing data
        for d in dimensions:
            d.load_file()

        # iterate through data
        for row in src:
            output = {k: exp[k](row) for k in self.fields.keys()}
            for dimension in dimensions:
                output.update(dimension.update_row(row))
            yield output

        # save dimensions
        for dimension in self.values():
            dimension.sync()

    def as_dict(self):
        """return configuraiton as map"""
        rv = {
            "fields": self.fields,
            "dimensions": {}
        }
        for k, v in self.items():
            rv["dimensions"][k] = v.as_dict()
        return rv

    @classmethod
    def from_dict(cls, map):
        """instantiate from map"""
        fields = map["fields"]
        dimensions = {
            k: Dimension(**v)
            for k, v in map["dimensions"].items()
        }
        return cls(fields, dimensions)
