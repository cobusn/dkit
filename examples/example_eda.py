"""Example usage of eda (Exploratory Data Analysis)"""
import sys; sys.path.insert(0, "..")  # noqa
from dkit.data.eda import SchemaMap
from dkit.etl import source

schema_map = SchemaMap(depth=800)
# fname = "data/nottem_temp.jsonl"
# fname = "data/acv.pkl.xz"
fname = "data/titanic.csv"

with source.load(fname) as data:
    schema_map.save_plot("struc_map.pdf", data)
