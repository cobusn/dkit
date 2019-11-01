"""
Example use of Pivot class
"""
import time
import sys; sys.path.insert(0, "..")

import tabulate
from dkit.data.manipulate import Pivot

the_data = [
            { "year": 1999, "name": "John", "id": 1, "points": 10},
            { "year": 1998, "name": "Andy", "id": 2, "points": 20},
            { "year": 1999, "name": "John", "id": 1, "points": 15},
            { "year": 1997, "name": "Susan", "id": 3, "points": 16},
    ]

p = Pivot(the_data, ["id", "name"], "year", "points", sum)

col_headings = p.column_headings
row_headings = p.row_headings
print(tabulate.tabulate(list(p), headers="keys"))


