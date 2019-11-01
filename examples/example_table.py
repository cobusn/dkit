import sys
sys.path.insert(0, "..") # noqa
from dkit.doc.document import Table
from dkit.data.fake_helper import persons
from pprint import PrettyPrinter

t = Table(
    persons(10),
    [
        Table.Field("first_name", "first name"),
        Table.Field("last_name", "last name"),
        Table.Field("gender"),
    ]
)

print(str(t))
pp = PrettyPrinter()
pp.pprint(t)
