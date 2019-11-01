from dataclasses import dataclass
from dkit.data.map_db import ObjectMap, Object


@dataclass
class Simple(Object):
    a: int
    b: int


m = ObjectMap(Simple)

m["one"] = Simple(1, 1)
m["two"] = Simple(2, 2)


print(m.as_dict())

n = ObjectMap(Simple)
n.from_dict(m.as_dict())

print(n.as_dict())
