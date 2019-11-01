from dataclasses import dataclass, asdict
from dkit.data.map_db import ObjectMapDB, Object


@dataclass
class Simple(Object):
    a: int
    b: int

    def as_dict(self):
        return asdict(self)


d = ObjectMapDB(schema={"items":  Simple})

d.items["one"] = Simple(1, 1)
d.items["two"] = Simple(2, 2)

print(d.as_dict())
