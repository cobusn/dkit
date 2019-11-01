import json
import pickle
from dataclasses import dataclass, asdict
from dkit.data.map_db import FileObjectMapDB, Object

@dataclass
class Simple(Object):
    a: int
    b: int

    def as_dict(self):
        return asdict(self)


# using Json
d = FileObjectMapDB(schema={"items":  Simple}, codec=json)
d.items["one"] = Simple(1,1)
d.items["two"] = Simple(2,2)
d.save("test.json")
d.load("test.json")

# using pickle
d = FileObjectMapDB(schema={"items":  Simple}, codec=pickle, binary=True)
d.items["one"] = Simple(1,1)
d.items["two"] = Simple(2,2)
d.save("test.pickle")
d.load("test.pickle")

print(d.as_dict())
