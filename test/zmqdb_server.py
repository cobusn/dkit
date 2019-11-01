import sys
import pickle
sys.path.insert(0, "..")

from dkit.data.zmqdb import PickleServer
ports = [
    "tcp://10.1.20.207:33333",
    "tcp://127.0.0.1:33321",
    "ipc://33321",
]


def create_data():
    db = {"numbers": {}}
    numbers = db["numbers"]
    for i in range(1000):
        numbers[i] = i
    with open("zmqdb.db", "bw") as wfile:
        pickle.dump(db, wfile)


def serve():
    PickleServer("zmqdb.db", port_list=ports).serve()


if __name__ == "__main__":
    create_data()
    serve()
