import zmq
import pickle
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from collections import MutableMapping

logger = logging.getLogger(__name__)


class BaseRequest(object):
    pass


@dataclass
class RequestLookup(BaseRequest):
    """request lookup on key"""
    entity: object
    key: object


@dataclass
class RequestData(BaseRequest):
    """request all data for entity"""
    entity: object


@dataclass
class RequestKeys(BaseRequest):
    """request all keys"""
    entity: object


@dataclass
class RequestState(BaseRequest):
    """request state of entity"""
    entity: object


class RequestKill(BaseRequest):
    """request kill"""
    pass


@dataclass
class Response(object):
    data: object = None
    error: str = None


class ZMQClientServer(ABC):

    socket_type = None

    def __init__(self, port="tcp://127.0.0.1:33321", verbose=False):
        self.port = port
        self.verbose = verbose
        self.connect()

    @abstractmethod
    def connect():
        pass


class Connection(ZMQClientServer):

    socket_type = zmq.REQ

    def __init__(self, port="tcp://127.0.0.1:33321", verbose=False):
        super().__init__(port=port, verbose=verbose)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def request_data(self, entity, key) -> Response:
        req = RequestData(entity)
        self.sock.send(pickle.dumps(req))
        resp = pickle.loads(self.sock.recv())
        return resp

    def request_lookup(self, entity, key) -> Response:
        req = RequestLookup(entity, key)
        self.sock.send(pickle.dumps(req))
        resp = pickle.loads(self.sock.recv())
        return resp

    def request_keys(self, entity) -> Response:
        req = RequestKeys(entity)
        self.sock.send(pickle.dumps(req))
        return pickle.loads(self.sock.recv()).data

    def request_kill(self):
        """send kill request"""
        self.sock.send(pickle.dumps(RequestKill()))
        self.sock.recv()

    def connect(self):
        self.context = zmq.Context()
        self.sock = self.context.socket(self.socket_type)
        self.sock.connect(self.port)

    def close(self):
        self.sock.close()

    def client(self, entity):
        return Client(self, entity)


class Client(MutableMapping):

    def __init__(self, connection, entity):
        self.connection = connection
        self.entity = entity
        self.__keys = None

    def __getitem__(self, key):
        return self.connection.request_lookup(self.entity, key).data

    def __iter__(self):
        if self.__keys is None:
            self.__keys = self.connection.request_keys(self.entity)
        yield from self.__keys

    def __delitem__(self, item):
        pass

    def __setitem__(self, item):
        pass

    def __len__(self):
        if self.__keys is None:
            self.__keys = self.connection.request_keys()
        return len(self.__keys)


class CachedClient(MutableMapping):

    def __init__(self, connection, entity, load_on_start=False):
        self.connection = connection
        self.entity = entity
        self.__keys = None
        self.__data = {}

    def __getitem__(self, key):
        try:
            data = self.__data[key]
        except KeyError:
            data = self.connection.query_data(self.entity, key).data
            self.__data[key] = data
        return data

    def __iter__(self):
        if self.__keys is None:
            self.__keys = self.connection.query_keys(self.entity)
        yield from self.__keys

    def __delitem__(self, item):
        pass

    def __setitem__(self, item):
        pass

    def __len__(self):
        if self.__keys is None:
            self.__keys = self.connection.query_keys()
        return len(self.__keys)


class BaseServer(object):

    socket_type = zmq.REP

    """Simple server"""
    def __init__(self, conn, port_list=None, verbose=False):
        self.conn = conn
        self.data = None
        self.load()
        self.dispatch_map = {
            RequestLookup: self.resp_lookup,
            RequestData: self.resp_data,
            RequestKeys: self.resp_keys,
            RequestKill: self.resp_kill,
            RequestState: self.resp_state,
        }
        self.kill = False
        if port_list is None:
            self.ports = ["tcp://127.0.0.1:33321"]
        else:
            self.ports = port_list
        self.verbose = verbose
        self.connect()

    @abstractmethod
    def load(self):
        """load data"""
        pass

    def connect(self):
        logger.info("starting {} using {}".format(
            self.__class__.__name__,
            self.socket_type
        ))
        self.context = zmq.Context()
        self.sock = self.context.socket(self.socket_type)
        for port in self.ports:
            logger.info(f"binding to port [{port}]")
            self.sock.bind(port)

    @abstractmethod
    def resp_data(self, request):
        """
        return all data for entity
        """
        return Response(None)

    @abstractmethod
    def resp_lookup(self, request):
        """
        return all data for entity
        """
        return Response(None)

    @abstractmethod
    def resp_keys(self, request):
        """
        return all keys for entity
        """
        return Response(None)

    def resp_kill(self, request):
        """
        set kill flag
        """
        logger.info("Received kill request")
        self.kill = True
        return Response("Ok")

    def resp_state(self, request):
        """state of data. 0 means clean"""
        return Response(0)

    def serve(self):
        logger.info("server operational")
        dispatch_map = self.dispatch_map
        while not self.kill:
            try:
                request = pickle.loads(self.sock.recv())
                dispatcher = dispatch_map[request.__class__]
                response = dispatcher(request)
                self.sock.send(pickle.dumps(response))
            except Exception as e:
                logger.error(str(e))


class PickleServer(BaseServer):
    """
    ZMQDB implementation that use pickle as a backend
    """
    def load(self):
        logger.info("Loading pickle file: '{}'".format(self.conn))
        with open(self.conn, "rb") as infile:
            self.data = pickle.load(infile)
        logger.info("File loading completed")

    def resp_data(self, request):
        """
        return all data for entity
        """
        response = Response(
            self.data[request.entity]
        )
        return response

    def resp_lookup(self, request):
        """
        return all data for entity
        """
        response = Response(
            self.data[request.entity][request.key]
        )
        return response

    def resp_keys(self, request):
        """
        return all keys for entity
        """
        response = Response(
            list(self.data[request.entity].keys())
        )
        return response


if __name__ == "__main__":
    import sys
    fname = sys.argv[1]
    PickleServer(fname).serve()
