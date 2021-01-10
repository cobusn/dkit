import zmq
import msgpack
from abc import ABC
from ..exceptions import DKitTimeoutException
from .introspection import is_list
from typing import List, Union


class _ZMQ_Interface(ABC):
    """base class for ZeroMQ interfaces"""
    def __init__(self, ports: Union[List[str], str], kind):
        if is_list(ports):
            self._ports = ports
        else:
            self._ports = [ports]
        self.context = zmq.Context.instance()
        self.kind = kind
        self.socket = self.context.socket(self.kind)
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def connect(self) -> "_ZMQ_Interface":
        for port in self._ports:
            self.socket.connect(port)
        return self

    def bind(self) -> "_ZMQ_Interface":
        for port in self._ports:
            self.socket.bind(port)
        return self

    def __str__(self):
        return f"ZMQ_Interface(port={self._port}, kind={self.kind})"


class _ZMQ_Receiver(_ZMQ_Interface):
    """receive messages"""
    def recv(self, timeout=-1):
        """Make a call and respond"""
        socks = dict(self.poller.poll(timeout=timeout))
        if self.socket in socks:
            data = self.socket.recv()
            return msgpack.unpackb(data, raw=False)
        else:
            raise DKitTimeoutException(f"Timed out after {timeout}.")


class _ZMQ_Sender(_ZMQ_Interface):
    """send messages"""
    def send(self, message):
        self.socket.send(
            msgpack.packb(
                message,
                use_bin_type=True
            )
        )


class Requester(_ZMQ_Interface):
    """request messages and receive a response"""
    def __init__(self, port):
        super().__init__(port, zmq.REQ)

    def request(self, message, timeout=-1):
        """Make a call and respond"""
        self.socket.send(msgpack.packb(message, use_bin_type=True))
        data = self.socket.recv()
        return msgpack.unpackb(data, raw=False)

    def trequest(self, message, timeout=-1):
        """Make a call and respond"""
        self.socket.send(msgpack.packb(message, use_bin_type=True))
        socks = dict(self.poller.poll(timeout=timeout))
        if self.socket in socks:
            data = self.socket.recv()
            return msgpack.unpackb(data, raw=False)
        else:
            raise DKitTimeoutException(f"Timed out after {timeout}")


class Puller(_ZMQ_Receiver):
    """Pull messages"""
    def __init__(self, port):
        super().__init__(port, zmq.PULL)


class Pusher(_ZMQ_Sender):
    """Push messages"""
    def __init__(self, ports):
        super().__init__(ports, zmq.PUSH)


class Responder(_ZMQ_Sender):
    """Respond to requests (zmq.REP)"""

    def __init__(self, ports):
        super().__init__(ports, zmq.REP)

    def recv(self):
        """Make a call and respond"""
        data = self.socket.recv()
        return msgpack.unpackb(data, raw=False)

    def trecv(self, timeout=-1):
        """Make a call and respond"""
        socks = dict(self.poller.poll(timeout=timeout))
        if self.socket in socks:
            data = self.socket.recv()
            return msgpack.unpackb(data, raw=False)
        else:
            raise DKitTimeoutException(f"Timed out after {timeout}")


class Router(Responder):

    def __init__(self, port):
        _ZMQ_Interface.__init__(self, port, zmq.ROUTER)


class Dealer(Requester):

    def __init__(self, port):
        _ZMQ_Interface.__init__(self, port, zmq.ROUTER)
