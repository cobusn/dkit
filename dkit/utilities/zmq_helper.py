import zmq
import msgpack
from abc import ABC
from ..exceptions import CkitTimeoutException


class _ZMQ_Interface(ABC):
    """base class for ZeroMQ interfaces"""
    def __init__(self, port, kind):
        self._port = port
        self._context = zmq.Context.instance()
        self._kind = kind
        self._sock = self._context.socket(self._kind)
        self._poller = zmq.Poller()
        self._poller.register(self._sock, zmq.POLLIN)

    def connect(self) -> "_ZMQ_Interface":
        self._sock.connect(self._port)
        return self

    def bind(self) -> "_ZMQ_Interface":
        self._sock.bind(self._port)
        return self

    def __str__(self):
        return f"ZMQ_Interface(port={self._port}, kind={self._kind})"


class _ZMQ_Receiver(_ZMQ_Interface):
    """receive messages"""
    def recv(self, timeout=-1):
        """Make a call and respond"""
        socks = dict(self._poller.poll(timeout=timeout))
        if self._sock in socks:
            data = self._sock.recv()
            return msgpack.unpackb(data, raw=False)
        else:
            raise CkitTimeoutException(f"Timed out after {timeout}.")


class _ZMQ_Sender(_ZMQ_Interface):
    """send messages"""
    def send(self, message):
        self._sock.send(
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
        self._sock.send(msgpack.packb(message, use_bin_type=True))
        socks = dict(self._poller.poll(timeout=timeout))
        if self._sock in socks:
            data = self._sock.recv()
            return msgpack.unpackb(data, raw=False)
        else:
            raise CkitTimeoutException(f"Timed out after {timeout}")


class Puller(_ZMQ_Receiver):
    """Pull messages"""
    def __init__(self, port):
        super().__init__(port, zmq.PULL)


class Pusher(_ZMQ_Sender):
    """Push messages"""
    def __init__(self, port):
        super().__init__(port, zmq.PUSH)


class Responder(_ZMQ_Sender):
    """Respond to requests (zmq.REP)"""

    def __init__(self, port):
        super().__init__(port, zmq.REP)

    def recv(self, timeout=-1):
        """Make a call and respond"""
        socks = dict(self._poller.poll(timeout=timeout))
        if self._sock in socks:
            data = self._sock.recv()
            return msgpack.unpackb(data, raw=False)
        else:
            raise CkitTimeoutException(f"Timed out after {timeout}")
