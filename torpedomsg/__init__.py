try:
    import ujson as json
except ImportError:
    import json

import functools
import struct
import tornado.gen
import tornado.tcpclient
import tornado.tcpserver


__all__ = ('TorpedoFramingMixin', 'TorpedoServer', 'TorpedoClient')
__version__ = '0.1'


class TorpedoFramingMixin(object):
    PACKET_FORMAT = '!L'
    PACKET_SIZE = struct.calcsize(PACKET_FORMAT)

    def _setup(self):
        self._connect_callback = None
        self._disconnect_callback = None
        self._message_callback = None        

    def _connect_handler(self, address, stream):
        self._connect_callback and self._connect_callback(address)

    def _disconnect_handler(self, address, stream):
        self._disconnect_callback and self._disconnect_callback(address)

    def _message_handler(self, address, stream, msg):
        self._message_callback and self._message_callback(address, msg)

    def _encode_msg(self, msg):
        return json.dumps(msg, ensure_ascii=False).encode('utf-8')

    def _decode_body(self, body):
        return json.loads(body.decode('utf-8'))

    def _pack_size(self, size):
        return struct.pack(self.PACKET_FORMAT, size)

    def _unpack_size(self, size):
        return struct.unpack(self.PACKET_FORMAT, size)[0]

    def _pack_msg(self, msg):
        body = self._encode_msg(msg)
        return self._pack_size(len(body)) + body

    def _batch_send_msg(self, streams, msg):
        """
        Send data length and data itself to all streams
        """
        count = 0
        if streams:
            packed_msg = self._pack_msg(msg)
            for stream in streams:
                if stream and not stream.closed():
                    stream.write(packed_msg)
                    count += 1
        return count

    @tornado.gen.coroutine
    def _send_msg(self, stream, msg):
        """
        Send data length and data
        """
        if stream and not stream.closed():
            yield stream.write(self._pack_msg(msg))

    @tornado.gen.coroutine
    def _read_msg(self, stream):
        """
        Read data length and data
        """
        if stream and not stream.closed():
            size = yield stream.read_bytes(self.PACKET_SIZE)
            body = yield stream.read_bytes(self._unpack_size(size))
            raise tornado.gen.Return(self._decode_body(body))

    @tornado.gen.coroutine
    def _handle_stream(self, address, stream):
        """
        Read loop
        """
        stream.set_close_callback(functools.partial(self._disconnect_handler,
                                                    address, stream))
        self.io_loop.add_callback(self._connect_handler, address, stream)
        while not stream.closed():
            try:
                msg = yield self._read_msg(stream)
            except tornado.iostream.StreamClosedError:
                break 
            else:
                self.io_loop.add_callback(self._message_handler, address,
                                          stream, msg)

    def set_connect_callback(self, callback):
        """
        address
        """
        self._connect_callback = callback

    def set_disconnect_callback(self, callback):
        """
        address
        """
        self._disconnect_callback = callback

    def set_message_callback(self, callback):
        """
        address. msg
        """
        self._message_callback = callback


class TorpedoServer(tornado.tcpserver.TCPServer, TorpedoFramingMixin):
    def __init__(self, host, port, *args, **kwargs):
        self._clients = {}
        self._setup()
        super(TorpedoServer, self).__init__(*args, **kwargs)
        self.listen(port, host)

    def _connect_handler(self, address, stream):
        self._clients[address] = stream
        super(TorpedoServer, self)._connect_handler(address, stream)

    def _disconnect_handler(self, address, stream):
        self._clients.pop(address, None)
        super(TorpedoServer, self)._disconnect_handler(address, stream)

    def handle_stream(self, stream, address):
        self.io_loop.add_callback(self._handle_stream, address, stream)

    def send(self, address, msg):
        return self._send_msg(self._clients.get(address), msg)

    def publish(self, msg):
        return self._batch_send_msg(self._clients.values(), msg)


class TorpedoClient(tornado.tcpclient.TCPClient, TorpedoFramingMixin):
    def __init__(self, host, port, *args, **kwargs):
        self._address = (host, port)
        self._closed = False
        self._reconnect_interval = kwargs.pop('reconnect_interval', 1)
        self._stream = None
        self._setup()
        super(TorpedoClient, self).__init__(*args, **kwargs)
        self.io_loop.add_callback(self._connect)

    @tornado.gen.coroutine
    def _connect(self):
        try:
            self._stream = yield self.connect(*self._address)
            yield self._handle_stream(self._address, self._stream)
        except:
            self._stream = None
        if not self._closed:
            self.io_loop.call_later(self._reconnect_interval, self._connect)

    def send(self, msg):
        return self._send_msg(self._stream, msg)

    def close(self):
        self._closed = True
        if self._stream and not self._stream.closed():
            self._stream.close()
