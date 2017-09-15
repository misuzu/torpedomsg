import cbor
import functools
import logging
import struct
import tornado.gen
import tornado.tcpclient
import tornado.tcpserver


__all__ = ('TorpedoFramingMixin', 'TorpedoServer', 'TorpedoClient')
__version__ = '0.7'


class TorpedoFramingMixin(object):
    PACKET_FORMAT = '!L'
    PACKET_SIZE = struct.calcsize(PACKET_FORMAT)
    PACKET_SIZE_LIMIT = 2 ** 24

    def _setup(self):
        self._connect_callback = None
        self._disconnect_callback = None
        self._message_callback = None

    def _connect_handler(self, address, stream):
        try:
            self._connect_callback and self._connect_callback(address)
        except Exception:
            logging.exception('error in connect_handler')

    def _disconnect_handler(self, address, stream):
        try:
            self._disconnect_callback and self._disconnect_callback(address)
        except Exception:
            logging.exception('error in disconnect_handler')

    def _message_handler(self, address, stream, msg):
        try:
            self._message_callback and self._message_callback(address, msg)
        except Exception:
            logging.exception('error in message_handler')

    def _encode_msg(self, msg):
        return cbor.dumps(msg)

    def _decode_body(self, body):
        return cbor.loads(body)

    def _pack_size(self, size):
        return struct.pack(self.PACKET_FORMAT, size)

    def _unpack_size(self, size):
        return struct.unpack(self.PACKET_FORMAT, size)[0]

    def _pack_msg(self, msg):
        body = self._encode_msg(msg)
        size = len(body)
        if size > self.PACKET_SIZE_LIMIT:
            raise ValueError('PACKET_SIZE_LIMIT reached')
        return self._pack_size(size) + body

    def _batch_send_msg(self, streams, msg):
        """
        Send data length and data itself to all streams
        """
        count = 0
        if streams:
            packed_msg = self._pack_msg(msg)
            for stream in streams:
                if stream is not None and not stream.closed():
                    stream.write(packed_msg, lambda: None)
                    count += 1
        return count

    def _send_msg(self, stream, msg):
        """
        Send data length and data
        """
        if stream is not None and not stream.closed():
            stream.write(self._pack_msg(msg), lambda: None)

    @tornado.gen.coroutine
    def _read_msg(self, stream):
        """
        Read data length and data
        """
        if stream is not None:
            packed_size = yield stream.read_bytes(self.PACKET_SIZE)
            size = self._unpack_size(packed_size)
            if size > self.PACKET_SIZE_LIMIT:
                raise ValueError('PACKET_SIZE_LIMIT reached')
            body = yield stream.read_bytes(size)
            return self._decode_body(body)

    @tornado.gen.coroutine
    def _magic_check(self, stream):
        magic = ('%s-%s' % (__name__, __version__)).encode('utf-8')
        try:
            stream.write(magic, lambda: None)
            remote_magic = (yield stream.read_bytes(len(magic)))
            if remote_magic == magic:
                return True
            logging.warning('magic mismatch %r != %r', remote_magic, magic)
        except tornado.iostream.StreamClosedError:
            pass
        return False

    @tornado.gen.coroutine
    def _handle_stream(self, address, stream):
        """
        Read loop
        """
        stream.set_nodelay(True)
        if not (yield self._magic_check(stream)):
            stream.close()
            return
        stream.set_close_callback(functools.partial(self._disconnect_handler,
                                                    address, stream))
        self._connect_handler(address, stream)
        while not stream.closed():
            try:
                msg = yield self._read_msg(stream)
            except tornado.iostream.StreamClosedError:
                break
            except ValueError:
                logging.exception('Invalid data received. Closing connection')
                stream.close()
                break
            self._message_handler(address, stream, msg)

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
        return self._handle_stream(address, stream)

    def send(self, address, msg):
        self._send_msg(self._clients.get(address), msg)

    def publish(self, msg):
        return self._batch_send_msg(self._clients.values(), msg)

    def close(self):
        self.stop()
        for stream in self._clients.values():
            stream.close()


class TorpedoClient(tornado.tcpclient.TCPClient, TorpedoFramingMixin):
    def __init__(self, host, port, *args, **kwargs):
        self._address = (host, port)
        self._closed = False
        self._reconnect_interval = kwargs.pop('reconnect_interval', 1)
        self._stream = None
        self._setup()
        super(TorpedoClient, self).__init__(*args, **kwargs)
        self.io_loop.add_callback(self._connect)

    def _check_socket_valid(self, socket):
        """
        http://sgros.blogspot.com/2013/08/tcp-client-self-connect.html
        """
        return socket.getpeername() != socket.getsockname()

    @tornado.gen.coroutine
    def _connect(self):
        try:
            self._stream = yield self.connect(*self._address)
        except Exception:
            # ignore connection issues
            pass
        if self._stream is not None:
            if self._check_socket_valid(self._stream.socket):
                yield self._handle_stream(self._address, self._stream)
            self._stream.close()
            self._stream = None
        if not self._closed:
            self.io_loop.call_later(self._reconnect_interval, self._connect)

    def connected(self):
        return self._stream is not None

    def send(self, msg):
        self._send_msg(self._stream, msg)

    def close(self):
        self._closed = True
        if self._stream is not None:
            self._stream.close()
            self._stream = None
