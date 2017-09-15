import logging
import signal
import tornado.ioloop
import tornado.log
import torpedomsg


tornado.log.enable_pretty_logging()


class LineReader(object):
    def __init__(self, host, port):
        self.client = torpedomsg.TorpedoClient(host, port)
        self.client.set_connect_callback(self.connect_callback)
        self.client.set_disconnect_callback(self.disconnect_callback)
        self.client.set_message_callback(self.message_callback)

    def connect_callback(self, address):
        logging.info('connected: %s:%s', *address)
        self.client.send({'cmd': 'snapshot'})

    def disconnect_callback(self, address):
        logging.info('disconnected: %s:%s', *address)

    def message_callback(self, address, msg):
        cmd = msg.get('cmd')
        data = msg.get('data')
        if cmd == 'updates' or cmd == 'snapshot':
            logging.info('%s: %s', cmd, len(data))


if __name__ == '__main__':
    ioloop = tornado.ioloop.IOLoop.instance()
    reader = LineReader('127.0.0.1', 8888)

    def handle_signal(sig, frame):
        logging.warning('received signal: %r', sig)
        ioloop.add_callback_from_signal(ioloop.stop)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    ioloop.start()
