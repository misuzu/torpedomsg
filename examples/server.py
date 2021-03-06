import logging
import random
import signal
import tornado.ioloop
import tornado.log
import torpedomsg


tornado.log.enable_pretty_logging()


class LinePublisher(object):
    def __init__(self, host, port):
        self.server = torpedomsg.TorpedoServer(host, port)
        self.server.set_connect_callback(self.connect_callback)
        self.server.set_disconnect_callback(self.disconnect_callback)
        self.server.set_message_callback(self.message_callback)
        self.publish()

    def connect_callback(self, address):
        logging.info('connected: %s:%s', *address)

    def disconnect_callback(self, address):
        logging.info('disconnected: %s:%s', *address)

    def message_callback(self, address, msg):
        logging.info('received %r from %r', msg, address)
        cmd = msg.get('cmd')
        if cmd == 'snapshot':
            data = list(range(1000))
            result = {
                'cmd': cmd,
                'data': data,
            }
            logging.info('%s: %s', cmd, len(data))
            self.server.send(address, result)

    def publish(self):
        data = list(range(random.randrange(300)))
        msg = {
            'cmd': 'updates',
            'data': data,
        }
        data and logging.info('updates: %s', len(data))
        self.server.publish(msg)
        self.server.io_loop.call_later(random.randrange(200, 800) / 1000.0,
                                       self.publish)

if __name__ == '__main__':
    ioloop = tornado.ioloop.IOLoop.instance()
    publisher = LinePublisher('127.0.0.1', 8888)

    def handle_signal(sig, frame):
        logging.warning('received signal: %r', sig)
        ioloop.add_callback_from_signal(ioloop.stop)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    ioloop.start()
