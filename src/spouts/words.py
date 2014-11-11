from __future__ import absolute_import, print_function, unicode_literals

import itertools
from streamparse.spout import Spout
from pika import BlockingConnection, ConnectionParameters
from pika.exceptions import ConnectionClosed

class WordSpout(Spout):

    def initialize(self, stormconf, context):
        self.m2m_conn = BlockingConnection(
            ConnectionParameters(
                host='192.168.33.10',
                port=5672))
        self.channel = self.m2m_conn.channel()
        self.channel.queue_declare(queue="storm", durable=True)
        self.channel.basic_qos(prefetch_count=1)

    def consume_aos(self, ch, method, properties, body):
        self.log(body)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        self.emit([self.body])

    def next_tuple(self):
        try:
            self.channel.basic_consume(self.consume_aos, queue="storm")
        except ConnectionClosed, e:
            pass

