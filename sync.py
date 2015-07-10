import kombu
from kombu.common import maybe_declare
from amqp import AccessRefused
import sys
import configparser
import os


def message_received(body, message):
    print("Received: " + str(body))
    print("Received Msg: " + str(message))
    message.ack()
    sys.stdout.flush()


settings_name = os.environ.get('SETTINGS')
print("Using {} settings".format(settings_name))
config = configparser.ConfigParser()
config.read('config.ini')
settings = config[settings_name]
hostname = "amqp://{}:{}@{}:{}".format(settings['MQ_USERNAME'], settings['MQ_PASSWORD'],
                                       settings['MQ_HOSTNAME'], settings['MQ_PORT'])


connection = kombu.Connection(hostname=hostname)
connection.connect()

exchange = kombu.Exchange(type="topic", name="new.bankruptcy")

channel = connection.channel()

exchange.maybe_bind(channel)
maybe_declare(exchange, channel)

queue = kombu.Queue(name='simple', exchange=exchange, routing_key='#')
queue.maybe_bind(channel)
try:
    queue.declare()
except AccessRefused:
    print("Access Refused", file=sys.stderr)
print("queue name, exchange, binding_key: {}, {}, {}".format(queue.name, queue.exchange, queue.routing_key))

consumer = kombu.Consumer(channel, queues=queue, callbacks=[message_received], accept=['json'])
consumer.consume()

print('channel_id: {}'.format(consumer.channel.channel_id))
print('queue(s): {}'.format(consumer.queues))

print('Consuming')
while True:
    try:
        connection.drain_events()
    except KeyboardInterrupt:
        print("Interrupted")
        break
consumer.close()