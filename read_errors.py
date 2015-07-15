import kombu
from kombu.common import maybe_declare
from amqp import AccessRefused
import sys

def message_received(body, message):
    print('------------ Error Retrieved ------------')
    print(body)
    message.ack()

hostname = "amqp://mquser:mqpassword@localhost:5672"
connection = kombu.Connection(hostname=hostname)
connection.connect()

exchange = kombu.Exchange(type="direct", name="synchroniser.error")

channel = connection.channel()

exchange.maybe_bind(channel)
maybe_declare(exchange, channel)

queue = kombu.Queue(name='sync_error', exchange=exchange, routing_key='sync_error')
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

while True:
    try:
        connection.drain_events()
    except KeyboardInterrupt:
        print("Interrupted")
        break