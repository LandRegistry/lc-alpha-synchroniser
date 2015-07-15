from application import app
from application.listener import message_received, listen
import kombu
from kombu.common import maybe_declare
from amqp import AccessRefused
import sys
from flask import Response
from log.logger import logger


def setup_incoming(hostname):
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
        logger.error("Access Refused")
    logger.debug("queue name, exchange, binding_key: {}, {}, {}".format(queue.name, queue.exchange, queue.routing_key))

    consumer = kombu.Consumer(channel, queues=queue, callbacks=[message_received], accept=['json'])
    consumer.consume()

    logger.debug('channel_id: {}'.format(consumer.channel.channel_id))
    logger.debug('queue(s): {}'.format(consumer.queues))
    return connection, consumer


def setup_error_queue(hostname):
    connection = kombu.Connection(hostname=hostname)
    connection.connect()

    exchange = kombu.Exchange(type="direct", name="synchroniser.error")
    channel = connection.channel()
    exchange.maybe_bind(channel)
    maybe_declare(exchange, channel)

    producer = kombu.Producer(channel, exchange=exchange, routing_key='sync_error')

    logger.debug('channel_id: {}'.format(producer.channel.channel_id))
    logger.debug('exchange: {}'.format(producer.exchange.name))
    logger.debug('routing_key: {}'.format(producer.routing_key))
    logger.debug('serializer: {}'.format(producer.serializer))
    return connection, producer


def run():
    logger.info("Synchroniser started")
    hostname = "amqp://{}:{}@{}:{}".format(app.config['MQ_USERNAME'], app.config['MQ_PASSWORD'],
                                           app.config['MQ_HOSTNAME'], app.config['MQ_PORT'])
    incoming_connection, incoming_consumer = setup_incoming(hostname)
    error_connection, error_producer = setup_error_queue(hostname)

    listen(incoming_connection, error_producer)
    incoming_consumer.close()

@app.route('/', methods=["GET"])
def root():
    return Response(status=200)