from application import app
from application.listener import message_received, listen, error_received, listen_for_errors
import kombu
from kombu.common import maybe_declare
from amqp import AccessRefused
import sys
from flask import Response
from log.logger import logger
import requests
from requests.auth import HTTPBasicAuth
import json


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


def setup_error_incoming(hostname):
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
        logger.error("Access Refused")
    logger.debug("queue name, exchange, binding_key: {}, {}, {}".format(queue.name, queue.exchange, queue.routing_key))

    consumer = kombu.Consumer(channel, queues=queue, callbacks=[error_received], accept=['json'])
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

    logger.debug('channel_id: {}; exchange: {}; routing_key: {}'.format(producer.channel.channel_id,
                                                                        producer.exchange.name,
                                                                        producer.routing_key))
    return connection, producer


def run():
    logger.info("Synchroniser started")
    hostname = "amqp://{}:{}@{}:{}".format(app.config['MQ_USERNAME'], app.config['MQ_PASSWORD'],
                                           app.config['MQ_HOSTNAME'], app.config['MQ_PORT'])
    incoming_connection, incoming_consumer = setup_incoming(hostname)
    error_connection, error_producer = setup_error_queue(hostname)

    listen(incoming_connection, error_producer)
    incoming_consumer.close()


def error_run():
    logger.info("Synchroniser Error-Watch Started")
    hostname = "amqp://{}:{}@{}:{}".format(app.config['MQ_USERNAME'], app.config['MQ_PASSWORD'],
                                           app.config['MQ_HOSTNAME'], app.config['MQ_PORT'])
    incoming_connection, incoming_consumer = setup_error_incoming(hostname)

    listen_for_errors(incoming_connection)
    incoming_consumer.close()


@app.route('/', methods=["GET"])
def root():
    logger.info("GET /")
    return Response(status=200)


# INTERIM CODE HERE
# This whole having a listener inside the application that issues the errors is just so
# we can do something with them. For Alpha, actually handling the errors isn't being covered.
@app.route('/queue/error', methods=['GET'])
def get_errors():
    logger.debug("GET on /queue/error")
    data = open("temp.txt", 'r').read()
    data = data.strip()
    data = "[{}]".format(",".join(data.split("\n")))
    return Response(data, status=200, mimetype='application/json')