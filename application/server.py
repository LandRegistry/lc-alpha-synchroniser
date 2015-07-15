from application import app
from application.listener import message_received, listen
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
    logger.info("GET /")
    return Response(status=200)

@app.route('/queues/error', methods=["GET"])
def error_queue():
    logger.debug("GET queues/error")
    uri = "http://localhost:{}/api/queues/%2F/sync_error".format(app.config["MQ_ADMIN_PORT"])
    auth = HTTPBasicAuth(app.config['MQ_USERNAME'], app.config['MQ_PASSWORD'])
    response = requests.get(uri, auth=auth)

    if response.status_code == 200:
        queue_data = response.json()
        data = {
            "queue_length": queue_data["messages"]
        }
        return Response(json.dumps(data), status=200)

    else:
        data = {
            "api_status": response.status_code
        }
        return Response(json.dumps(data), status=500)
