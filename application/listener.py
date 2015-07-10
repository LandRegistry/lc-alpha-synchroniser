import sys


def message_received(body, message):
    print("Received: " + str(body))
    print("Received Msg: " + str(message))
    message.ack()
    sys.stdout.flush()