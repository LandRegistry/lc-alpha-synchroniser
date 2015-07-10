import configparser
import os


directory = os.path.dirname(os.path.realpath(__file__))
filename = os.path.join(directory, os.pardir, 'config.ini')

settings_name = os.environ.get('SETTINGS')
print("Using {} settings".format(settings_name))

config = configparser.ConfigParser()
config.read(filename)
settings = config[settings_name]
hostname = "amqp://{}:{}@{}:{}".format(settings['MQ_USERNAME'], settings['MQ_PASSWORD'],
                                       settings['MQ_HOSTNAME'], settings['MQ_PORT'])