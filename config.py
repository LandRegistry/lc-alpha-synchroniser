import os


class Config(object):
    DEBUG = False


class DevelopmentConfig(object):
    DEBUG = True
    REGISTER_URI = "http://localhost:5004"
    LEGACY_DB_URI = "http://localhost:5007"
    MQ_USERNAME = "mquser"
    MQ_PASSWORD = "mqpassword"
    MQ_HOSTNAME = "localhost"
    MQ_PORT = "5672"
    MQ_ADMIN_PORT = "15672"