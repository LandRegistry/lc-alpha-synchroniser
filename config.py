

class Config(object):
    DEBUG = False
    APPLICATION_NAME = "lc-synchroniser"


class DevelopmentConfig(Config):
    DEBUG = True
    REGISTER_URI = "http://localhost:5004"
    LEGACY_DB_URI = "http://localhost:5007"
    CASEWORK_API_URI = "http://localhost:5006"
    MQ_USERNAME = "mquser"
    MQ_PASSWORD = "mqpassword"
    MQ_HOSTNAME = "localhost"
    MQ_PORT = "5672"


class PreviewConfig(Config):
    REGISTER_URI = "http://localhost:5004"
    LEGACY_DB_URI = "http://localhost:5007"
    CASEWORK_API_URI = "http://localhost:5006"
    MQ_USERNAME = "mquser"
    MQ_PASSWORD = "mqpassword"
    MQ_HOSTNAME = "localhost"
    MQ_PORT = "5672"
