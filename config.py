import os


class Config(object):
    DEBUG = os.getenv('DEBUG', True)
    APPLICATION_NAME = "lc-synchroniser"
    REGISTER_URI = os.getenv('LAND_CHARGES_URL', "http://localhost:5004")
    #LEGACY_DB_URI = os.getenv('LEGACY_ADAPTER_URL', "http://10.0.2.2:15007")
    LEGACY_DB_URI = os.getenv('LEGACY_ADAPTER_URL', "http://localhost:5007")
    CASEWORK_API_URI = os.getenv('CASEWORK_API_URL', "http://localhost:5006")
    AMQP_URI = os.getenv("AMQP_URI", "amqp://mquser:mqpassword@localhost:5672")

