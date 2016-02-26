import os


class Config(object):
    DEBUG = os.getenv('DEBUG', True)
    APPLICATION_NAME = "lc-synchroniser"
    REGISTER_URI = os.getenv('LAND_CHARGES_URL', "http://localhost:5004")
    LEGACY_DB_URI = os.getenv('LEGACY_ADAPTER_URL', "http://10.0.2.2:15007")#"http://localhost:5007")
    CASEWORK_API_URI = os.getenv('CASEWORK_API_URL', "http://localhost:5006")
    MQ_USERNAME = os.getenv("MQ_USERNAME", "mquser")
    MQ_PASSWORD = os.getenv("MQ_PASSWORD", "mqpassword")
    MQ_HOSTNAME = os.getenv("MQ_HOST", "localhost")
    MQ_PORT = os.getenv("MQ_PORT", "5672")

