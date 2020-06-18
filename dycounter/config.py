import os

class ConfigManager(object):
    """
    An config manager for loading from environment or default if settings isn't exists
    """

    ENVIRONMENT = os.environ.get("ENVIRONMENT", 'production')
    #time
    DEFAULT_TIMEZONE = os.environ.get("DEFAULT_TIMEZONE", 'Asia/Saigon')
    AGG_BACKFILL = bool(os.environ.get("AGG_BACKFILL") or False)
    AGG_DELAY = int(os.environ.get("AGG_DELAY") or 30)
    BATCH_INTERVAL = int(os.environ.get("BATCH_INTERVAL") or 10)
    BATCH_UNIT = os.environ.get("BATCH_UNIT", 'second')

    #Druid
    FRAUD_DRUID_URL = os.environ.get("FRAUD_DRUID_URL")
    FRAUD_DRUID_HOST = os.environ.get("FRAUD_DRUID_HOST")
    FRAUD_DRUID_PORT = os.environ.get("FRAUD_DRUID_PORT")
    FRAUD_DRUID_SCHEME = os.environ.get("FRAUD_DRUID_SCHEME")
    FRAUD_DRUID_USER = os.environ.get("FRAUD_DRUID_USER")
    FRAUD_DRUID_PASS = os.environ.get("FRAUD_DRUID_PASS")
    FRAUD_DRUID_PATH = os.environ.get("FRAUD_DRUID_PATH")
    FRAUD_DRUID_OMS_SOURCE = os.environ.get("FRAUD_DRUID_OMS_SOURCE", 'fds_transformer_dev')

    #Mongo
    FRAUD_MONGO_USER = os.environ.get("FRAUD_MONGO_USER")
    FRAUD_MONGO_PASS = os.environ.get("FRAUD_MONGO_PASS")
    FRAUD_MONGO_AUTH = os.environ.get("FRAUD_MONGO_AUTH")
    FRAUD_MONGO_MECHANISM = os.environ.get("FRAUD_MONGO_MECHANISM")
    FRAUD_MONGO_HOST = os.environ.get("FRAUD_MONGO_HOST", 'localhost')
    FRAUD_MONGO_PORT = int(os.environ.get("FRAUD_MONGO_PORT") or 27017)
    MONGO_POOL_MAX = int(os.environ.get("MONGO_POOL_MAX") or 100)
    MONGO_POOL_TIMEOUT = int(os.environ.get("MONGO_POOL_TIMEOUT") or 200)
    MONGO_AUTO_CONNECT = bool(os.environ.get("MONGO_AUTO_CONNECT") or False)
    CONFIG_GLOBAL_COL_MG = "config_global"