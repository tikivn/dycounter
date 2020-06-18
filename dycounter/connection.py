import sys
sys.path.append("..")
from pymongo import MongoClient
from pydruid.client import PyDruid
from dycounter.config import ConfigManager as cm

class Connector(object):
    @staticmethod
    def init_druid():
        fraud_druid = PyDruid(cm.FRAUD_DRUID_URL, cm.FRAUD_DRUID_PATH)
        fraud_druid.set_basic_auth_credentials(cm.FRAUD_DRUID_USER, cm.FRAUD_DRUID_PASS)
        return fraud_druid

    @staticmethod
    def init_mongo():
        return MongoClient(cm.FRAUD_MONGO_HOST, port=cm.FRAUD_MONGO_PORT)
        # return MongoClient(
        #     cm.FRAUD_MONGO_HOST,
        #     username=cm.FRAUD_MONGO_USER,
        #     password=cm.FRAUD_MONGO_PASS,
        #     authSource=cm.FRAUD_MONGO_AUTH,
        #     authMechanism=cm.FRAUD_MONGO_MECHANISM,
        #     maxPoolSize=cm.MONGO_POOL_MAX,
        #     waitQueueTimeoutMS=cm.MONGO_POOL_TIMEOUT,
        #     connect=cm.MONGO_AUTO_CONNECT
        # )