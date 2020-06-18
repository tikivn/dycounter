from dycounter.connection import Connector
from queue import Queue
import time

class SinglePool(object):
    unlocked = Queue()
    
    def validate(self, obj):
        return True

    def create(self): pass

    def get(self):
        while not self.unlocked.empty():
            obj = self.unlocked.get()
            if self.validate(obj):
                return obj
        return self.create()

    def put(self, obj):
        self.unlocked.put(obj)


         
class MongoReusable(SinglePool):

    def create(self):
        return Connector.init_mongo()
