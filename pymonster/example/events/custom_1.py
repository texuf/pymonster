#Event Example:
from pymonster import EventBase

class Event(EventBase):
    def log(self, msg):
        print '[CustomEventLogger][%s] %s' % (self.collection_name,msg)
        EventBase.log(self, msg)