from pymonster import ConsumerBase

class Consumer(ConsumerBase):
    def consume(self, event_instance, event_data):
        print '[CustomConsumer][%s][%s #%d]: %s' % (self.collection_name, event_instance.collection_name, event_data['_id'], str(event_data['msg'])) 