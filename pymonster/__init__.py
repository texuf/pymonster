from datetime import datetime, timedelta

'''
#Event Example:
from pymonster import EventBase
class Event(EventBase):
    def log(self, msg):
        print '[CustomEventLogger][%s] %s' % (self.collection_name,msg)
        EventBase.log(self, msg)

#Consumer Example
from pymonster import ConsumerBase
class Consumer(ConsumerBase):
    def consume(self, event_instance, event_data):
        print '[CustomConsumer][%s][%s #%d]: %s' % (self.collection_name, event_instance.collection_name, event_data['_id'], str(event_data['msg'])) 
'''


verbose = False
allow_undeclared_events = True

#constants
COUNTER_COLLECTION_NAME = 'event_counters'
CONSUMER_COLLECTION_NAME = 'event_consumers'
CONSUMER_ID_BASE = 'consumer'
CONSUMER_CUSTOM_CLASS_NAME = 'Consumer'
EVENT_COLLECTION_NAME_BASE = 'events'
EVENT_CUSTOM_CLASS_NAME = 'Event'
#globals
g_event_consumers = []

class dbwrapper():
    ''' simple wrapper for throwing errors if the db isn't set up '''
    def __init__(self, value=None):
        self.__db = value
    def _assert_db(self):
        assert self.__db is not None, 'Initialize pymonster db by setting pymonster.db = (your MongoClient instance)'
    def __set__(self, obj, value):
        self.__db  = value
        self.__getattr__ = self.__db.__getattr__
    def __get__(self, obj, objType):
        self._assert_db()
        return self.__db
    def __getitem__(self, name):
        self._assert_db()
        return self.__db.__getitem__(name)
db = dbwrapper()

def default_logger(msg):
    print msg

logger = default_logger

class counter():
    @staticmethod
    def get_next(name):
        #TODO check for errors here
        return int( db[COUNTER_COLLECTION_NAME].find_and_modify(  
                                                          query = { '_id':name }
                                                        , update = { '$inc': { 'next': 1 } }
                                                        , new=True
                                                        , upsert=True
                                                       )['next']
                  )


class PkgExplorer(object):
    ''' finds your packages by dot notation, instanciates instance of target_class_name and returns it. target class should inherit from PkgExplorer '''
    def __init__(self, pkg_name, collection_name, target_class_name, target_base_class, allow_undeclared_modules=False):
        assert pkg_name is not None, 'Specifiy a package as a source of your events'
        assert collection_name is not None, 'Specifiy a base name for your event collectinos, i.e. "events" or "consumer"'
        self.collection_name = collection_name
        self.pkg_name = pkg_name
        self.target_class_name = target_class_name
        self.target_base_class = target_base_class
        self.cache = {}
        self.allow_undeclared_modules = allow_undeclared_modules

    def __getattr__(self, name):
        if name not in self.cache:
            new_pkg_name = '%s.%s' % (self.pkg_name, name)
            new_collection_name = '%s_%s' % (self.collection_name, name)
            try:
                module_module = __import__(new_pkg_name, fromlist='dummy')
            except ImportError as e:
                if not self.allow_undeclared_modules or not allow_undeclared_events:
                    if name in str(e):
                        raise ImportError('No module named %s, declare an event or create a proper module...' % new_pkg_name)
                    else:
                        raise
                b = new_pkg_name.split('.')
                possible_pkgs = ['.'.join(b[i:len(b)]) for i in range(1, len(b))]
                known_error_msg = 'No module named '
                if str(e).replace(known_error_msg, '') not in possible_pkgs:
                    raise #we only want to catch the import error if it's a module that doesn't exist, otherwise reraise
                module_module = None
            if module_module is not None and hasattr(module_module, self.target_class_name):
                self.cache[name] = getattr(module_module, self.target_class_name)(new_pkg_name, new_collection_name)
            else:
                self.cache[name] = self.target_base_class(new_pkg_name, new_collection_name)
        return self.cache[name]


class EventManager(PkgExplorer):
    ''' instanciate to interact with events '''
    def __init__(   self
                    , pkg_name
                    , collection_name_base = EVENT_COLLECTION_NAME_BASE
                ):
        PkgExplorer.__init__(self, pkg_name, collection_name_base, EVENT_CUSTOM_CLASS_NAME, EventBase, allow_undeclared_modules=True)


class EventBase(EventManager):
    ''' derive all "Event" classes from EventBase '''
    def __init__(self, pkg_name, collection_name):
        EventManager.__init__(self, pkg_name, collection_name)

    def log(self, msg):
        if verbose: logger( '[Event][%s] %s' % (self.collection_name,msg) )
        db[self.collection_name].insert(
                {
                    '_id':counter.get_next(self.collection_name)
                    , 'createdAt':datetime.now()
                    , 'msg':msg
                    , 'consumedBy':{}
                }
            )
    def count(self):
        return db[self.collection_name].count()

    def get_next(self, consumer_name):
        consumed_by = 'consumedBy.%s' % consumer_name
        return db[self.collection_name].find_and_modify(
                                            query={
                                                consumed_by:None
                                            }
                                            , update={
                                                '$set':{consumed_by:datetime.now()}
                                            }
                                            , upsert=False
                                            , new=False
                                            , sort=[('_id',1)]
            )


class ConsumerManager(PkgExplorer):
    ''' object for keeping track of event/consumer state '''
    def __init__(   self
                    , pkg_name
                    , collection_name_base = CONSUMER_ID_BASE
                ):
        PkgExplorer.__init__(self, pkg_name, collection_name_base, CONSUMER_CUSTOM_CLASS_NAME, ConsumerBase, allow_undeclared_modules=False)


class ConsumerBase(ConsumerManager):
    ''' derive all "Consumer" classes from ConsumerBase '''
    def __init__(self, pkg_name, collection_name):
        ConsumerManager.__init__(self, pkg_name, collection_name)

    def consume(self, event_instance, event_data):
        logger( '[Consumer][%s][%s #%d]: %s' % (self.collection_name, event_instance.collection_name, event_data['_id'], str(event_data['msg'])) )


def register_events(event_consumers):
    g_event_consumers.extend( event_consumers )



def consume_event(event_instance, consumer_name):
    while(True):
        event_data = event_instance.get_next(consumer_name)
        if event_data is not None:
            logger( 'consuming [%s][%s #%d]' % (consumer_name, event_instance.collection_name, event_data['_id']) )
            yield event_data
        else:
            break

def expire_consumer(event_instance, consumer, log_results=True):
    while(True):
        event_data = event_instance.get_next(consumer.collection_name)
        if event_data is not None:
            if log_results:
                logger( 'Expiring [%s][%s #%d]' % (consumer.collection_name, event_instance.collection_name, event_data['_id']) )
        else:
            break


def consume_events():
    ''' to be run in another process, hopefully another pyhsical machine '''
    for event_consumer in g_event_consumers:
        event_instance = event_consumer[0]
        consumer = event_consumer[1]
        kwargs = event_consumer[2] if len(event_consumer) > 2 else None
        start_time = datetime.now()
        while( (datetime.now() - start_time) < timedelta(minutes=5) ):
            event_data = event_instance.get_next(consumer.collection_name)
            if event_data is not None:
                logger( 'Consuming [%s][%s #%d]' % (consumer.collection_name, event_instance.collection_name, event_data['_id']) )
                if kwargs is not None:
                    consumer.consume(event_instance, event_data, **kwargs)
                else:
                    consumer.consume(event_instance, event_data)
            else:
                break
        #some logging to know if we're not processing things
        if event_data is not None:
            logger('failed to exhaust consumer [%s]' % consumer.collection_name)



def consume_events_loop(sleep_amount=.1):
    ''' consumes events until something bad happens '''
    from time import sleep
    while(True):
        consume_events()
        sleep(sleep_amount)

from threading import Thread, Event

def consume_events_debug_thread():
    ''' consumes events in a back ground thread
    implementation:
    import pymonster
    thread_stopper = pymonster.consume_events_debug_thread()
    thread_stopper.set() #to stop thread
    '''
    import time
    thread_stopper = Event()
    def _internal_consume_events(n, thread_stopper):
        while not thread_stopper.is_set():
            consume_events()
            time.sleep(n)

    t = Thread(target=_internal_consume_events, args=(1,thread_stopper))
    t.start()
    return thread_stopper




