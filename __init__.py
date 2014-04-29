from datetime import datetime

'''
#Event Example:
from pymonster import EventBase
class Event(EventBase):
    def log(self, param1, param2):
        assert isinstance(param1, unicode), 'param1 must be unicode'
        EventBase.log(self, {'param1':param1, 'param2':param2})

#Consumer Example
from pymonster import ConsumerBase
class Consumer(ConsumerBase):
    def consume(self, event_instance, event_cursor):
        for event_dict in event_cursor:
            print '[Custom Consumer :)][%s]: %s' % (self.collection_name, str(event_dict['msg'])) 
'''

#constants
COUNTER_COLLECTION_NAME = 'event_counters'
CONSUMER_COLLECTION_NAME = 'event_consumers'
CONSUMER_ID_BASE = 'consumers'
CONSUMER_CUSTOM_CLASS_NAME = 'Consumer'
EVENT_COLLECTION_NAME_BASE = 'events'
EVENT_CUSTOM_CLASS_NAME = 'Event'
#globals
g_event_consumers = []
verbose = False

class dbwrapper():
    ''' simple wrapper for throwing errors if the db isn't set up '''
    def __init__(self, value=None):
        self.__db = value
    def _assert_db(self):
        assert pymongo.db is not None, 'Initialize db by setting pymonster.db = (your MongoClient instance)'
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
    def __init__(self, pkg_name, collection_name, target_class_name, target_base_class):
        assert pkg_name is not None, 'Specifiy a package as a source of your events'
        assert collection_name is not None, 'Specifiy a base name for your event collectinos, i.e. "events"'
        self.collection_name = collection_name
        self.pkg_name = pkg_name
        self.target_class_name = target_class_name
        self.target_base_class = target_base_class
        self.cache = {}

    def __getattr__(self, name):
        if name not in self.cache:
            new_pkg_name = '%s.%s' % (self.pkg_name, name)
            new_collection_name = '%s_%s' % (self.collection_name, name)
            try:
                module_module = __import__(new_pkg_name, fromlist='dummy')
            except ImportError as e:
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
    def __init__(self, pkg_name, collection_name_base=EVENT_COLLECTION_NAME_BASE):
        PkgExplorer.__init__(self, pkg_name, collection_name_base, EVENT_CUSTOM_CLASS_NAME, EventBase)


class EventBase(EventManager):
    ''' derive all "Event" classes from EventBase '''
    def __init__(self, pkg_name, collection_name):
        EventManager.__init__(self, pkg_name, collection_name)

    def log(self, msg):
        if verbose: print '[Event][%s] %s' % (self.collection_name,msg)
        db[self.collection_name].insert(
                {
                    '_id':counter.get_next(self.collection_name),
                    'createdAt':datetime.now(),
                    'msg':msg,
                }
            )

    def find_one(self):
        return db[self.collection_name].find_one( {}, sort=[('_id',-1)] )

    def find_range(self, gt_id, lte_id):
        return db[self.collection_name].find(  {
                                                    '$and':[
                                                        {'_id':{ '$gt':gt_id }}, 
                                                        {'_id':{ '$lte':lte_id }},
                                                    ]
                                                }, sort=[('_id', 1)] )


class ConsumerManager(PkgExplorer):
    ''' object for keeping track of event/consumer state '''
    def __init__(self, pkg_name, collection_name_base=CONSUMER_ID_BASE):
        PkgExplorer.__init__(self, pkg_name, collection_name_base, CONSUMER_CUSTOM_CLASS_NAME, ConsumerBase)


class ConsumerBase(ConsumerManager):
    ''' derive all "Consumer" classes from ConsumerBase '''
    def __init__(self, pkg_name, collection_name):
        ConsumerManager.__init__(self, pkg_name, collection_name)

    def consume(self, event_instance, event_cursor):
        #print '\n'
        #print '[Consumer][%s] %d %s...' % (self.collection_name, event_cursor.count(), event_instance.collection_name)
        for event_dict in event_cursor:
            print '[Consumer][%s]: %s' % (self.collection_name, str(event_dict['msg'])) 

    def find_one(self):
        ''' humble attempt at the beginning of a thread or multi process safe archetecture, not too worried about the queries, we always search for an _id '''
        return db[CONSUMER_COLLECTION_NAME].find_and_modify(    query={ '_id':self.collection_name }
                                                                , update= {
                                                                    '$set':{'consumeStartedAt': datetime.now()}
                                                                }
                                                                , new=True
                                                                , upsert=True
                                                            )
        
    def update(self, data):
        db[CONSUMER_COLLECTION_NAME].update({'_id':self.collection_name}, { '$set':data })


def register_events(event_consumers):
    g_event_consumers.extend( event_consumers )


def consume_events(verbose=True):
    ''' 
        to be run in another process, hopefully another pyhsical machine 
        tip, to test, run in a while loop in a second terminal or run consume_events_debug_thread
    '''
    for event_consumer in g_event_consumers:
        event_instance = event_consumer[0]
        consumer = event_consumer[1]
        frequency = event_consumer[2] if len(event_consumer) > 2 else 0
        consumer_data = consumer.find_one()
        if consumer_data is not None:
            #fun stuff to do here, save consumeStaredAt under your machine name and processid 
            #check for other machines that have this process open
            #if any other machines have had this open for a long time, mark it for investigation, ping machine in charge
            latest_event = event_instance.find_one()
            last_consumed_id = consumer_data.get('lastConsumedEventId', 0)
            if latest_event is not None and latest_event['_id'] > last_consumed_id:
                cursor = event_instance.find_range(last_consumed_id, latest_event['_id'])
                #if we ever go concurrrent we will need a try catch, help us all
                consumer.consume(event_instance, cursor)
                last_consumed_id = latest_event['_id']
            consumer.update(
                    {
                        'consumeCompletedAt': datetime.now()
                        , 'lastConsumedEventId': last_consumed_id
                    }
                )


def consume_events_debug_thread(verbose=True):
    ''' implementation:
    import pymonster
    thread_stopper = pymonster.consume_events_debug_thread()
    thread_stopper.set() #to stop thread
    '''
    from threading import Thread, Event
    import time
    thread_stopper = Event()
    def _internal_consume_events(n, thread_stopper):
        while not thread_stopper.is_set():
            consume_events()
            time.sleep(n)

    t = Thread(target=_internal_consume_events, args=(1,thread_stopper))
    t.start()
    return thread_stopper





