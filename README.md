pymonster
=========

PyMonster is a flexible python event logging and consuming library. It is an implementation of the description of an event system used at Stripe called Monster outlined in this talk: [Theres a Monster in my Closet]( http://www.mongodb.com/presentations/theres-monster-my-closet-architecture-mongodb-powered-event-processing-system)

##Requirements
+ [MongoDb](http://docs.mongodb.org/manual/tutorial/getting-started/) or [MongoMock](https://github.com/vmalloc/mongomock)
+ [Python 2.7](https://www.python.org/download)
+ pip ( $ easy_install pip )
+ virtualenv ( $ pip install virtualenv )



##Example

###Setup (shell commands)
These commands make a directory called pymonster-test, setup a virtual python environment via virtualenv, and install the necessary dependancies, and open a python terminal

    mkdir pymonster-test
    cd pymonster-test
    virtualenv venv --distribute
    source venv/bin/activate
    pip install pymongo mongomock
    pip install -e git://github.com/texuf/pymonster/#egg=pymonster
    python

###Logging Events
Now we're going to instantiate our event manager, point it to some of our example classes and start logging events

    import os
    import pymonster
    import pymongo as mongo
    #import mongomock as mongo #use this if you want to use mongomock instead of a running mongo instance
    
    #set verbosity for the demo
    pymonster.verbose = True

    #initialize your db
    db_uri = os.environ.get('MONGO_URI', 'mongodb://localhost')
    db_name = os.environ.get('MONGO_DB', 'pymonster-test')
    pymonster.db = mongo.MongoClient(db_uri)[db_name]
    
    #create some events
    events = pymonster.EventManager(
            pkg_name='pymonster.example.events'
        )
    events.event_1.log('a really cool message')
    events.user.bought_shirt.log({'user':'gordon','quantity':2})

You should see this output:

    [Event][events_event_1] a really cool message
    [Event][events_user_bought_shirt] {'user': 'gordon', 'quantity': 2}

And, if you take a peek in the mongo shell you will see these events in separate collections in your database. Take note, we don't have to create any classes to be able to use these events, but we can!

###Custom Events

When we created our events object, we used the __pkg_name="pymonster.example.events"__ in this folder is a file called **custom\_1** which contains an Event class with a custom log message. 

    #source from: /pymonster/example/events/custom_1.py
    from pymonster import EventBase
    
    class Event(EventBase):
        def log(self, msg):
            print '[CustomEventLogger][%s] %s' % (self.collection_name,msg)
            EventBase.log(self, msg)

If we call it:

    events.custom_1.log('logging a really great message')

You will see this output:

    [CustomEventLogger][events_custom_1] logging a really great message
    [Event][events_custom_1] logging a really great message

Using custom events you can add parameters to the log call, assert that the parameters are of the correct type, and manipulate data before logging

###Consuming events

 In another terminal (or several terminals) run the following, 
 
    $ cd pymonster-test
    $ source venv/bin/activate
    $ python

    import pymonster
    consumers = pymonster.ConsumerManager(
          pkg_name='pymonster.example.consumers'
        )

    pymonster.register_events(
            [
                [events.event_1,                        consumers.event_1]
                , [events.user.bought_shirt,        consumers.user.bought_a_shirt]
                , [events.custom_1,                   consumers.custom_1]
            ]
        )
    pymonster.consume_events_loop() 

Now, in the original terminal, fire off some events and watch them get consumed
    
    for i in range(20):
        events.event_1.log('Logging event_1 %d' % i)
        events.custom_1.log('Logging custom event %d' % i)

###Other things

If you're worried about getting bitten by undeclared events and consumers, just turn that feature off

    pymonster.allow_undeclared_events = False


Email me with any feedback at austinellis@gmail.com and check out [www.mighytspring.com](https://www.mightyspring.com/getstarted)!

