lru
===

Simple Least Recently Used (LRU) Cache implementation


Usage
------

Instantiate a cache collection object specifying storage parameters.  The cache object
itself is thread safe.  However, depending on the storage backend, it may not be safe
to open a cache store multiple times.

    from lru import LRUCache

    # Open cache store (all arguments are optional)
    cache = LRUCache(
        storage = MemoryStorage() or ShelvedStorage(path=''),
        max_size = 1000000,
        sizeof = lambda o: len(str(o)),
        max_age = timedelta(minutes=15))
        
    # Add items with keys
    cache['name'] = "Bob"
    cache['age'] = 12
    
    # Check for items in cache
    if 'age' in cache:
        print(cache['age'])
        
        
Cache Objects
-------------

Cached data can be any variable, and must be cached using a string key.
It's up to you to ensure that you don't mutate objects returned from the cache, as
storage won't autmatically update from changes.
        
        
Cache Parameters
----------------

The LRUCache containter parameters are:

 - **storage**: Where to store cached data.  See Storages.
 - **sizeof**: Callable to estiamte the size of an object being cached.
 - **max_size**: Maximum size before starting to forget cached items.
 - **max_age**: All cached items will expire after this amount of time.