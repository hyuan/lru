python-lru
==========

Least Recently Used (LRU) Cache implementation


Usage
------

Instantiate a cache collection object specifying storage parameters.  The cache object
itself is thread safe.  However, depending on the storage backend, it may not be safe
to open a cache store multiple times.

    from lru import LRUCache, ItemNotCached

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
    try:
        age = cache['age']
    except ItemNotCached:
        print("No age")
        
        
Cache Objects
-------------

Cached data can be any variable, and must be cached using a string key.
It's up to you to ensure that you don't mutate objects returned from the cache, as
storage won't automatically update from changes.
        
        
Cache Parameters
----------------

The LRUCache container  parameters are:

 - **storage**: Where to store cached data.  See Storages.
 - **sizeof**: Callable to estimate the size of an object being cached.
 - **max_size**: Maximum size before starting to forget cached items.
 - **max_age**: All cached items will expire after this amount of time.
 - **storage**: Object to use to store cached data
 
### Storages

There are a few storage classes provided.  All are inherited from CacheStorage

 - MemoryStorage: Caches data in memory
 - ShelvedStorage: Caches data in [shelve](https://docs.python.org/3/library/shelve.html).  Really only useful if you're caching large objects.
 - Sqlite3Storage: Slowest storage engine, but possibly accessible from multiple processes?
 