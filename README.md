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


FileCache Class
---------------

A LRU cache built to store files on top of LRUCache.  Stores files in a defined
directory along with a sqlite3 DB (Sqlite3Storage) to track file usage and
metadata, evicting files to make space when needed.

### Basic Usage:


    from lru import FileCache
    
    cache = FileCache('path/to/cache', max_size=10*1024*1024, max_age=timedelta(days=1))
    
    with cache.get('my_file.dat') as file:
        
        # While within with context, cache knows not to delete file
        
        if not file.in_cache:
            
            # Have to get the file from somewhere else.
            
            # Then copy in:
            if have_file_source_on_disk:
                file.copy_from(path_on_disk)
                
            # or open file for writing
            else:
                with file.open('wt') as fh:
                    fh.write("File Contents")
        
        else:
            # Get path to file in cache to read from
            do_something_with(file.path)
            
            # Or make a copy out
            file.copy_to('another/path')
            
            # Can read and write file.metadata as dict
            if 'project' in file.metadata:
                print("File is part of project " + file.metadata['project'])
            else:
                file.metadata['project'] = input("What project is this: ")
            
            # Want to delete from cache?
            file.discard() # Will delete when exiting with context
                
            
            
 