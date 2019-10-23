from datetime import datetime
from threading import RLock

from .MemoryStorage import MemoryStorage


class LRUCache:
    '''
    Collection of data where data may be removed to make room

    Each peace of data is indexed by a unique key.

    Least Recent Used implies that when room is needed in the
    collection, whatever key has been accessed least recently
    is silently removed from the collection.

    Actual storage of the data depends on the storage object
    attached, and defaults to in-memory (MemoryStorage)
    '''

    def __init__(self, storage=None, max_size=None, sizeof=None, max_age=None):
        '''
        :param storage: Storage for data (CacheStorage)
        :param max_size: Maximum size to store in cache
        :param sizeof: Function to use for calculating the size of data cached
        :param max_age: Max time to hold cached items for (timedelta)
        '''
        self.storage = storage or MemoryStorage()
        self.max_size = max_size
        self.__sizeof = sizeof
        self.max_age = max_age
        self.lock = RLock()


    def __setitem__(self, key, data):
        '''Add item to the cache'''

        # Determine size of data
        if self.__sizeof is not None:
            try:
                size = self.__sizeof(data)
            except AttributeError:
                size = 0
        else:
            size = 0

        with self.__lock:

            self.storage.remove_items_older_than(datetime.now() - self.max_age)

            # Remove item if already in cache
            if self.storage.has(key):
                self.storage.remove(key)

            # Sanity check: Data too big for storage
            if self.max_size is not None and size > self.max_size:
                return

            # Make sure there is space
            if self.max_size > 0:
                while self.storage.total_size_stored + size >= self.max_size:
                    self.storage.remove(self.storage.next_to_remove())

            # Save item
            self.storage.add(
                key = key,
                data = data,
                size = size)


    def __getitem__(self, key):
        '''Get data from cache'''
        with self.__lock:
            self.storage.remove_items_older_than(datetime.now() - self.max_age)
            try:
                data = self.storage.get(key)
                self.storage.touch_last_used(key)
                return data
            except KeyError:
                return None


    def __contains__(self, key):
        with self.__lock:
            return self.has(key)

