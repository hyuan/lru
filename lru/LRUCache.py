import sys

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

    Note: It's up to the storage class to enforce item expiring.
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
        self.put(key, data)


    def put(self, key, data, expires_in=None, size=None):
        '''
        Add an object to the cache

        :param key: Key to use to retrieve this item.
        :param data: The actual item to cache.
        :param expires_in: timedelta to specify when object should expire
        :param size: Size of the entry if known (will skip sizeof calc)
        :return:
        '''

        # Determine size of data
        if size is None:
            if self.__sizeof is not None:
                try:
                    size = self.__sizeof(data)
                except AttributeError:
                    size = 0
            else:
                size = sys.getsizeof(data)

        # Time to expire
        if expires_in is not None:
            expire_after = datetime.now() + expires_in
        else:
            expire_after = datetime.now() + self.max_age

        # Manipulate storage
        with self.lock:

            # Sanity check: Data too big for storage
            if self.max_size is not None and size > self.max_size:
                return

            # Make sure there is space
            self.storage.make_room_for(size, self.max_size)

            # Save item
            self.storage.add(
                key = key,
                data = data,
                size = size,
                expire_after = expire_after)


    def __getitem__(self, key):
        '''Get data from cache'''
        with self.lock:
            data = self.storage.get(key)
            self.storage.touch_last_used(key)
            return data


    def get(self, key):
        return self[key]


    def __contains__(self, key):
        with self.lock:
            return self.storage.has(key)


    def __delitem__(self, key):
        self.storage.remove(key)


    def remove(self, key):
        del self[key]


    def close(self):
        self.storage.close()
        self.storage = None


    def clean(self):
        '''Clean old entries out of cache'''
        self.storage.remove_expired()

