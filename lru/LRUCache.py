import sys

from datetime import datetime
from threading import RLock

from .CacheStorage import CachedItem, ItemNotCached
from .MemoryStorage import MemoryStorage


class ItemExpired(ItemNotCached): pass


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
                size = self.__sizeof(data)
            else:
                size = sys.getsizeof(key) + sys.getsizeof(data)

        # Time to expire
        if expires_in is not None:
            expire_after = datetime.now() + expires_in
        elif self.max_age is not None:
            expire_after = datetime.now() + self.max_age
        else:
            expire_after = None

        item = CachedItem(data, size=size, expires=expire_after)

        # Manipulate storage
        with self.lock:

            # Remove item if already exists
            if self.storage.has_key(key):
                self._remove_item_from_storage(key)

            # Sanity check: Data too big for storage
            if self.max_size is not None and size > self.max_size:
                return

            # Make sure there is space
            if self.max_size is not None:
                self.make_room_for(size)

            # Save item
            self.storage.add(key, item)


    def get(self, key):
        return self[key]


    def __getitem__(self, key):
        '''Get data from cache'''
        with self.lock:
            item = self.storage.get(key)
            if item.expires_at is not None and item.expires_at < datetime.now():
                self.remove(key)
                raise ItemExpired()
            self.storage.touch_last_used(key)
            return item.data


    def __setitem__(self, key, data):
        '''Add item to the cache'''
        self.put(key, data)


    def keys(self):
        with self.lock:
            return self.storage.keys()


    def items(self):
        with self.lock:
            for key, item in self.storage.items():
                yield key, item.data


    def _remove_item_from_storage(self, key):
        '''
        Remove an item from storage

        Intended for internal use.  No state checking
        '''
        with self.lock:
            self.storage.remove(key)


    def __delitem__(self, key):
        self._remove_item_from_storage(key)


    def remove(self, key):
        self._remove_item_from_storage(key)


    @property
    def num_items(self):
        return self.storage.num_items


    def close(self):
        with self.lock:
            self.storage.close()
            self.storage = None


    def clean_expired(self):
        '''Clean old entries out of cache'''
        with self.lock:
            for key, item in self.storage.expired_items():
                self.remove(key)


    @property
    def total_size_stored(self):
        return self.storage.total_size_stored


    def make_room_for(self, size):
        '''
        Make room for a new item of the given size

        Note: Possible race condition if storage supports multiple LRUCache objects
              in separate processes and called concurrently.  Solve this in storage
              engine implementation if needed.

        :param size: Size of the new object coming in
        :param max_size: Size limit for the cache storage
        '''
        with self.lock:
            if self.max_size > 0 and size > 0:
                while self.storage.total_size_stored + size > self.max_size:
                    self.storage.pop_oldest()


