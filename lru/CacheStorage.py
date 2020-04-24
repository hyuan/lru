from abc import ABC, abstractmethod
from abc import ABC, abstractmethod
from datetime import datetime

class ItemNotCached(KeyError): pass
class DuplicateKeyOnAdd(KeyError): pass
class NoItemsCached(Exception): pass


class CachedItem:
    '''The carier for an item to be cached'''

    def __init__(self, data, expires=None, size=None):
        self.data = data
        self.expires_at = expires
        self.size = size


    def copy(self):
        return CachedItem(
            data = self.data,
            expires = self.expires_at,
            size = self.size,
        )



class CacheStorage(ABC):
    '''
    Interface for storing and retrieving data for LRUCache

    The CacheStorage stores and retrieves CachedItem objects.
    It will also need to keep indexes to assist with making
    qureies against the data to identify the least recently
    used object or expired objects.
    '''

    def __init__(self):
        pass


    @property
    @abstractmethod
    def total_size_stored(self):
        '''Total size of cached data'''


    @property
    @abstractmethod
    def num_items(self):
        '''Total size of cached data'''


    @abstractmethod
    def keys(self):
        '''All cache keys'''


    def items(self):
        '''All cache keys and items'''
        for key in self.keys():
            try:
                yield key, self.get(key)
            except KeyError:
                pass # Item was removed before we could grab it



    @abstractmethod
    def add(self, key, item):
        '''
        Add an item to the storage

        Note: It's up to the storage engine to make room for the item if full

        :param key: Key to retrieve data with
        :param item: CachedItem
        :raises DuplicateKeyOnAdd:
            LRUCache works to make sure items are unique when queued.  However,
            if a conflict is encountered
        '''


    @abstractmethod
    def get(self, key):
        '''
        Get data by key

        :param key: Key identifying
        :return: CachedItem
        :raises ItemNotCached: If item not in cache
        '''


    def __getitem__(self, key):
        return self.get(key)


    @abstractmethod
    def remove(self, key):
        '''
        Remove a cached item from by it's key

        :raises ItemNotCached: If item not in cache
        '''


    @abstractmethod
    def pop_oldest(self):
        '''
        Select next key to remove (least recently used)

        :return: (key, item)
        '''


    @abstractmethod
    def close(self):
        '''Close storage and sync to disk'''


    def expired_items(self):
        '''
        Find and return keys for any expired items (up to LRUCache to remove)

        :return: generator (key, CachedItem)
        '''

         # TODO: Should we be indexing by expire time?  Tried to do a heapq.
         #  but ended up have to scan the whole queue for each removal

        now = datetime.now()
        for key, item in self.items():
            if item.expires_at is not None and item.expires_at < now:
                yield key, item


    @abstractmethod
    def touch_last_used(self, key):
        '''Mark item as just used'''