from datetime import datetime
import heapq

from .CacheStorage import CacheStorage, DuplicateKeyOnAdd, ItemNotCached
from .LRULinkedList import LRULinkedList


class MemoryStorage(CacheStorage):
    '''Stores cached data in-memory'''

    def __init__(self):
        super().__init__()
        self.__items = LRULinkedList()
        self.__total_size = 0


    @property
    def total_size_stored(self):
        return self.__total_size


    @property
    def num_items(self):
        return len(self.__items)


    def keys(self):
        '''All cache keys'''
        return self.__items.keys()


    def items(self):
        return self.__items.items()


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
        # Add item to linked list
        try:
            self.__items.add_new(key, item)
        except KeyError:
            raise DuplicateKeyOnAdd()

        # Update stats
        self.__total_size += item.size


    def get(self, key):
        '''
        Get data by key

        Note: check to make sure item isn't expired

        :param key: Key identifying
        :return: Data that was cached
        :raises ItemNotCached: If item is not cached
        '''
        try:
            return self.__items[key]
        except KeyError:
            raise ItemNotCached()

        
    def has_key(self, key):
        '''
        Check to see if key exists.

        This is only called by the LRUCache class after locking storage.  It's
        not intended to be used outside of lru as typcially you want to just try
        and get your key and let if fail if not present.

        Doesn't check expired.  Just checks to see if key is in storage

        :param key: Key being queried
        :return: True if key is stored in storage
        '''
        return key in self.__items


    def remove(self, key):
        '''Remove a cached item by it's key'''
        try:
            item = self.__items[key]
            self.__items.remove(key)
            self.__total_size -= item.size
            return item
        except KeyError:
            raise ItemNotCached()

            
    def touch_last_used(self, key):
        '''Mark an item as recently used'''
        self.__items.mark_used(key)


    def pop_oldest(self):
        '''
        Select next key to remove (least recently used)

        :return: (key, item)
        '''
        key, item = self.__items.pop_oldest()
        self.__total_size -= item.size
        return key, item


    def close(self):
        pass
