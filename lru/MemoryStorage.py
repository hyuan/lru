from datetime import datetime
from collections import OrderedDict

from .CacheStorage import CacheStorage


class CachedDataItem:
    '''Container to encapsulate cached data'''

    def __init__(self, key, data, last_used, size):
        self.key = key
        self.last_used = last_used
        self.data = data
        self.size = size

    @property
    def age(self):
        return datetime.now() - self.last_used



class MemoryStorage(CacheStorage):
    '''Stores cached data in-memory'''

    def __init__(self):
        self.__items = dict()
        self.__key_priority = list() # (end of list is most recent used key)
        self.__total_size = 0
        self.__oldest_lru = None

    
    def has(self, key):
        '''Check to see if key is in storage'''
        return key in self.__items


    def add(self, key, data, last_used=None, size=0):
        '''
        Add an item to the storage and update LRU tracking

        :param key: Key to retrieve data with
        :param data: Data to be stored
        :param last_used: Timestamp entriy was last used (default now)
        :param size: Size of the data item
        '''

        item = CachedDataItem(
            key = key,
            data = data,
            last_used = last_used,
            size = size)

        if item.last_used is None:
            item.last_used = datetime.now()
        
        # Save
        self.__key_priority.append(key)
        self.__items[key] = item
        self.__total_size += item.size
        if self.__oldest_lru is None:
            self.__oldest_lru = None


    def get(self, key):
        '''
        Get data by key

        :param key: Key identifying
        :return: Data that was cached
        :raises KeyError: If key not in collection
        '''
        self.touch_last_used(key)
        return self.__items[key]
        
        
    def remove(self, key):
        '''Remove a cached item from by it's key'''
        
        if key in self.__items:
            self.__total_size -= self.__items[key].size
            del self.__items[key]
            self.__key_priority.remove(key)
            
            
    def touch_last_used(self, key):
        '''Mark an item as recently used'''

        if key in self.__items:
            self.__key_priority.remove(key)
            self.__items[key].last_used = datetime.now()
        self.__key_priority.append(key)

        oldest_key = self.__key_priority[-1]
        self.__oldest_lru = self.__items[key].last_used


    def next_to_remove(self):
        '''Select next key to remove (least recently used)'''
        if len(self.__key_priority) > 0:
            return self.__key_priority[0]


    def remove_items_older_than(self, ts):
        '''
        Remove any items older than this.

        Note: It's up to the storage implementation to make sure this is
        done efficiently.  This is called after every add() and before any
        get().

        :param ts: datetime to remove prior to
        '''
        while self.__oldest_lru < ts:
            self.remove(self.next_to_remove())
