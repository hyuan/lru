from abc import ABC, abstractmethod

class CacheStorage(ABC):
    '''
    Interface for storing and retrieving data for LRUCache

    Cache storage is also expected to keep an index of keys for
    data stored.  Default implementation keeps that index in-memory.

    Note: It's up to the storage implementation to make sure expired items
    are removed.
    '''

    @property
    @abstractmethod
    def total_size_stored(self):
        '''Total size of cached data'''


    @property
    @abstractmethod
    def count_items(self):
        '''Total size of cached data'''


    @abstractmethod
    def has(self, key):
        '''Check to see if key is in storage'''


    def make_room_for(self, size, max_size):
        '''
        Make room for a new item of the given size

        Note: Possible race condition if storage supports multiple LRUCache objects
              in separate processes and called concurrently.  Solve this in storage
              engine implementation if needed.

        :param size: Size of the new object coming in
        :param max_size: Size limit for the cache storage
        '''
        if max_size > 0 and size > 0:
            while self.total_size_stored + size >= max_size:
                self.remove(self.next_to_remove())


    @abstractmethod
    def add(self, key, data, last_used=None, size=0, expire_after=None):
        '''
        Add an item to the storage

        Note: It's up to the storage engine to make room for the item if full

        :param key: Key to retrieve data with
        :param data: Data to be stored
        :param last_used: Timestamp entriy was last used (default now)
        :param size: Size of the data item
        :param expire_after: When to expire this data (datetime)
        '''


    @abstractmethod
    def get(self, key):
        '''
        Get data by key

        Note: check to make sure item isn't expired

        :param key: Key identifying
        :return: Data that was cached
        :raises KeyError: If key not in collection
        '''


    @abstractmethod
    def remove(self, key):
        '''Remove a cached item from by it's key'''


    @abstractmethod
    def touch_last_used(self, key):
        '''Mark an item as recently used'''


    @abstractmethod
    def next_to_remove(self):
        '''Select next key to remove (least recently used)'''


