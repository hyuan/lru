from abc import ABC, abstractmethod

class CacheStorage(ABC):
    '''
    Interface for storing and retrieving data for LRUCache

    Cache storage is also expected to keep an index of keys for
    data stored.  Default implementation keeps that index in-memory.
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


    @abstractmethod
    def add(self, key, data, last_used=None, size=0):
        '''
        Add an item to the storage

        :param key: Key to retrieve data with
        :param data: Data to be stored
        :param last_used: Timestamp entriy was last used (default now)
        :param size: Size of the data item
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


    @abstractmethod
    def next_to_remove(self):
        '''Select next key to remove (least recently used)'''


    @abstractmethod
    def remove_items_older_than(self, ts):
        '''
        Remove any items older than this.

        Note: It's up to the storage implementation to make sure this is
        done efficiently.  This is called after every add() and before any
        get().

        :param ts: datetime to remove prior to
        '''

