import shelve
#from datetime import datetime

from .CacheStorage import CacheStorage


# TODO: Need to add __oldest_lru as in MemoryStorage, implement remaining methods, and test


class ShelvedStorage(CacheStorage):
    '''Storage which saves entries to disk using shelve'''


    def __init__(self, path):
        '''
        :param path: Path to save shelf to
        '''
        self.__path = path
        self.__item_shelf = shelve.open(path)

        self.__key_priority = list() # (end of list is most recent used key)
        self.__total_size = 0

        self._read_existing_shelf_entries()


    def _read_existing_shelf_entries(self):
        '''Index items that are already in the shelf'''

        self.__total_size = 0

        last_used = dict()

        for key in self.__item_shelf:
            last_used[key] = self.__item_shelf['last_used']
            self.__total_size += self.__item_shelf['size']

        self.__key_priority = [key for (ts, key) in sorted(last_used.items(), key=lambda t: t[0])]


    def total_size_stored(self):
        '''Total size of cached data'''
        return self.__total_size


    def count_items(self):
        '''Total size of cached data'''
        return len(self.__item_shelf)


    def has(self, key):
        '''Check to see if key is in storage'''
        return key in self.__item_shelf


    def add(self, key, data, last_used=None, size=0):
        '''
        Add an item to the storage and update LRU tracking

        :param key: Key to retrieve data with
        :param data: Data to be stored
        :param last_used: Timestamp entriy was last used (default now)
        :param size: Size of the data item
        '''
        self.__item_shelf[key] = {
            'data': data,
            'last_used': last_used,
            'size': size,
        }

        self.__key_priority.append(size)
        self.__total_size += size


    def remove(self, key):
        '''Remove a cached item from by it's key'''
        if self.has(key):
            self.__total_size -= self.__item_shelf[key].size
            del self.__item_shelf[key]


    def touch_last_used(self, key):
        '''Mark an item as recently used'''
        self.__key_priority.remove(key)
        self.__key_priority.append(key)


    def next_to_remove(self):
        '''Select next key to remove (least recently used)'''
        if len(self.__key_priority) > 0:
            return self.__key_priority[0]
