from datetime import datetime
import heapq

from .CacheStorage import CacheStorage


class CachedDataItem:
    '''Container to encapsulate cached data'''

    def __init__(self, key, data, last_used, size, expire_after):
        self.key = key
        self.last_used = last_used
        self.data = data
        self.size = size
        self.expire_after = expire_after

    @property
    def age(self):
        return datetime.now() - self.last_used



class MemoryStorage(CacheStorage):
    '''Stores cached data in-memory'''

    def __init__(self, evicted_callback=None):
        super().__init__(evicted_callback=evicted_callback)
        self.__items = dict()
        self.__key_priority = list() # (end of list is most recent used key)
        self.__expire_queue = list() # Index of when items expire
        self.__total_size = 0
        self.__oldest_lru = None

    @property
    def total_size_stored(self):
        return self.__total_size

    @property
    def count_items(self):
        return len(self.__items)


    def has(self, key):
        '''Check to see if key is in storage'''
        return key in self.__items


    def add(self, key, data, last_used=None, size=0, expire_after=None):
        '''
        Add an item to the storage and update LRU tracking

        :param key: Key to retrieve data with
        :param data: Data to be stored
        :param last_used: Timestamp entriy was last used (default now)
        :param size: Size of the data item
        :param expire_after: When to expire this data (datetime)
        '''

        # Remove item if already in cache
        if self.has(key):
            self.remove(key)

        item = CachedDataItem(
            key = key,
            data = data,
            last_used = last_used,
            size = size,
            expire_after = expire_after)

        if item.last_used is None:
            item.last_used = datetime.now()
        
        # Save
        self.__key_priority.append(key)
        self.__items[key] = item
        self.__total_size += item.size
        if self.__oldest_lru is None:
            self.__oldest_lru = None

        if expire_after is not None:
            heapq.heappush(self.__expire_queue, (item.expire_after, key))


    def get(self, key):
        '''
        Get data by key

        Note: check to make sure item isn't expired

        :param key: Key identifying
        :return: Data that was cached
        :raises KeyError: If key not in collection
        '''
        item = self.__items[key]
        if item.expire_after > datetime.now():
            self.touch_last_used(key)
            return item.data
        else:
            raise KeyError("%s has expired" % (key))
        
        
    def remove(self, key):
        '''Remove a cached item from by it's key'''
        
        if key in self.__items:
            self.notify_evicted(key)
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


    def make_room_for(self, size, max_size):
        '''
        Make room for a new item of the given size

        Note: Possible race condition if storage supports multiple LRUCache objects
              in separate processes and called concurrently.  Solve this in storage
              engine implementation if needed.

        :param size: Size of the new object coming in
        :param max_size: Size limit for the cache storage
        '''

        while len(self.__key_priority) > 0 and self.__total_size + size > max_size:
            self.remove(self.next_to_remove())


    def remove_expired(self):
        '''Remove any expired keys'''
        now = datetime.now()
        try:
            while self.__expire_queue[0][0] <= now:
                key = heapq.heappop()[1]
                if key in self:
                    self.remove(key)
        except IndexError:
            # Queue probably empty
            return


    def close(self):
        pass