import os
import shelve
import heapq
from datetime import datetime

from .CacheStorage import CachedItem, NoItemCached
from .MemoryStorage import MemoryStorage


class ShelfCorrupted(NoItemCached): pass


def to_ts(t):
    if t is not None:
        return t.timestamp()


def from_ts(t):
    if t is not None:
        return datetime.fromtimestamp(t)


class ShelvedStorage(MemoryStorage):
    '''
    Storage which saves entry data to disk using shelve

    Shelved data looks like:
    [key] = {
        'data': ...,
        'expires_at': 18489185.1230,
        'size':  123,
        'used': 18489184.1230
    }
    '''

    DATA = 'd'
    METADATA = 'm'

    EXPIRES = 'e'
    SIZE = 's'
    LRU_ORDER = 'o'

    def __init__(self, path):
        '''
        :param path: Path to save shelf to
        '''
        super().__init__()
        self.__path = path
        self.__item_shelf = shelve.open(path)
        self.__total_size = 0

        self._read_existing_shelf_entries()


    def _read_existing_shelf_entries(self):
        '''Load cache back in from shelf'''

        # Collect cached item metadata
        metadata = list()
        for shelf_key in self.__item_shelf:
            if shelf_key.startswith(self.METADATA):
                key = shelf_key[len(self.METADATA):]
                metadata.append((key, self.__item_shelf[shelf_key]))

        # Re-add to memory LRU, oldest first
        for key, item_data in sorted(metadata, key=lambda t: t[1][self.LRU_ORDER]):
            super().add(key, CachedItem(
                data = None,
                expires = from_ts(item_data[self.EXPIRES]),
                size = item_data[self.SIZE],
            ))


    def add(self, key, item):

        lru_order = datetime.now().timestamp()

        # Add memory entry to track LRU
        super().add(key, CachedItem(
            data = None,
            expires = item.expires_at,
            size = item.size
        ))

        # Save item data to shelf
        self.__item_shelf[self.DATA + key] = item.data
        self.__item_shelf[self.METADATA + key] = {
            self.EXPIRES: to_ts(item.expires_at),
            self.SIZE: item.size,
            self.LRU_ORDER: lru_order,
        }


    def get(self, key):
        mem_item = super().get(key)
        try:
            item = mem_item.copy()
            item.data = self.__item_shelf[self.DATA + key]
            return item
        except Exception as e:
            raise ShelfCorrupted(str(e))


    def items(self):
        for key in self.keys():
            yield key, self.get(key)


    def remove(self, key):
        '''Remove a cached item by it's key'''
        self._rm_from_shelf(key)
        super().remove(key)


    def _rm_from_shelf(self, key):

        try:
            del self.__item_shelf[self.DATA + key]
        except KeyError:
            pass

        try:
            del self.__item_shelf[self.METADATA + key]
        except KeyError:
            pass


    def touch_last_used(self, key):
        '''Mark an item as recently used'''

        try:
            metadata = self.__item_shelf[self.METADATA + key].copy()
            metadata[self.LRU_ORDER] = to_ts(datetime.now())
            super().touch_last_used(key)
        except KeyError:
            self.remove(key)

    def pop_oldest(self):
        key, item = super().pop_oldest()
        self._rm_from_shelf(key)
        return key, item


    def close(self):
        self.__item_shelf.close()


    # def _read_existing_shelf_entries(self):
    #     '''Index items that are already in the shelf'''
    #
    #     self.__total_size = 0
    #
    #     last_used = dict()
    #
    #     for key in self.__item_shelf:
    #         last_used[key] = self.__item_shelf['last_used']
    #         self.__total_size += self.__item_shelf['size']
    #
    #     self.__key_priority = [key for (ts, key) in sorted(last_used.items(), key=lambda t: t[0])]
    #     self.__expire_index = heapq.heapify([(item['expires'], key) for (key, item) in self.__item_shelf.items()]) or list()
    #
    #
    # def total_size_stored(self):
    #     '''Total size of cached data'''
    #     return self.__total_size
    #
    #
    # def count_items(self):
    #     '''Total size of cached data'''
    #     return len(self.__item_shelf)
    #
    #
    # def keys(self):
    #     '''All cache keys'''
    #     return self.__item_shelf.keys()
    #
    #
    # def items(self):
    #     '''All cache keys and items'''
    #     for key in self.keys():
    #         yield key, self[key]
    #
    #
    # def has(self, key):
    #     '''Check to see if key is in storage'''
    #     return key in self.__item_shelf
    #
    #
    # def add(self, key, data, last_used=None, size=0, expire_after=None):
    #     '''
    #     Add an item to the storage and update LRU tracking
    #
    #     :param key: Key to retrieve data with
    #     :param data: Data to be stored
    #     :param last_used: Timestamp entriy was last used (default now)
    #     :param size: Size of the data item
    #     :param expire_after: When to expire this data (datetime)
    #     '''
    #
    #     # Remove item if already in cache
    #     if self.has(key):
    #         self.remove(key)
    #
    #     self.__item_shelf[key] = {
    #         'data': data,
    #         'last_used': last_used,
    #         'size': size,
    #         'expires': expire_after,
    #     }
    #
    #     self.__key_priority.append(key)
    #     self.__total_size += size
    #     if expire_after is not None:
    #         heapq.heappush(self.__expire_index, (expire_after, key))
    #
    #     # Check for expired items
    #     self.remove_expired()
    #
    #
    # def get(self, key):
    #     '''
    #     Get data by key
    #
    #     Note: check to make sure item isn't expired
    #
    #     :param key: Key identifying
    #     :return: Data that was cached
    #     :raises KeyError: If key not in collection
    #     '''
    #
    #     # Get item
    #     try:
    #         item = self.__item_shelf[key]
    #     except KeyError:
    #         raise KeyError()
    #
    #     # Check if expired
    #     if item['expires'] is not None and item['expires'] < datetime.now():
    #         self._remove_expired_key(key)
    #         raise KeyError("Item %s has expired" % (key))
    #
    #     # Mark item as last used
    #     self.__key_priority.remove(key)
    #     self.__key_priority.append(key)
    #
    #     return item['data']
    #
    #
    # def remove_expired(self):
    #     '''Remove all expired times'''
    #     now = datetime.now()
    #     while len(self.__expire_index) > 0 and self.__expire_index[0][0] < now:
    #         expired_at, key = heapq.heappop(self.__expire_index)
    #         if key in self.__item_shelf:
    #             if expired_at < now: # Redundant, but feels good
    #                 self._remove_expired_key(key)
    #
    #
    # def _remove_expired_key(self, key):
    #     '''Remove and expired item'''
    #
    #     # Get item
    #     try:
    #         item = self.__item_shelf[key]
    #     except KeyError:
    #         return
    #
    #     self.notify_evicted(key)
    #
    #     # Remove item
    #     del self.__item_shelf[key]
    #     self.__key_priority.remove(key)
    #     self.__total_size = max(0, self.__total_size-item['size'])
    #     # self.__expire_index is cleaned up in add()
    #
    #
    # def remove(self, key):
    #     '''Remove a cached item from by it's key'''
    #     if self.has(key):
    #         self.notify_evicted(key)
    #         self.__total_size -= self.__item_shelf[key].size
    #         del self.__item_shelf[key]
    #
    #
    # def touch_last_used(self, key):
    #     '''Mark an item as recently used'''
    #     self.__key_priority.remove(key)
    #     self.__key_priority.append(key)
    #
    #
    # def next_to_remove(self):
    #     '''Select next key to remove (least recently used)'''
    #     if len(self.__key_priority) > 0:
    #         return self.__key_priority[0]
    #
    #
    # def make_room_for(self, size, max_size):
    #     '''
    #     Make room for a new item of the given size
    #
    #     Note: Possible race condition if storage supports multiple LRUCache objects
    #           in separate processes and called concurrently.  Solve this in storage
    #           engine implementation if needed.
    #
    #     :param size: Size of the new object coming in
    #     :param max_size: Size limit for the cache storage
    #     '''
    #     now = datetime.now()
    #     try:
    #         while self.__expire_index[0][0] <= now:
    #             key = heapq.heappop()[1]
    #             if key in self:
    #                 self.remove(key)
    #     except IndexError:
    #         # Queue probably empty
    #         return
    #
    #
    # def close(self):
    #     self.__item_shelf.close()