
import json
from datetime import datetime
from tempfile import TemporaryFile
from textwrap import dedent
from contextlib import closing
import sqlite3
import logging

from .CacheStorage import CacheStorage, CachedItem
from .CacheStorage import ItemNotCached, NoItemsCached
from .utils import LargeKeyList

class CachedDataCorrupt(ItemNotCached): pass


class Sqlite3Storage(CacheStorage):
    '''Storage which saves entries to disk using sqlite3 and relies on sqlite3 indexes'''


    def __init__(self, path, evicted_callback=None):
        '''
        :param path: Path to save shelf to
        '''
        super().__init__()
        self.__path = path
        self.__db = sqlite3.connect(
            self.__path,
            detect_types=sqlite3.PARSE_DECLTYPES) # Allows parsing of timestamp

        self.__size_cache = None
        self.__cnt_cache = None

        self._init_db()


    @property
    def path(self):
        return self.__path


    def _init_db(self):
        with closing(self.__db.cursor()) as curs:

            curs.execute(dedent("""\
                CREATE TABLE IF NOT EXISTS cache_entries (
                    cache_key   text NOT NULL PRIMARY KEY,
                    item_data   text NOT NULL,
                    item_size   UNSIGNED INTEGER,
                    last_used   timestamp NOT NULL,
                    expires     timestamp
                );
                """))

            curs.execute(dedent("""\
                CREATE TABLE IF NOT EXISTS key_lists (
                    list_id     integer NOT NULL,
                    key         text NOT NULL
                );
                """))

            try:
                curs.execute(dedent("CREATE INDEX last_used_idx ON cache_entries (last_used)"))
                curs.execute(dedent("CREATE INDEX expires_idx ON cache_entries (expires)"))
            except sqlite3.OperationalError as e:
                if "already exists" in str(e):
                    pass
                else:
                    raise

            self.__cnt_cache, self.__size_cache = self._get_cnt_and_size_from_db()

            self.__db.commit()


    def _get_cnt_and_size_from_db(self):
        with closing(self.__db.cursor()) as curs:
            sql = "SELECT count() as cnt, sum(item_size) as sz FROM cache_entries"
            for row in curs.execute(sql):
                return row[0], row[1] or 0


    @property
    def total_size_stored(self):
        '''Total size of cached data'''
        return self.__size_cache


    @property
    def num_items(self):
        '''Total size of cached data'''
        return self.__cnt_cache


    def keys(self):
        '''All cache keys'''
        with closing(self.__db.cursor()) as curs:
            sql = "SELECT cache_key FROM cache_entries WHERE expires IS NULL or expires > ?"
            return LargeKeyList([row[0] for row in curs.execute(sql, (datetime.now(), ))])


    # def items(self):
    #     '''All cache keys and items'''
    #     for cache_key in self.keys():
    #         yield cache_key, self.get(cache_key)
    #
    #

    def add(self, key, item):
        '''
        Add an item to the storage and update LRU tracking

        :param key: Key to retrieve data with
        :param data: Data to be stored
        :param last_used: Timestamp entriy was last used (default now)
        :param size: Size of the data item
        :param expire_after: When to expire this data (datetime)
        '''
        with closing(self.__db.cursor()) as curs:

            # Encode data
            data = json.dumps(item.data)

            # Save item_data
            sql = dedent("""\
                INSERT INTO cache_entries
                (
                    cache_key,
                    item_data,
                    item_size,
                    last_used,
                    expires
                )
                VALUES
                (?, ?, ?, ?, ?)
                """)
            curs.execute(sql, (
                key,
                data,
                item.size,
                datetime.now(),
                item.expires_at))

            # Update stats cache
            if item.size is not None:
                self.__size_cache += item.size
            self.__cnt_cache += 1

            self.__db.commit()


    def get(self, key):
        '''
        Get data by key

        Note: check to make sure item isn't expired

        :param key: Key identifying
        :return: Data that was cached
        :raises KeyError: If key not in collection
        '''
        with closing(self.__db.cursor()) as curs:

            # Get entry
            sql = dedent("""\
                SELECT
                    item_data,
                    item_size,
                    expires
                FROM cache_entries
                WHERE cache_key = ?
                """)
            for item_data, item_size, expires in curs.execute(sql, (key, )):

                # Decode data
                try:
                    item_data = json.loads(item_data)
                except Exception as e:
                    self.remove(key)
                    raise CachedDataCorrupt('Item data corrupt: %s: %s' % (
                        e.__class__.__name__, str(e)))

                # Return item
                return CachedItem(
                    data = item_data,
                    expires = expires,
                    size = item_size
                )

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

        with closing(self.__db.cursor()) as curs:

            # Look for key
            sql = dedent("""\
                SELECT
                    count(*) as CNT
                FROM cache_entries
                WHERE cache_key = ?
                """)
            for row in curs.execute(sql, (key, )):
                return row[0] != 0


    def expired_items(self):
        '''
        Find and return keys for any expired items (up to LRUCache to remove)

        :return: generator (key, CachedItem)
        '''

        with closing(self.__db.cursor()) as curs:
            now = datetime.now()
            sql = "SELECT cache_key FROM cache_entries WHERE expires IS NOT NULL and expires < ?"
            keys = LargeKeyList([row[0] for row in curs.execute(sql, (now, ))])
            for key in keys:
                yield key, self.get(key)


    def remove(self, key):
        '''Remove a cached item from by it's key'''

        try:
            item = self.get(key)
        except ItemNotCached:
            raise ItemNotCached()

        with closing(self.__db.cursor()) as curs:

            sql = "DELETE FROM cache_entries WHERE cache_key = ?"
            curs.execute(sql, (key,))
            if curs.rowcount == 1:
                self.__cnt_cache -= 1
                self.__size_cache -= item.size
            else:
                raise ItemNotCached()
            self.__db.commit()


    def touch_last_used(self, key):
        '''Mark an item as recently used'''
        with closing(self.__db.cursor()) as curs:
            now = datetime.now()
            sql = "UPDATE cache_entries SET last_used = ? WHERE cache_key = ?"
            curs.execute(sql, (now, key))
            self.__db.commit()


    def oldest_key(self):
        with closing(self.__db.cursor()) as curs:
            sql = "SELECT cache_key FROM cache_entries ORDER BY last_used LIMIT 1"
            for row in curs.execute(sql):
                return row[0]


    def pop_oldest(self):
        '''
        Select next key to remove (least recently used)

        :return: (key, item)
        '''
        key = self.oldest_key()
        if key:
            item = self.get(key)
            self.remove(key)
            return key, item
        raise NoItemsCached


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
    #     self.remove_expired()
    #     while self.__size_cache + size > max_size and self.__size_cache > 0:
    #
    #         with closing(self.__db.cursor()) as curs:
    #
    #             keys = list()
    #             size_removed = 0
    #
    #             # Select keys to remove
    #             sql = dedent("""\
    #                 SELECT cache_key, item_size
    #                 FROM cache_entries
    #                 WHERE item_size IS NOT NULL and item_size > 0
    #                 ORDER BY last_used DESC
    #                 LIMIT 500
    #                 """)
    #
    #             for row in curs.execute(sql):
    #                 key = row[0]
    #                 item_size = row[1]
    #                 keys.append(key)
    #                 size_removed += item_size
    #                 if self.__size_cache - size_removed + size < max_size:
    #                     break
    #
    #             sql = "DELETE FROM cache_entries WHERE cache_key IN (%s)" % (', '.join(['?']*len(keys)))
    #             curs.execute(sql, (keys, ))
    #
    #             self.__db.commit()
    #
    #             self.__cnt_cache, self.__size_cache = self._get_cnt_and_size_from_db()


    def close(self):
        if self.__db is None:
            return
        self.__db.close()
        self.__db = None