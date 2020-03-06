
import json
from datetime import datetime
from textwrap import dedent
from contextlib import closing
import sqlite3

from .CacheStorage import CacheStorage


# TODO: Need to add __oldest_lru as in MemoryStorage, implement remaining methods, and test


class Sqlite3Storage(CacheStorage):
    '''Storage which saves entries to disk using sqlite3 and relies on sqlite3 indexes'''

    TS_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, path, evicted_callback=None):
        '''
        :param path: Path to save shelf to
        '''
        super().__init__(evicted_callback=evicted_callback)
        self.__path = path
        self.__db = sqlite3.connect(self.__path)
        self.__size_cache = None
        self.__cnt_cache = None

        self._init_db()


    def _init_db(self):
        with closing(self.__db.cursor()) as curs:

            curs.execute(dedent("""\
                CREATE TABLE IF NOT EXISTS cache_entries (
                    cache_key   text NOT NULL PRIMARY KEY,
                    entry       text NOT NULL,
                    entry_size  UNSIGNED INTEGER,
                    last_used   datetime NOT NULL,
                    expires     datetime
                );
                """))

            # TODO: Catch already exists error
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
            sql = "SELECT count() as cnt, sum(entry_size) as sz FROM cache_entries"
            for row in curs.execute(sql):
                return row[0], row[1] or 0


    def total_size_stored(self):
        '''Total size of cached data'''
        return self.__size_cache


    def count_items(self):
        '''Total size of cached data'''
        return self.__cnt_cache


    def has(self, key):
        '''Check to see if key is in storage'''
        with closing(self.__db.cursor()) as curs:
            sql = "SELECT cache_key FROM cache_entries WHERE cache_key = ?"
            for row in curs.execute(sql, (key, )):
                return True
            return False


    def add(self, key, data, last_used=None, size=0, expire_after=None):
        '''
        Add an item to the storage and update LRU tracking

        :param key: Key to retrieve data with
        :param data: Data to be stored
        :param last_used: Timestamp entriy was last used (default now)
        :param size: Size of the data item
        :param expire_after: When to expire this data (datetime)
        '''

        with closing(self.__db.cursor()) as curs:

            # Remove item if already in cache
            self.remove(key)

            # Encode data
            data = json.dumps(data)

            # Encode timestamps
            if last_used is None:
                last_used = datetime.now()
            last_used = last_used.strftime(self.TS_FORMAT)

            if expire_after is not None:
                expire_after = expire_after.strftime(self.TS_FORMAT)

            # Save entry
            sql = dedent("""\
                INSERT INTO cache_entries
                (
                    cache_key,
                    entry,
                    entry_size,
                    last_used,  
                    expires
                )
                VALUES
                (?, ?, ?, ?, ?)
                """)
            curs.execute(sql, (key, data, size, last_used, expire_after))

            # Update stats cache
            if size is not None:
                self.__size_cache += size
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

            data = None

            # Get entry
            now = datetime.now().strftime(self.TS_FORMAT)
            sql = "SELECT entry FROM cache_entries WHERE cache_key = ? and (expires IS NULL or expires > ?)"
            for row in curs.execute(sql, (key, now)):
                data = row[0]

            # Decode data
            if data:
                try:
                    return json.loads(data)
                except Exception as e:
                    raise KeyError("Couldn't decode cache entry for %s: %s: %s" % (
                        key, e.__class__.__name__, str(e)))

            raise KeyError("No cached entry for " + str(key))



    def remove_expired(self):
        '''Remove all expired items'''

        with closing(self.__db.cursor()) as curs:

            now = datetime.now().strftime(self.TS_FORMAT)
            sql = "DELETE FROM cache_entries WHERE expires IS NOT NULL and expires < ?)"
            curs.execute(sql, (now, ))

            self.__cnt_cache, self.__size_cache = self._get_cnt_and_size_from_db()

            self.__db.commit()


    def _get_entry_size(self, key):
        with closing(self.__db.cursor()) as curs:
            sql = "SELECT entry_size FROM cache_entries WHERE cache_key = ?"
            for row in curs.execute(sql, (key, )):
                return row[0]
            return None


    def remove(self, key):
        '''Remove a cached item from by it's key'''

        if self.has(key):

            entry_size = self._get_entry_size(key)
            if entry_size is not None:
                self.__size_cache -= entry_size

            with closing(self.__db.cursor()) as curs:
                sql = "DELETE FROM cache_entries WHERE cache_key = ?"
                curs.execute(sql, (key,))
                self.__db.commit()

            self.__cnt_cache -= 1


    def touch_last_used(self, key):
        '''Mark an item as recently used'''
        with closing(self.__db.cursor()) as curs:
            now = datetime.now().strftime(self.TS_FORMAT)
            sql = "UPDATE cache_entries SET last_used = ? WHERE cache_key = ?"
            curs.execute(sql, (now, key))
            self.__db.commit()


    def next_to_remove(self):
        '''Select next key to remove (least recently used)'''
        with closing(self.__db.cursor()) as curs:
            sql = "SELECT cache_key FROM cache_entries ORDER BY last_used DESC LIMIT 1"
            for row in curs.execute(sql):
                return row[0]


    def make_room_for(self, size, max_size):
        '''
        Make room for a new item of the given size

        Note: Possible race condition if storage supports multiple LRUCache objects
              in separate processes and called concurrently.  Solve this in storage
              engine implementation if needed.

        :param size: Size of the new object coming in
        :param max_size: Size limit for the cache storage
        '''

        while self.__size_cache + size > max_size and self.__size_cache > 0:

            with closing(self.__db.cursor()) as curs:

                keys = list()
                size_removed = 0

                # Select keys to remove
                sql = dedent("""\
                    SELECT cache_key, entry_size
                    FROM cache_entries
                    WHERE entry_size IS NOT NULL and entry_size > 0
                    ORDER BY last_used DESC
                    LIMIT 500
                    """)

                for row in curs.execute(sql):
                    key = row[0]
                    entry_size = row[1]
                    keys.append(key)
                    size_removed += entry_size
                    if self.__size_cache - size_removed + size < max_size:
                        break

                sql = "DELETE FROM cache_entries WHERE cache_key IN (%s)" % (', '.join(['?']*len(keys)))
                curs.execute(sql, (keys, ))

                self.__db.commit()

                self.__cnt_cache, self.__size_cache = self._get_cnt_and_size_from_db()


    def close(self):
        self.__db.close()