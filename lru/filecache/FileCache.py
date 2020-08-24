import os

from .. import LRUCache, ItemNotCached

from .exceptions import CachedFileLocked
from .storage import FileCacheStorage

class FileCache(LRUCache):
    '''
    LRU Cache of files

    A LRU cache built to store files on top of LRUCache.  Stores files in a defined
    directory along with a sqlite3 DB (Sqlite3Storage) to track file usage and
    metadata, evicting files to make space when needed.

    Notes:
     - Keys are file names (relate to path)
     - Can have key without file
    '''

    def __init__(self, path, max_size=None, max_age=None):
        '''
        :param path: Path to directory to cache files in
        :param max_size: Maximum number of bytes to store
        :param max_age:
            Maximum age to store for.
            Must call .clean_expired() to enact removal of expired
        '''

        if not os.path.exists(path):
            raise ValueError("Directory doesn't exist: " + path)
        if not os.path.isdir(path):
            raise ValueError("Path is not a directory: " + path)

        super().__init__(
            storage = FileCacheStorage(path),
            max_size = max_size,
            max_age = max_age)
        
        if os.path.exists(os.path.join(path, "index.db")):
            self.migrate_v1_cache(path)



    @property
    def path(self):
        '''Path to directory where cached files are stored'''
        return self.storage.path



    def get(self, name, blocking=True):
        '''
        Get CachedFileHandle for desired cached file.

        See CachedFileHandle.get()

        :param name: Path to the file to get handle for.
        :param blocking:
            If a handle is already open for the given name, then wait
            for lock to be released.  Else, throw CachedFileLocked
        :return:
        '''
        handle = self.storage.get(name, blocking=blocking, filecache=self)
        if not handle.expires_in and self.max_age:
            handle.expires_in = self.max_age
        return handle


    def __getitem__(self, name):
        '''Get metadata for item'''
        with self.get(name) as handle:
            return handle.metadata


    def put(self, name, metadata=None, local_path=None, expires_in=None, blocking=True):
        '''
        Put an item into the cache

        :param name: Name of the file entry
        :param metadata: to set
        :param local_path: Path of
        :param expires_in:
        :return:
        '''
        with self.get(name, blocking=blocking) as handle:
            handle.metadata = metadata
            handle.copy_from(local_path)
            if expires_in:
                handle.expire_in(expires_in)



    def __setitem__(self, name, metadata):
        '''Set metadata for item'''
        with self.get(name) as handle:
            handle.metadata = metadata


    def handle_released(self, handle):
        '''Callback to note that a file handle from get() got released'''

        # Do accounting to see if we have too many files
        if handle.changed:
            if self.max_size:
                while self.storage.total_size_stored > self.max_size:
                    self.storage.pop_oldest()



    def migrate_v1_cache(self, path):
        '''Migrate v1 data to new cache format'''

        print("Migrating from v1 cache")

        import sqlite3
        import json

        db_path = os.path.join(path, 'index.db')
        db = sqlite3.connect(db_path)

        sql = """\
            select cache_key, item_data
            from cache_entries
            order by last_used
            """

        curs = db.cursor()
        for name, metadata in curs.execute(sql):
            print(" - " + name)

            metadata = json.loads(metadata)

            v1_path = os.path.join(path, 'files', name)
            if os.path.exists(v1_path):

                with self.get(name, blocking=True) as item:

                    item.copy_from(v1_path)
                    item.metadata = metadata

                os.unlink(v1_path)

        db.close()
        os.unlink(db_path)






