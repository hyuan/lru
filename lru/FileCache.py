import os
import sys
from contextlib import AbstractContextManager
from shutil import copyfile

from .LRUCache import LRUCache, ItemNotCached
from .Sqlite3Storage import Sqlite3Storage


class CachedFileInUse(ValueError): pass


class CachedFileHandle(AbstractContextManager):
    '''
    Holds a reference to a file in the cache

    Handle is intended to be used in with context that 'locks' the referenced
    file until it is not longer being used preventing it from being removed
    by a another call to FileCache.get() or FileCache.clean_expired(), etc.
    '''

    def __init__(self, cache, name, metadata):
        '''
        :param cache: Reference back to FileCache
        :param name: Name of the file in the cache
        :param metadata: Metadata of the file
        '''
        self.__cache = cache
        self.__name = name
        self.__in_cache = os.path.exists(self.path)
        self.__discarded = False
        self.metadata = metadata
        self.__init_metadata = self.metadata.copy()


    def __str__(self):
        return self.name


    def __repr__(self):
        return "%s('%s', '%s')" % (
            self.__class__.__name__,
            self.__cache.path,
            self.__name)


    @property
    def cache(self):
        '''Cache object this handle belongs to'''
        return self.__cache


    @property
    def name(self):
        '''Path to location in cache file is stored in'''
        return self.__name


    @property
    def path(self):
        '''Path to location in cache file is stored in'''
        return os.path.normpath(os.path.join(self.__cache.files_path, self.__name))


    @property
    def in_cache(self):
        return self.__in_cache


    @property
    def metadata_changed(self):
        return self.metadata != self.__init_metadata


    def open(self, mode):
        '''
        Open file in cache directory

        :param mode: Mode to open in
        '''

        # TODO: Ever want to track file handles are closed before release()?

        self._mk_path_dir()
        return open(self.path, mode=mode)


    def copy_from(self, path):
        '''
        Copy a file into the cache from disk

        If file already exists in path, will overwrite and update

        :param path: Path on disk to copy from
        '''
        if not os.path.isfile(path):
            raise ValueError("Path is not an existing file: " + path)
        if os.path.exists(self.path) and os.path.samefile(path, self.path):
            raise ValueError("Can't copy from self")

        self._mk_path_dir()

        copyfile(path, self.path)


    def _mk_path_dir(self):
        '''Create directory to save this file to in the cache file store'''
        parent = os.path.dirname(self.path)
        if not os.path.exists(parent):
            os.makedirs(parent)


    def copy_to(self, path):
        '''
        Copy a file out of the cache

        :param path: Path on disk to copy to
        '''
        copyfile(self.path, path)


    def discard(self):
        '''Mark file to be discarded from cache'''
        self.__discarded = True


    @property
    def discarded(self):
        '''Has file been marked to be discarded'''
        return self.__discarded


    # File is assumed locked before handle is created, so no __enter__


    def __exit__(self, exc_type, exc_value, traceback):
        '''release lock'''
        self.release()


    def release(self):
        '''
        Tell cache we're done working with the file.

        This causes the metadata to be written to disk, and the file
        tracking to be updated
        '''
        self.__cache.release_handle(handle=self)


class FileMetaDataCache(LRUCache):

    def __init__(self, file_store_path, storage=None, max_size=None, sizeof=None, max_age=None):
        super().__init__(storage, max_size, sizeof, max_age)
        self.__in_use_names = set()
        self.__file_store_path = file_store_path


    def is_in_use(self, name):
        return name in self.__in_use_names


    def mark_in_use(self, name):
        if name in self.__in_use_names:
            raise ValueError("Already in locks")
        self.__in_use_names.add(name)


    def mark_not_in_use(self, name):
        if name not in self.__in_use_names:
            raise ValueError("Name not in locks")
        self.__in_use_names.remove(name)


    def remove(self, name):
        if not self.is_in_use(name):
            super().remove(name)
            path = os.path.join(self.__file_store_path, name)
            os.remove(path)


class FileCache:
    '''
    LRU Cache of files

    A LRU cache built to store files on top of LRUCache.  Stores files in a defined
    directory along with a sqlite3 DB (Sqlite3Storage) to track file usage and
    metadata, evicting files to make space when needed.
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

        self.__path = path
        self.__cache = FileMetaDataCache(
            file_store_path = self.files_path,
            storage = Sqlite3Storage(path=os.path.join(path, 'index.db')),
            max_size = max_size,
            max_age = max_age)
        self.__open_states = dict()


    @property
    def path(self):
        '''Path to directory where cached files are stored'''
        return self.__path


    @property
    def files_path(self):
        return os.path.join(self.__path, 'files')


    def get(self, name):
        '''
        Get a file handle from the cache

        :param name: Name to identify the file.  Can have slashes.
        :return: CachedFileHandle
        '''
        name = os.path.normpath(name)

        if self.__cache.is_in_use(name):
            raise CachedFileInUse()
        self.__cache.mark_in_use(name)

        try:
            metadata = self.__cache[name]
        except ItemNotCached:
            metadata = dict()

        handle = CachedFileHandle(cache = self, name = name, metadata = metadata)

        if os.path.exists(handle.path) and not os.path.isfile(handle.path):
            raise ValueError("File name '%s' already exists as directory in cache" % (name))

        if os.path.exists(handle.path):
            self.__open_states[name] = {
                'exists': True,
                'size': os.path.getsize(handle.path),
                'mtime': os.path.getmtime(handle.path),
            }
        else:
            self.__open_states[name] = {
                'exists': False,
            }

        return handle



    def release_handle(self, handle):
        '''
        A handle has been released for a cched file.

        Check for changes on the file or metadata writes that need to be stored,
        and update file size

        :param handle: CachedFileHandle that was released
        '''

        open_state = self.__open_states[handle.name]
        del self.__open_states[handle.name]

        # Check if file has been marked for discard
        if handle.discarded:
            del self.__cache[handle.name]

        # Check to see if file is present and has changed
        elif os.path.exists(handle.path):
            size = os.path.getsize(handle.path)
            mtime = os.path.getmtime(handle.path)
            if not open_state['exists'] or open_state['size'] != size or open_state['mtime'] != mtime:
                self.__cache.put(
                    key = handle.name,
                    data = handle.metadata,
                    size = sys.getsizeof(handle.metadata) + size
                )

        self.__cache.mark_not_in_use(handle.name)


    def close(self):
        self.__cache.close()

