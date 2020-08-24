import os
import weakref
import json
from datetime import datetime
from pathlib import Path
import sqlite3

from filelock import FileLock, Timeout

from ..CacheStorage import CacheStorage
from .size import AutoUpdatingSize
from .handle import CachedFileHandle
from .exceptions import CachedFileLocked, OpenHandlesExist
from ..exceptions import NoItemsCached


def find_files(root, recurse=True):
    for name in os.listdir(root):
        path = os.path.join(root, name)
        if os.path.isfile(path):
            yield name
        elif os.path.isdir(path) and recurse:
            for child in find_files(path):
                yield os.path.join(name, child)


def make_parent_dir(path):
    parent = os.path.dirname(path)
    if not os.path.exists(parent):
        os.makedirs(parent)


class FileCacheStorage(CacheStorage):


    EXPIRE_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

    def __init__(self, path):
        self.__path = path
        self.__size = AutoUpdatingSize(self.__path)
        self.__live_handles = dict()

        self.check_version()


    VERSION = 2
    def check_version(self):
        path = os.path.join(self.__path, '.cache_version')
        if not os.path.exists(path):
            with open(path, 'wt') as fh:
                fh.write(str(self.VERSION))
        else:
            with open(path, "rt") as fh:
                version = fh.read().strip()
                if version != str(self.VERSION):
                    raise ValueError("Cache version is %s.  Expected %s" % (version, self.VERSION))


    @property
    def path(self):
        return self.__path


    @property
    def total_size_stored(self):
        '''Total size of cached data'''
        return self.__size.size


    @property
    def num_items(self):
        '''Number of items in cache'''
        return self.__size.count


    @staticmethod
    def normalize_name(name):
        '''
        Normalize a file name in the storage

        :param name: Path to file relative to cache storage root
        :return: str
        '''
        name = os.path.normpath(name).replace("\\", "/").lstrip('/')
        if name.endswith('/'):
            raise ValueError("FileCache item name can't end with /")
        return name


    def _metadata_path(self, name):
        '''
        Calc path to save metadata for a file stored in the cache

        :param name: Name of the file relative to cache root
        :return: Full path to file
        '''
        return os.path.normpath(os.path.join(
            self.__path,
            self.normalize_name(name) + '.metadata'
        ))


    def _lock_path(self, name):
        '''
        Calc path for item lock file

        :param name: Name of the file relative to cache root
        :return: Full path to file
        '''
        return os.path.normpath(os.path.join(
            self.__path,
            self.normalize_name(name) + '.lock'
        ))


    def _file_path(self, name):
        '''
        Calc path to store file data for item

        :param name: Name of the file relative to cache root
        :return: Full path to file
        '''
        return os.path.normpath(os.path.join(
            self.__path,
            self.normalize_name(name) + '.file'
        ))


    def _get_lock_for(self, name, blocking):
        '''Get a lock for the named file'''

        # Check to see if there's already a handle out
        name = self.normalize_name(name)
        if name in self.__live_handles:
            if self.__live_handles[name]():
                raise CachedFileLocked("Live handle already present for " + name)
            else:
                self._handle_lost(name)

        # Timeout
        if blocking:
            timeout = -1
        else:
            timeout = 5

        # Create file lock
        lock_path = self._lock_path(name)
        make_parent_dir(lock_path)
        lock = FileLock(lock_path, timeout)

        lock.acquire()

        return lock


    def _handle_lost(self, name):
        '''Called when a handle was not released but discovered lost'''
        print("WARNING: handle for cache file %s lost without being released" % (name))
        del self.__live_handles[name]


    def _read_metadata(self, name):
        '''
        Read metadata from disk if present

        :param name: Name of the file
        :return: parsed metadata
        '''
        path = self._metadata_path(name)
        size = 0
        if os.path.exists(path):
            try:

                # Check metadata size
                size = os.path.getsize(path)

                # Decode json
                with open(path, 'rt') as fh:
                    return json.load(fh), size
            except:
                pass

        return dict(), size


    def add(self, key, item):
        '''
        Add an item to the storage

        Note: LRUCache will make room before adding

        :param key: Key to retrieve data with
        :param item: CachedItem
        :raises DuplicateKeyOnAdd:
            LRUCache works to make sure items are unique when queued.  However,
            if a conflict is encountered
        '''
        raise Exception("FileCacheStorage doesn't use add().  Use get() to get a handle")



    def get(self, name, blocking, filecache):
        '''
        Gets a CachedFileHandle providing access to cached file and metadata

        Will return a handle for any name provided, even if not in the cache.
        This is needed to provide mechanism to copy in file under lock.
        Check item.exists() to see if the cached file exists.

        :param name: Path to the file handel to retrieve (relative to cache storage root)
        :param blocking: If True, block until can get file lock
        :param filecache: Need reference to FileCache to pu tin handle
        :return: CachedFileHandle
        '''

        name = self.normalize_name(name)

        # Get lock to file
        lock = self._get_lock_for(name, blocking=blocking)

        # Retrieve metadata if exists
        itemdata, metadata_size = self._read_metadata(name)

        # Check to see if entry has expired
        expires_at = None
        now = datetime.now()
        try:
            if 'expires' in itemdata and itemdata['expires'] is not None:
                expires_at = datetime.strptime(itemdata['expires'], self.EXPIRE_TIMESTAMP_FORMAT)
                if expires_at < now:
                    raise Exception("Expired")
        except:
            self.__delete_item_data(name)
            metadata_size = 0
            metadata = dict()
            expires_at = None

        # Check file size
        file_path = self._file_path(name)
        if os.path.exists(file_path):
            try:
                file_size = os.path.getsize(file_path)
            except Exception as e:
                raise CachedFileLocked("Failed to get size of %s: %s: %s" % (
                    file_path, e.__class__.__name__, str(e)))
        else:
            file_size = 0

        # Get metadata
        try:
            metadata = itemdata['metadata']
        except KeyError:
            metadata = dict()

        # Build handle
        handle = CachedFileHandle(
            filecache = filecache,
            storage = self,
            name = name,
            lock = lock,
            metadata = metadata,
            path = file_path,
            init_size = metadata_size + file_size
        )

        # Save expire time
        if expires_at:
            handle.expires_in = expires_at - now

        # Return
        self.__live_handles[name] = weakref.ref(handle)
        return handle


    def handle_released(self, handle, lock, init_size):
        '''
        Called when a handle is being released, and file should be committed back to DB

        :param handle: Handle being released
        :param lock: The file lock in the handle
        :param init_size: The size of the entry initially
        '''

        if handle.name not in self.__live_handles:
            raise ValueError("Don't have a record of handle %s being live" % (handle.name))

        # If discarded, delete files
        if handle.discarded:
            self.__delete_item_data(handle.name)

        else:

            # Write metadata
            if handle.changed:
                if handle.expires_in is not None:
                    expires_at = datetime.now() + handle.expires_in
                    expires_at = expires_at.strftime(self.EXPIRE_TIMESTAMP_FORMAT)
                else:
                    expires_at = None
                with open(self._metadata_path(handle.name), 'wt') as fh:
                    json.dump({'expires': expires_at, 'metadata': handle.metadata}, fh, indent=4)
            else:
                Path(self._metadata_path(handle.name)).touch()

            # Update accounting
            if init_size > 0:
                self.__size.subtract(init_size)
            new_size = 0
            for path in self._metadata_path(handle.name), self._file_path(handle.name):
                if os.path.exists(path):
                    new_size += os.path.getsize(path)
            self.__size.add(new_size)

        lock.release()
        del self.__live_handles[handle.name]




    def touch_last_used(self, name, blocking):
        '''Mark item as just used'''
        with self._get_lock_for(name):
            path = self._metadata_path(name)
            if os.path.exists(path):
                Path(path).touch()


    def remove(self, name, blocking):
        '''
        Remove a cached item from by it's key

        :raises ItemNotCached: If item not in cache
        '''
        with self._get_lock_for(name, blocking=blocking):
            self.__delete_item_data(name)


    def __delete_item_data(self, name):
        '''Remove an item from the cache (assumed we have a lock on the file)'''
        bytes_deleted = 0
        for path in self._file_path(name), self._metadata_path(name):
            if os.path.exists(path):
                bytes_deleted += os.path.getsize(path)
                os.remove(path)
        if bytes_deleted > 0:
            self.__size.subtract(bytes_deleted)
        else:
            raise NoItemsCached("Name '%s' not cached, so can't remove" % (name))

        # Remove empty parent directories
        parent = os.path.dirname(self._file_path(name))
        while not os.path.samefile(parent, self.__path):
            try:
                os.rmdir(parent)
                parent = os.path.dirname(parent)
            except OSError:
                return


    def has_key(self, name):
        '''
        Check to see if key exists.

        This is only called by the LRUCache class after locking storage.  It's
        not intended to be used outside of lru as typically you want to just try
        and get your key and let if fail if not present.

        Doesn't check expired.  Just checks to see if key is in storage

        :param name: Name of the file
        :return: True if key is stored in storage
        '''
        path = self._metadata_path(name)
        return os.path.exists(name)


    def keys(self):
        '''All cache keys'''
        for path in find_files(self.path):
            if os.path.basename(path).endswith('.metadata'):
                name = path[:-1*len('.metadata')]
                yield self.normalize_name(name)


    def pop_oldest(self):
        '''
        Remove oldest item from the cache (least recently used)

        :return: (key, item)
        '''

        # Have to sort all the files by their mtime
        oldest_mtime = None
        oldest_name = None
        for name in self.keys():
            path = self._metadata_path(name)
            mtime = os.path.getmtime(path)
            if oldest_mtime is None or mtime < oldest_mtime:
                oldest_mtime = mtime
                oldest_name = name

        # TODO: Could potentially cache oldest mtimes for successive calls

        if oldest_name:
            with self._get_lock_for(oldest_name, blocking=False):
                self.__delete_item_data(oldest_name)
            return oldest_name, None
        else:
            raise ValueError("No data in cache")


    def expired_items(self):
        '''
        Find and return keys for any expired items (up to LRUCache to remove)

        :return: generator (key, CachedItem)
        '''

        now = datetime.now()
        for name in self.keys():
            expires_at = None
            try:
                with open(self._metadata_path(name), "rt") as fh:
                    itemdata = json.load(fh)
                    expires_at = datetime.strptime(itemdata['expires'], self.EXPIRE_TIMESTAMP_FORMAT)
            except:
                pass

            if expires_at and expires_at < now:
                yield name, None




    def close(self):
        '''Close storage and sync to disk'''
        # Does really do anything for this storage type.  But will check for outstanding handles
        open_handles = set()
        for name, handle in self.__live_handles.items():
            handle = handle() # resolve weakref
            if handle:
                open_handles.add(name)
            else:
                self._handle_lost(name)
        if len(open_handles) > 0:
            if len(open_handles) < 10:
                raise OpenHandlesExist("Handles still in use for: " + ', '.join(open_handles))
            else:
                raise OpenHandlesExist("%d handles still in use" % (len(open_handles)))



