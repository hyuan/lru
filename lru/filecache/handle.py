import os
import sys
from contextlib import AbstractContextManager
from shutil import copyfile

class CachedFileHandle(AbstractContextManager):
    '''
    Holds a reference to a file in the cache

    Handle is intended to be used in with context that 'locks' the referenced
    file until it is not longer being used preventing it from being removed
    by a another call to FileCache.get() or FileCache.clean_expired(), etc.
    '''


    def __init__(self, filecache, storage, name, lock, metadata, path, init_size):
        '''
        :param filecache: Reference back to filecache
        :param storage: Reference back to storage
        :param name: Name of the file in the cache
        :param lock: Lock for this entry
        :param metadata: Metadata from storage
        :param path: Path to the file inside the cache
        :param init_size: Size of the entry at the beginning
        '''
        self.__filecache = filecache
        self.__storage = storage
        self.__name = name
        self.__lock = lock
        self.__path = path
        self.__discarded = False
        self.metadata = metadata
        self.__init_metadata = self.metadata.copy()
        self.__init_size = init_size
        self.expires_in = None
        self.__changed = False



    def release(self):
        '''
        Tell storage we're done working with the file.

        This causes the metadata to be written to disk, and the file
        tracking to be updated
        '''
        self._assert_not_released()
        self.__storage.handle_released(
            handle = self,
            lock = self.__lock,
            init_size = self.__init_size)
        self.__lock = None
        self.__filecache.handle_released(self)


    def _assert_not_released(self):
        if self.__lock is None:
            raise Exception("Can't call after releasing handle")


    def __str__(self):
        return self.name


    def __repr__(self):
        return "CachedFileHandle('%s')" % (self.__name)


    @property
    def name(self):
        '''Path to location in cache file is stored in'''
        return self.__name


    @property
    def path(self):
        '''Path to location in cache file is stored in'''
        return self.__path


    def exists(self):
        return os.path.exists(self.__path)


    @property
    def in_cache(self):
        return self.exists()


    @property
    def metadata_changed(self):
        return self.metadata != self.__init_metadata


    @property
    def changed(self):
        '''Was the file or metadata changed?'''
        return self.__changed or self.metadata_changed


    def open(self, mode):
        '''
        Open file in cache directory

        :param mode: Mode to open in
        '''
        self._assert_not_released()

        # TODO: Ever want to track file handles are closed before release()?

        if 'w' in mode.lower():
            self.__changed = True

        self._mk_path_dir()
        return open(self.__path, mode=mode)


    def copy_from(self, path):
        '''
        Copy a file into the cache from disk

        If file already exists in path, will overwrite and update

        :param path: Path on disk to copy from
        '''
        self._assert_not_released()

        if not os.path.isfile(path):
            raise ValueError("Path is not an existing file: " + path)
        if os.path.exists(self.path) and os.path.samefile(path, self.path):
            raise ValueError("Can't copy from self")

        self._mk_path_dir()

        copyfile(path, self.__path)
        self.__changed = True


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
        self._assert_not_released()
        copyfile(self.__path, path)


    def discard(self):
        '''Mark file to be discarded from cache'''
        self._assert_not_released()
        self.__discarded = True
        self.__changed = True


    @property
    def discarded(self):
        '''Has file been marked to be discarded'''
        return self.__discarded


    def __exit__(self, exc_type, exc_value, traceback):
        '''release lock'''
        self.release()


