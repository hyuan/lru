import os
from datetime import datetime, timedelta


def find(root, recurse=True):
    for name in os.listdir(root):
        path = os.path.join(root, name)
        if os.path.isfile(path):
            yield name
        elif os.path.isdir(path) and recurse:
            for child in find(path):
                yield os.path.join(name, child)


class SizeAutoUpdated(Exception): pass


class AutoUpdatingSize:
    '''
    Class to track the size of the currect cache.

    This value is held in memory for some time and updated as files are added
    and removed.  However, it's recalculated from disk every so often as other
    processes can potentially modify the cache files.
    '''

    DEFAULT_RESCAN_EVERY = timedelta(minutes=10)

    def __init__(self, path, rescan_every=DEFAULT_RESCAN_EVERY):
        self.__path = path
        self.__size = 0
        self.__count = 0
        self.rescan_every = rescan_every
        self.__next_scan = None


    @property
    def size(self):
        if self.__next_scan is None or self.__next_scan < datetime.now():
            self.rescan_disk()
        return self.__size


    @property
    def count(self):
        if self.__next_scan is None or self.__next_scan < datetime.now():
            self.rescan_disk()
        return self.__count


    def rescan_disk(self):
        '''Scan through the files on the disk to get the current size'''
        self.__size = 0
        self.__count = 0

        for file in find(self.__path):

            path = os.path.join(self.__path, file)

            try:
                self.__size += os.path.getsize(path)
            except Exception as e:
                print("FAILED to get size of %s: %s: %s" % (path, e.__class__.__name__, str(e)))

            if os.path.basename(path).endswith('.metadata'):
                self.__count += 1

        self.__next_scan = datetime.now() + self.rescan_every


    def add(self, bytes, files=1):
        self.__size += bytes
        self.__count += files


    def subtract(self, bytes, files=1):
        self.__size -= bytes
        self.__count -= files






