import os
import shelve
from shutil import rmtree

from tempfile import TemporaryDirectory

class LargeKeyList:
    '''Can hold a large number of keys'''

    CHUNK_SIZE = 1000

    def __init__(self, first_items=None, single_pass = True):
        self.__tmp = None
        self.__shelf = None
        self.__reading = False
        self.__closed = False
        self.__chunk_cnt = 1
        self.__last_chunk = list()  # Store here unless we get too many
        self.__single_pass = single_pass
        if first_items:
            for item in first_items:
                self.append(item)


    def append(self, key):
        '''Add item to end of list'''

        if self.__reading:
            raise Exception("append(), then read")
        if self.__closed:
            raise Exception("already closed")

        # Move chunks to disk
        if len(self.__last_chunk) >= self.CHUNK_SIZE:

            if self.__tmp is None:
                self.__tmp = TemporaryDirectory()
                self.__shelf = shelve.open(os.path.join(self.__tmp.name, "keys"))

            self.__shelf[str(self.__chunk_cnt)] = self.__last_chunk
            self.__chunk_cnt += 1
            self.__last_chunk = list()

        self.__last_chunk.append(key)


    def all(self):
        '''Yield back all the keys'''

        if self.__closed:
            raise Exception("already closed")
        self.__reading = True

        for i in range(self.__chunk_cnt-1):
            for key in self.__shelf[str(i+1)]:
                yield key

        for key in self.__last_chunk:
            yield key

        if self.__single_pass:
            self.close()


    def __iter__(self):
        return self.all()


    def close(self):
        if self.__shelf:
            self.__shelf.close()
            self.__shelf = None
        if self.__tmp:
            self.__tmp.cleanup()
            self.__tmp = None
        self.__closed = True

