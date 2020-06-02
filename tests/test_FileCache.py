import os
from datetime import timedelta, datetime
from tempfile import mktemp, mkdtemp
import unittest
from unittest import TestCase
from shutil import rmtree
from time import sleep

from lru import FileCache, CachedFileInUse


UNITTEST_TMP_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '.unitest_tmp')

class TestFileCache(TestCase):


    def setUp(self):
        if os.path.exists(UNITTEST_TMP_DIR):
            rmtree(UNITTEST_TMP_DIR)
        os.mkdir(UNITTEST_TMP_DIR)


    def tearDown(self):
        if os.path.exists(UNITTEST_TMP_DIR):
            rmtree(UNITTEST_TMP_DIR)


    def _make_cache(self, max_size=None, max_age=None):
        path = mkdtemp(dir=UNITTEST_TMP_DIR)
        return FileCache(path, max_size=max_size, max_age=max_age)



    def test_put_and_get(self):
        '''Test that we can put and then get a file'''

        cache = self._make_cache()

        with cache.get('file_a.dat') as file:
            self.assertFalse(file.in_cache)
            with file.open('wt') as fh:
                fh.write("Test content")

        with cache.get('file_a.dat') as file:
            self.assertTrue(file.in_cache)
            with file.open('rt') as fh:
                self.assertEqual(fh.read(), "Test content")

        cache.close()


    def test_copy_from(self):
        '''Test that we can put and then get a file'''

        cache = self._make_cache()

        local_path = mktemp(dir=UNITTEST_TMP_DIR)
        with open(local_path, 'wt') as fh:
            fh.write("Test content")

        with cache.get('file_a.dat') as file:
            self.assertFalse(file.in_cache)
            file.copy_from(local_path)

        with cache.get('file_a.dat') as file:
            self.assertTrue(file.in_cache)
            with file.open('rt') as fh:
                self.assertEqual(fh.read(), "Test content")

        os.unlink(local_path)
        cache.close()


    def test_copy_to(self):
        '''Test that we can put and then get a file'''

        cache = self._make_cache()

        local_path = mktemp(dir=UNITTEST_TMP_DIR)

        with cache.get('file_a.dat') as file:
            self.assertFalse(file.in_cache)
            with file.open('wt') as fh:
                fh.write("Test content")

        with cache.get('file_a.dat') as file:
            self.assertTrue(file.in_cache)
            file.copy_to(local_path)
            with open(local_path, 'rt') as fh:
                self.assertEqual(fh.read(), "Test content")

        os.unlink(local_path)
        cache.close()


    def test_put_and_get_metadata(self):
        '''Test that we can put and then get a file'''

        cache = self._make_cache()

        with cache.get('file_a.dat') as file:
            with file.open('wt') as fh:
                fh.write("Test content")
            file.metadata['test'] = 'test_value'

        with cache.get('file_a.dat') as file:
            self.assertTrue(file.in_cache)
            self.assertEqual(file.metadata['test'], 'test_value')

        cache.close()


    def test_cant_open_twice(self):
        cache = self._make_cache()
        with cache.get('file_a.dat') as file:
            with self.assertRaises(CachedFileInUse):
                with cache.get('file_a.dat') as file2:
                    pass
        cache.close()


    def test_discard(self):

        cache = self._make_cache()

        with cache.get('file_a.dat') as file:
            with file.open('wt') as fh:
                fh.write("Test content")
            file.metadata['test'] = 'test_value'

        with cache.get('file_a.dat') as file:
            self.assertFalse(file.discarded)
            file.discard()
            self.assertTrue(file.discarded)

        with cache.get('file_a.dat') as file:
            self.assertFalse(file.in_cache)
            self.assertFalse(os.path.exists(file.path))

        cache.close()


    def test_immediate_discard(self):

        cache = self._make_cache()

        with cache.get('file_a.dat') as file:
            with file.open('wt') as fh:
                fh.write("Test content")
            file.metadata['test'] = 'test_value'
            file.discard()

        with cache.get('file_a.dat') as file:
            self.assertFalse(file.in_cache)
            self.assertFalse(os.path.exists(file.path))

        cache.close()


    def test_num_files(self):
        cache = self._make_cache()

        with cache.get('file_a.dat') as file:
            with file.open('wt') as fh:
                fh.write("Test content")

        with cache.get('file_b.dat') as file:
            with file.open('wt') as fh:
                fh.write("Test content")

        self.assertEqual(cache.num_files, 2)


    def test_list_names(self):
        for scenario, cache in self._build_lru_configurations():
            with self.subTest(scenario=scenario):
                cache['abc'] = {'my_data': 'a'}
                cache['xyz'] = {'my_data': 'b'}
                self.assertEqual(set(cache.keys()), set(['abc', 'xyz']))


    # def test_list_items(self):
    #     for scenario, cache in self._build_lru_configurations():
    #         with self.subTest(scenario=scenario):
    #             cache['abc'] = {'my_data': 'a'}
    #             cache['xyz'] = {'my_data': 'b'}
    #
    #             rtn = dict()
    #             for key, data in cache.items():
    #                 rtn[key] = data
    #
    #             self.assertEqual(rtn, {
    #                 'abc': {'my_data': 'a'},
    #                 'xyz': {'my_data': 'b'},
    #             })
    #
    #
    # def test_item_replaced(self):
    #     for storage_name, storage in self._build_storages():
    #         cache = LRUCache(storage=storage, sizeof=lambda d: 1)
    #         with self.subTest(scenario=storage_name):
    #             cache['abc'] = {'my_data': 'a'}
    #             cache['abc'] = {'my_data': 'b'}
    #
    #             self.assertEqual(cache['abc'], {'my_data': 'b'})
    #             self.assertEqual(cache.total_size_stored, 1)
    #             self.assertEqual(cache.num_items, 1)


    def test_lru_evict(self):
        for storage_name, storage in self._build_storages():
            cache = LRUCache(storage=storage, max_size=2, max_age=None, sizeof=lambda i: 1)
            with self.subTest(scenario=storage_name):
                cache['abc'] = {'my_data': 'a'}
                cache['def'] = {'my_data': 'b'}
                cache['ghi'] = {'my_data': 'c'}
                with self.assertRaises(ItemNotCached):
                    cache['abc']
                self.assertEqual(cache.total_size_stored, 2)


    def test_item_expired(self):
        for storage_name, storage in self._build_storages():
            cache = LRUCache(storage=storage, max_age=timedelta(seconds=1))
            with self.subTest(scenario=storage_name):
                cache['abc'] = {'my_data': 'a'}
                sleep(1.1)
                with self.assertRaises(ItemExpired):
                    cache['abc']
                self.assertEqual(cache.total_size_stored, 0)


    # def test_set_item_expired(self):
    #     for storage_name, storage in self._build_storages():
    #         cache = LRUCache(storage=storage)
    #         with self.subTest(scenario=storage_name):
    #             cache.put(key='abc', data={'my_data': 'a'}, expires_in=timedelta(seconds=1))
    #
    #             # Make sure not expired yet
    #             self.assertEqual(cache['abc'], {'my_data': 'a'})
    #
    #             # Check expires
    #             sleep(1.1)
    #             with self.assertRaises(ItemExpired):
    #                 cache['abc']
    #             self.assertEqual(cache.total_size_stored, 0)


    def test_clean_expired(self):
        for storage_name, storage in self._build_storages():
            cache = LRUCache(storage=storage, sizeof=lambda i: 1)
            with self.subTest(scenario=storage_name):
                cache.put(key='abc', data={'my_data': 'a'}, expires_in=timedelta(seconds=1))
                cache.put(key='def', data={'my_data': 'a'}, expires_in=timedelta(seconds=1))
                cache.put(key='ghi', data={'my_data': 'a'}, expires_in=timedelta(days=1))
                cache.put(key='jkl', data={'my_data': 'a'})
                self.assertEqual(cache.total_size_stored, 4)
                sleep(1.1)
                cache.clean_expired()
                self.assertEqual(cache.total_size_stored, 2)


    # def test_item_too_big(self):
    #     for storage_name, storage in self._build_storages():
    #         cache = LRUCache(storage=storage, max_size=2, max_age=None, sizeof=lambda i: 10)
    #         with self.subTest(scenario=storage_name):
    #             cache['abc'] = {'my_data': 'a'}
    #             with self.assertRaises(ItemNotCached):
    #                 cache['abc']
    #             self.assertEqual(cache.total_size_stored, 0)
    #
    #
    # def test_removed_not_in(self):
    #     for scenario, cache in self._build_lru_configurations():
    #         with self.subTest(scenario=scenario):
    #             cache['abc'] = {'my_data': 'a'}
    #             del cache['abc']
    #             cache['def'] = {'my_data': 'b'}
    #             with self.assertRaises(ItemNotCached):
    #                 cache['abc']
    #             self.assertEqual(cache['def'], {'my_data': 'b'})
    #
    #
    # DISK_STORAGES = (ShelvedStorage, Sqlite3Storage)
    #
    # def test_can_reopen(self):
    #     for storage_class in self.DISK_STORAGES:
    #         with self.subTest(scenario=storage_class.__name__):
    #             path = mktemp(dir=UNITTEST_TMP_DIR)
    #
    #             cache = LRUCache(storage=storage_class(path=path))
    #             cache['abc'] = {'my_data': 'a'}
    #             cache.close()
    #
    #             cache = LRUCache(storage=storage_class(path=path))
    #             self.assertEqual(cache['abc'], {'my_data': 'a'})
    #             cache.close()
    #
    #
    # def test_reopen_preserves_lru(self):
    #     for storage_class in self.DISK_STORAGES:
    #         with self.subTest(scenario=storage_class.__name__):
    #             path = mktemp(dir=UNITTEST_TMP_DIR)
    #
    #             cache = LRUCache(storage=storage_class(path=path), max_size=2, sizeof=lambda i: 1)
    #             cache['abc'] = {'my_data': 'a'}
    #             cache['def'] = {'my_data': 'b'}
    #             cache.close()
    #
    #             cache = LRUCache(storage=storage_class(path=path), max_size=2, sizeof=lambda i: 1)
    #             cache['ghi'] = {'my_data': 'c'}
    #             with self.assertRaises(ItemNotCached):
    #                 cache['abc']
    #             self.assertEqual(cache.total_size_stored, 2)
    #             self.assertEqual(cache['def'], {'my_data': 'b'})
    #             self.assertEqual(cache['ghi'], {'my_data': 'c'})
    #             cache.close()
    #
    #
    # def test_put_and_get_large_cnt(self):
    #     '''Test that we can put and then get a value'''
    #     run_limit = timedelta(seconds=6)
    #     for scenario, cache in self._build_lru_configurations():
    #         with self.subTest(scenario=scenario):
    #
    #             started = datetime.now()
    #
    #             for i in range (10000):
    #                 cache[str(i)] = {'d': i}
    #                 if datetime.now() - started > run_limit:
    #                     print("Only added %d items in %d secs for %s" % (
    #                         i+1, run_limit.total_seconds(), scenario))
    #                     break
    #
    #             self.assertEqual(cache[str(i-1)], {'d': i-1})


if __name__ == '__main__':
    unittest.main()
