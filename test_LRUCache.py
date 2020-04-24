import os
from datetime import timedelta
from tempfile import mktemp
from unittest import TestCase
from shutil import rmtree
from time import sleep

from lru import MemoryStorage, ShelvedStorage, Sqlite3Storage
from lru import LRUCache
from lru import NoItemCached, ItemExpired


UNITTEST_TMP_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '.unitest_tmp')

class TestLRUCache(TestCase):


    def _build_storages(self):

        yield 'memory', MemoryStorage()

        # path = mktemp(dir=UNITTEST_TMP_DIR)
        # yield 'shelved', ShelvedStorage(path=path)

        # path = mktemp(dir=UNITTEST_TMP_DIR)
        # yield 'sqlite3', Sqlite3Storage(path=path)


    def _build_lru_configurations(self, storages=None, sizeof=None):

        for storage_name, storage in storages or self._build_storages():

            yield storage_name + ' noopts', LRUCache(storage=storage, max_size=None, max_age=None, sizeof=sizeof)
            yield storage_name + ' w/maxsize', LRUCache(storage=storage, max_size=1024, max_age=None, sizeof=sizeof)
            yield storage_name + ' w/maxage', LRUCache(storage=storage, max_size=None, max_age=timedelta(days=1), sizeof=sizeof)
            yield storage_name + ' w/maxsize&age', LRUCache(storage=storage, max_size=1024, max_age=timedelta(days=1), sizeof=sizeof)


    def setUp(self):
        if os.path.exists(UNITTEST_TMP_DIR):
            rmtree(UNITTEST_TMP_DIR)
        os.mkdir(UNITTEST_TMP_DIR)


    def tearDown(self):
        if os.path.exists(UNITTEST_TMP_DIR):
            rmtree(UNITTEST_TMP_DIR)


    def test_put_and_get(self):
        '''Test that we can put and then get a value'''

        for scenario, cache in self._build_lru_configurations():
            with self.subTest(scenario=scenario):
                cache['abc'] = {'my_data': 'a'}
                cache['def'] = {'my_data': 'b'}
                cache['xyz'] = {'my_data': 'c'}
                self.assertEqual(cache['abc'], {'my_data': 'a'})
                self.assertEqual(cache['def'], {'my_data': 'b'})
                self.assertEqual(cache['xyz'], {'my_data': 'c'})
                self.assertEqual(cache.get('xyz'), {'my_data': 'c'})
                cache.close()


    def test_cache_miss(self):
        for scenario, cache in self._build_lru_configurations():
            with self.subTest(scenario=scenario):
                cache['abc'] = {'my_data': 'a'}
                with self.assertRaises(NoItemCached):
                    cache['xyz']


    def test_custom_size_func(self):
        for scenario, cache in self._build_lru_configurations(sizeof=lambda d: 1):
            with self.subTest(scenario=scenario):
                cache['abc'] = {'my_data': 'a'}
                cache['xyz'] = {'my_data': 'a'}
                self.assertEqual(cache.total_size_stored, 2)


    def test_lru_evict(self):
        for storage_name, storage in self._build_storages():
            cache = LRUCache(storage=storage, max_size=2, max_age=None, sizeof=lambda i: 1)
            with self.subTest(scenario=storage_name):
                cache['abc'] = {'my_data': 'a'}
                cache['def'] = {'my_data': 'b'}
                cache['ghi'] = {'my_data': 'c'}
                with self.assertRaises(NoItemCached):
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


    def test_set_item_expired(self):
        for storage_name, storage in self._build_storages():
            cache = LRUCache(storage=storage)
            with self.subTest(scenario=storage_name):
                cache.put(key='abc', data={'my_data': 'a'}, expires_in=timedelta(seconds=1))

                # Make sure not expired yet
                self.assertEqual(cache['abc'], {'my_data': 'a'})

                # Check expires
                sleep(1.1)
                with self.assertRaises(ItemExpired):
                    cache['abc']
                self.assertEqual(cache.total_size_stored, 0)


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


    def test_item_too_big(self):
        for storage_name, storage in self._build_storages():
            cache = LRUCache(storage=storage, max_size=2, max_age=None, sizeof=lambda i: 10)
            with self.subTest(scenario=storage_name):
                cache['abc'] = {'my_data': 'a'}
                with self.assertRaises(NoItemCached):
                    cache['abc']
                self.assertEqual(cache.total_size_stored, 0)



    # def test_removed_keyerror(self):
    #     for scenario, cache in self._build_lru_configurations():
    #         with self.subTest(scenario=scenario):
    #             cache['abc'] = {'my_data': 'a'}
    #             del cache['abc']
    #             with self.assertRaises(KeyError):
    #                 cache['abc']
    #
    # def test_removed_not_has(self):
    #     for scenario, cache in self._build_lru_configurations():
    #         with self.subTest(scenario=scenario):
    #             cache['abc'] = {'my_data': 'a'}
    #             del cache['abc']
    #             self.assertFalse(cache.has('abc'))
    #
    # def test_removed_not_in(self):
    #     for scenario, cache in self._build_lru_configurations():
    #         with self.subTest(scenario=scenario):
    #             cache['abc'] = {'my_data': 'a'}
    #             del cache['abc']
    #             self.assertFalse('abc' not in cache)


if __name__ == '__main__':
    unittest.main()