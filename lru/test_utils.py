from unittest import TestCase


from .utils import LargeKeyList


class TestLargeKeyList(TestCase):

    def test_init_items(self):
        l = LargeKeyList(['a', 'b', 'c'])
        self.assertEqual(list(l), ['a', 'b', 'c'])


    def test_basic_usage(self):
        l = LargeKeyList()
        l.append('a')
        l.append('b')
        l.append('c')

        self.assertEqual(list(l), ['a', 'b', 'c'])


    def test_append_then_read(self):
        l = LargeKeyList()
        l.append('a')
        list(l.all())
        with self.assertRaises(Exception):
            l.append('b')


    def test_large_list(self):
        keys = [str(i) for i in range(LargeKeyList.CHUNK_SIZE*3+2)]
        self.assertEqual(list(LargeKeyList(keys)), keys)


