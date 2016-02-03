
from unittest.case import TestCase
import unittest

import os
import shutil
from putil.reflection import EggCache


repo = 'https://pypi.python.org/packages/source/i/iomock'
egg = 'iomock-0.1.tar.gz#md5=13d842a2f989922034601233c826590e'


class TestReflection(TestCase):
    @unittest.skip("Leads to SSL error")
    def setUp(self):
        self.cache = '/tmp/test-cache-%d' % os.getpid()
        os.mkdir(self.cache)
        self.subject = EggCache(cache_dir=self.cache)

    def tearDown(self):
        shutil.rmtree(self.cache)

    def testGetEgg(self):
        path = self.subject.get_egg(egg, repo)
        self.assertTrue(os.path.exists(path))

    def testOnlyDownloadOnce(self):
        # download once
        path = self.subject.get_egg(egg, repo)
        self.assertNotEquals(0, int(os.stat(path).st_size))

        # truncate file
        with open(path, 'w'): pass
        self.assertEquals(0, int(os.stat(path).st_size))

        # request again: should NOT download, sees file already exists
        path = self.subject.get_egg(egg, repo)
        self.assertEquals(0, int(os.stat(path).st_size))

if __name__ == '__main__':
    unittest.main()
