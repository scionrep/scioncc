
from unittest.case import TestCase
import unittest

import os
import shutil
from ooi.reflection import EggCache

repo = 'http://sddevrepo.oceanobservatories.org/releases'
egg = 'seabird_sbe54tps_ooicore-0.0.4-py2.7.egg'

class TestReflection(TestCase):
    def setUp(self):
        self.cache = '/tmp/test-cache-%d'%os.getpid()
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
