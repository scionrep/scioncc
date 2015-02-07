
from putil.poller import BlockingDirectoryIterator
from unittest.case import TestCase
import unittest

from uuid import uuid4
import os
import shutil
from gevent import spawn
from time import sleep

class TestFileIteration(TestCase):
    def setUp(self):
        self.dir = '/tmp/%s' % uuid4()
        os.mkdir(self.dir)
    def tearDown(self):
        shutil.rmtree(self.dir)

    def testFilePoll(self):
        """
        in another thread, BlockingDirectoryIterator will listen for and append new files to a list
        this test creates/removes files and checks what is in the list
        """
        self.exception = None
        self.values = []
        self.target = BlockingDirectoryIterator(self.dir,'A*.DAT',.1)
        thread = spawn(self._listen_for_files)

        # can read existing files
        self._create_file('A001.DAT')
        self._create_file('A002.DAT')
        sleep(0.25)
        self.assertEqual(['A001.DAT','A002.DAT'], self.values)

        # responds to new files added
        self._create_file('A003.DAT')
        self._create_file('A004.DAT')
        sleep(0.1)
        self.assertEqual(['A001.DAT','A002.DAT','A003.DAT','A004.DAT'], self.values)

        # ignores files out of sequence
        self._create_file('A005.DAT')
        self._create_file('A000.DAT')
        sleep(0.1)
        self.assertEqual(['A001.DAT','A002.DAT','A003.DAT','A004.DAT','A005.DAT'], self.values)

        # fails when file removed
        os.remove(self.dir+'/A005.DAT')
        sleep(0.2)
        self.assertEqual(['A001.DAT','A002.DAT','A003.DAT','A004.DAT','A005.DAT'], self.values)
        self.assertTrue(thread.dead)
        self.assertTrue(self.exception is not None)

    def _create_file(self, name):
        with file(self.dir+'/'+name,'w+'):
            pass

    def _listen_for_files(self):
        try:
            for f in self.target.get_files():
                self.values.append(f.split('/')[-1])
        except Exception as e:
            self.exception = e


if __name__ == '__main__':
    unittest.main()
