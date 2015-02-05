
from ooi.logging import config
import logging
import unittest
from unittest.case import TestCase
import os.path
import os
import ooi.exception

LOGFILE='/tmp/unittest-raw.log'
CONFIGFILE='logging.yml'

class TestRawLogger(TestCase):
    def setUp(self):
        # clear file
        try: os.remove(LOGFILE)
        except: pass
        # configure logging system
        path = os.path.dirname(__file__) + '/' + CONFIGFILE
        config.replace_configuration(path)
        self.log = logging.getLogger('raw')
        self.count = 0

    def tearDown(self):
        try: os.remove(LOGFILE)
        except: pass

    def read_file(self):
        handle_record_dict = self._handle
        with open(LOGFILE, 'r') as f:
            contents = f.read()
        exec contents

    def test_simple_message(self):
        """ make sure message makes file grow """
        self.assertEquals(0, os.path.getsize(LOGFILE))
        self.log.info("short message")
        self.assertTrue(os.path.getsize(LOGFILE)>0)
        self.read_file()
        self.assertEquals(1, self.count)

    def test_multiple_messages(self):
        """ multiple log statements can be read back as multiple dictionaries """
        self.assertEquals(0, os.path.getsize(LOGFILE))
        self.log.info("short message")
        self.log.info("another short message")
        self.log.info("a third short message")
        self.assertTrue(os.path.getsize(LOGFILE)>0)
        self.read_file()
        self.assertEquals(3, self.count)

    def test_multi_line_messages(self):
        """ newline won't break parsing back to dict """
        self.assertEquals(0, os.path.getsize(LOGFILE))
        self.log.info("short message")
        self.log.info("this message\nspans multiple\nlines of text")
        self.assertTrue(os.path.getsize(LOGFILE)>0)
        self.read_file()
        self.assertEquals(2, self.count)

    def test_messages_with_stack(self):
        """ record can contain stack trace """
        self.assertEquals(0, os.path.getsize(LOGFILE))
        try:
            awjrht()
        except:
            self.log.info("message and stack", exc_info=True)
        self.assertTrue(os.path.getsize(LOGFILE)>0)
        self.read_file()
        self.assertEquals(1, self.count)

    def _handle(self, dict):
        self.count+=1

if __name__ == '__main__':
    unittest.main()