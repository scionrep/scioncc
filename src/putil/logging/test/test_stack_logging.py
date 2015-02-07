
from putil.logging import config
import logging
import unittest
from unittest.case import TestCase
import os.path
import os
import putil.exception

LOGFILE='/tmp/unittest-stack.log'
CONFIGFILE='logging.yml'

class TestStackLogger(TestCase):
    def setUp(self):
        # clear file
        try: os.remove(LOGFILE)
        except: pass
        # configure logging system
        path = os.path.dirname(__file__) + '/' + CONFIGFILE
        config.replace_configuration(path)
        self.log = logging.getLogger('stack')

    def tearDown(self):
        try: os.remove(LOGFILE)
        except: pass

    def get_lines(self):
        with open(LOGFILE, 'r') as f:
            return f.readlines()

    def test_simple_message(self):
        """ simple message is just one line """
        self.assertEquals(0, os.path.getsize(LOGFILE))
        self.log.info("short message")
        self.assertEquals(1, len(self.get_lines()))

    def test_generic_exception(self):
        """ stack trace should be compact form """
        try:
            splotgorpsh()
        except:
            self.log.error('fralnclumpf', exc_info=True)
        lines = self.get_lines()
        self.assertEquals(3, len(lines))
        self.assertTrue("ERROR" in lines[0])
        self.assertTrue("-----" in lines[1])
        self.assertTrue("splotgorpsh" in lines[2])

    def test_chained_exception(self):
        """ chained stack traces should be compact, columns should be aligned """
        try:
            try:
                splotgorpsh()
            except:
                raise putil.exception.ApplicationException()
        except:
            self.log.error('fralnclumpf', exc_info=True)

        lines = self.get_lines()
        self.assertTrue(len(lines)>4)
        self.assertTrue("ERROR" in lines[0])
        self.assertTrue("-----" in lines[1])

        # stack output should be aligned, filename:line with : as 40th character on line
        aligned_count = 0
        for line in lines:
            if len(line)>40 and line[40]==':':
                aligned_count+=1
        # 3 of the lines won't be stack traces
        self.assertTrue(len(lines)-aligned_count==3, msg='%d of %d lines aligned'%(aligned_count,len(lines)))

if __name__ == '__main__':
    unittest.main()