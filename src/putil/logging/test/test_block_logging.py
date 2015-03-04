import logging
import os

from putil.logging import config
from putil.testing import UtilTest

LOGFILE = '/tmp/unittest-block.log'
CONFIGFILE = 'logging.yml'


class TestBlockLogger(UtilTest):
    def setUp(self):
        # clear file
        try: os.remove(LOGFILE)
        except: pass
        # configure logging system
        path = os.path.dirname(__file__) + '/' + CONFIGFILE
        config.replace_configuration(path)
        self.log = logging.getLogger('block')

    def tearDown(self):
        try: os.remove(LOGFILE)
        except: pass

    def test_write_short_message(self):
        #""" show a short message is not immediately written to logfile """
        self.assertEquals(0, os.path.getsize(LOGFILE))
        self.log.info('short message')
        self.assertEquals(0, os.path.getsize(LOGFILE))

    def test_write_lots_of_short_messages(self):
        #""" show short messages are eventually written to logfile in block increments """
        self.assertEquals(0, os.path.getsize(LOGFILE))
        for x in xrange(50):
            self.log.info('short message')
        self.assertTrue(os.path.getsize(LOGFILE)>0)
        self.assertTrue(os.path.getsize(LOGFILE)%512==0)

    def test_write_long_message(self):
        #""" show long message may be partially written in block increments """
        msg = 'now is the time for all good men to come to the aid of their king!' * 10
        self.assertTrue(len(msg)>512)
        self.assertTrue(len(msg)<1024)
        self.assertEquals(0, os.path.getsize(LOGFILE))
        self.log.info(msg)
        self.assertEquals(512, os.path.getsize(LOGFILE))

    def test_write_severe_message(self):
        #""" show even short messages are written immediately if severity is high enough """
        self.assertEquals(0, os.path.getsize(LOGFILE))
        self.log.info("small message")
        self.assertEquals(0, os.path.getsize(LOGFILE))
        self.log.error("small message")
        self.assertTrue(os.path.getsize(LOGFILE)>0)

    def test_returns_to_block(self):
        #""" show after severe message writes non-block boundary, future writes will return to block boundary """
        # due to severe message, not on block boundary
        self.assertEquals(0, os.path.getsize(LOGFILE))
        self.log.error("small message")
        self.assertTrue(os.path.getsize(LOGFILE)>0)
        self.assertTrue(os.path.getsize(LOGFILE)%512<>0)
        # but next write will return to block boundary
        msg = 'now is the time for all good men to come to the aid of their king!' * 10
        self.log.info(msg)
        self.assertTrue(os.path.getsize(LOGFILE)%512==0)
