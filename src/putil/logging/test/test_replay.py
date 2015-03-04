import logging
import os

from putil.logging import config
from putil.testing import UtilTest
import putil.exception
import putil.logging.replay

RAW_LOGFILE = '/tmp/unittest-raw.log'
REPLAY_LOGFILE = '/tmp/unittest-replay.log'
CONFIGFILE = 'logging.yml'


class TestRawLogger(UtilTest):
    def setUp(self):
        # clear file
        try: os.remove(RAW_LOGFILE)
        except: pass
        try: os.remove(REPLAY_LOGFILE)
        except: pass
        # configure logging system
        path = os.path.dirname(__file__) + '/' + CONFIGFILE
        config.replace_configuration(path)
        self.log = logging.getLogger('raw')

        replay_log = logging.getLogger('replay')
        handler = replay_log.handlers[0]
        self.replay = putil.logging.replay.Replayer(handler)

    def tearDown(self):
        try: os.remove(RAW_LOGFILE)
        except: pass
        try: os.remove(REPLAY_LOGFILE)
        except: pass

    def read_file(self):
        with open(REPLAY_LOGFILE, 'r') as f:
            contents = f.readlines()
        self.count = len(contents)

    def test_simple_message(self):
        #""" make sure message makes file grow """
        self.assertEquals(0, os.path.getsize(RAW_LOGFILE))
        self.log.info("short message")
        self.replay.relay(RAW_LOGFILE)
        self.assertTrue(os.path.getsize(REPLAY_LOGFILE)>0)
        self.read_file()
        self.assertEquals(1, self.count)

    def test_multiple_messages(self):
        #""" multiple log statements can be read back as multiple dictionaries """
        self.assertEquals(0, os.path.getsize(RAW_LOGFILE))
        self.log.info("short message")
        self.log.info("another short message")
        self.log.info("a third short message")
        self.replay.relay(RAW_LOGFILE)
        self.assertTrue(os.path.getsize(REPLAY_LOGFILE)>0)
        self.read_file()
        self.assertEquals(3, self.count)

    def test_multi_line_messages(self):
        #""" newline won't break parsing back to dict """
        self.assertEquals(0, os.path.getsize(RAW_LOGFILE))
        self.log.info("short message")
        self.log.info("this message\nspans multiple\nlines of text")
        self.replay.relay(RAW_LOGFILE)
        self.assertTrue(os.path.getsize(REPLAY_LOGFILE)>0)
        self.read_file()
        self.assertTrue(self.count>2)

    def test_messages_with_stack(self):
        #""" record can contain stack trace """
        self.assertEquals(0, os.path.getsize(RAW_LOGFILE))
        try:
            awjrht()
        except:
            self.log.info("message and stack", exc_info=True)
        self.replay.relay(RAW_LOGFILE)
        self.assertTrue(os.path.getsize(REPLAY_LOGFILE)>0)
        self.read_file()
        self.assertTrue(self.count>1)
