#!/usr/bin/env python

__author__ = 'Adam R. Smith'


import gevent
import time

from pyon.util.int_test import IonIntegrationTestCase

from pyon.util.async import blocking_cb
from nose.plugins.attrib import attr

class Timer(object):
    '''
    Simple context manager to measure the time to execute a block of code
    '''
    def __init__(self):
        object.__init__(self)
        self.dt = None

    def __enter__(self):
        self._t = time.time()
        return self

    def __exit__(self, type, value, traceback):
        self.dt = time.time() - self._t



@attr('UNIT')
class AsyncTest(IonIntegrationTestCase):
    def i_call_callbacks(self, cb):
        cb(1, 2, 3, foo='bar')

    def test_blocking(self):
        a, b, c, misc = blocking_cb(self.i_call_callbacks, cb_arg='cb')
        self.assertEqual((a, b, c, misc), (1, 2, 3, {'foo': 'bar'}))
