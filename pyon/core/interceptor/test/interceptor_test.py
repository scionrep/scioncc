'''
@author Luke Campbell <lcampbell@asascience.com>
@file pyon/core/interceptor/test/interceptor_test.py
@description test lib for interceptor
'''
import unittest
from pyon.core.interceptor.encode import EncodeInterceptor
from pyon.core.interceptor.interceptor import Invocation
from pyon.util import log
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr

try:
    import numpy as np
    _have_numpy = True
except ImportError as e:
    _have_numpy = False

@attr('UNIT')
class InterceptorTest(PyonTestCase):
    @unittest.skipIf(not _have_numpy,'No numpy')
    def test_numpy_codec(self):

        a = np.array([90,8010,3,14112,3.14159265358979323846264],dtype='float32')

        print 'Array Type:',type(a[0])
        print 'List Type:',type(a.tolist()[0])

        invoke = Invocation()
        invoke.message = a
        codec = EncodeInterceptor()

        print 'LSSLSJSL',type(invoke.message)
        mangled = codec.outgoing(invoke)

        received = codec.incoming(mangled)

        b = received.message
        self.assertTrue((a==b).all())

    @unittest.skipIf(not _have_numpy,'No numpy')
    def test_packed_numpy(self):
        a = np.array([(90,8010,3,14112,3.14159265358979323846264)],dtype='float32')
        invoke = Invocation()
        invoke.message = {'double stuffed':[a,a,a]}
        codec = EncodeInterceptor()

        mangled = codec.outgoing(invoke)

        received = codec.incoming(mangled)

        b = received.message
        c = b.get('double stuffed')
        for d in c:
            self.assertTrue((a==d).all())
