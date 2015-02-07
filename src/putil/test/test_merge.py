import os
import putil.testing
from putil.exception import ApplicationException
from unittest.case import TestCase
import unittest

class TargetException(ApplicationException):
    def __init__(self):
        super(TargetException,self).__init__()
        self.drop_chained_init_frame()

class TestException(TestCase, putil.testing.ImportTest):
    def __init__(self):
        TestCase.__init__(self, methodName='runTest')
        putil.testing.ImportTest.__init__(self, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__name__))),'src'), 'putil')

    def willRaiseGeneric(self):
        return 1/0
    def willRaiseTarget(self):
        raise TargetException()
    def willRaiseCaused(self):
        try:
            self.willRaiseGeneric()
        except Exception,e:
            raise TargetException()
    def willRaiseCausedIndirect(self):
        try:
            self.willRaiseGeneric()
        except Exception,e:
            self.willRaiseTarget()

    def testTargetOnly(self):
        #""" make sure ApplicationException captures stack when thrown """
        caught = None
        try:
            self.willRaiseTarget()
        except Exception,e:
            caught = e

        self.assertTrue(isinstance(caught, TargetException), msg="exception is %s"%caught.__class__.__name__)
        self.assertTrue(caught.get_stack())
        self.assertEqual(caught.get_stack()[-1][2], "willRaiseTarget", msg=caught.get_stack()[-1][2])

    def testCause(self):
        caught = None
        try:
            self.willRaiseCaused()
        except Exception,e:
            caught = e

        self.assertTrue(isinstance(caught, TargetException))

        self.assertTrue(caught.get_cause())
        self.assertTrue(caught.get_cause_stack())
        self.assertEqual(caught.get_stack()[-1][2], "willRaiseCaused", msg='raised at '+repr(caught.get_stack()))
        self.assertEqual(caught.get_cause_stack()[-1][2], "willRaiseGeneric", msg='caused by '+repr(caught.get_cause_stack()))

    def testIndirect(self):
        caught = None
        try:
            self.willRaiseCausedIndirect()
        except Exception,e:
            caught = e

        self.assertTrue(isinstance(caught, TargetException))

        self.assertTrue(caught.get_cause())
        self.assertTrue(caught.get_cause_stack())
        self.assertEqual(caught.get_stack()[-1][2], "willRaiseTarget", msg='raised at '+repr(caught.get_stack()))
        self.assertEqual(caught.get_cause_stack()[-1][2], "willRaiseGeneric", msg='caused by '+repr(caught.get_cause_stack()))


if __name__ == '__main__':
    unittest.main()