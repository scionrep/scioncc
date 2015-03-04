
from putil.exception import ApplicationException
from putil.testing import UtilTest


class TargetException(ApplicationException):

    def __init__(self, *args, **kwargs):
        super(TargetException, self).__init__(*args, **kwargs)
        self.drop_chained_init_frame()


class TestException(UtilTest):

    def willRaiseGeneric(self):
        return 1/0

    def willRaiseTarget(self, msg="primary", cause=None):
        if cause:
            raise TargetException(msg, cause=cause)
        else:
            raise TargetException(msg)

    def willRaiseCaused(self):
        try:
            self.willRaiseGeneric()
        except Exception as ex:
            raise TargetException("secondary", cause=ex)

    def willRaiseCausedTarget(self):
        try:
            self.willRaiseTarget()
        except Exception as ex:
            raise TargetException("secondary", cause=ex)

    def willRaiseCausedIndirect(self):
        try:
            self.willRaiseGeneric()
        except Exception as ex:
            self.willRaiseTarget("secondary", cause=ex)

    def testTargetOnly(self):
        #make sure ApplicationException captures stack when thrown
        caught = None
        try:
            self.willRaiseTarget()
        except Exception as ex:
            caught = ex

        self.assertTrue(isinstance(caught, TargetException), msg="exception is %s" % caught.__class__.__name__)
        self.assertTrue(caught.get_stack())
        self.assertEqual(caught.get_stack()[-1][2], "willRaiseTarget", msg=caught.get_stack()[-1][2])

    def testCause(self):
        caught = None
        try:
            self.willRaiseCaused()
        except Exception as ex:
            caught = ex

        self.assertTrue(isinstance(caught, TargetException))

        self.assertTrue(caught.get_cause())
        self.assertTrue(caught.get_cause_stack())
        self.assertEqual(caught.get_stack()[-1][2], "willRaiseCaused", msg='raised at '+repr(caught.get_stack()))
        self.assertEqual(caught.get_cause_stack()[-1][2], "willRaiseGeneric", msg='caused by '+repr(caught.get_cause_stack()))

    def testCauseTarget(self):
        caught = None
        try:
            self.willRaiseCausedTarget()
        except Exception as ex:
            caught = ex

        self.assertTrue(isinstance(caught, TargetException))

        self.assertTrue(caught.get_cause())
        self.assertTrue(caught.get_cause_stack())
        self.assertEqual(caught.get_stack()[-1][2], "willRaiseCausedTarget", msg='raised at '+repr(caught.get_stack()))
        self.assertEqual(caught.get_cause_stack()[-1][2], "willRaiseTarget", msg='caused by '+repr(caught.get_cause_stack()))

        self.assertEquals(len(caught.get_stacks()), 2)

    def testIndirect(self):
        caught = None
        try:
            self.willRaiseCausedIndirect()
        except Exception as ex:
            caught = ex

        self.assertTrue(isinstance(caught, TargetException))

        self.assertTrue(caught.get_cause())
        self.assertTrue(caught.get_cause_stack())
        self.assertEqual(caught.get_stack()[-1][2], "willRaiseTarget", msg='raised at '+repr(caught.get_stack()))
        self.assertEqual(caught.get_cause_stack()[-1][2], "willRaiseGeneric", msg='caused by '+repr(caught.get_cause_stack()))
