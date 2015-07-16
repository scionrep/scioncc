#!/usr/bin/env python

__author__ = 'Michael Meisiger'

from nose.plugins.attrib import attr
import gevent

from pyon.util.int_test import IntegrationTestCase

from pyon.net.endpoint import Publisher
from pyon.public import PRED, RT, BadRequest, NotFound, CFG, log
from ion.core.process.proc_util import AsyncResultWaiter, AsyncResultMsg


@attr('INT', group='cei')
class TestProcUtil(IntegrationTestCase):

    def setUp(self):
        self._start_container()

    def test_async_result(self):
        request_id = "request_foo"
        waiter = AsyncResultWaiter()
        self.assertFalse(waiter.async_res.ready())
        token = waiter.activate()
        self.assertFalse(waiter.async_res.ready())
        log.info("Wait token: %s", token)

        pub = Publisher(to_name=token)
        async_msg = AsyncResultMsg(request_id=request_id)
        pub.publish(async_msg)

        res = waiter.await(timeout=1, request_id=request_id)
        self.assertTrue(waiter.async_res.ready())
        self.assertIsInstance(res, AsyncResultMsg)
        self.assertEqual(res.__dict__, async_msg.__dict__)
