#!/usr/bin/env python

__author__ = 'Luke Campbell <LCampbell@ASAScience.com>'

from gevent.event import Event
from nose.plugins.attrib import attr

from pyon.core.bootstrap import get_sys_name
from pyon.ion.stream import StreamSubscriber, StreamPublisher
from pyon.ion.process import SimpleProcess
from pyon.util.int_test import IonIntegrationTestCase

from interface.objects import StreamRoute


@attr('INT')
class StreamPubsubTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()

        self.queue_cleanup = []
        self.exchange_cleanup = []

    def tearDown(self):
        for queue in self.queue_cleanup:
            xn = self.container.ex_manager.create_queue_xn(queue)
            xn.delete()
        for exchange in self.exchange_cleanup:
            xp = self.container.ex_manager.create_xp(exchange)
            xp.delete()

    def test_stream_pub_sub(self):
        self.verified = Event()
        self.route = StreamRoute(routing_key='stream_name')

        def verify(message, route, stream):
            self.assertEquals(message, 'test')
            self.assertEquals(route.routing_key, self.route.routing_key)
            self.assertTrue(route.exchange_point.startswith(get_sys_name()))
            self.assertEquals(stream, 'stream_name')
            self.verified.set()

        sub_proc = SimpleProcess()
        sub_proc.container = self.container

        sub1 = StreamSubscriber(process=sub_proc, exchange_name='stream_name', callback=verify)
        sub1.add_stream_subscription("stream_name")
        sub1.start()
        self.queue_cleanup.append('data.stream_name')

        pub_proc = SimpleProcess()
        pub_proc.container = self.container

        pub1 = StreamPublisher(process=pub_proc, stream=self.route)
        sub1.xn.bind(self.route.routing_key, pub1.xp)

        pub1.publish('test')

        self.assertTrue(self.verified.wait(2))

    def test_stream_pub_sub_xp(self):
        self.verified = Event()
        self.route = StreamRoute(exchange_point='xp_test', routing_key='stream_name')

        def verify(message, route, stream):
            self.assertEquals(message, 'test')
            self.assertEquals(route.routing_key, self.route.routing_key)
            self.assertTrue(route.exchange_point.endswith(self.route.exchange_point))
            self.assertEquals(stream, 'stream_name')
            self.verified.set()

        sub_proc = SimpleProcess()
        sub_proc.container = self.container

        sub1 = StreamSubscriber(process=sub_proc, exchange_name='stream_name', exchange_point="xp_test", callback=verify)
        sub1.add_stream_subscription(self.route)
        sub1.start()
        self.queue_cleanup.append('xp_test.stream_name')
        self.exchange_cleanup.append('xp_test')

        pub_proc = SimpleProcess()
        pub_proc.container = self.container

        pub1 = StreamPublisher(process=pub_proc, stream=self.route)
        sub1.xn.bind(self.route.routing_key, pub1.xp)

        pub1.publish('test')

        self.assertTrue(self.verified.wait(2))

