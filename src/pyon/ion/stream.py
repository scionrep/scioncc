#!/usr/bin/env python

""" Stream-based publishing and subscribing """

__author__ = 'Luke Campbell <LCampbell@ASAScience.com>, Michael Meisinger'

import gevent

from pyon.core.bootstrap import get_sys_name, CFG
from pyon.core.exception import BadRequest
from pyon.net.endpoint import Publisher, Subscriber
from pyon.ion.identifier import create_simple_unique_id
from pyon.ion.service import BaseService
from pyon.util.log import log

from interface.objects import StreamRoute

DEFAULT_SYSTEM_XS = "system"
DEFAULT_DATA_XP = "data"


class StreamPublisher(Publisher):
    """
    Publishes outgoing messages on "streams", while setting proper message headers.
    """

    def __init__(self, process, stream, **kwargs):
        """
        Creates a StreamPublisher which publishes to the specified stream
        and is attached to the specified process.
        @param process   The IonProcess to attach to.
        @param stream    Name of the stream or StreamRoute object
        """
        super(StreamPublisher, self).__init__()
        if not isinstance(process, BaseService):
            raise BadRequest("No valid process provided.")
        if isinstance(stream, basestring):
            self.stream_route = StreamRoute(routing_key=stream)
        elif isinstance(stream, StreamRoute):
            self.stream_route = stream
        else:
            raise BadRequest("No valid stream information provided.")

        self.container = process.container
        self.xp_name = get_streaming_xp(self.stream_route.exchange_point)

        if self.container and self.container.has_capability(self.container.CCAP.EXCHANGE_MANAGER):
            self.xp = self.container.ex_manager.create_xp(self.stream_route.exchange_point or DEFAULT_DATA_XP)
            self.xp_route = self.xp.create_route(self.stream_route.routing_key)
        else:
            self.xp = self.xp_name
            self.xp_route = self.stream_route.routing_key

        to_name = (self.xp_name, self.stream_route.routing_key)
        Publisher.__init__(self, to_name=to_name, **kwargs)

    def publish(self, msg, *args, **kwargs):
        """
        Encapsulates and publishes a message; the message is sent to either the specified
        stream/route or the stream/route specified at instantiation
        """
        pub_hdrs = self._get_publish_headers(msg, kwargs)
        super(StreamPublisher, self).publish(msg, to_name=self._send_name, headers=pub_hdrs)

    def _get_publish_headers(self, msg, kwargs):
        headers = {}
        if "headers" in kwargs:
            headers.update(kwargs["headers"])
        headers.update({'exchange_point': self.xp_name,
                        'stream': self.stream_route.routing_key})
        return headers


class StreamSubscriber(Subscriber):
    """
    StreamSubscriber is a subscribing class to be attached to an ION process.

    The callback should accept three parameters:
      message      The incoming message
      stream_route The route from where the message came.
      stream_id    The identifier of the stream.
    """
    def __init__(self, process, exchange_name=None, stream=None, exchange_point=None, callback=None):
        """
        Creates a new StreamSubscriber which will listen on the specified queue (exchange_name).
        @param process        The IonProcess to attach to.
        @param exchange_name  The subscribing queue name.
        @param stream         (optional) Name of the stream or StreamRoute object, to subscribe to
        @param callback       The callback to execute upon receipt of a packet.
        """
        if not isinstance(process, BaseService):
            raise BadRequest("No valid process provided.")

        self.queue_name = exchange_name or ("subsc_" + create_simple_unique_id())
        self.streams = []

        self.container = process.container
        exchange_point = exchange_point or DEFAULT_DATA_XP
        self.xp_name = get_streaming_xp(exchange_point)
        self.xp = self.container.ex_manager.create_xp(exchange_point)

        self.xn = self.container.ex_manager.create_queue_xn(exchange_name, xs=self.xp)
        self.started = False
        self.callback = callback or process.call_process

        super(StreamSubscriber, self).__init__(from_name=self.xn, callback=self.preprocess)

        if stream:
            self.add_stream_subscription(stream)

    def add_stream_subscription(self, stream):
        if isinstance(stream, basestring):
            stream_route = StreamRoute(routing_key=stream)
        elif isinstance(stream, StreamRoute):
            stream_route = stream
        else:
            raise BadRequest("No valid stream information provided.")

        xp = self.container.ex_manager.create_xp(stream_route.exchange_point or DEFAULT_DATA_XP)
        self.xn.bind(stream_route.routing_key, xp)

        self.streams.append(stream_route)

    def remove_stream_subscription(self, stream):
        if isinstance(stream, basestring):
            stream_route = StreamRoute(routing_key=stream)
        elif isinstance(stream, StreamRoute):
            stream_route = stream
        else:
            raise BadRequest("No valid stream information provided.")
        existing_st = None
        for st in self.streams:
            if st.routing_key == stream_route.routing_key and st.exchange_point == stream_route.exchange_point:
                self.streams.remove(st)
                existing_st = st
                break
        if existing_st:
            xp = get_streaming_xp(stream_route.exchange_point)
            self.xn.unbind(existing_st.routing_key, xp)
        else:
            raise BadRequest("Stream was not a subscription")


    def preprocess(self, msg, headers):
        """
        Unwrap the incoming message and calls the callback.
        @param msg     The incoming packet.
        @param headers The headers of the incoming message.
        """
        route = StreamRoute(headers['exchange_point'], headers['routing_key'])
        self.callback(msg, route, headers['stream'])

    def start(self):
        """
        Begins consuming on the queue.
        """
        if self.started:
            raise BadRequest("Already started")
        self.started = True
        self.greenlet = gevent.spawn(self.listen)
        self.greenlet._glname = "StreamSubscriber"

    def stop(self):
        """
        Ceases consuming on the queue.
        """
        if not self.started:
            raise BadRequest("Subscriber is not running.")
        self.close()
        self.greenlet.join(timeout=10)
        self.greenlet.kill()
        self.started = False


class StandaloneStreamPublisher(Publisher):
    """
    StandaloneStreamPublisher is a Publishing endpoint which uses streams but
    does not belong to a process.

    This endpoint is intended for testing and debugging not to be used in service
    or process implementations.
    """
    def __init__(self, stream_id, stream_route):
        """
        Creates a new StandaloneStreamPublisher
        @param stream_id    The stream identifier
        @param stream_route The StreamRoute to publish on.
        """
        super(StandaloneStreamPublisher, self).__init__()
        from pyon.container.cc import Container
        self.stream_id = stream_id
        if not isinstance(stream_route, StreamRoute):
            raise BadRequest('stream route is not valid')
        self.stream_route = stream_route

        self.xp = Container.instance.ex_manager.create_xp(stream_route.exchange_point)
        self.xp_route = self.xp.create_route(stream_route.routing_key)


    def publish(self, msg, stream_id='', stream_route=None):
        """
        Encapsulates and publishes the message on the specified stream/route or
        the one specified at instantiation.
        @param msg          Outgoing message
        @param stream_id    Stream Identifier
        @param stream_route Stream Route
        """
        from pyon.container.cc import Container
        stream_id = stream_id or self.stream_id
        xp = self.xp
        xp_route = self.xp_route
        if stream_route:
            xp = Container.instance.ex_manager.create_xp(stream_route.exchange_point)
            xp_route = xp.create_route(stream_route.routing_key)
        stream_route = stream_route or self.stream_route
        super(StandaloneStreamPublisher, self).publish(msg, to_name=xp_route, headers={'exchange_point': stream_route.exchange_point, 'stream': stream_id or self.stream_id})


class StandaloneStreamSubscriber(Subscriber):
    """
    StandaloneStreamSubscriber is a Subscribing endpoint which uses Streams but
    does not belong to a process.

    This endpoint is intended for testing and debugging not to be used in service
    or process implementations.
    """
    def __init__(self, exchange_name, callback):
        """
        Creates a new StandaloneStreamSubscriber
        @param exchange_name The name of the queue to listen on.
        @param callback      The callback to execute on receipt of a packet
        """
        from pyon.container.cc import Container
        self.xn = Container.instance.ex_manager.create_queue_xn(exchange_name)
        self.callback = callback
        self.started = False
        super(StandaloneStreamSubscriber, self).__init__(name=self.xn, callback=self.preprocess)

    def preprocess(self, msg, headers):
        """
        Performs de-encapsulation of incoming packets and calls the callback.
        @param msg     The incoming packet.
        @param headers The headers of the incoming message.
        """
        route = StreamRoute(headers['exchange_point'], headers['routing_key'])
        self.callback(msg, route, headers['stream'])

    def start(self):
        """
        Begin consuming on the queue.
        """
        self.started = True
        self.greenlet = gevent.spawn(self.listen)
        self.greenlet._glname = "StandaloneStreamSubscriber"

    def stop(self):
        """
        Cease consuming on the queue.
        """
        if not self.started:
            raise BadRequest("Subscriber is not running.")
        self.close()
        self.greenlet.join(timeout=10)
        self.greenlet.kill()
        self.started = False


def get_streaming_xp(streaming_xp_name=None):
    root_xs = CFG.get_safe("exchange.core.system_xs", DEFAULT_SYSTEM_XS)
    events_xp = streaming_xp_name or CFG.get_safe("exchange.core.data_streams", DEFAULT_DATA_XP)
    return "%s.%s.%s" % (get_sys_name(), root_xs, events_xp)
