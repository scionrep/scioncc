#!/usr/bin/env python

"""Provides the communication layer above channels."""

from gevent import event
from gevent.lock import RLock
from gevent.timeout import Timeout
from zope import interface
import pprint
import uuid
import time
import inspect
import traceback
import sys
from types import MethodType
import threading

from pyon.core import bootstrap, exception
from pyon.core.bootstrap import CFG, IonObject
from pyon.core.exception import ExceptionFactory, IonException, BadRequest, Unauthorized
from pyon.net.channel import ChannelClosedError, PublisherChannel, ListenChannel, SubscriberChannel, ServerChannel, BidirClientChannel
from pyon.core.interceptor.interceptor import Invocation, process_interceptors
from pyon.util.containers import get_ion_ts, get_ion_ts_millis
from pyon.util.log import log
from pyon.net.transport import NameTrio, BaseTransport, XOTransport

# create special logging category for RPC message tracking
import logging
rpclog = logging.getLogger('rpc')

# create global accumulator for RPC times
from putil.timer import Timer, Accumulator
stats = Accumulator(keys='!total', persist=True)

# Callback hooks for message in and out. Signature: def callback(msg, headers, env)
callback_msg_out = None
callback_msg_in = None

MSG_HEADER_PROTOCOL_RPC = "rpc"
MSG_HEADER_LANGUAGE_DEFAULT = "scioncc"
MSG_HEADER_ENCODING_DEFAULT = "msgpack"


# -----------------------------------------------------------------------------
# BASE CLASSES
#

class EndpointError(StandardError):
    pass


class EndpointUnit(object):
    """
    A unit of conversation or one-way messaging.

    An EndpointUnit is produced by Endpoints and exist solely for the duration of one
    conversation. It can be thought of as a telephone call.

    In the case of request-response, an EndpointUnit is created on each side of the
    conversation, and exists for the life of that request and response. It is then
    torn down.

    You typically do not need to deal with these objects - they are created for you
    by an BaseEndpoint-derived class and encapsulate the "business-logic" of the communication,
    on top of the Channel layer which is the "transport" aka AMQP or otherwise.
    """

    channel = None
    _endpoint = None
    _interceptors = None

    def __init__(self, endpoint=None, interceptors=None):
        self._endpoint = endpoint
        self.interceptors = interceptors
        self._unique_name = uuid.uuid4().hex    # MM: For use as send/destination name (in part. RPC queues)

    @property
    def interceptors(self):
        if self._interceptors is not None:
            return self._interceptors

        assert self._endpoint, "No endpoint attached"
        return self._endpoint.interceptors

    @interceptors.setter
    def interceptors(self, value):
        self._interceptors = value

    def attach_channel(self, channel):
        self.channel = channel

    def _build_invocation(self, **kwargs):
        """
        Builds an Invocation instance to be used by the interceptor stack.
        This method exists so we can override it in derived classes (ex with a process).
        """
        inv = Invocation(**kwargs)
        return inv

    def _message_received(self, msg, headers):
        """
        Entry point for received messages in below channel layer.

        This method should not be overridden unless you are familiar with how the interceptor stack and
        friends work!
        """
        return self.message_received(msg, headers)

    def intercept_in(self, msg, headers):
        """
        Builds an invocation and runs interceptors on it, direction: in.

        This is called manually by the endpoint layer at receiving points (client recv, get_one/listen etc).

        @returns    A 2-tuple of message, headers after going through the interceptors.
        """
        inv = self._build_invocation(path=Invocation.PATH_IN,
                                     message=msg,
                                     headers=headers)
        inv_prime = self._intercept_msg_in(inv)
        new_msg = inv_prime.message
        new_headers = inv_prime.headers

        return new_msg, new_headers

    def _intercept_msg_in(self, inv):
        """
        Performs interceptions of incoming messages.
        Override this to change what interceptor stack to go through and ordering.

        @param inv      An Invocation instance.
        @returns        A processed Invocation instance.
        """
        inv_prime = process_interceptors(self.interceptors["message_incoming"] if "message_incoming" in self.interceptors else [], inv)
        return inv_prime

    def message_received(self, msg, headers):
        """
        """
        pass

    def send(self, msg, headers=None, **kwargs):
        """
        Public send method.
        Calls _build_msg (_build_header and _build_payload), then _send which puts it through the Interceptor stack(s).

        @param  msg         The message to send. Will be passed into _build_payload. You may modify the contents there.
        @param  headers     Optional headers to send. Will override anything produced by _build_header.
        @param  kwargs      Passed through to _send.
        """
        _msg, _header = self._build_msg(msg, headers)
        if headers:
            _header.update(headers)
        return self._send(_msg, _header, **kwargs)

    def _send(self, msg, headers=None, **kwargs):
        """
        Handles the send interaction with the Channel.

        Override this method to get custom behavior of how you want your endpoint unit to operate.
        Kwargs passed into send will be forwarded here. They are not used in this base method.

        @returns    A 2-tuple of the message body sent and the message headers sent. These are
                    post-interceptor. Derivations will likely override the return value.
        """
        new_msg, new_headers = self.intercept_out(msg, headers)

        # Provide a hook for all outgoing messages before they hit transport
        trigger_msg_out_callback(new_msg, new_headers, self)

        self.channel.send(new_msg, new_headers)

        return new_msg, new_headers

    def intercept_out(self, msg, headers):
        """
        Builds an invocation and runs interceptors on it, direction: out.

        This is called manually by the endpoint layer at sending points.

        @returns    A 2-tuple of message, headers after going through the interceptors.
        """
        inv = self._build_invocation(path=Invocation.PATH_OUT,
            message=msg,
            headers=headers)
        inv_prime = self._intercept_msg_out(inv)
        new_msg = inv_prime.message
        new_headers = inv_prime.headers

        return new_msg, new_headers

    def _intercept_msg_out(self, inv):
        """
        Performs interceptions of outgoing messages.
        Override this to change what interceptor stack to go through and ordering.

        @param  inv     An Invocation instance.
        @returns        A processed Invocation instance.
        """
        inv_prime = process_interceptors(self.interceptors["message_outgoing"] if "message_outgoing" in self.interceptors else [], inv)
        return inv_prime

    def close(self):
        if self.channel is not None:
            ev = self.channel.close()
            if not ev.wait(timeout=3):
                log.warn("Channel (%s) close did not respond in time, giving up", self.channel.get_channel_id())

    def _build_header(self, raw_msg, raw_headers):
        """
        Assembles the headers of a message from the raw message's content or raw headers.
        
        Any headers passed in here are strictly for reference. Headers set in there will take
        precedence and override any headers with the same key.
        """
        return {'ts': get_ion_ts()}

    def _build_payload(self, raw_msg, raw_headers):
        """
        Assembles the payload of a message from the raw message's content.

        @TODO will this be used? seems unlikely right now.
        """
        return raw_msg

    def _build_msg(self, raw_msg, raw_headers):
        """
        Builds a message (headers/payload) from the raw message's content.
        You typically do not need to override this method, but override the _build_header
        and _build_payload methods.

        @returns A 2-tuple of payload, headers
        """
        header = self._build_header(raw_msg, raw_headers)
        payload = self._build_payload(raw_msg, raw_headers)

        return payload, header


class BaseEndpoint(object):
    """
    An Endpoint is an object capable of communication with one or more other Endpoints.

    You should not use this BaseEndpoint base class directly, but one of the derived types such as
    RPCServer, Publisher, Subscriber, etc.

    An BaseEndpoint creates EndpointUnits, which are instances of communication/conversation,
    like a Factory.
    """
    endpoint_unit_type = EndpointUnit
    channel_type = BidirClientChannel
    node = None     # connection to the broker, basically

    _interceptors = None

    def __init__(self, node=None, transport=None):
        self.node = node
        self._transport = transport

    @classmethod
    def _get_container_instance(cls):
        """
        Helper method to return the singleton Container.instance.
        This method helps single responsibility of _ensure_node and makes testing much easier.

        We have to late import Container because Container depends on ProcessRPCServer in this file.

        This is a classmethod so we can use it from other places.
        """
        from pyon.container.cc import Container
        return Container.instance

    def _ensure_node(self):
        """
        Makes sure a node exists in this endpoint, and if it can, pulls from the Container singleton.
        This method is automatically called before accessing the node in both create_endpoint and in
        ListeningBaseEndpoint.listen.
        """

        if not self.node:
            container_instance = self._get_container_instance()
            if container_instance:
                self.node = container_instance.node
            else:
                raise EndpointError("Cannot pull node from Container.instance and no node specified")

    @property
    def interceptors(self):
        if self._interceptors is not None:
            return self._interceptors

        assert self.node, "No node attached"
        return self.node.interceptors

    @interceptors.setter
    def interceptors(self, value):
        self._interceptors = value

    def create_endpoint(self, to_name=None, existing_channel=None, **kwargs):
        """
        Main callback to instantiate a conversation from the endpoint, combining channel and
        endpoint unit. Can be overridden as needed.
        @param  to_name     Either a string or a 2-tuple of (exchange, name)
        """
        if existing_channel:
            ch = existing_channel
        else:
            self._ensure_node()
            ch = self._create_channel()

        e = self.endpoint_unit_type(endpoint=self, **kwargs)
        e.attach_channel(ch)

        return e

    def _create_channel(self, transport=None):
        """
        Creates a channel, used by create_endpoint.
        Can pass additional kwargs in to be passed through to the channel provider.
        """
        return self.node.channel(self.channel_type, transport=transport)

    def close(self):
        """
        To be defined by derived classes. Cleanup any resources here, such as channels being open.
        """
        pass

    def _ensure_name_trio(self, name):
        """
        Helper method returning a NameTrio for the the passed in name, which can be either str, a tuple
        or already a NameTrio (exchange, name, optional queue_name).

        If name is a str, assumes system RPC exchange point by default. Override where appropriate
        """
        if not isinstance(name, NameTrio):
            sys_ex = "%s.%s" % (bootstrap.get_sys_name(), CFG.get_safe('exchange.core.system_xs', 'system'))
            name = NameTrio(sys_ex, name)   # if name is a tuple it takes precedence
            #log.debug("MAKING NAMETRIO %s\n%s", name, ''.join(traceback.format_list(traceback.extract_stack()[-10:-1])))

        return name


class SendingBaseEndpoint(BaseEndpoint):
    """
    Send message communications.
    """

    def __init__(self, node=None, to_name=None, transport=None):
        BaseEndpoint.__init__(self, node=node, transport=transport)

        # Set destination name as NameTrio - can also be an XO
        self._send_name = self._ensure_name_trio(to_name)

        # Set node from XO
        if isinstance(self._send_name, XOTransport):
            if self.node is not None and self.node != self._send_name.node:
                log.warn("SendingBaseEndpoint.__init__: setting new node from XO")
            self.node = self._send_name.node

    def create_endpoint(self, to_name=None, existing_channel=None, **kwargs):
        if to_name is not None and isinstance(to_name, XOTransport):
            if self.node is not None and self.node != to_name.node:
                log.warn("SendingBaseEndpoint.create_endpoint: setting new node from XO when node already set")

            self.node = to_name.node

        ep_unit = BaseEndpoint.create_endpoint(self, to_name=to_name, existing_channel=existing_channel, **kwargs)

        name = to_name or self._send_name
        assert name
        name = self._ensure_name_trio(name)

        ep_unit.channel.connect(name)
        return ep_unit

    def _create_channel(self, transport=None):
        """ Overrides the BaseEndpoint create channel to supply a transport if our send_name is one. """
        if transport is None:
            if isinstance(self._send_name, BaseTransport):
                transport = self._send_name
            elif self._transport is not None:
                transport = self._transport

        return BaseEndpoint._create_channel(self, transport=transport)


class MessageObject(object):
    """
    Received message wrapper.

    Contains a body, headers, and a delivery_tag. Internally used by listen, the
    standard method used by ListeningBaseEndpoint, but will be returned to you
    if you use get_one_msg or get_n_msgs. If using the latter, you are responsible
    for calling ack or reject.

    make_body calls the endpoint's interceptor incoming stack - this may potentially
    raise an IonException in normal program flow. If this happens, the body/headers
    attributes will remain None and the error attribute will be set. Calling route()
    will be a no-op, but ack/reject work.
    """
    def __init__(self, msgtuple, ackmethod, rejectmethod, ep_unit):
        """
        Creates a MessageObject.

        @param  msgtuple        A 3-tuple of (body, headers, delivery_tag)
        @param  ackmethod       A callable to call to ack a message.
        @param  rejectmethod    A callable to call to reject a message.
        @param  ep_unit         An EndpointUnit.
        """
        self.ackmethod = ackmethod
        self.rejectmethod = rejectmethod
        self.endpoint = ep_unit    # Actually an EndpointUnit

        self.raw_body, self.raw_headers, self.delivery_tag = msgtuple
        self.body = None
        self.headers = None
        self.error = None

        # Provide a hook for any message received
        trigger_msg_in_callback(self.raw_body, self.raw_headers, self.delivery_tag, self.endpoint)

    def make_body(self):
        """
        Runs received raw message through the endpoint's interceptors.
        """
        try:
            self.body, self.headers = self.endpoint.intercept_in(self.raw_body, self.raw_headers)
        except Exception as ex:
            # This could be the policy interceptor raising Unauthorized
            if isinstance(ex, Unauthorized):
                log.info("Inbound message Unauthorized")
            else:
                log.info("Error in inbound message interceptors", exc_info=True)
            self.error = ex

    def ack(self):
        """
        Passthrough to underlying channel's ack.

        Must call this if using get_one_msg/get_n_msgs.
        """
        self.ackmethod(self.delivery_tag)

    def reject(self, requeue=False):
        """
        Passthrough to underlying channel's reject.

        Must call this if using get_one_msg/get_n_msgs.
        """
        self.rejectmethod(self.delivery_tag, requeue=requeue)

    def route(self):
        """
        Call default endpoint's _message_received, where business logic takes place.

        For instance, a Subscriber would call the registered callback, or an RPCServer would
        call the Service's operation.

        You are likely not to use this if using get_one_msg/get_n_msgs.
        """
        if self.error is not None:
            log.info("Refusing to deliver a MessageObject with an error")
            return

        self.endpoint._message_received(self.body, self.headers)


class ListeningBaseEndpoint(BaseEndpoint):
    """
    Message receive communications.
    """

    channel_type = ListenChannel

    def __init__(self, node=None, from_name=None, binding=None, transport=None):
        BaseEndpoint.__init__(self, node=node, transport=transport)

        # Set origin as NameTrio - can also be an XO
        self._recv_name = self._ensure_name_trio(from_name)

        # Set node from XO
        if isinstance(self._recv_name, XOTransport):
            if self.node is not None and self.node != self._recv_name.node:
                log.warn("ListeningBaseEndpoint.__init__: setting new node from XO when node already set")
            self.node = self._recv_name.node

        self._ready_event = event.Event()
        self._binding = binding
        self._chan = None

    def _create_channel(self, **kwargs):
        """
        Overrides the BaseEndpoint create channel to supply a transport if our recv name is one.
        """
        if isinstance(self._recv_name, BaseTransport):
            kwargs.update({'transport': self._recv_name})
        elif self._transport is not None:
            kwargs.update({'transport': self._transport})

        return BaseEndpoint._create_channel(self, **kwargs)

    def get_ready_event(self):
        """
        Returns an async event you can .wait() on.
        Indicates when listen() is ready to start listening.
        """
        return self._ready_event

    def _setup_listener(self, name, binding=None):
        self._chan.setup_listener(name, binding=binding)

    def listen(self, binding=None, thread_name=None):
        """
        Main driving method for ListeningBaseEndpoint.

        Should be spawned in a greenlet. This method creates/sets up a channel to listen,
        starts listening, and consumes one-by-one messages in a loop until the Endpoint is closed.
        """

        if thread_name:
            threading.current_thread().name = thread_name

        self.prepare_listener(binding=binding)

        # Notify any listeners of our readiness
        self._ready_event.set()

        while True:
            m = None
            try:
                m = self.get_one_msg()
                m.route()       # call default handler

            except ChannelClosedError as ex:
                break
            finally:
                # ChannelClosedError will go into here too, so make sure we have a message object to ack with
                if m is not None:
                    m.ack()

    def prepare_listener(self, binding=None):
        """ Creates a channel, prepares it, and begins consuming on it. """
        self.initialize(binding=binding)
        self.activate()

    def initialize(self, binding=None):
        """
        Creates a channel and prepares it for use. After this, the endpoint is in the ready state.
        """
        binding = binding or self._binding or self._recv_name.binding

        self._ensure_node()
        self._chan = self._create_channel()

        # @TODO this does not feel right
        if isinstance(self._recv_name, BaseTransport):
            self._recv_name.setup_listener(binding, self._setup_listener)
            self._chan._recv_name = self._recv_name
        else:
            self._setup_listener(self._recv_name, binding=binding)

    def activate(self):
        """
        Begins consuming. Can only be called after initialize.
        """
        assert self._chan
        self._chan.start_consume()

    def deactivate(self):
        """
        Stops consuming. Can only be called after initialize and activate.
        """
        assert self._chan
        self._chan.stop_consume()       # channel will yell at you if this is invalid

    def _get_n_msgs(self, num=1, timeout=None):
        """
        Internal method to accept n messages, create MessageObject wrappers, return them.

        Blocks until all messages are received, or the optional timeout is reached.

        INBOUND INTERCEPTORS ARE PROCESSED HERE. If the Interceptor stack throws an IonException,
        the response will be sent immediately and the MessageObject returned to you will not have
        body/headers set and will have error set. You should expect to check body/headers or error.
        """
        assert self._chan, "_get_n_msgs: needs the endpoint to have been initialized"

        mos = []
        newch = self._chan.accept(n=num, timeout=timeout)
        qsize = newch._recv_queue.qsize()
        if qsize == 0:
            self._chan.exit_accept()
            return []

        for x in xrange(newch._recv_queue.qsize()):
            mo = MessageObject(newch.recv(), newch.ack, newch.reject, self.create_endpoint(existing_channel=newch))
            mo.make_body()      # puts through EP interceptors
            mos.append(mo)
            log_message("MESSAGE RECV >>> RPC-request", mo.raw_body, mo.raw_headers, self._recv_name,
                        mo.delivery_tag, is_send=False)

        return mos

    def get_one_msg(self, timeout=None):
        """
        Receives one message.

        Blocks until one message is received, or the optional timeout is reached.

        INBOUND INTERCEPTORS ARE PROCESSED HERE. If the Interceptor stack throws an IonException,
        the response will be sent immediately and the MessageObject returned to you will not have
        body/headers set and will have error set. You should expect to check body/headers or error.

        @raises ChannelClosedError  If the channel has been closed.
        @raises Timeout             If no messages available when timeout is reached.
        @returns                    A MessageObject.
        """
        mos = self._get_n_msgs(num=1, timeout=timeout)
        return mos[0]

    def get_n_msgs(self, num, timeout=None):
        """
        Receives num messages.

        INBOUND INTERCEPTORS ARE PROCESSED HERE. If the Interceptor stack throws an IonException,
        the response will be sent immediately and the MessageObject returned to you will not have
        body/headers set and will have error set. You should expect to check body/headers or error.

        Blocks until all messages received, or the optional timeout is reached.
        @raises ChannelClosedError  If the channel has been closed.
        @raises Timeout             If no messages available when timeout is reached.
        @returns                    A list of MessageObjects.
        """
        return self._get_n_msgs(num, timeout=timeout)

    def get_all_msgs(self, timeout=None):
        """
        Receives all available messages on the queue.

        WARNING: If the queue is not exclusive, there is a possibility this method behaves incorrectly.
        You should always pass a timeout to this method.

        Blocks until all messages received, or the optional timeout is reached.
        @raises ChannelClosedError  If the channel has been closed.
        @raises Timeout             If no messages available when timeout is reached.
        @returns                    A list of MessageObjects.
        """
        n, _ = self.get_stats()
        return self._get_n_msgs(n, timeout=timeout)

    def close(self):
        BaseEndpoint.close(self)
        ev = self._chan.close()

        if not ev.wait(timeout=3):
            log.warn("Listen channel (%s) close did not respond in time, giving up", self._chan.get_channel_id())

    def get_stats(self):
        """
        Returns a tuple of the form (# ready messages, # of consumers).

        This endpoint must have been initialized in order to have a valid queue
        to work on.

        Passes down to the channel layer to get this info.
        """
        if not self._chan:
            raise EndpointError("No channel attached")

        return self._chan.get_stats()


# -----------------------------------------------------------------------------
# PUBLISH/SUBSCRIBE
#

class PublisherEndpointUnit(EndpointUnit):
    pass


class Publisher(SendingBaseEndpoint):
    """
    Simple publisher sends out broadcast messages.
    """

    endpoint_unit_type = PublisherEndpointUnit
    channel_type = PublisherChannel

    def __init__(self, **kwargs):
        self._pub_ep = None   # A cached EndpointUnit for publishing to the default to_name
        SendingBaseEndpoint.__init__(self, **kwargs)

    def publish(self, msg, to_name=None, headers=None):
        if to_name is not None:
            to_name = self._ensure_name_trio(to_name)

        ep_unit = None
        if to_name is None:
            # We can use the default publish EndpointUnit

            if self._pub_ep is None:         # Create the default publish EndpointUnit
                # Check that we got a to_name (_send_name) in the constructor
                if self._send_name is None:
                    raise EndpointError("Publisher has no address to send to")

                self._pub_ep = self.create_endpoint(self._send_name)
                self._pub_ep.channel.connect(self._send_name)

            ep_unit = self._pub_ep
        else:
            ep_unit = self.create_endpoint(to_name)
            ep_unit.channel.connect(to_name)

        ep_unit.send(msg, headers)
        if ep_unit != self._pub_ep:
            ep_unit.close()

    def close(self):
        """ Closes the opened publishing channel, if we've opened it previously. """
        if self._pub_ep:
            self._pub_ep.close()


class SubscriberEndpointUnit(EndpointUnit):
    """
    @TODO: Should have routing mechanics, possibly shared with other listener endpoint types
    """
    def __init__(self, callback, **kwargs):
        EndpointUnit.__init__(self, **kwargs)
        self.set_callback(callback)

    def set_callback(self, callback):
        """
        Sets the callback to be used by this SubscriberEndpointUnit when a message is received.
        """
        self._callback = callback

    def message_received(self, msg, headers):
        EndpointUnit.message_received(self, msg, headers)
        assert self._callback, "No callback provided, cannot route subscribed message"

        self._make_routing_call(self._callback, None, msg, headers)

    def _make_routing_call(self, call, timeout, *op_args, **op_kwargs):
        """
        Calls into the routing object. May be overridden at a lower level.
        """
        # @TODO respect timeout
        return call(*op_args, **op_kwargs)


class Subscriber(ListeningBaseEndpoint):
    """
    Subscribes to messages that match the given binding.
    Uses queue name as binding as default. Creates anonymous queue if no queue name provided.
    Supports consuming from shared queues, if multiple Subscribers us the same queue name and binding.

    Known queue:  name=(xp, thename), binding=None
    New queue:    name=None or (xp, None), binding=your binding
    """

    endpoint_unit_type = SubscriberEndpointUnit
    channel_type = SubscriberChannel

    def __init__(self, callback=None, **kwargs):
        """
        @param  callback should be a callable with two args: msg, headers
        """
        self._callback = callback
        ListeningBaseEndpoint.__init__(self, **kwargs)

    def create_endpoint(self, **kwargs):
        return ListeningBaseEndpoint.create_endpoint(self, callback=self._callback, **kwargs)

    def __str__(self):
        return "Subscriber: recv_name: %s, cb: %s" % (str(self._recv_name), str(self._callback))

# -----------------------------------------------------------------------------
# BIDIRECTIONAL ENDPOINTS
#
class BidirectionalEndpointUnit(EndpointUnit):
    """
    An interaction with communication in both ways, starting with a send.
    """
    pass


class BidirectionalListeningEndpointUnit(EndpointUnit):
    """
    An interaction with communication in both ways, starting with a receive.
    """
    pass


# -----------------------------------------------------------------------------
#  REQUEST-RESPONSE and RPC
#

class RequestEndpointUnit(BidirectionalEndpointUnit):
    """
    A request-response interaction, requester side.
    """

    def _get_response(self, conv_id, timeout):
        """
        Gets a response message to the conv_id within the given timeout.

        @raises Timeout
        @return A 2-tuple of the received message body and received message headers.
        """
        with Timeout(seconds=timeout):

            # start consuming
            self.channel.start_consume()

            # Consume in a loop: if we get a message not intended for us, we discard
            # it and consume again
            while True:
                rmsg, rheaders, rdtag = self.channel.recv()

                # Provide a hook for any message received
                trigger_msg_in_callback(rmsg, rheaders, rdtag, self)

                try:
                    nm, nh = self.intercept_in(rmsg, rheaders)
                finally:
                    self.channel.ack(rdtag)

                # is this the message we are looking for?
                if 'conv-id' in nh and nh['conv-id'] == conv_id:
                    return nm, nh   # breaks loop
                else:
                    log.warn("Discarding unknown message, likely from a previous timed out request (conv-id: %s, seq: %s, perf: %s)",
                             nh.get('conv-id', "unset"), nh.get('conv-seq', 'unset'), nh.get('performative', 'unset'))

    def _send(self, msg, headers=None, **kwargs):
        """ Handles an RPC send with response timeout """

        # could have a specified timeout in kwargs
        if 'timeout' in kwargs and kwargs['timeout'] is not None:
            timeout = kwargs['timeout']
        else:
            timeout = CFG.get_safe('container.messaging.timeout.receive', 10)

        # we have a timeout, update reply-by header
        headers['reply-by'] = str(int(headers['ts']) + int(timeout * 1000))
        ep_name = NameTrio(self.channel._send_name.exchange)    # anonymous queue
        # TODO: Set a better name for RPC response queue with system prefix
        #ep_name = NameTrio(self.channel._send_name.exchange, self._unique_name)
        self.channel.setup_listener(ep_name)

        # Call base _send, and get back the actual headers that were sent.
        # Extract the conv-id so we can tell the listener what is valid.
        _, sent_headers = BidirectionalEndpointUnit._send(self, msg, headers=headers)
        try:
            result_data, result_headers = self._get_response(sent_headers['conv-id'], timeout)
        except Timeout:
            raise exception.Timeout('Request timed out (%d sec) waiting for response from %s, conv %s' % (
                    timeout, str(self.channel._send_name), sent_headers['conv-id']))

        return result_data, result_headers

    def _build_header(self, raw_msg, raw_headers):
        """
        Sets headers common to Request-Response patterns.
        """
        headers = BidirectionalEndpointUnit._build_header(self, raw_msg, raw_headers)
        headers['performative'] = 'request'
        if self.channel and self.channel._send_name and isinstance(self.channel._send_name, NameTrio):
            # Receiver is exchange,queue combination
            headers['receiver'] = "%s,%s" % (self.channel._send_name.exchange, self.channel._send_name.queue)

        return headers


class RequestResponseClient(SendingBaseEndpoint):
    """
    Endpoint that sends a request, waits for a response.
    """
    endpoint_unit_type = RequestEndpointUnit

    def request(self, msg, headers=None, timeout=None):
        ep_unit = self.create_endpoint(self._send_name)
        try:
            retval, headers = ep_unit.send(msg, headers=headers, timeout=timeout)
        finally:
            # always close, even if endpoint raised a logical exception
            ep_unit.close()
        return retval


class ResponseEndpointUnit(BidirectionalListeningEndpointUnit):
    """
    A request-response interaction, provider/listener side.
    """

    def _build_header(self, raw_msg, raw_headers):
        """
        Sets headers common to Response side of Request-Response patterns, non-ion-specific.
        """
        headers = BidirectionalListeningEndpointUnit._build_header(self, raw_msg, raw_headers)
        headers['performative'] = 'inform-result'                       # overriden by response pattern, feels wrong
        #TODO - figure out why _send_name would not be there
        if self.channel and hasattr(self.channel, '_send_name') and self.channel._send_name and isinstance(self.channel._send_name, NameTrio):
            # Receiver is exchange,queue combination
            headers['receiver'] = "%s,%s" % (self.channel._send_name.exchange, self.channel._send_name.queue)
        headers['language'] = MSG_HEADER_LANGUAGE_DEFAULT
        headers['encoding'] = MSG_HEADER_ENCODING_DEFAULT
        headers['format'] = raw_msg.__class__.__name__      # Type of message (from generated interface class)

        return headers


class RequestResponseServer(ListeningBaseEndpoint):
    """
    Endpoint for request-response, server side.
    """
    endpoint_unit_type = ResponseEndpointUnit
    channel_type = ServerChannel


class RPCRequestEndpointUnit(RequestEndpointUnit):
    """
    Endpoint unit for RPC protocol, sender side.
    """
    exception_factory = ExceptionFactory()

    def _send(self, msg, headers=None, **kwargs):
        log_message("MESSAGE SEND >>> RPC-request", msg, headers, is_send=True)
        timer = Timer(logger=None) if stats.is_log_enabled() else None

        ######
        ###### THIS IS WHERE A BLOCKING RPC REQUEST IS PERFORMED ######
        ######
        res, res_headers = RequestEndpointUnit._send(self, msg, headers=headers, **kwargs)

        if timer:
            # record elapsed time in RPC stats
            receiver = headers.get('receiver', '?')  # header field is generally: exchange,queue
            receiver = receiver.split(',')[-1]       # want to log just the service_name for consistency
            receiver = receiver.split('.')[-1]       # want to log just the service_name for consistency
            stepid = 'rpc-client.%s.%s=%s' % (receiver, headers.get('op', '?'), res_headers["status_code"])
            timer.complete_step(stepid)
            stats.add(timer)
        log_message("MESSAGE RECV >>> RPC-reply", res, res_headers, is_send=False)

        # Check response header
        if res_headers["status_code"] != 200:
            stacks = None
            if isinstance(res, list):
                stacks = res
                # stack information is passed as a list of tuples (label, stack)
                # default label for new IonException is '__init__',
                # but change the label of the first remote exception to show RPC invocation.
                # other stacks would have already had labels updated.
                new_label = 'in remote call to %s' % (headers.get('receiver', '?'))  # res_headers['receiver']
                top_stack = stacks[0][1]
                stacks[0] = (new_label, top_stack)
            log.info("RPCRequestEndpointUnit received an error (%d): %s", res_headers['status_code'], res_headers['error_message'])
            ex = self.exception_factory.create_exception(res_headers["status_code"], res_headers["error_message"], stacks=stacks)
            raise ex

        return res, res_headers

    conv_id_counter = 0
    _lock = RLock()       # @TODO: is this safe?
    _conv_id_root = None

    def _build_conv_id(self):
        """
        Builds a unique conversation id based on the container name.
        """
        with RPCRequestEndpointUnit._lock:
            RPCRequestEndpointUnit.conv_id_counter += 1

            if not RPCRequestEndpointUnit._conv_id_root:
                # set default to use uuid-4, similar to what we'd get out of the container id anyway
                RPCRequestEndpointUnit._conv_id_root = str(uuid.uuid4())[0:6]

                # try to get the real one from the container, but do it safely
                try:
                    from pyon.container.cc import Container
                    if Container.instance and Container.instance.id:
                        RPCRequestEndpointUnit._conv_id_root = Container.instance.id
                except:
                    pass

        return "%s-%d" % (RPCRequestEndpointUnit._conv_id_root, RPCRequestEndpointUnit.conv_id_counter)

    def _build_header(self, raw_msg, raw_headers):
        """
        Build header override.

        This should set header values that are invariant or have nothing to do with the specific
        call being made (such as op).
        """
        headers = RequestEndpointUnit._build_header(self, raw_msg, raw_headers)
        headers['protocol'] = MSG_HEADER_PROTOCOL_RPC
        headers['language'] = MSG_HEADER_LANGUAGE_DEFAULT
        headers['encoding'] = MSG_HEADER_ENCODING_DEFAULT
        headers['format'] = raw_msg.__class__.__name__      # The type name
        headers['reply-by'] = 'todo'                        # set by _send override @TODO should be set here

        # Use the headers for conv-id and conv-seq if passed in from higher level API
        headers['conv-id'] = raw_headers['conv-id'] if raw_headers and 'conv-id' in raw_headers else self._build_conv_id()
        headers['conv-seq'] = raw_headers['conv-seq'] if raw_headers and 'conv-seq' in raw_headers else 1 #@TODO will not work well with agree/status etc

        return headers


class RPCClient(RequestResponseClient):
    """
    Base RPCClient class.

    RPC Clients are defined via generate_interfaces for each service, but also may be defined
    on the fly by instantiating one and passing a service Interface class (from the same files
    as the predefined clients).
    """
    endpoint_unit_type = RPCRequestEndpointUnit

    def __init__(self, iface=None, **kwargs):
        # Add dynamic operations from interface or schema (optional)
        if isinstance(iface, interface.interface.InterfaceClass):
            self._define_interface(iface)
        elif isinstance(iface, dict) and "op_list" in dict and "operations" in dict:
            self._define_from_schema(iface)

        RequestResponseClient.__init__(self, **kwargs)

    def _define_interface(self, iface):
        """ Sets callable operations on this client instance from a zope interface definition. """
        methods = iface.namesAndDescriptions()

        # Get the name of the svc for object name building from the name of the interface class (HACK)
        svc_name = iface.getName()[1:]

        for name, command in methods:
            in_obj_name = "%s_%s_in" % (svc_name, name)
            doc = command.getDoc()

            self._set_svc_method(name, in_obj_name, command.getSignatureInfo()['positional'], doc)

    def _define_from_schema(self, svc_def):
        """ Sets callable operations on this client instance from a service schema definition. """
        svc_name = svc_def["name"]
        for op_name in svc_def["op_list"]:
            op_def = svc_def["operations"][op_name]
            in_obj_name = "%s_%s_in" % (svc_name, op_name)
            callargs = op_def["in_list"]
            doc = op_def["description"]

            self._set_svc_method(op_name, in_obj_name, callargs, doc)

    def _set_svc_method(self, name, in_obj, callargs, doc):
        """
        Common method to properly set a friendly-named remote call method on this RPCClient.

        Only supports keyword arguments for now.
        """
        def svcmethod(self, *args, **kwargs):
            # We have no way of getting correct order
            if args:
                raise BadRequest("Illegal to use positional args when calling a dynamically generated remote method")
            headers = kwargs.pop('headers', None)
            ionobj = IonObject(in_obj, **kwargs)
            return self.request(ionobj, op=name, headers=headers)

        newmethod = svcmethod
        newmethod.__doc__ = doc
        setattr(self.__class__, name, newmethod)

    def request(self, msg, headers=None, op=None, timeout=None):
        """
        Request override for RPCClients.

        Puts the op into the headers and calls the base class version.
        """
        assert op
        assert headers is None or isinstance(headers, dict)

        if headers is not None:
            headers = headers.copy()
        else:
            headers = {}

        headers['op'] = op

        return RequestResponseClient.request(self, msg, headers=headers, timeout=timeout)


class RPCResponseEndpointUnit(ResponseEndpointUnit):
    def __init__(self, routing_obj=None, **kwargs):
        ResponseEndpointUnit.__init__(self, **kwargs)
        self._routing_obj = routing_obj

    def intercept_in(self, msg, headers):
        """
        ERR This is wrong
        """

        try:
            new_msg, new_headers = ResponseEndpointUnit.intercept_in(self, msg, headers)
            return new_msg, new_headers

        except Exception as ex:
            log.debug("server exception being passed to client", exc_info=True)
            result = ""
            if isinstance(ex, IonException):
                result = ex.get_stacks()

            response_headers = self._create_error_response(ex)

            response_headers['protocol'] = headers.get('protocol', '')
            response_headers['conv-id'] = headers.get('conv-id', '')
            response_headers['conv-seq'] = headers.get('conv-seq', 1) + 1

            self.send(result, response_headers)

            # reraise for someone else to catch
            raise

    def _message_received(self, msg, headers):
        """
        Internal _message_received override.

        We need to be able to detect IonExceptions raised in the Interceptor stacks as well as in the actual
        call to the op we're routing into. This override will handle the return value being sent to the caller.
        """
        result = None
        response_headers = {}

        ts = get_ion_ts()
        response_headers['msg-rcvd'] = ts

        timer = Timer(logger=None) if stats.is_log_enabled() else None

        ######
        ###### THIS IS WHERE AN ENDPOINT OPERATION EXCEPTION IS HANDLED  ######
        ######
        try:
            # execute interceptor stack, calls into our message_received
            result, new_response_headers = ResponseEndpointUnit._message_received(self, msg, headers)
            response_headers.update(new_response_headers)

        except Exception as ex:
            log.debug("server exception being passed to client", exc_info=True)
            result = ""
            if isinstance(ex, IonException):
                result = ex.get_stacks()
            response_headers = self._create_error_response(ex)

        finally:
            # REPLIES: propagate some headers
            response_headers['protocol'] = headers.get('protocol', '')
            response_headers['conv-id'] = headers.get('conv-id', '')
            response_headers['conv-seq'] = headers.get('conv-seq', 1) + 1
        ######
        ######
        ######

        if timer:
            # record elapsed time in RPC stats
            op = headers.get('op', '')
            if op:
                receiver = headers.get('receiver', '?')  # header field is generally: exchange,queue
                receiver = receiver.split(',')[-1]       # want to log just the service_name for consistency
                receiver = receiver.split('.')[-1]       # want to log just the service_name for consistency
                stepid = 'rpc-server.%s.%s=%s' % (receiver, headers.get('op', '?'), response_headers["status_code"])
            else:
                parts = headers.get('routing_key', 'unknown').split('.')
                stepid = 'server.' + '.'.join( [ parts[i] for i in xrange(min(3, len(parts))) ] )
            timer.complete_step(stepid)
            stats.add(timer)

        try:
            return self.send(result, response_headers)
        except IonException as ex:
            # Catch exception within sending interceptor stack, so if we catch an IonException, send that instead
            result = ""
            response_headers = self._create_error_response(ex)

            response_headers['error_message'] = "(while trying to send RPC response for conv-id %s) %s" % (headers.get('conv-id'), response_headers['error_message'])
            response_headers['protocol'] = headers.get('protocol', '')
            response_headers['conv-id'] = headers.get('conv-id', '')
            response_headers['conv-seq'] = headers.get('conv-seq', 1) + 1

            return self.send(result, response_headers)

    def _send(self, msg, headers=None, **kwargs):
        """
        Override for more accurate reply log message.
        """
        log_message("MESSAGE SEND <<< RPC-reply", msg, headers, is_send=True)
        return ResponseEndpointUnit._send(self, msg, headers=headers, **kwargs)

    def message_received(self, msg, headers):
        """
        Process a received message and deliver to a target object.
        Exceptions raised during message processing will be passed through.
        Subclasses can override this call.
        """
        assert self._routing_obj, "How did I get created without a routing object?"

        cmd_arg_obj = msg
        cmd_op = headers.get('op', None)

        # get timeout
        timeout = self._calculate_timeout(headers)

        # transform cmd_arg_obj into a dict
        if hasattr(cmd_arg_obj, '__dict__'):
            cmd_arg_obj = cmd_arg_obj.__dict__
        elif isinstance(cmd_arg_obj, dict):
            pass
        else:
            raise BadRequest("Unknown message type, cannot convert into kwarg dict: %s" % type(cmd_arg_obj))

        # op name must exist!
        if not hasattr(self._routing_obj, cmd_op):
            raise BadRequest("Unknown op name: %s" % cmd_op)

        ro_meth = getattr(self._routing_obj, cmd_op)

        # check arguments (as long as it is a function. might be a mock in testing.)
        # @TODO doesn't really feel correct.
        if isinstance(ro_meth, MethodType):
            ro_meth_args = inspect.getargspec(ro_meth)

            # if the keyword one is not none, we can support anything
            if ro_meth_args[2] is None:
                for arg_name in cmd_arg_obj:
                    if not arg_name in ro_meth_args[0]:
                        return None, self._create_error_response(code=400, msg="Argument %s not present in op signature" % arg_name)

        ######
        ###### THIS IS WHERE THE ENDPOINT OPERATION IS CALLED ######
        ######
        result = self._make_routing_call(ro_meth, timeout, **cmd_arg_obj)
        response_headers = {'status_code': 200,
                            'error_message': ''}
        ######
        ######
        ######

        return result, response_headers

    def _calculate_timeout(self, headers):
        """
        Takes incoming message headers and calculates an integer value in seconds to be used for timeouts.

        @return None or an integer value in seconds.
        """
        if not ('ts' in headers and 'reply-by' in headers):
            return None

        ts = int(headers['ts'])
        reply_by = int(headers['reply-by'])
        latency = get_ion_ts_millis() - ts         # we don't have access to response headers here, so calc again, not too big of a deal

        # reply-by minus timestamp gives us max allowable, subtract 2x observed latency, give 10% margin, and convert to integers
        to_val = int((reply_by - ts - 2 * latency) / 1000 * 0.9)

        #log.debug("calculated timeout val of %s for conv-id %s", to_val, headers.get('conv-id', 'NONE'))

        return to_val

    def _create_error_response(self, ex=None, code=500, msg=None):
        if ex is not None:
            if isinstance(ex, IonException):
                code = ex.get_status_code()
                # Force str - otherwise pika aborts due to bad headers
                msg = str(ex.get_error_message())
            else:
                msg = "%s (%s)" % (str(ex.message), type(ex))

        headers = {"status_code": code,
                   "error_message": msg,
                   "performative": "failure"}
        return headers

    def _make_routing_call(self, call, timeout, *op_args, **op_kwargs):
        """
        Calls into the routing object.

        May be overridden at a lower level.
        """
        return call(*op_args, **op_kwargs)       # REMOVED TIMEOUT
        #try:
        #    with Timeout(timeout):
        #        return call(*op_args, **op_kwargs)
        #except Timeout:
        #    # cleanup shouldn't be needed, executes in same greenlet as current
        #    raise exception.Timeout("Timed out making call to service (non-ION process)")


class RPCServer(RequestResponseServer):
    endpoint_unit_type = RPCResponseEndpointUnit

    def __init__(self, service=None, **kwargs):
        assert service
        self._service = service
        RequestResponseServer.__init__(self, **kwargs)

    def create_endpoint(self, **kwargs):
        """
        @TODO: push this into RequestResponseServer
        """
        return RequestResponseServer.create_endpoint(self, routing_obj=self._service, **kwargs)

    def __str__(self):
        return "RPCServer: recv_name: %s" % (str(self._recv_name))


def log_message(prefix="MESSAGE", msg=None, headers=None, recv=None, delivery_tag=None, is_send=True):
    """
    Print a comprehensive summary of a received message.
    NOTE: This is an expensive operation
    """
    if rpclog.isEnabledFor(logging.DEBUG):
        try:
            headers = headers or {}
            _sender = headers.get("sender", "?") + "(" + headers.get("sender-name", "") + ")"
            _send_hl, _recv_hl = ("###", "") if is_send else ("", "###")

            if recv and hasattr(recv, "__iter__"):
                recv = ".".join(str(item) for item in recv if item)
            _recv = headers.get("receiver", "?")
            _opstat = "op=%s"%headers.get("op", "") if "op" in headers else "status=%s" % headers.get("status_code", "")
            try:
                import msgpack
                _msg = msgpack.unpackb(msg)
                _msg = str(_msg)
            except Exception:
                _msg = str(msg)
            _msg = _msg[0:400]+"..." if len(_msg) > 400 else _msg
            _delivery = "\nDELIVERY: tag=%s"%delivery_tag if delivery_tag else ""
            rpclog.debug("%s: %s%s%s -> %s%s%s %s:\nHEADERS: %s\nCONTENT: %s%s",
                prefix, _send_hl, _sender, _send_hl, _recv_hl, _recv, _recv_hl, _opstat, str(headers), _msg, _delivery)
        except Exception as ex:
            log.warning("%s log error: %s", prefix, str(ex))


def trigger_msg_out_callback(body, headers, ep_unit):
    """Triggers a hook on message send"""
    if callback_msg_out:
        try:
            env = {}
            if hasattr(ep_unit, "channel"):
                env["routing_key"] = str(getattr(ep_unit.channel, "_send_name", "?"))
            if hasattr(ep_unit, "_process"):
                env["process"] = ep_unit._process
            env["ep_type"] = type(ep_unit)
            callback_msg_out(body, dict(headers), env)  # Must copy headers because they get muted during processing
        except Exception as ex:
            log.warn("Message out callback error: %s", str(ex))


def trigger_msg_in_callback(body, headers, delivery_tag, ep_unit):
    """Triggers a hook on message receive"""
    if callback_msg_in:
        try:
            env = {}
            env["delivery_tag"] = delivery_tag
            if hasattr(ep_unit, "_process"):
                env["process"] = ep_unit._process
            if hasattr(ep_unit, "_endpoint"):
                env["recv_name"] = str(getattr(ep_unit._endpoint, "_recv_name", ""))
            env["ep_type"] = type(ep_unit)
            callback_msg_in(body, dict(headers), env)  # Must copy headers because they get muted during processing
        except Exception as ex:
            log.warn("Message in callback error: %s", str(ex))
