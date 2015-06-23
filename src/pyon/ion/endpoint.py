#!/usr/bin/env python

"""Messaging endpoints that are ION process aware"""

__author__ = 'Michael Meisinger, David Stuebe, Dave Foster <dfoster@asascience.com>'

from gevent.timeout import Timeout

from pyon.core import MSG_HEADER_ACTOR, MSG_HEADER_VALID, MSG_HEADER_ROLES, MSG_HEADER_TOKENS
from pyon.core.exception import Timeout as IonTimeout
from pyon.net.transport import BaseTransport
from pyon.net.endpoint import (Publisher, Subscriber, EndpointUnit, process_interceptors, RPCRequestEndpointUnit,
        BaseEndpoint, RPCClient, RPCResponseEndpointUnit, RPCServer, PublisherEndpointUnit, SubscriberEndpointUnit)
from pyon.ion.event import BaseEventSubscriberMixin
from pyon.util.log import log


# -----------------------------------------------------------------------------
# PROCESS LEVEL ENDPOINTS
#

class ProcessEndpointUnitMixin(EndpointUnit):
    """
    Common-base mixin for Process related endpoints.

    This reduces code duplication on either side of the ProcessRPCRequest/ProcessRPCResponse.
    """
    def __init__(self, process=None, **kwargs):
        EndpointUnit.__init__(self, **kwargs)
        self._process = process

    def get_context(self):
        """
        Gets context used to build headers for the conversation.

        This method may be overridden for advanced purposes.
        """
        if hasattr(self._process, 'get_context'):
            return self._process.get_context()
        else:
            return None

    def _build_invocation(self, **kwargs):
        newkwargs = kwargs.copy()
        newkwargs.update({'process': self._process})

        inv = EndpointUnit._build_invocation(self, **newkwargs)
        return inv

    def _intercept_msg_in(self, inv):
        """
        Override for incoming message interception.

        This is a request, so the order should be Message, Process
        """
        inv_one = EndpointUnit._intercept_msg_in(self, inv)
        inv_two = process_interceptors(self.interceptors["process_incoming"] if "process_incoming" in self.interceptors else [], inv_one)
        return inv_two

    def _intercept_msg_out(self, inv):
        """
        Override for outgoing message interception.

        This is request, so the order should be Process, Message
        """
        inv_one = process_interceptors(self.interceptors["process_outgoing"] if "process_outgoing" in self.interceptors else [], inv)
        inv_two = EndpointUnit._intercept_msg_out(self, inv_one)

        return inv_two

    def _build_header(self, raw_msg, raw_headers):
        """
        Builds the header for this Process-level RPC conversation.
        """
        header = EndpointUnit._build_header(self, raw_msg, raw_headers)

        # Add our process identity to the headers (as sender)
        header.update({'sender-name': self._process.name or 'unnamed-process',     # @TODO
                       'sender': self._process.id})

        if hasattr(self._process, 'process_type'):
            header.update({'sender-type': self._process.process_type or 'unknown-process-type'})
            if self._process.process_type == 'service' and hasattr(self.channel, '_send_name'):
                header.update({'sender-service': "%s,%s" % (self.channel._send_name.exchange, self._process.name)})

        # Use received message headers context to set security attributes forward
        context = self.get_context()
        if isinstance(context, dict):
            new_header = self.build_security_headers(context)
            header.update(new_header)
        else:
            # no context? we're the originator of the message then
            container_id = BaseEndpoint._get_container_instance().id
            header['origin-container-id'] = container_id

            # This is the originating conversation
            if 'conv-id' in raw_headers:
                header['original-conv-id'] = raw_headers['conv-id']

        return header

    @classmethod
    def build_security_headers(cls, context):
        """
        Examining context, builds a set of headers containing necessary forwarded items.
        Returns a new dictionary containing headers from the context that are important.
        """
        header = {}

        # fwd on actor specific information, according to common message format spec
        actor_id            = context.get(MSG_HEADER_ACTOR, None)
        actor_roles         = context.get(MSG_HEADER_ROLES, None)
        actor_tokens        = context.get(MSG_HEADER_TOKENS, None)
        expiry              = context.get(MSG_HEADER_VALID, None)
        container_id        = context.get('origin-container-id', None)
        original_conv_id    = context.get('original-conv-id', None)
        conv_id             = context.get('conv-id', None)

        # If an actor-id is specified then there may be other associated data that needs to be passed on
        if actor_id:
            header[MSG_HEADER_ACTOR] = actor_id
            if actor_roles:
                header[MSG_HEADER_ROLES] = actor_roles

        # This set of tokens is set independently of the actor
        if actor_tokens:
            header[MSG_HEADER_TOKENS] = actor_tokens
        if expiry:
            header[MSG_HEADER_VALID] = expiry
        if container_id:
            header['origin-container-id'] = container_id

        if original_conv_id:
            # Since this is not the originating message, this must be a requests within an existing conversation,
            # so track original conversation
            header['original-conv-id'] = original_conv_id
        else:
            if conv_id:
                header['original-conv-id'] = conv_id

        return header


class ProcessRPCRequestEndpointUnit(ProcessEndpointUnitMixin, RPCRequestEndpointUnit):
    def __init__(self, process=None, **kwargs):
        ProcessEndpointUnitMixin.__init__(self, process=process)
        RPCRequestEndpointUnit.__init__(self, **kwargs)

    def _build_header(self, raw_msg, raw_headers):
        """
        Override to direct the calls in _build_header - first the RPCRequest side, then the Process mixin.
        """

        header1 = RPCRequestEndpointUnit._build_header(self, raw_msg, raw_headers)
        header2 = ProcessEndpointUnitMixin._build_header(self, raw_msg, raw_headers)

        header1.update(header2)

        return header1


class ProcessRPCClient(RPCClient):
    """
    Requester side of RPC
    """
    endpoint_unit_type = ProcessRPCRequestEndpointUnit

    def __init__(self, process=None, **kwargs):
        self._process = process

        if 'to_name' in kwargs and kwargs['to_name'] is not None and not isinstance(kwargs['to_name'], BaseTransport):
            container = (hasattr(self._process, 'container') and self._process.container) or self._get_container_instance()
            if container:
                # Client creates the service XN
                kwargs['to_name'] = container.create_service_xn(kwargs['to_name'])
            else:
                log.info('No container at ProcessRPCClient init time, will wait until message send to upgrade to Exchange Object')

        RPCClient.__init__(self, **kwargs)

    def create_endpoint(self, to_name=None, existing_channel=None, **kwargs):
        if not self._process:
            raise StandardError("No Process specified")

        # upgrade to exchange object
        if to_name is None and not isinstance(self._send_name, BaseTransport):
            container = self._process.container or self._get_container_instance()
            if not container:
                raise StandardError("No container found, can not upgrade to ExchangeObject")

            self._send_name = container.create_service_xn(self._send_name)

        # upgrade one timers too
        if to_name is not None and not isinstance(to_name, BaseTransport):
            container = self._process.container or self._get_container_instance()
            if not container:
                raise StandardError("No container found, can not upgrade to ExchangeObject")

            to_name = container.create_service_xn(to_name)

        newkwargs = kwargs.copy()
        newkwargs['process'] = self._process
        return RPCClient.create_endpoint(self, to_name, existing_channel, **newkwargs)


class ProcessRPCResponseEndpointUnit(ProcessEndpointUnitMixin, RPCResponseEndpointUnit):
    def __init__(self, process=None, routing_call=None, **kwargs):
        ProcessEndpointUnitMixin.__init__(self, process=process)
        RPCResponseEndpointUnit.__init__(self, **kwargs)

        self._routing_call = routing_call

    def _message_received(self, msg, headers):
        """
        Message received override for processes. Sets the process context here to be used for
        subsequent calls out by this process to other processes, or replies.
        """

        ######
        ###### THIS IS WHERE THE THREAD LOCAL HEADERS CONTEXT IS SET ######
        ######

        # With the property _routing_call set, as is the case 95% of the time in the Process-level endpoints,
        # we have to set the call context from the ION process' calling greenlet, as context is greenlet-specific.
        # This is done in the _make_routing_call override here, passing it the context to be set.
        # See also IonProcessThread._control_flow.

        with self._process.push_context(headers):
            return RPCResponseEndpointUnit._message_received(self, msg, headers)

    def message_received(self, msg, headers):
        """Hook for checking governance pre-conditions before calling a service operation
        """
        gc = self._routing_obj.container.governance_controller
        if gc:
            gc.check_process_operation_preconditions(self._routing_obj, msg, headers)

        result, response_headers = RPCResponseEndpointUnit.message_received(self, msg, headers)

        # decorate our response_headers with process-saturation, as we need them to be set in the headers
        # earlier than send/build_header so the sampling takes notice
        try:
            response_headers['process-saturation'] = self._get_process_saturation()
        except Exception as ex:
            log.warn("Could not set process-saturation header, ignoring: %s", ex)

        return result, response_headers

    def _build_header(self, raw_msg, raw_headers):
        """
        Override to direct the calls in _build_header - first the RPCResponse side, then the Process mixin.
        """
        header1 = RPCResponseEndpointUnit._build_header(self, raw_msg, raw_headers)
        header2 = ProcessEndpointUnitMixin._build_header(self, raw_msg, raw_headers)

        header1.update(header2)

        return header1

    def _make_routing_call(self, call, timeout, *op_args, **op_kwargs):
        if not self._routing_call:
            return RPCResponseEndpointUnit._make_routing_call(self, call, timeout, *op_args, **op_kwargs)

        ctx = self._process.get_context()
        ar = self._routing_call(call, ctx, *op_args, **op_kwargs)
        res = ar.get()
        # REMOVED TIMEOUT
        #try:
        #    res = ar.get(timeout=timeout)
        #except Timeout:
        #    # cancel or abort current processing
        #    self._process._process.cancel_or_abort_call(ar)
        #    raise IonTimeout("Process did not execute in allotted time")    # will be returned to caller via messaging

        # Persistent process state handling
        if hasattr(self._process, "_proc_state"):
            if self._process._proc_state_changed:
                log.debug("Process %s state changed. State=%s", self._process.id, self._process._proc_state)
                self._process._flush_state()
        return res

    def _get_process_saturation(self):
        """
        Gets the process' saturation, as an integer percentage (process time / total time).
        """
        total, _, proc, interval, interval_run = self._process._process.time_stats  # we want the ION proc's stats
        return str(int(interval_run / float(interval) * 100))  # Percentage in current (partial) and prior interval


class ProcessRPCServer(RPCServer):
    endpoint_unit_type = ProcessRPCResponseEndpointUnit

    def __init__(self, process=None, routing_call=None, **kwargs):
        assert process
        self._process = process
        self._routing_call = routing_call

        # don't make people set service and process when they're almost always the same
        if "service" not in kwargs:
            kwargs = kwargs.copy()
            kwargs['service'] = process

        RPCServer.__init__(self, **kwargs)

    @property
    def routing_call(self):
        return self._routing_call

    @routing_call.setter
    def routing_call(self, value):
        self._routing_call = value

    def create_endpoint(self, **kwargs):
        newkwargs = kwargs.copy()
        newkwargs['process'] = self._process
        newkwargs['routing_call'] = self._routing_call
        return RPCServer.create_endpoint(self, **newkwargs)

    def __str__(self):
        return "ProcessRPCServer at %s:\n\trecv_name: %s\n\tprocess: %s" % (hex(id(self)), str(self._recv_name), str(self._process))


class ProcessPublisherEndpointUnit(ProcessEndpointUnitMixin, PublisherEndpointUnit):
    def __init__(self, process=None, **kwargs):
        ProcessEndpointUnitMixin.__init__(self, process=process)
        PublisherEndpointUnit.__init__(self, **kwargs)

    def _build_header(self, raw_msg, raw_headers):
        """
        Override to direct the calls in _build_header - first the Publisher, then the Process mixin.
        """
        header1 = PublisherEndpointUnit._build_header(self, raw_msg, raw_headers)
        header2 = ProcessEndpointUnitMixin._build_header(self, raw_msg, raw_headers)

        header1.update(header2)

        return header1


class ProcessPublisher(Publisher):
    endpoint_unit_type = ProcessPublisherEndpointUnit

    def __init__(self, process=None, **kwargs):
        assert process
        self._process = process
        Publisher.__init__(self, **kwargs)

    def create_endpoint(self, *args, **kwargs):
        newkwargs = kwargs.copy()
        newkwargs['process'] = self._process
        return Publisher.create_endpoint(self, *args, **newkwargs)


class PublisherError(StandardError):
    """
    An exception class for errors in the subscriber
    """
    pass


class SubscriberError(StandardError):
    """
    An exception class for errors in the subscriber
    """
    pass


class ProcessSubscriberEndpointUnit(ProcessEndpointUnitMixin, SubscriberEndpointUnit):
    def __init__(self, process=None, callback=None, routing_call=None, **kwargs):
        ProcessEndpointUnitMixin.__init__(self, process=process)
        SubscriberEndpointUnit.__init__(self, callback=callback, **kwargs)
        self._routing_call = routing_call

    def _message_received(self, msg, headers):
        """
        Message received override.

        Sets the process' context here to be picked up by subsequent calls out by this service to other services, or replies.
        """
        ######
        ###### THIS IS WHERE THE THREAD LOCAL HEADERS CONTEXT IS SET ######
        ######

        # With the property _routing_call set, as is the case 95% of the time in the Process-level endpoints,
        # we have to set the call context from the ION process' calling greenlet, as context is greenlet-specific.
        # This is done in the _make_routing_call override here, passing it the context to be set.
        # See also IonProcessThread._control_flow.

        with self._process.push_context(headers):
            return SubscriberEndpointUnit._message_received(self, msg, headers)

    def _build_header(self, raw_msg, raw_headers):
        """
        Override to direct the calls in _build_header - first the Subscriber, then the Process mixin.
        """
        header1 = SubscriberEndpointUnit._build_header(self, raw_msg, raw_headers)
        header2 = ProcessEndpointUnitMixin._build_header(self, raw_msg, raw_headers)

        header1.update(header2)

        return header1

    def _make_routing_call(self, call, timeout, *op_args, **op_kwargs):
        if not self._routing_call:
            return SubscriberEndpointUnit._make_routing_call(self, call, timeout, *op_args, **op_kwargs)

        ctx = self._process.get_context()
        ar = self._routing_call(call, ctx, *op_args, **op_kwargs)
        return ar.get()  # timeout=timeout)  # REMOVED TIMEOUT


class ProcessSubscriber(Subscriber):
    """
    Process aware general pattern subscriber. Executes callbacks within process control greenlet.
    """
    endpoint_unit_type = ProcessSubscriberEndpointUnit

    def __init__(self, process=None, routing_call=None, **kwargs):
        assert process
        self._process = process
        self._routing_call = routing_call
        Subscriber.__init__(self, **kwargs)

    @property
    def routing_call(self):
        return self._routing_call

    @routing_call.setter
    def routing_call(self, value):
        self._routing_call = value

    def create_endpoint(self, **kwargs):
        newkwargs = kwargs.copy()
        newkwargs['process'] = self._process
        newkwargs['routing_call'] = self._routing_call
        return Subscriber.create_endpoint(self, **newkwargs)

    def __str__(self):
        return "ProcessSubscriber at %s:\n\trecv_name: %s\n\tprocess: %s\n\tcb: %s" % (
                hex(id(self)), str(self._recv_name), str(self._process), str(self._callback))


class ProcessEventSubscriber(ProcessSubscriber, BaseEventSubscriberMixin):
    """
    Process aware event subscriber. Executes callbacks within process control greenlet.
    """
    def __init__(self, xp_name=None, event_type=None, origin=None, queue_name=None, callback=None,
                 sub_type=None, origin_type=None, process=None, routing_call=None, auto_delete=None, *args, **kwargs):

        BaseEventSubscriberMixin.__init__(self, event_type=event_type, sub_type=sub_type,
                                          origin=origin, origin_type=origin_type,
                                          xp_name=xp_name, queue_name=queue_name, auto_delete=auto_delete)

        log.debug("ProcessEventSubscriber events pattern %s", self.binding)

        ProcessSubscriber.__init__(self, from_name=self._ev_recv_name, binding=self.binding,
                                   callback=callback, process=process, routing_call=routing_call,
                                   auto_delete=self._auto_delete, **kwargs)

    def __str__(self):
        return "ProcessEventSubscriber at %s:\n\trecv_name: %s\n\tprocess: %s\n\tcb: %s" % (
                hex(id(self)), str(self._recv_name), str(self._process), str(self._callback))
