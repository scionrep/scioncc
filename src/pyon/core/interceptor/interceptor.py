#!/usr/bin/env python

__author__ = 'Dave Foster <dfoster@asascience.com>, Thomas R. Lennan, Michael Meisinger'

from pyon.core import PROCTYPE_SERVICE, PROCTYPE_AGENT, PROCTYPE_SIMPLE


class Invocation(object):
    """
    Wrapper object for a message with headers and additional annotations to be passed to interceptors.
    Helpers translate messaging specific headers into higher level attributes.
    """

    # Event outbound processing path
    PATH_OUT = 'outgoing'

    # Event inbound processing path
    PATH_IN = 'incoming'

    def __init__(self, **kwargs):
        self.args = kwargs
        self.path = kwargs.get('path')
        self.message = kwargs.get('message')
        self.headers = kwargs.get('headers') or {}  # ensure dict

        self.message_annotations = {}

    def get_invocation_process_type(self):
        process = self.get_arg_value('process')

        if not process:
            return PROCTYPE_SIMPLE

        return getattr(process, 'process_type', PROCTYPE_SIMPLE)

    def get_message_sender(self):
        sender_type = self.get_header_value('sender-type', 'Unknown')
        if sender_type == PROCTYPE_SERVICE:
            sender_header = self.get_header_value('sender-service', 'Unknown')
            sender = self.get_service_name(sender_header)
        else:
            sender = self.get_header_value('sender', 'Unknown')

        return sender, sender_type

    def get_message_sender_queue(self):
        sender_queue = self.get_header_value('reply-to', 'todo')
        if sender_queue == 'todo':
            return None

        # TODO: Check this logic going against anonymous queues
        index = sender_queue.find('amq')
        if (index != -1): sender_queue = sender_queue[index:]
        return sender_queue

    def get_message_receiver(self):
        """Returns a receiver identifier based on the type of process.
        Service: unqualified service name
        Agent: Resource type or if absent process name
        Other: Process name
        """
        process = self.get_arg_value('process')
        if not process:
            return 'Unknown'

        process_type = self.get_invocation_process_type()
        if process_type == PROCTYPE_SERVICE:
            receiver_header = self.get_header_value('receiver', 'Unknown')
            receiver = self.get_service_name(receiver_header)
            return receiver

        elif process_type == PROCTYPE_AGENT:
            if process.resource_type is None:
                return process.name
            else:
                return process.resource_type

        else:
            return process.name

    def get_arg_value(self, arg_name, default_value=None):
        """Returns the value of of the specified arg or the specified default value
        """
        value = self.args[arg_name] if arg_name in self.args and self.args[arg_name] != '' else default_value
        return value

    def get_header_value(self, header_name, default_value=None):
        """Returns the value of of the specified header or the specified default value
        """
        value = self.headers[header_name] if header_name in self.headers and self.headers[header_name] != '' else default_value
        return value

    def get_service_name(self, header_value):
        """Return the service name from a messaging receiver (two value tuple of
        exchange, queue=qualified service name)
        """
        value_list = header_value.split(',')
        value = value_list[1] if len(value_list) > 1 else value_list[0]
        value = value.rsplit(".", 1)[-1]       # Remove any name qualification (e.g. sysname.system.)
        return value.strip()


class Interceptor(object):
    """
    Basic interceptor model.
    """
    def configure(self, config):
        pass

    def outgoing(self, invocation):
        pass

    def incoming(self, invocation):
        pass


def process_interceptors(interceptors, invocation):
    for interceptor in interceptors:
        func = getattr(interceptor, invocation.path)
        invocation = func(invocation)
    return invocation
