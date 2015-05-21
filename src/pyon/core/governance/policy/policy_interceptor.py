#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisinger'

import pickle

from ndg.xacml.core.context.result import Decision

from pyon.core import PROCTYPE_AGENT, PROCTYPE_SERVICE
from pyon.core import MSG_HEADER_ACTOR, MSG_HEADER_TOKENS, MSG_HEADER_OP, MSG_HEADER_FORMAT, MSG_HEADER_PERFORMATIVE
from pyon.core.bootstrap import CFG
from pyon.core.governance import ANONYMOUS_ACTOR, DECORATOR_RESOURCE_ID, DECORATOR_USER_CONTEXT_ID, DECORATOR_ALWAYS_VERIFY_POLICY
from pyon.core.governance.governance_interceptor import BaseInternalGovernanceInterceptor
from pyon.core.governance.governance_dispatcher import GovernanceDispatcher
from pyon.core.object import IonObjectBase
from pyon.core.registry import is_ion_object, message_classes, has_class_decorator
from pyon.util.containers import current_time_millis
from pyon.util.log import log


PERMIT_SUB_CALLS = 'PERMIT_SUB_CALLS'


def create_policy_token(originating_container, actor_id, requesting_message, token):
    """Factory method to create a policy token"""
    p = PolicyToken(originating_container, actor_id, requesting_message, token)
    return pickle.dumps(p)


class PolicyToken(object):

    def __init__(self, originating_container, actor_id, requesting_message, token):
        self.originator = originating_container
        self.actor_id = actor_id
        self.requesting_message = requesting_message
        self.token = token

        timeout = CFG.get_safe('container.messaging.timeout.receive', 30)
        self.expire_time = current_time_millis() + (timeout * 1000)  # Set the expire time to current time + timeout in ms

    def is_expired(self):
        return current_time_millis() > self.expire_time

    def is_token(self, token):
        return self.token == token


class PolicyInterceptor(BaseInternalGovernanceInterceptor):

    def _set_header_from_decorator(self, invocation, decorator_name, header_name):
        # Check for a field with the given decorator and if found, then set given header
        # with that field's value or if the decorator specifies a field within an object,
        # then use the object's field value (i.e. _id)
        field = invocation.message.find_field_for_decorator(decorator_name)
        if field is not None and hasattr(invocation.message, field):
            deco_value = invocation.message.get_decorator_value(field, decorator_name)
            if deco_value:
                # Assume that if there is a value, then it is specifying a field in the object
                fld_value = getattr(invocation.message, field)
                if getattr(fld_value, deco_value) is not None:
                    invocation.headers[header_name] = getattr(fld_value, deco_value)
            else:
                if getattr(invocation.message, field) is not None:
                    invocation.headers[header_name] = getattr(invocation.message, field)

    def outgoing(self, invocation):
        """Policy governance process interceptor for messages sent.
        Adds some governance relevant headers.
        """
        #log.trace("PolicyInterceptor.outgoing: %s", invocation.get_arg_value('process', invocation))
        try:
            if isinstance(invocation.message, IonObjectBase):
                self._set_header_from_decorator(invocation, DECORATOR_RESOURCE_ID, 'resource-id')
                self._set_header_from_decorator(invocation, DECORATOR_USER_CONTEXT_ID, 'user-context-id')

        except Exception:
            log.exception("Policy interceptor outgoing error")

        return invocation

    def incoming(self, invocation):
        """Policy governance process interceptor for messages received.
        Checks policy based on message headers.
        """
        #log.trace("PolicyInterceptor.incoming: %s", invocation.get_arg_value('process', invocation))
        #print "========"
        #print invocation.headers

        # If missing the performative header, consider it as a failure message.
        msg_performative = invocation.get_header_value(MSG_HEADER_PERFORMATIVE, 'failure')
        message_format = invocation.get_header_value(MSG_HEADER_FORMAT, '')
        op = invocation.get_header_value(MSG_HEADER_OP, 'unknown')
        process_type = invocation.get_invocation_process_type()
        sender, sender_type = invocation.get_message_sender()

        # TODO - This should be removed once better process security is implemented
        # We assume all external requests have security headers set (even anonymous calls),
        # so that calls from within the system (e.g. headless processes) can be considered trusted.
        policy_loaded = CFG.get_safe('system.load_policy', False)
        if policy_loaded:
            # With policy: maintain the actor id
            actor_id = invocation.get_header_value(MSG_HEADER_ACTOR, None)
        else:
            # Without policy: default to anonymous
            actor_id = invocation.get_header_value(MSG_HEADER_ACTOR, ANONYMOUS_ACTOR)

        # Only check messages marked as the initial rpc request
        # TODO - remove the actor_id is not None when headless process have actor_ids
        if msg_performative == 'request' and actor_id is not None:
            receiver = invocation.get_message_receiver()

            # Can't check policy if the controller is not initialized
            if self.governance_controller is None:
                log.debug("Skipping policy check for %s(%s) since governance_controller is None", receiver, op)
                invocation.message_annotations[GovernanceDispatcher.POLICY__STATUS_ANNOTATION] = GovernanceDispatcher.STATUS_SKIPPED
                return invocation

            # No need to check for requests from the system actor - should increase performance during startup
            if actor_id == self.governance_controller.system_actor_id:
                log.debug("Skipping policy check for %s(%s) for the system actor", receiver, op)
                invocation.message_annotations[GovernanceDispatcher.POLICY__STATUS_ANNOTATION] = GovernanceDispatcher.STATUS_SKIPPED
                return invocation

            # Check to see if there is a AlwaysVerifyPolicy decorator
            always_verify_policy = False
            if is_ion_object(message_format):
                try:
                    msg_class = message_classes[message_format]
                    always_verify_policy = has_class_decorator(msg_class, DECORATOR_ALWAYS_VERIFY_POLICY)
                except Exception:
                    pass

            # For services only - if this is a sub RPC request from a higher level service that has
            # already been validated and set a token then skip checking policy yet again - should help
            # with performance and to simplify policy.
            # All calls from the RMS must be checked; it acts as generic facade forwarding many kinds of requests
            if not always_verify_policy and process_type == PROCTYPE_SERVICE and sender != 'resource_management' and \
                    self.has_valid_token(invocation, PERMIT_SUB_CALLS):
                #log.debug("Skipping policy check for service call %s %s since token is valid", receiver, op)
                invocation.message_annotations[GovernanceDispatcher.POLICY__STATUS_ANNOTATION] = GovernanceDispatcher.STATUS_SKIPPED
                return invocation

            #log.debug("Checking request for %s: %s(%s) from %s  ", process_type, receiver, op, actor_id)

            # Annotate the message has started policy checking
            invocation.message_annotations[GovernanceDispatcher.POLICY__STATUS_ANNOTATION] = GovernanceDispatcher.STATUS_STARTED

            ret = None

            # ---- POLICY RULE CHECKS HERE ----
            # First check for Org boundary policies if the container is configured as such
            org_id = self.governance_controller.get_container_org_boundary_id()
            if org_id is not None:
                ret = self.governance_controller.policy_decision_point_manager.check_resource_request_policies(invocation, org_id)

            if str(ret) != Decision.DENY_STR:
                # Next check endpoint process specific policies
                if process_type == PROCTYPE_AGENT:
                    ret = self.governance_controller.policy_decision_point_manager.check_agent_request_policies(invocation)

                elif process_type == PROCTYPE_SERVICE:
                    ret = self.governance_controller.policy_decision_point_manager.check_service_request_policies(invocation)

            #log.debug("Policy Decision: %s", ret)
            # ---- POLICY RULE CHECKS END ----

            # Annotate the message has completed policy checking
            invocation.message_annotations[GovernanceDispatcher.POLICY__STATUS_ANNOTATION] = GovernanceDispatcher.STATUS_COMPLETE

            if ret is not None:
                if str(ret) == Decision.DENY_STR:
                    self.annotate_denied_message(invocation)
                else:
                    self.permit_sub_rpc_calls_token(invocation)

        else:
            invocation.message_annotations[GovernanceDispatcher.POLICY__STATUS_ANNOTATION] = GovernanceDispatcher.STATUS_SKIPPED

        return invocation

    def annotate_denied_message(self, invocation):
        # TODO - Fix this to use the proper annotation reference and figure out special cases
        if MSG_HEADER_OP in invocation.headers and invocation.headers[MSG_HEADER_OP] != 'start_rel_from_url':
            invocation.message_annotations[GovernanceDispatcher.POLICY__STATUS_ANNOTATION] = GovernanceDispatcher.STATUS_REJECT

    def permit_sub_rpc_calls_token(self, invocation):
        actor_tokens = invocation.get_header_value(MSG_HEADER_TOKENS, None)
        if actor_tokens is None:
            actor_tokens = list()
            invocation.headers[MSG_HEADER_TOKENS] = actor_tokens

        # See if this token exists already
        for tok in actor_tokens:
            pol_tok = pickle.loads(tok)
            if pol_tok.is_token(PERMIT_SUB_CALLS):
                return

        # Not found, so create a new one
        container_id = invocation.get_header_value('origin-container-id', None)
        actor_id = invocation.get_header_value(MSG_HEADER_ACTOR, ANONYMOUS_ACTOR)
        requesting_message = invocation.get_header_value(MSG_HEADER_FORMAT, 'Unknown')

        # Create a token that subsequent resource_registry calls are allowed
        token = create_policy_token(container_id, actor_id, requesting_message, PERMIT_SUB_CALLS)
        actor_tokens.append(token)

    def has_valid_token(self, invocation, token):
        actor_tokens = invocation.get_header_value(MSG_HEADER_TOKENS, None)
        if actor_tokens is None:
            return False

        # See if this token exists already
        for tok in actor_tokens:
            pol_tok = pickle.loads(tok)
            if pol_tok.is_token(token) and not pol_tok.is_expired():
                return True

        return False


