#!/usr/bin/env python

"""Governance related constants and helper functions used within the Container."""

__author__ = 'Stephen P. Henrie, Michael Meisinger'

from pyon.core import (bootstrap, MSG_HEADER_ACTOR, MSG_HEADER_ROLES, MSG_HEADER_OP, MSG_HEADER_RESOURCE_ID,
                       MSG_HEADER_VALID, MSG_HEADER_USER_CONTEXT_ID)
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, Inconsistent
from pyon.ion.resource import RT, PRED, OT
from pyon.util.containers import get_safe, get_ion_ts_millis
from pyon.util.log import log

# These constants are ubiquitous, so define in the container
DEFAULT_ACTOR_ID = 'anonymous'
ANONYMOUS_ACTOR = DEFAULT_ACTOR_ID

MODERATOR_ROLE = 'MODERATOR'   # Can act upon resource within the specific Org, managerial permissions in Org
OPERATOR_ROLE = 'OPERATOR'     # Can act upon resource within the specific Org, action permissions in Org
MEMBER_ROLE = 'MEMBER'         # Can access resources within the specific Org
SUPERUSER_ROLE = 'SUPERUSER'   # Can act upon resources across all Orgs with superuser access

# Decorator names for service operations and their parameters
DECORATOR_OP_VERB = "OperationVerb"
DECORATOR_ALWAYS_VERIFY_POLICY = "AlwaysVerifyPolicy"
DECORATOR_RESOURCE_ID = "ResourceId"
DECORATOR_USER_CONTEXT_ID = "UserContextId"


def get_role_message_headers(org_roles):
    """
    Iterate the Org(s) that the user belongs to and create a header that lists only the
    role names per Org (governance name) assigned to the user,
    e.g. {'ION': ['MEMBER', 'OPERATOR'], 'Org2': ['MEMBER']}
    """
    role_header = dict()
    try:
        for org in org_roles:
            role_header[org] = []
            for role in org_roles[org]:
                role_header[org].append(role.governance_name)

    except Exception:
        log.exception("Cannot build role message header")

    return role_header


def build_actor_header(actor_id=None, actor_roles=None):
    """
    Build the message header used by governance to identify the actor and roles.
    """
    return {MSG_HEADER_ACTOR: actor_id or DEFAULT_ACTOR_ID,
            MSG_HEADER_ROLES: actor_roles or {}}


def get_actor_header(actor_id):
    """
    Returns the actor related message headers for a specific actor_id.
    Will return anonymous if the actor_id is not found.
    """
    actor_header = build_actor_header(DEFAULT_ACTOR_ID, {})

    if actor_id:
        try:
            header_roles = find_roles_by_actor(actor_id)
            actor_header = build_actor_header(actor_id, header_roles)
        except Exception:
            log.exception("Cannot build actor message header")

    return actor_header


def has_org_role(role_header=None, org_governance_name=None, role_name=None):
    """
    Check the ion-actor-roles message header to see if this actor has the specified role in the specified Org.
    Parameter role_name can be a string with the name of a user role or a list of user role names, which will
    recursively call this same method for each role name in the list until one is found or the list is exhausted.
    """
    if role_header is None or org_governance_name is None or role_name is None:
        raise BadRequest("One of the parameters to this method are not set")

    if isinstance(role_name, list):
        for role in role_name:
            if has_org_role(role_header, org_governance_name, role):
                return True
    else:
        if org_governance_name in role_header:
            if role_name in role_header[org_governance_name]:
                return True

    return False


def find_roles_by_actor(actor_id=None):
    """
    Returns a dict of all User Roles roles by Org Name associated with the specified actor
    """
    if actor_id is None or not len(actor_id):
        raise BadRequest("The actor_id parameter is missing")

    role_dict = dict()

    gov_controller = bootstrap.container_instance.governance_controller
    role_list, _ = gov_controller.rr.find_objects(actor_id, PRED.hasRole, RT.UserRole)

    for role in role_list:
        if role.org_governance_name not in role_dict:
            role_dict[role.org_governance_name] = list()

        role_dict[role.org_governance_name].append(role.governance_name)

    # Membership in ION Org is implied
    if gov_controller.system_root_org_name not in role_dict:
        role_dict[gov_controller.system_root_org_name] = list()

    role_dict[gov_controller.system_root_org_name].append(MEMBER_ROLE)

    return role_dict


def get_system_actor():
    """
    Returns the ION system actor defined in the Resource Registry as ActorIdentity resource.
    Returns None if not found.
    """
    try:
        gov_controller = bootstrap.container_instance.governance_controller
        system_actor_name = get_safe(gov_controller.CFG, "system.system_actor", "ionsystem")
        system_actor, _ = gov_controller.rr.find_resources(RT.ActorIdentity, name=system_actor_name, id_only=False)
        if not system_actor:
            return None

        return system_actor[0]

    except Exception:
        log.exception("Cannot retrieve system actor")
        return None


def is_system_actor(actor_id):
    """
    Is this the specified actor_id the system actor
    """
    system_actor = get_system_actor()
    if system_actor is not None and system_actor._id == actor_id:
        return True

    return False

def get_system_actor_header(system_actor=None):
    """
    Returns the actor related message headers for a the ION System Actor
    """
    try:
        if system_actor is None:
            system_actor = get_system_actor()

        if not system_actor or system_actor is None:
            log.warn("The ION System Actor was not found; defaulting to anonymous actor")
            actor_header = get_actor_header(None)
        else:
            actor_header = get_actor_header(system_actor._id)

        return actor_header

    except Exception:
        log.exception("Could not get system actor header")
        return get_actor_header(None)


def get_valid_principal_commitments(principal_id=None, consumer_id=None):
    """
    Returns the list of valid commitments for the specified principal (org or actor.
    If optional consumer_id (actor) is supplied, then filtered by consumer_id
    """
    log.debug("Finding commitments for principal: %s", principal_id)
    if principal_id is None:
        return None

    try:
        gov_controller = bootstrap.container_instance.governance_controller
        commitments, _ = gov_controller.rr.find_objects(principal_id, PRED.hasCommitment, RT.Commitment, id_only=False)
        if not commitments:
            return None

        cur_time = get_ion_ts_millis()
        commitment_list = [com for com in commitments if (consumer_id == None or com.consumer == consumer_id) and \
                    (int(com.expiration) == 0 or (int(com.expiration) > 0 and cur_time < int(com.expiration)))]
        if commitment_list:
            return commitment_list

    except Exception:
        log.exception("Could not determine actor resource commitments")

    return None


def get_valid_resource_commitments(resource_id=None, actor_id=None):
    """
    Returns the list of valid commitments for the specified resource.
    If optional actor_id is supplied, then filtered by actor_id
    """
    log.debug("Finding commitments for resource_id: %s and actor_id: %s", resource_id, actor_id)
    if resource_id is None:
        return None

    try:
        gov_controller = bootstrap.container_instance.governance_controller
        commitments, _ = gov_controller.rr.find_subjects(RT.Commitment, PRED.hasTarget, resource_id, id_only=False)
        if not commitments:
            return None

        cur_time = get_ion_ts_millis()
        commitment_list = [com for com in commitments if (actor_id == None or com.consumer == actor_id) and \
                    (int(com.expiration) == 0 or (int(com.expiration) > 0 and cur_time < int(com.expiration)))]
        if commitment_list:
            return commitment_list

    except Exception:
        log.exception("Could not determine actor resource commitments")

    return None


def has_valid_resource_commitments(actor_id, resource_id):
    """
    Returns a ResourceCommitmentStatus object indicating the commitment status between this resource/actor
    Can only have an exclusive commitment if actor already has a shared commitment.
    """
    ret_status = IonObject(OT.ResourceCommitmentStatus)
    commitments = get_valid_resource_commitments(resource_id, actor_id)
    if commitments is None:
        # No commitments were found between this resource_id and actor_id - so return default object with
        # fields set to False
        return ret_status

    ret_status.shared = True

    for com in commitments:
        if com.commitment.exclusive == True:
            # Found an exclusive commitment
            ret_status.exclusive = True
            return ret_status

    # Only a shared commitment was found
    return ret_status


def has_valid_shared_resource_commitment(actor_id=None, resource_id=None):
    """
    This method returns True if the specified actor_id has acquired shared access for the specified resource id, otherwise False.
    """
    if actor_id is None or resource_id is None:
        raise BadRequest('One or all of the method parameters are not set')

    commitment_status =  has_valid_resource_commitments(actor_id, resource_id)

    return commitment_status.shared


def has_valid_exclusive_resource_commitment(actor_id=None, resource_id=None):
    """
    This method returns True if the specified actor_id has acquired exclusive access for the specified resource id, otherwise False.
    """
    if actor_id is None or resource_id is None:
        raise BadRequest('One or all of the method parameters are not set')

    commitment_status = has_valid_resource_commitments(actor_id, resource_id)

    # If the resource has not been acquired for sharing, then it can't have been acquired exclusively
    if not commitment_status.shared:
        return False

    return commitment_status.exclusive


def is_resource_owner(actor_id=None, resource_id=None):
    """
    Returns True if the specified actor_id is an Owner of the specified resource id, otherwise False
    """
    if actor_id is None or resource_id is None:
        raise BadRequest('One or all of the method parameters are not set')

    gov_controller = bootstrap.container_instance.governance_controller
    owners =  gov_controller.rr.find_objects(subject=resource_id, predicate=PRED.hasOwner, object_type=RT.ActorIdentity, id_only=True)

    if actor_id not in owners[0]:
        return False

    return True


class GovernanceHeaderValues(object):
    """
    A helper class for containing governance values from a message header
    """

    def __init__(self, headers, process=None, resource_id_required=True):
        """
        Helpers for retrieving governance related values: op, actor_id, actor_roles, resource_id from the message header
        @param headers:
        @param resource_id_required: True if the message header must have a resource-id field and value.
        """
        if not headers or not isinstance(headers, dict):
            raise BadRequest("The headers parameter is not a valid message header dictionary")

        self._op = headers.get(MSG_HEADER_OP, "Unknown-Operation")

        if process is not None and hasattr(process, 'name'):
            self._process_name = process.name
        else:
            if 'process' in headers:
                if getattr(headers['process'], 'name'):
                    self._process_name = headers['process'].name
                else:
                    self._process_name = "Unknown-Process"
            else:
                self._process_name = "Unknown-Process"


        # The self.name references below should be provided by the running ION process (service, agent, etc),
        # which will be using this class.
        if MSG_HEADER_ACTOR in headers:
            self._actor_id = headers[MSG_HEADER_ACTOR]
        else:
            raise Inconsistent('%s(%s) has been denied since the ion-actor-id can not be found in the message headers' % (self._process_name, self._op))

        if MSG_HEADER_ROLES in headers:
            self._actor_roles = headers[MSG_HEADER_ROLES]
        else:
            raise Inconsistent('%s(%s) has been denied since the ion-actor-roles can not be found in the message headers' % (self._process_name, self._op))

        if MSG_HEADER_RESOURCE_ID in headers:
            self._resource_id = headers[MSG_HEADER_RESOURCE_ID]
        else:
            if resource_id_required:
                raise Inconsistent('%s(%s) has been denied since the resource-id can not be found in the message headers' % (self._process_name, self._op))
            self._resource_id = ''

        self._user_context_id = headers.get(MSG_HEADER_USER_CONTEXT_ID, None)

    @property
    def op(self):
        return self._op

    @property
    def actor_id(self):
        return self._actor_id

    @property
    def actor_roles(self):
        return self._actor_roles

    @property
    def resource_id(self):
        return self._resource_id

    @property
    def user_context_id(self):
        return self._user_context_id

    @property
    def process_name(self):
        return self._process_name
