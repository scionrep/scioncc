#!/usr/bin/env python

__author__ = "Stephen P. Henrie, Michael Meisinger"

from pyon.public import CFG, IonObject, LCS, RT, PRED, OT, Inconsistent, NotFound, BadRequest, log, EventPublisher
from pyon.core.governance import MODERATOR_ROLE, MEMBER_ROLE, OPERATOR_ROLE
from pyon.core.governance.negotiation import Negotiation
from pyon.core.registry import issubtype
from pyon.ion.directory import Directory
from pyon.util.containers import is_basic_identifier, get_ion_ts, create_basic_identifier, get_ion_ts_millis

from interface.objects import ProposalStatusEnum, ProposalOriginatorEnum, NegotiationStatusEnum
from interface.services.core.iorg_management_service import BaseOrgManagementService


# Supported Negotiations - perhaps move these to data at some point if there are
# more negotiation types and/or remove references to local functions to make this more dynamic
negotiation_rules = {
    "EnrollmentProposal": {
        'pre_conditions': ['is_registered(sap.consumer)',
                           'not is_enrolled(sap.provider,sap.consumer)',
                           'not is_enroll_negotiation_open(sap.provider,sap.consumer)'],
        'accept_action': 'enroll_member(sap.provider,sap.consumer)',
        'auto_accept': True
    },

    "RequestRoleProposal": {
        'pre_conditions': ['is_enrolled(sap.provider,sap.consumer)',
                           'not has_role(sap.provider,sap.consumer,sap.role_name)'],
        'accept_action': 'grant_role(sap.provider,sap.consumer,sap.role_name)',
        'auto_accept': True
    },

    "AcquireResourceProposal": {
        'pre_conditions': ['is_enrolled(sap.provider,sap.consumer)',
                           'has_role(sap.provider,sap.consumer,"' + OPERATOR_ROLE + '")',
                           'is_resource_shared(sap.provider,sap.resource_id)'],
        'accept_action': 'acquire_resource(sap)',
        'auto_accept': True
    },

    "AcquireResourceExclusiveProposal": {
        'pre_conditions': ['is_resource_acquired(sap.consumer, sap.resource_id)',
                           'not is_resource_acquired_exclusively(sap.consumer, sap.resource_id)'],
        'accept_action': 'acquire_resource(sap)',
        'auto_accept': True
    }
}


class OrgManagementService(BaseOrgManagementService):
    """
    Services to define and administer an Org (organization, facility), to enroll/remove members and to provide
    access to the resources of an Org to enrolled or affiliated entities (identities). Contains contract
    and commitment repository
    """
    def on_init(self):
        self.rr = self.clients.resource_registry
        self.event_pub = EventPublisher(process=self)
        self.negotiation_handler = Negotiation(self, negotiation_rules, self.event_pub)
        self.root_org_id = None

    def _get_root_org_name(self):
        if self.container is None or self.container.governance_controller is None:
            return CFG.get_safe("system.root_org", "ION")

        return self.container.governance_controller.system_root_org_name

    def _get_root_org_id(self):
        if not self.root_org_id:
            root_org = self.find_org()
            self.root_org_id = root_org._id
        return self.root_org_id

    def _validate_user_role(self, arg_name, role_name, org_id):
        """
        Check that the given argument is a resource id, by retrieving the resource from the
        resource registry. Additionally checks type and returns the result object
        """
        if not role_name:
            raise BadRequest("The argument '%s' is missing" % arg_name)
        if not org_id:
            raise BadRequest("The argument 'org_id' is missing")
        user_role = self._find_org_role(org_id, role_name)
        return user_role

    # -------------------------------------------------------------------------
    # Org management (CRUD)

    def create_org(self, org=None):
        """Creates an Org based on the provided object. The id string returned
        is the internal id by which Org will be identified in the data store.
        """
        # Only allow one root ION Org in the system
        self._validate_resource_obj("org", org, RT.Org, checks="noid,name,unique")

        # If this governance identifier is not set, then set to a safe version of the org name.
        if not org.org_governance_name:
            org.org_governance_name = create_basic_identifier(org.name)
        if not is_basic_identifier(org.org_governance_name):
            raise BadRequest("The Org org_governance_name '%s' contains invalid characters" % org.org_governance_name)

        org_id, _ = self.rr.create(org)

        # Instantiate a Directory for this Org
        directory = Directory(orgname=org.name)

        # Instantiate initial set of User Roles for this Org
        self._create_org_roles(org_id)

        return org_id

    def _create_org_roles(self, org_id):
        # Instantiate initial set of User Roles for this Org
        manager_role = IonObject(RT.UserRole, name="MODERATOR", governance_name=MODERATOR_ROLE,
                                 description="Manage organization members, resources and roles")
        self.add_org_role(org_id, manager_role)

        operator_role = IonObject(RT.UserRole, name="OPERATOR", governance_name=OPERATOR_ROLE,
                                description="Modify and control organization resources")
        self.add_org_role(org_id, operator_role)

        member_role = IonObject(RT.UserRole, name="MEMBER", governance_name=MEMBER_ROLE,
                                description="Access organization resources")
        self.add_org_role(org_id, member_role)

    def update_org(self, org=None):
        """Updates the Org based on provided object.
        """
        old_org = self._validate_resource_obj("org", org, RT.Org, checks="id")
        if org.org_governance_name != old_org.org_governance_name:
            raise BadRequest("Cannot update Org org_governance_name")

        self.rr.update(org)

    def read_org(self, org_id=""):
        """Returns the Org object for the specified id.
        Throws exception if id does not match any persisted Org objects.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)

        return org_obj

    def delete_org(self, org_id=""):
        """Permanently deletes Org object with the specified
        id. Throws exception if id does not match any persisted Org object.
        """
        self._validate_resource_id("org_id", org_id, RT.Org)

        self.rr.delete(org_id)

    def find_org(self, name=""):
        """Finds an Org object with the specified name. Defaults to the
        root ION object. Throws a NotFound exception if the object does not exist.
        """

        # Default to the root ION Org if not specified
        if not name:
            name = self._get_root_org_name()

        res_list, _ = self.rr.find_resources(restype=RT.Org, name=name, id_only=False)
        if not res_list:
            raise NotFound("The Org with name %s does not exist" % name)
        return res_list[0]

    # -------------------------------------------------------------------------
    # Org roles

    def add_org_role(self, org_id="", user_role=None):
        """Adds a UserRole to an Org, if the role by the specified
       name does not exist.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        self._validate_resource_obj("user_role", user_role, RT.UserRole, checks="noid,name")
        if not is_basic_identifier(user_role.governance_name):
            raise BadRequest("Invalid role governance_name")

        user_role.org_governance_name = org_obj.org_governance_name

        try:
            self._find_org_role(org_id, user_role.governance_name)
            raise BadRequest("Role '%s' is already associated with this Org" % user_role.governance_name)
        except NotFound:
            pass

        user_role_id, _ = self.rr.create(user_role)

        self.rr.create_association(org_obj, PRED.hasRole, user_role_id)

        return user_role_id

    def remove_org_role(self, org_id="", role_name="", force_removal=False):
        """Removes a UserRole from an Org. The UserRole will not be removed if there are
       users associated with the UserRole unless the force_removal parameter is set to True
        """
        self._validate_resource_id("org_id", org_id, RT.Org)
        user_role = self._validate_user_role("role_name", role_name, org_id)

        if not force_removal:
            alist, _ = self.rr.find_subjects(RT.ActorIdentity, PRED.hasRole, user_role)
            if alist:
                raise BadRequest("UserRole %s still in use and cannot be removed" % user_role.name)

        self.rr.delete(user_role._id)

    def find_org_role_by_name(self, org_id="", role_name=""):
        """Returns the UserRole object for the specified name in the Org.
        """
        self._validate_resource_id("org_id", org_id, RT.Org)
        user_role = self._find_org_role(org_id, role_name)

        return user_role

    def _find_org_role(self, org_id="", role_name=""):
        if not org_id:
            raise BadRequest("The org_id argument is missing")
        if not role_name:
            raise BadRequest("The role_name argument is missing")

        # Iterating (vs. query) is just fine, because the number of org roles is sufficiently small
        org_roles = self._list_org_roles(org_id)
        for role in org_roles:
            if role.governance_name == role_name:
                return role

        raise NotFound("Role %s not found in Org id=%s" % (role_name, org_id))

    def list_org_roles(self, org_id=""):
        """Returns a list of roles available in an Org. Will throw a not NotFound exception
        if none of the specified ids do not exist.
        """
        self._validate_resource_id("org_id", org_id, RT.Org)
        return self._list_org_roles(org_id)

    def _list_org_roles(self, org_id=""):
        if not org_id:
            raise BadRequest("Illegal org_id")

        role_list, _ = self.rr.find_objects(org_id, PRED.hasRole, RT.UserRole, id_only=False)
        return role_list

    # -------------------------------------------------------------------------
    # Negotiations

    def negotiate(self, sap=None):
        """A generic operation for negotiating actions with an Org, such as for enrollment, role request
        or to acquire a resource managed by the Org. The ServiceAgreementProposal object is used to
        specify conditions of the proposal as well as counter proposals and the Org will create a
        Negotiation resource to track the history and status of the negotiation.
        """

        if sap is None or (sap.type_ != OT.ServiceAgreementProposal and not issubtype(sap.type_, OT.ServiceAgreementProposal)):
            raise BadRequest("The sap argument must be a valid ServiceAgreementProposal object")

        if sap.proposal_status == ProposalStatusEnum.INITIAL:
            neg_id = self.negotiation_handler.create_negotiation(sap)

            org = self.read_org(org_id=sap.provider)

            # Publish an event indicating an Negotiation has been initiated
            self.event_pub.publish_event(event_type=OT.OrgNegotiationInitiatedEvent, origin=org._id, origin_type="Org",
                                         description=sap.description, org_name=org.name,
                                         negotiation_id=neg_id, sub_type=sap.type_)

            # Synchronize the internal reference for later use
            sap.negotiation_id = neg_id

        # Get the most recent version of the Negotiation resource
        negotiation = self.negotiation_handler.read_negotiation(sap)

        # Update the Negotiation object with the latest SAP
        neg_id = self.negotiation_handler.update_negotiation(sap)

        # Get the most recent version of the Negotiation resource
        negotiation = self.rr.read(neg_id)

        # hardcoding some rules at the moment - could be replaced by a Rules Engine
        if sap.type_ == OT.AcquireResourceExclusiveProposal:

            if self.is_resource_acquired_exclusively(None, sap.resource_id):
                # Automatically accept the proposal for exclusive access if it is not already acquired exclusively
                provider_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.REJECTED, ProposalOriginatorEnum.PROVIDER)

                rejection_reason = "The resource has already been acquired exclusively"

                # Update the Negotiation object with the latest SAP
                neg_id = self.negotiation_handler.update_negotiation(provider_accept_sap, rejection_reason)

                # Get the most recent version of the Negotiation resource
                negotiation = self.rr.read(neg_id)

            else:
                # Automatically reject the proposal if the expiration request is greater than 12 hours from now or 0
                cur_time = int(get_ion_ts())
                expiration = int(cur_time +  ( 12 * 60 * 60 * 1000 )) # 12 hours from now
                if int(sap.expiration) == 0 or int(sap.expiration) > expiration:
                    # Automatically accept the proposal for exclusive access if it is not already acquired exclusively
                    provider_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.REJECTED,
                                                                              ProposalOriginatorEnum.PROVIDER)

                    rejection_reason = "A proposal to acquire a resource exclusively must be more than 0 and be less than 12 hours."

                    # Update the Negotiation object with the latest SAP
                    neg_id = self.negotiation_handler.update_negotiation(provider_accept_sap, rejection_reason)

                    # Get the most recent version of the Negotiation resource
                    negotiation = self.rr.read(neg_id)

                else:
                    # Automatically accept the proposal for exclusive access if it is not already acquired exclusively
                    provider_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED,
                                                                              ProposalOriginatorEnum.PROVIDER)

                    # Update the Negotiation object with the latest SAP
                    neg_id = self.negotiation_handler.update_negotiation(provider_accept_sap)

                    # Get the most recent version of the Negotiation resource
                    negotiation = self.rr.read(neg_id)

        # Check to see if the rules allow for auto acceptance of the negotiations -
        # where the second party is assumed to accept if the
        # first party accepts.
        if negotiation_rules[sap.type_]["auto_accept"]:
            # Automatically accept for the consumer if the Org Manager as provider accepts the proposal
            latest_sap = negotiation.proposals[-1]

            if latest_sap.proposal_status == ProposalStatusEnum.ACCEPTED and latest_sap.originator == ProposalOriginatorEnum.PROVIDER:
                consumer_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED)

                # Update the Negotiation object with the latest SAP
                neg_id = self.negotiation_handler.update_negotiation(consumer_accept_sap)

                # Get the most recent version of the Negotiation resource
                negotiation = self.rr.read(neg_id)

            elif latest_sap.proposal_status == ProposalStatusEnum.ACCEPTED and latest_sap.originator == ProposalOriginatorEnum.CONSUMER:
                provider_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)

                # Update the Negotiation object with the latest SAP
                neg_id = self.negotiation_handler.update_negotiation(provider_accept_sap)

                # Get the most recent version of the Negotiation resource
                negotiation = self.rr.read(neg_id)

        # Return the latest proposal
        return negotiation.proposals[-1]

    def find_org_negotiations(self, org_id="", proposal_type="", negotiation_status=-1):
        """Returns a list of negotiations for an Org. An optional proposal_type can be supplied
        or else all proposals will be returned. An optional negotiation_status can be supplied
        or else all proposals will be returned.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)

        neg_list, _ = self.rr.find_objects(org_id, PRED.hasNegotiation)

        if proposal_type:
            neg_list = [neg for neg in neg_list if neg.proposals[0].type_ == proposal_type]
        if negotiation_status > -1:
            neg_list = [neg for neg in neg_list if neg.negotiation_status == negotiation_status]

        return neg_list

    def find_org_closed_negotiations(self, org_id="", proposal_type=""):
        """Returns a list of closed negotiations for an Org - those which are Accepted or Rejected.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)

        neg_list, _ = self.rr.find_objects(org_id, PRED.hasNegotiation)

        if proposal_type:
            neg_list = [neg for neg in neg_list if neg.proposals[0].type_ == proposal_type]

        neg_list = [neg for neg in neg_list if neg.negotiation_status != NegotiationStatusEnum.OPEN]

        return neg_list

    def find_user_negotiations(self, actor_id="", org_id="", proposal_type="", negotiation_status=-1):
        """Returns a list of negotiations for a specified Actor. All negotiations for all Orgs will be returned
        unless an org_id is specified. An optional proposal_type can be supplied
        or else all proposals will be returned. An optional negotiation_status can be provided
        or else all proposals will be returned.
        """
        actor_obj = self._validate_resource_id("actor_id", actor_id, RT.ActorIdentity)
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org, optional=True)

        neg_list, _ = self.rr.find_objects(actor_obj, PRED.hasNegotiation)

        if org_id:
            neg_list = [neg for neg in neg_list if neg.proposals[0].provider == org_id]

        if proposal_type:
            neg_list = [neg for neg in neg_list if neg.proposals[0].type_ == proposal_type]
        if negotiation_status > -1:
            neg_list = [neg for neg in neg_list if neg.negotiation_status == negotiation_status]

        return neg_list

    # -------------------------------------------------------------------------
    # Member management

    def enroll_member(self, org_id="", actor_id=""):
        """Enrolls an actor into an Org so that they may find and negotiate to use
        resources of the Org. Membership in the ION Org is implied by registration
        with the system, so a membership association to the ION Org is not maintained.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        actor_obj = self._validate_resource_id("actor_id", actor_id, RT.ActorIdentity)

        if org_obj.name == self._get_root_org_name():
            raise BadRequest("A request to enroll in the root ION Org is not allowed")

        self.rr.create_association(org_obj, PRED.hasMember, actor_obj)

        member_role = self.find_org_role_by_name(org_id, MEMBER_ROLE)
        self._add_role_association(org_obj, actor_obj, member_role)

        self.event_pub.publish_event(event_type=OT.OrgMembershipGrantedEvent, origin=org_id, origin_type="Org",
                                     description="The member has enrolled in the Org",
                                     actor_id=actor_id, org_name=org_obj.name)

    def cancel_member_enrollment(self, org_id="", actor_id=""):
        """Cancels the membership of a specific actor actor within the specified Org.
        Once canceled, the actor will no longer have access to the resource of that Org.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        actor_obj = self._validate_resource_id("actor_id", actor_id, RT.ActorIdentity)

        if org_obj.name == self._get_root_org_name():
            raise BadRequest("A request to cancel enrollment in the root ION Org is not allowed")

        # First remove all associations to any roles
        role_list = self.list_actor_roles(actor_id, org_id)
        for user_role in role_list:
            self._delete_role_association(org_obj, actor_obj, user_role)

        # Finally remove the association to the Org
        aid = self.rr.get_association(org_obj, PRED.hasMember, actor_obj)
        if not aid:
            raise NotFound("The membership association between the specified actor and Org is not found")

        self.rr.delete_association(aid)

        self.event_pub.publish_event(event_type=OT.OrgMembershipCancelledEvent, origin=org_id, origin_type="Org",
                                     description="The member has cancelled enrollment in the Org",
                                     actor_id=actor_id, org_name=org_obj.name)

    def is_registered(self, actor_id=""):
        """Returns True if the specified actor_id is registered with the ION system; otherwise False.
        """
        if not actor_id:
            raise BadRequest("The actor_id argument is missing")
        try:
            # If this ID happens to be another resource, just return false
            user = self.rr.read(actor_id)
            if user.type_ == RT.ActorIdentity and user.lcstate != LCS.DELETED:
                return True
        except NotFound:
            pass

        return False

    def is_enrolled(self, org_id="", actor_id=""):
        """Returns True if the specified actor_id is enrolled in the Org and False if not.
        Throws a NotFound exception if neither id is found.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        actor_obj = self._validate_resource_id("actor_id", actor_id, RT.ActorIdentity)

        return self._is_enrolled(org_id, actor_id)

    def _is_enrolled(self, org_id="", actor_id=""):
        # Membership into the Root ION Org is implied as part of registration
        if org_id == self._get_root_org_id():
            return True

        try:
            self.rr.get_association(org_id, PRED.hasMember, actor_id)
        except NotFound:
            return False

        return True

    def list_enrolled_actors(self, org_id=""):
        """Returns a list of users enrolled in an Org. Will throw a not NotFound exception
        if none of the specified ids do not exist.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)

        # Membership into the Root ION Org is implied as part of registration
        if org_obj.name == self._get_root_org_name():
            user_list, _ = self.rr.find_resources(RT.ActorIdentity)
        else:
            user_list, _ = self.rr.find_objects(org_obj, PRED.hasMember, RT.ActorIdentity)

        return user_list

    def list_orgs_for_actor(self, actor_id=""):
        """Returns a list of Orgs that the actor is enrolled in. Will throw a not NotFound exception
        if none of the specified ids do not exist.
        """
        actor_obj = self._validate_resource_id("actor_id", actor_id, RT.ActorIdentity)

        org_list, _ = self.rr.find_subjects(RT.Org, PRED.hasMember, actor_obj)

        # Membership into the Root ION Org is implied as part of registration
        ion_org = self.find_org()
        org_list.append(ion_org)

        return org_list

    # -------------------------------------------------------------------------
    # Org role management

    def grant_role(self, org_id="", actor_id="", role_name="", scope=None):
        """Grants a defined role within an organization to a specific actor. A role of Member is
        automatically implied with successful enrollment. Will throw a not NotFound exception
        if none of the specified ids or role_name does not exist.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        actor_obj = self._validate_resource_id("actor_id", actor_id, RT.ActorIdentity)
        user_role = self._validate_user_role("role_name", role_name, org_id)

        if not self._is_enrolled(org_id, actor_id):
            raise BadRequest("The actor is not a member of the specified Org (%s)" % org_obj.name)

        self._add_role_association(org_obj, actor_obj, user_role)

    def _add_role_association(self, org, actor, user_role):
        self.rr.create_association(actor, PRED.hasRole, user_role)

        self.event_pub.publish_event(event_type=OT.UserRoleGrantedEvent, origin=org._id, origin_type="Org",
                                     sub_type=user_role.governance_name,
                                     description="Granted the %s role" % user_role.name,
                                     actor_id=actor._id, role_name=user_role.governance_name, org_name=org.name)

    def revoke_role(self, org_id="", actor_id="", role_name=""):
        """Revokes a defined Role within an organization to a specific actor.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        actor_obj = self._validate_resource_id("actor_id", actor_id, RT.ActorIdentity)
        user_role = self._validate_user_role("role_name", role_name, org_id)

        self._delete_role_association(org_obj, actor_obj, user_role)

    def _delete_role_association(self, org, actor, user_role):
        aid = self.rr.get_association(actor, PRED.hasRole, user_role)
        if not aid:
            raise NotFound("ActorIdentity %s to UserRole %s association not found" % (actor._id, user_role._id))

        self.rr.delete_association(aid)

        self.event_pub.publish_event(event_type=OT.UserRoleRevokedEvent, origin=org._id, origin_type="Org",
                                     sub_type=user_role.governance_name,
                                     description="Revoked the %s role" % user_role.name,
                                     actor_id=actor._id, role_name=user_role.governance_name, org_name=org.name)

    def has_role(self, org_id="", actor_id="", role_name=""):
        """Returns True if the specified actor_id has the specified role_name in the Org and False if not.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        actor_obj = self._validate_resource_id("actor_id", actor_id, RT.ActorIdentity)
        if not role_name:
            raise BadRequest("Invalid argument role_name")

        if org_id == self._get_root_org_id() and role_name == MEMBER_ROLE:
            return True

        role_list, _ = self.rr.find_objects(actor_id, PRED.hasRole, RT.UserRole, id_only=False)
        for role in role_list:
            if role.governance_name == role_name and role.org_governance_name == org_obj.org_governance_name:
                return True

        return False

    def list_actor_roles(self, actor_id="", org_id=""):
        """Returns a list of User Roles for a specific actor in an Org.
        """
        actor_obj = self._validate_resource_id("actor_id", actor_id, RT.ActorIdentity)
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org, optional=True)

        role_list, _ = self.rr.find_objects(actor_id, PRED.hasRole, RT.UserRole, id_only=False)

        if org_id:
            role_list = [r for r in role_list if r.org_governance_name == org_obj.org_governance_name]

        if not org_id or org_id == self._get_root_org_id():
            # Because a user is automatically enrolled with the ION Org then the membership role
            # is implied - so add it to the list
            member_role = self._find_org_role(self._get_root_org_id(), MEMBER_ROLE)
            role_list.append(member_role)

        return role_list

    # -------------------------------------------------------------------------
    # Resource sharing in Org

    def share_resource(self, org_id="", resource_id=""):
        """Share a resource with the specified Org. Once shared, the resource will be added to a directory
        of available resources within the Org.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        resource_obj = self._validate_resource_id("resource_id", resource_id)

        self.rr.create_association(org_obj, PRED.hasResource, resource_obj)

        self.event_pub.publish_event(event_type=OT.ResourceSharedEvent, origin=org_obj._id, origin_type="Org",
                                     sub_type=resource_obj.type_,
                                     description="The resource has been shared in the Org",
                                     resource_id=resource_id, org_name=org_obj.name )

    def unshare_resource(self, org_id="", resource_id=""):
        """Unshare a resource with the specified Org. Once unshared, the resource will be
        removed from the directory of available resources within the Org.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        resource_obj = self._validate_resource_id("resource_id", resource_id)

        aid = self.rr.get_association(org_obj, PRED.hasResource, resource_obj)
        if not aid:
            raise NotFound("Association between Resource and Org not found")

        self.rr.delete_association(aid)

        self.event_pub.publish_event(event_type=OT.ResourceUnsharedEvent, origin=org_obj._id, origin_type="Org",
                                     sub_type=resource_obj.type_,
                                     description="The resource has been unshared in the Org",
                                     resource_id=resource_id, org_name=org_obj.name )

    def is_resource_shared(self, org_id="", resource_id=""):
        """Returns True if the resource has been shared in the specified org_id; otherwise False is returned.
        """
        self._validate_resource_id("org_id", org_id, RT.Org)
        self._validate_resource_id("resource_id", resource_id)

        org_ids, _ = self.rr.find_subjects(RT.Org, PRED.hasResource, resource_id, id_only=True)
        return org_id in org_ids

    def list_shared_resources(self, org_id=''):
        self._validate_resource_id("org_id", org_id, RT.Org)

        if org_id == self._get_root_org_id():
            # All resources - reject for security reasons
            raise BadRequest("Cannot enumerate resources for root Org")

        res_objs, _ = self.rr.find_objects(org_id, PRED.hasResource, id_only=False)
        return res_objs

    def list_orgs_for_resource(self, resource_id=''):
        self._validate_resource_id("resource_id", resource_id)

        org_objs, _ = self.rr.find_subjects(RT.Org, PRED.hasResource, resource_id, id_only=False)
        root_org = self.find_org()
        org_objs.append(root_org)

        return org_objs

    # -------------------------------------------------------------------------
    # Resource commitments

    def acquire_resource(self, sap=None):
        """Creates a Commitment for the specified resource for a specified user within the
        specified Org as defined in the proposal. Once shared, the resource is committed to the user.
        """
        if not sap:
            raise BadRequest("The sap argument is missing")

        if sap.type_ == OT.AcquireResourceExclusiveProposal:
            exclusive = True
        else:
            exclusive = False

        commitment_id = self.create_resource_commitment(sap.provider, sap.consumer, sap.resource_id,
                                                        exclusive, int(sap.expiration))

        # Create association between the Commitment and the Negotiation objects
        self.rr.create_association(sap.negotiation_id, PRED.hasContract, commitment_id)

        return commitment_id

    def create_resource_commitment(self, org_id="", actor_id="", resource_id="", exclusive=False, expiration=0):
        """Creates a Commitment for the specified resource for a specified actor within
        the specified Org. Once shared, the resource is committed to the actor.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org, optional=True)
        actor_obj = self._validate_resource_id("actor_id", actor_id, RT.ActorIdentity)
        resource_obj = self._validate_resource_id("resource_id", resource_id)

        if org_id:
            # Check that resource is shared in Org?
            pass

        res_commitment = IonObject(OT.ResourceCommitment, resource_id=resource_id, exclusive=exclusive)

        commitment = IonObject(RT.Commitment, name="", provider=org_id, consumer=actor_id, commitment=res_commitment,
                               description="Resource Commitment", expiration=str(expiration))

        commitment._id, commitment._rev = self.rr.create(commitment)

        # Creating associations to all related objects
        self.rr.create_association(actor_id, PRED.hasCommitment, commitment._id)
        self.rr.create_association(commitment._id, PRED.hasTarget, resource_id)

        if org_id:
            self.rr.create_association(org_id, PRED.hasCommitment, commitment._id)

            self.event_pub.publish_event(event_type=OT.ResourceCommitmentCreatedEvent,
                                         origin=org_id, origin_type="Org", sub_type=resource_obj.type_,
                                         description="The resource has been committed by the Org",
                                         resource_id=resource_id, org_name=org_obj.name,
                                         commitment_id=commitment._id, commitment_type=commitment.commitment.type_)

        return commitment._id

    def release_commitment(self, commitment_id=""):
        """Release the commitment that was created for resources.
        The commitment is retained in DELETED state for historic records.
        """
        commitment_obj = self._validate_resource_id("commitment_id", commitment_id, RT.Commitment)

        self.rr.lcs_delete(commitment_id)

        self.event_pub.publish_event(event_type=OT.ResourceCommitmentReleasedEvent,
                                     origin=commitment_obj.provider, origin_type="Org", sub_type="",
                                     description="The resource has been uncommitted by the Org",
                                     resource_id=commitment_obj.commitment.resource_id,
                                     commitment_id=commitment_id, commitment_type=commitment_obj.commitment.type_)

    def find_commitments(self, org_id='', resource_id='', actor_id='', exclusive=False, include_expired=False):
        """Returns all commitments in specified org and optionally a given actor and/or optionally a given resource.
        If exclusive == True, only return exclusive commitments.
        """
        self._validate_resource_id("org_id", org_id, RT.Org, optional=True)
        self._validate_resource_id("actor_id", actor_id, RT.ActorIdentity, optional=True)
        if not org_id and not resource_id and not actor_id:
            raise BadRequest("Must restrict search for commitments")

        if resource_id:
            com_objs, _ = self.rr.find_subjects(RT.Commitment, PRED.hasTarget, resource_id, id_only=False)
            if actor_id:
                com_objs = [c for c in com_objs if c.consumer == actor_id]
            if org_id:
                com_objs = [c for c in com_objs if c.provider == org_id]
        elif actor_id:
            com_objs, _ = self.rr.find_objects(actor_id, PRED.hasCommitment, RT.Commitment, id_only=False)
            if org_id:
                com_objs = [c for c in com_objs if c.provider == org_id]
        else:
            com_objs, _ = self.rr.find_objects(org_id, PRED.hasCommitment, RT.Commitment, id_only=False)

        if exclusive:
            com_objs = [c for c in com_objs if c.commitment.type_ == OT.ResourceCommitment and c.commitment.exclusive]
        else:
            com_objs = [c for c in com_objs if c.commitment.type_ != OT.ResourceCommitment or (
                        c.commitment.type_ == OT.ResourceCommitment and not c.commitment.exclusive)]
        if not include_expired:
            cur_time = get_ion_ts_millis()
            com_objs = [c for c in com_objs if int(c.expiration) == 0 or cur_time < int(c.expiration)]

        return com_objs

    def is_resource_acquired(self, actor_id="", resource_id=""):
        """Returns True if the specified resource_id has been acquired. The actor_id
        is optional, as the operation can return True if the resource is acquired by
        any actor or specifically by the specified actor_id, otherwise False is returned.
        """
        return self._is_resource_acquired(actor_id, resource_id, exclusive=False)

    def is_resource_acquired_exclusively(self, actor_id="", resource_id=""):
        """Returns True if the specified resource_id has been acquired exclusively.
        The actor_id is optional, as the operation can return True if the resource
        is acquired exclusively by any actor or specifically by the specified
        actor_id, otherwise False is returned.
        """
        return self._is_resource_acquired(actor_id, resource_id, exclusive=True)

    def _is_resource_acquired(self, actor_id="", resource_id="", exclusive=False):
        if not resource_id:
            raise BadRequest("The resource_id argument is missing")

        try:
            com_objs = self.find_commitments(resource_id=resource_id, actor_id=actor_id, exclusive=exclusive)
            return bool(com_objs)

        except Exception as ex:
            log.exception("Error checking acquired status, actor_id=%s, resource_id=%s" % (actor_id, resource_id))

        return False

    def find_acquired_resources(self, org_id='', actor_id='', exclusive=False, include_expired=False):
        if not org_id and not actor_id:
            raise BadRequest("Must provide org_id or actor_id")

        com_objs = self.find_commitments(org_id=org_id, actor_id=actor_id,
                                         exclusive=exclusive, include_expired=include_expired)

        res_ids = {c.commitment.resource_id for c in com_objs if c.commitment.type_ == OT.ResourceCommitment}
        res_objs = self.rr.read_mult(list(res_ids))

        return res_objs

    # -------------------------------------------------------------------------
    # Org containers

    def is_in_org(self, container):
        container_list, _ = self.rr.find_subjects(RT.Org, PRED.hasResource, container)
        return bool(container_list)

    def find_org_containers(self, org_id=""):
        """Returns a list of containers associated with an Org. Will throw a not NotFound exception
        if the specified id does not exist.
        TODO: Fix inefficient implementation with index
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)

        # Containers in the Root ION Org are implied
        if org_obj.org_governance_name == self._get_root_org_name():
            container_list, _ = self.rr.find_resources(RT.CapabilityContainer)
            container_list[:] = [container for container in container_list if not self.is_in_org(container)]
        else:
            container_list, _ = self.rr.find_objects(org_obj, PRED.hasResource, RT.CapabilityContainer)

        return container_list

    def affiliate_org(self, org_id="", affiliate_org_id=""):
        """Creates an association between multiple Orgs as an affiliation
        so that they may coordinate activities between them.
        Throws a NotFound exception if neither id is found.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        affiliate_org_obj = self._validate_resource_id("affiliate_org_id", affiliate_org_id, RT.Org)

        aid = self.rr.create_association(org_obj, PRED.hasAffiliation, affiliate_org_obj)
        if not aid:
            return False

    def unaffiliate_org(self, org_id="", affiliate_org_id=""):
        """Removes an association between multiple Orgs as an affiliation.
        Throws a NotFound exception if neither id is found.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        affiliate_org_obj = self._validate_resource_id("affiliate_org_id", affiliate_org_id, RT.Org)

        aid = self.rr.get_association(org_obj, PRED.hasAffiliation, affiliate_org_obj)
        if not aid:
            raise NotFound("The affiliation association between the specified Orgs is not found")

        self.rr.delete_association(aid)

    # Local helper functions are below - do not remove them

    def is_enroll_negotiation_open(self, org_id, actor_id):
        try:
            neg_list = self.find_user_negotiations(actor_id, org_id, proposal_type=OT.EnrollmentProposal,
                                                   negotiation_status=NegotiationStatusEnum.OPEN )

            if neg_list:
                return True

        except Exception as ex:
            log.exception("org_id:%s and actor_id:%s" % (org_id, actor_id))

        return False
