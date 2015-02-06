#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisinger'

from pyon.public import CFG, IonObject, RT, PRED, OT, Inconsistent, NotFound, BadRequest, log, EventPublisher
from pyon.core.governance import MODERATOR_ROLE, MEMBER_ROLE, OPERATOR_ROLE
from pyon.core.governance.negotiation import Negotiation
from pyon.core.registry import issubtype
from pyon.ion.directory import Directory
from pyon.util.containers import is_basic_identifier, get_ion_ts, create_basic_identifier

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
        self.event_pub = EventPublisher(process=self)
        self.negotiation_handler = Negotiation(self, negotiation_rules, self.event_pub)

    def _get_root_org_name(self):
        if self.container is None or self.container.governance_controller is None:
            return CFG.get_safe('system.root_org', "ION")

        return self.container.governance_controller.system_root_org_name

    def _validate_parameters(self, **kwargs):
        parameter_objects = dict()
        org_id = None

        if 'org_id' in kwargs:
            org_id = kwargs['org_id']
            if not org_id:
                raise BadRequest("The org_id argument is missing")
            org = self.clients.resource_registry.read(org_id)
            if not org:
                raise NotFound("Org %s does not exist" % org_id)
            if org.type_ != RT.Org:
                raise BadRequest("Resource with given id is not an Org -- SPOOFING ALERT")
            parameter_objects['org'] = org

        if 'actor_id' in kwargs:
            actor_id = kwargs['actor_id']
            if not actor_id:
                raise BadRequest("The actor_id argument is missing")
            actor = self.clients.resource_registry.read(actor_id)
            if not actor:
                raise NotFound("Actor %s does not exist" % actor)
            if actor.type_ != RT.ActorIdentity:
                raise BadRequest("Resource with given id is not an ActorIdentity -- SPOOFING ALERT")
            parameter_objects['actor'] = actor

        if 'role_name' in kwargs:
            role_name = kwargs['role_name']
            if not role_name:
                raise BadRequest("The role_name argument is missing")
            if org_id is None:
                raise BadRequest("The org_id argument is missing")
            user_role = self._find_role(org_id, role_name)
            if user_role is None:
                raise BadRequest("The User Role '%s' does not exist for this Org" % role_name)
            if user_role.type_ != RT.UserRole:
                raise BadRequest("Resource with given id is not an UserRole -- SPOOFING ALERT")
            parameter_objects['user_role'] = user_role

        if 'resource_id' in kwargs:
            resource_id = kwargs['resource_id']
            if not resource_id:
                raise BadRequest("The resource_id argument is missing")
            resource = self.clients.resource_registry.read(resource_id)
            if not resource:
                raise NotFound("Resource %s does not exist" % resource_id)
            parameter_objects['resource'] = resource

        if 'negotiation_id' in kwargs:
            negotiation_id = kwargs['negotiation_id']
            if not negotiation_id:
                raise BadRequest("The negotiation_id argument is missing")
            negotiation = self.clients.resource_registry.read(negotiation_id)
            if not negotiation:
                raise NotFound("Negotiation %s does not exist" % negotiation_id)
            if negotiation.type_ != RT.Negotiation:
                raise BadRequest("Resource with given id is not a Negotiation -- SPOOFING ALERT")
            parameter_objects['negotiation'] = negotiation

        if 'affiliate_org_id' in kwargs:
            affiliate_org_id = kwargs['affiliate_org_id']
            if not affiliate_org_id:
                raise BadRequest("The affiliate_org_id argument is missing")
            affiliate_org = self.clients.resource_registry.read(affiliate_org_id)
            if not affiliate_org:
                raise NotFound("Org %s does not exist" % affiliate_org_id)
            if affiliate_org.type_ != RT.Org:
                raise BadRequest("Resource with given id is not an Org -- SPOOFING ALERT")
            parameter_objects['affiliate_org'] = affiliate_org

        return parameter_objects

    def create_org(self, org=None):
        """Creates an Org based on the provided object. The id string returned
        is the internal id by which Org will be identified in the data store.
        """
        if not org:
            raise BadRequest("The org argument is missing")
        if not org.name:
            raise BadRequest("Invalid Org name")

        # Only allow one root ION Org in the system
        res_list, _ = self.clients.resource_registry.find_resources(restype=RT.Org, name=org.name)
        if len(res_list) > 0:
            raise BadRequest('Org named %s already exists' % org.name)

        # If this governance identifier is not set, then set to a safe version of the org name.
        if not org.org_governance_name:
            org.org_governance_name = create_basic_identifier(org.name)
        if not is_basic_identifier(org.org_governance_name):
            raise BadRequest("The Org org_governance_name '%s' contains invalid characters" % org.org_governance_name)

        org_id, _ = self.clients.resource_registry.create(org)

        # Instantiate a Directory for this Org
        directory = Directory(orgname=org.name)

        # Instantiate initial set of User Roles for this Org
        self._create_org_roles(org_id)

        return org_id

    def _create_org_roles(self, org_id):
        # Instantiate initial set of User Roles for this Org
        manager_role = IonObject(RT.UserRole, name='MODERATOR', governance_name=MODERATOR_ROLE,
                                 description='Manage organization members, resources and roles')
        self.add_user_role(org_id, manager_role)

        operator_role = IonObject(RT.UserRole, name='OPERATOR', governance_name=OPERATOR_ROLE,
                                description='Modify and control organization resources')
        self.add_user_role(org_id, operator_role)

        member_role = IonObject(RT.UserRole, name='MEMBER', governance_name=MEMBER_ROLE,
                                description='Access organization resources')
        self.add_user_role(org_id, member_role)

    def update_org(self, org=None):
        """Updates the Org based on provided object.
        """
        if not org:
            raise BadRequest("The org argument is missing")
        if not org._id:
            raise BadRequest("Org argument has no id")
        old_org = self.clients.resource_registry.read(org._id)
        if old_org.type_ != RT.Org:
            raise BadRequest("Updated Org invalid id and type -- SPOOFING ALERT")
        if org.org_governance_name != old_org.org_governance_name:
            raise BadRequest("Cannot update Org org_governance_name")

        self.clients.resource_registry.update(org)

    def read_org(self, org_id=''):
        """Returns the Org object for the specified id.
        Throws exception if id does not match any persisted Org objects.
        """
        param_objects = self._validate_parameters(org_id=org_id)

        return param_objects['org']

    def delete_org(self, org_id=''):
        """Permanently deletes Org object with the specified
        id. Throws exception if id does not match any persisted Org object.
        """
        self._validate_parameters(org_id=org_id)

        self.clients.resource_registry.delete(org_id)

    def find_org(self, name=''):
        """Finds an Org object with the specified name. Defaults to the
        root ION object. Throws a NotFound exception if the object does not exist.
        """

        # Default to the root ION Org if not specified
        if not name:
            name = self._get_root_org_name()

        res_list, _ = self.clients.resource_registry.find_resources(restype=RT.Org, name=name)
        if not res_list:
            raise NotFound('The Org with name %s does not exist' % name)
        return res_list[0]

    # -------------------------------------------------------------------------
    # Org roles

    def add_user_role(self, org_id='', user_role=None):
        """Adds a UserRole to an Org. Will call Policy Management Service to actually
        create the role object that is passed in, if the role by the specified
        name does not exist. Throws exception if either id does not exist.
        """
        param_objects = self._validate_parameters(org_id=org_id)
        org = param_objects['org']

        if not user_role:
            raise BadRequest("The user_role parameter is missing")

        if self._find_role(org_id, user_role.governance_name) is not None:
            raise BadRequest("The user role '%s' is already associated with this Org" % user_role.governance_name)

        user_role.org_governance_name = org.org_governance_name
        user_role_id = self.clients.policy_management.create_role(user_role)

        self.clients.resource_registry.create_association(org, PRED.hasRole, user_role_id)

        return user_role_id

    def remove_user_role(self, org_id='', role_name='', force_removal=False):
        """Removes a UserRole from an Org. The UserRole will not be removed if there are
        users associated with the UserRole unless the force_removal parameter is set to True
        Throws exception if either id does not exist.
        """
        param_objects = self._validate_parameters(org_id=org_id, role_name=role_name)
        org = param_objects['org']
        user_role = param_objects['user_role']

        if not force_removal:
            alist, _ = self.clients.resource_registry.find_subjects(RT.ActorIdentity, PRED.hasRole, user_role)
            if len(alist) > 0:
                raise BadRequest('The User Role %s cannot be removed as there are %s users associated to it' %
                                 (user_role.name, str(len(alist))))

        # Finally remove the association to the Org
        aid = self.clients.resource_registry.get_association(org, PRED.hasRole, user_role)
        if not aid:
            raise NotFound("The role association between the specified Org (%s) and UserRole (%s) is not found" %
                           (org_id, user_role.name))

        self.clients.resource_registry.delete_association(aid)

        return True

    def find_org_role_by_name(self, org_id='', role_name=''):
        """Returns the User Role object for the specified name in the Org.
        Throws exception if name does not match any persisted User Role or the Org does not exist.
        objects.
        """
        param_objects = self._validate_parameters(org_id=org_id, role_name=role_name)
        user_role = param_objects['user_role']

        return user_role

    def _find_role(self, org_id='', role_name=''):
        if not org_id:
            raise BadRequest("The org_id parameter is missing")
        if not role_name:
            raise BadRequest("The governance_name parameter is missing")

        # Iterating (vs. query) is just fine, because the number of org roles is sufficiently small
        org_roles = self.find_org_roles(org_id)
        for role in org_roles:
            if role.governance_name == role_name:
                return role

        return None

    def find_org_roles(self, org_id=''):
        """Returns a list of roles available in an Org. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param org_id    str
        @retval user_role_list    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id)
        org = param_objects['org']

        role_list,_ = self.clients.resource_registry.find_objects(org, PRED.hasRole, RT.UserRole)

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
            raise BadRequest('The sap argument must be a valid ServiceAgreementProposal object')

        if sap.proposal_status == ProposalStatusEnum.INITIAL:
            neg_id = self.negotiation_handler.create_negotiation(sap)

            org = self.read_org(org_id=sap.provider)

            # Publish an event indicating an Negotiation has been initiated
            self.event_pub.publish_event(event_type=OT.OrgNegotiationInitiatedEvent, origin=org._id, origin_type='Org',
                description=sap.description, org_name=org.name, negotiation_id=neg_id, sub_type=sap.type_)

            # Synchronize the internal reference for later use
            sap.negotiation_id = neg_id

        # Get the most recent version of the Negotiation resource
        negotiation = self.negotiation_handler.read_negotiation(sap)

        # Update the Negotiation object with the latest SAP
        neg_id = self.negotiation_handler.update_negotiation(sap)

        # Get the most recent version of the Negotiation resource
        negotiation = self.clients.resource_registry.read(neg_id)

        # hardcoding some rules at the moment - could be replaced by a Rules Engine
        if sap.type_ == OT.AcquireResourceExclusiveProposal:

            if self.is_resource_acquired_exclusively(None, sap.resource_id):
                # Automatically accept the proposal for exclusive access if it is not already acquired exclusively
                provider_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.REJECTED, ProposalOriginatorEnum.PROVIDER)

                rejection_reason = "The resource has already been acquired exclusively"

                # Update the Negotiation object with the latest SAP
                neg_id = self.negotiation_handler.update_negotiation(provider_accept_sap, rejection_reason)

                # Get the most recent version of the Negotiation resource
                negotiation = self.clients.resource_registry.read(neg_id)

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
                    negotiation = self.clients.resource_registry.read(neg_id)

                else:

                    # Automatically accept the proposal for exclusive access if it is not already acquired exclusively
                    provider_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED,
                                                                              ProposalOriginatorEnum.PROVIDER)

                    # Update the Negotiation object with the latest SAP
                    neg_id = self.negotiation_handler.update_negotiation(provider_accept_sap)

                    # Get the most recent version of the Negotiation resource
                    negotiation = self.clients.resource_registry.read(neg_id)

        # Check to see if the rules allow for auto acceptance of the negotiations -
        # where the second party is assumed to accept if the
        # first party accepts.
        if negotiation_rules[sap.type_]['auto_accept']:

            # Automatically accept for the consumer if the Org Manager as provider accepts the proposal
            latest_sap = negotiation.proposals[-1]

            if latest_sap.proposal_status == ProposalStatusEnum.ACCEPTED and latest_sap.originator == ProposalOriginatorEnum.PROVIDER:
                consumer_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED)

                # Update the Negotiation object with the latest SAP
                neg_id = self.negotiation_handler.update_negotiation(consumer_accept_sap)

                # Get the most recent version of the Negotiation resource
                negotiation = self.clients.resource_registry.read(neg_id)

            elif latest_sap.proposal_status == ProposalStatusEnum.ACCEPTED and latest_sap.originator == ProposalOriginatorEnum.CONSUMER:
                provider_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)

                # Update the Negotiation object with the latest SAP
                neg_id = self.negotiation_handler.update_negotiation(provider_accept_sap)

                # Get the most recent version of the Negotiation resource
                negotiation = self.clients.resource_registry.read(neg_id)

        # Return the latest proposal
        return negotiation.proposals[-1]

    def find_org_negotiations(self, org_id='', proposal_type='', negotiation_status=-1):
        """Returns a list of negotiations for an Org. An optional proposal_type can be supplied
        or else all proposals will be returned. An optional negotiation_status can be supplied
        or else all proposals will be returned. Will throw a not NotFound exception
        if any of the specified ids do not exist.
        """
        param_objects = self._validate_parameters(org_id=org_id)

        neg_list, _ = self.clients.resource_registry.find_objects(org_id, PRED.hasNegotiation)

        if proposal_type:
            neg_list = [neg for neg in neg_list if neg.proposals[0].type_ == proposal_type]
        if negotiation_status > -1:
            neg_list = [neg for neg in neg_list if neg.negotiation_status == negotiation_status]

        return neg_list

    def find_org_closed_negotiations(self, org_id='', proposal_type=''):
        """Returns a list of closed negotiations for an Org - those which are Accepted or Rejected.
        Will throw a not NotFound exception if any of the specified ids do not exist.

        @param org_id    str
        @param proposal_type    str
        @retval negotiation    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id)

        neg_list, _ = self.clients.resource_registry.find_objects(org_id, PRED.hasNegotiation)

        if proposal_type:
            neg_list = [neg for neg in neg_list if neg.proposals[0].type_ == proposal_type]

        neg_list = [neg for neg in neg_list if neg.negotiation_status != NegotiationStatusEnum.OPEN]

        return neg_list

    def find_user_negotiations(self, actor_id='', org_id='', proposal_type='', negotiation_status=-1):
        """Returns a list of negotiations for a specified Actor. All negotiations for all Orgs will be returned
        unless an org_id is specified. An optional proposal_type can be supplied
        or else all proposals will be returned. An optional negotiation_status can be provided
        or else all proposals will be returned. Will throw a not NotFound exception
        if any of the specified ids do not exist.
        """
        param_objects = self._validate_parameters(actor_id=actor_id)
        actor = param_objects['actor']

        neg_list, _ = self.clients.resource_registry.find_objects(actor, PRED.hasNegotiation)

        if org_id:
            param_objects = self._validate_parameters(org_id=org_id)
            org = param_objects['org']

            neg_list = [neg for neg in neg_list if neg.proposals[0].provider == org_id]

        if proposal_type:
            neg_list = [neg for neg in neg_list if neg.proposals[0].type_ == proposal_type]
        if negotiation_status > -1:
            neg_list = [neg for neg in neg_list if neg.negotiation_status == negotiation_status]

        return neg_list

    # -------------------------------------------------------------------------
    # Member management

    def enroll_member(self, org_id='', actor_id=''):
        """Enrolls a specified actor into the specified Org so that they may find and negotiate to use resources
        of the Org. Membership in the ION Org is implied by registration with the system, so a membership
        association to the ION Org is not maintained. Throws a NotFound exception if neither id is found.
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id)
        org = param_objects['org']
        actor = param_objects['actor']

        if org.name == self._get_root_org_name():
            raise BadRequest("A request to enroll in the root ION Org is not allowed")

        aid = self.clients.resource_registry.create_association(org, PRED.hasMembership, actor)
        if not aid:
            return False

        member_role = self.find_org_role_by_name(org._id, MEMBER_ROLE)
        self._add_role_association(org, actor, member_role)

        self.event_pub.publish_event(event_type=OT.OrgMembershipGrantedEvent, origin=org._id, origin_type='Org',
            description='The member has enrolled in the Org', actor_id=actor._id, org_name=org.name)

        return True

    def cancel_member_enrollment(self, org_id='', actor_id=''):
        """Cancels the membership of a specific actor actor within the specified Org. Once canceled, the actor will no longer
        have access to the resource of that Org. Throws a NotFound exception if neither id is found.
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id)
        org = param_objects['org']
        actor = param_objects['actor']

        if org.name == self._get_root_org_name():
            raise BadRequest("A request to cancel enrollment in the root ION Org is not allowed")

        # First remove all associations to any roles
        role_list = self.find_org_roles_by_user(org_id, actor_id)
        for user_role in role_list:
            self._delete_role_association(org, actor, user_role)

        # Finally remove the association to the Org
        aid = self.clients.resource_registry.get_association(org, PRED.hasMembership, actor)
        if not aid:
            raise NotFound("The membership association between the specified actor and Org is not found")

        self.clients.resource_registry.delete_association(aid)

        self.event_pub.publish_event(event_type=OT.OrgMembershipCancelledEvent, origin=org._id, origin_type='Org',
            description='The member has cancelled enrollment in the Org', actor_id=actor._id, org_name=org.name )

        return True

    def is_registered(self, actor_id=''):
        """Returns True if the specified actor_id is registered with the ION system; otherwise False.
        """
        if not actor_id:
            raise BadRequest("The actor_id parameter is missing")

        try:
            user = self.clients.resource_registry.read(actor_id)
            return True
        except Exception as e:
            log.error('is_registered: %s for actor_id:%s' %  (e.message, actor_id))

        return False

    def is_enrolled(self, org_id='', actor_id=''):
        """Returns True if the specified actor_id is enrolled in the Org and False if not.
        Throws a NotFound exception if neither id is found.
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id)
        org = param_objects['org']
        actor = param_objects['actor']

        # Membership into the Root ION Org is implied as part of registration
        if org.name == self._get_root_org_name():
            return True

        try:
            aid = self.clients.resource_registry.get_association(org, PRED.hasMembership, actor)
        except NotFound:
            return False

        return True

    def find_enrolled_users(self, org_id=''):
        """Returns a list of users enrolled in an Org. Will throw a not NotFound exception
        if none of the specified ids do not exist.
        """
        param_objects = self._validate_parameters(org_id=org_id)
        org = param_objects['org']

        # Membership into the Root ION Org is implied as part of registration
        if org.name == self._get_root_org_name():
            user_list, _ = self.clients.resource_registry.find_resources(RT.ActorIdentity)
        else:
            user_list, _ = self.clients.resource_registry.find_objects(org, PRED.hasMembership, RT.ActorIdentity)

        return user_list

    def find_enrolled_orgs(self, actor_id=''):
        """Returns a list of Orgs that the actor is enrolled in. Will throw a not NotFound exception
        if none of the specified ids do not exist.
        """
        param_objects = self._validate_parameters(actor_id=actor_id)
        actor = param_objects['actor']

        org_list, _ = self.clients.resource_registry.find_subjects(RT.Org, PRED.hasMembership, actor)

        # Membership into the Root ION Org is implied as part of registration
        ion_org = self.find_org()
        org_list.append(ion_org)

        return org_list

    # -------------------------------------------------------------------------
    # Org role management

    def grant_role(self, org_id='', actor_id='', role_name='', scope=None):
        """Grants a defined role within an organization to a specific actor. A role of Member is
        automatically implied with successful enrollment. Will throw a not NotFound exception
        if none of the specified ids or role_name does not exist.
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id, role_name=role_name)
        org = param_objects['org']
        actor = param_objects['actor']
        user_role = param_objects['user_role']

        if not self.is_enrolled(org_id, actor_id):
            raise BadRequest("The actor is not a member of the specified Org (%s)" % org.name)

        ret = self._add_role_association(org, actor, user_role)

        return ret

    def _add_role_association(self, org, actor, user_role):
        aid = self.clients.resource_registry.create_association(actor, PRED.hasRole, user_role)
        if not aid:
            return False

        self.event_pub.publish_event(event_type=OT.UserRoleGrantedEvent, origin=org._id, origin_type='Org', sub_type=user_role.governance_name,
            description='Granted the %s role' % user_role.name,
            actor_id=actor._id, role_name=user_role.governance_name, org_name=org.name )

        return True

    def _delete_role_association(self, org, actor, user_role):
        aid = self.clients.resource_registry.get_association(actor, PRED.hasRole, user_role)
        if not aid:
            raise NotFound("The association between the specified ActorIdentity %s and User Role %s was not found" % (actor._id, user_role._id))

        self.clients.resource_registry.delete_association(aid)

        self.event_pub.publish_event(event_type=OT.UserRoleRevokedEvent, origin=org._id, origin_type='Org', sub_type=user_role.governance_name,
            description='Revoked the %s role' % user_role.name,
            actor_id=actor._id, role_name=user_role.governance_name, org_name=org.name )

        return True

    def revoke_role(self, org_id='', actor_id='', role_name=''):
        """Revokes a defined Role within an organization to a specific actor. Will throw a not NotFound exception
        if none of the specified ids or role_name does not exist.
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id, role_name=role_name)
        org = param_objects['org']
        actor = param_objects['actor']
        user_role = param_objects['user_role']

        ret = self._delete_role_association(org, actor, user_role)

        return ret

    def has_role(self, org_id='', actor_id='', role_name=''):
        """Returns True if the specified actor_id has the specified role_name in the Org and False if not.
        Throws a NotFound exception if neither id is found.
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id, role_name=role_name)
        org = param_objects['org']
        actor = param_objects['actor']

        role_list = self._find_org_roles_by_user(org, actor)

        for role in role_list:
            if role.governance_name == role_name:
                return True

        return False

    def _find_org_roles_by_user(self, org=None, actor=None):
        if org is None:
            raise BadRequest("The org parameter is missing")
        if actor is None:
            raise BadRequest("The actor parameter is missing")

        role_list, _ = self.clients.resource_registry.find_objects(actor, PRED.hasRole, RT.UserRole)

        # Iterate the list of roles associated with user and filter by the org_id.
        # TODO - replace this when better query is available
        ret_list = []
        for role in role_list:
            if role.org_governance_name == org.org_governance_name:
                ret_list.append(role)

        if org.org_governance_name == self.container.governance_controller.system_root_org_name:
            # Because a user is automatically enrolled with the ION Org then the membership role
            # is implied - so add it to the list
            member_role = self._find_role(org._id, MEMBER_ROLE)
            if member_role is None:
                raise Inconsistent('The %s User Role is not found.' % MEMBER_ROLE)

            ret_list.append(member_role)

        return ret_list

    def find_org_roles_by_user(self, org_id='', actor_id=''):
        """Returns a list of User Roles for a specific actor in an Org.
        Will throw a not NotFound exception if either of the IDs do not exist.
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id)
        org = param_objects['org']
        actor = param_objects['actor']

        role_list = self._find_org_roles_by_user(org, actor)

        return role_list

    def find_all_roles_by_user(self, actor_id=''):
        """Returns a list of all User Roles roles by Org associated with the specified actor.
        Will throw a not NotFound exception if either of the IDs do not exist.
        """
        param_objects = self._validate_parameters(actor_id=actor_id)
        actor = param_objects['actor']

        ret_val = dict()

        org_list = self.find_enrolled_orgs(actor_id)

        # Membership with the ION Root Org is implied
        for org in org_list:
            role_list = self._find_org_roles_by_user(org, actor)
            ret_val[org.org_governance_name] = role_list

        return ret_val

    # -------------------------------------------------------------------------
    # Resource sharing in Org

    def share_resource(self, org_id='', resource_id=''):
        """Share a specified resource with the specified Org. Once shared, the resource will be added to a directory
        of available resources within the Org. Throws a NotFound exception if neither id is found.
        """
        param_objects = self._validate_parameters(org_id=org_id, resource_id=resource_id)
        org = param_objects['org']
        resource = param_objects['resource']

        aid = self.clients.resource_registry.create_association(org, PRED.hasResource, resource)
        if not aid:
            return False

        self.event_pub.publish_event(event_type=OT.ResourceSharedEvent, origin=org._id, origin_type='Org', sub_type=resource.type_,
            description='The resource has been shared in the Org', resource_id=resource_id, org_name=org.name )

        return True

    def unshare_resource(self, org_id='', resource_id=''):
        """Unshare a specified resource with the specified Org. Once unshared, the resource will be removed from a directory
        of available resources within the Org. Throws a NotFound exception if neither id is found.
        """
        param_objects = self._validate_parameters(org_id=org_id, resource_id=resource_id)
        org = param_objects['org']
        resource = param_objects['resource']

        aid = self.clients.resource_registry.get_association(org, PRED.hasResource, resource)
        if not aid:
            raise NotFound("The shared association between the specified resource and Org is not found")

        self.clients.resource_registry.delete_association(aid)

        self.event_pub.publish_event(event_type=OT.ResourceUnsharedEvent, origin=org._id, origin_type='Org', sub_type=resource.type_,
            description='The resource has been unshared in the Org', resource_id=resource_id, org_name=org.name )

        return True

    def is_resource_shared(self, org_id='', resource_id=''):
        if not org_id:
            raise BadRequest("The org_id parameter is missing")
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        try:
            res_list, _ = self.clients.resource_registry.find_objects(org_id, PRED.hasResource)

            if res_list:
                for res in res_list:
                    if res._id == resource_id:
                        return True

        except Exception as e:
            log.error('is_resource_shared: %s for org_id:%s and resource_id:%s' % (e.message, org_id, resource_id))

        return False

    def acquire_resource(self, sap=None):
        """Creates a Commitment Resource for the specified resource for a specified user withing the specified Org as defined in the
        proposal. Once shared, the resource is committed to the user. Throws a NotFound exception if none of the ids are found.
        """
        if not sap:
            raise BadRequest("The sap parameter is missing")

        if sap.type_ == OT.AcquireResourceExclusiveProposal:
            exclusive = True
        else:
            exclusive = False

        commitment_id = self.create_resource_commitment(sap.provider, sap.consumer, sap.resource_id, exclusive, int(sap.expiration))

        # Create association between the Commitment and the Negotiation objects
        self.clients.resource_registry.create_association(sap.negotiation_id, PRED.hasContract, commitment_id)

        return commitment_id

    def create_resource_commitment(self, org_id='', actor_id='', resource_id='', exclusive=False, expiration=0):
        """Creates a Commitment Resource for the specified resource for a specified actor withing the specified Org. Once shared,
        the resource is committed to the actor. Throws a NotFound exception if none of the ids are found.
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id, resource_id=resource_id)
        org = param_objects['org']
        actor = param_objects['actor']
        resource = param_objects['resource']

        res_commitment = IonObject(OT.ResourceCommitment, resource_id=resource_id, exclusive=exclusive)

        commitment = IonObject(RT.Commitment, name='', provider=org_id, consumer=actor_id, commitment=res_commitment,
             description='Resource Commitment', expiration=str(expiration))

        commitment_id, commitment_rev = self.clients.resource_registry.create(commitment)
        commitment._id = commitment_id
        commitment._rev = commitment_rev

        # Creating associations to all related objects
        self.clients.resource_registry.create_association(org_id, PRED.hasCommitment, commitment_id)
        self.clients.resource_registry.create_association(actor_id, PRED.hasCommitment, commitment_id)
        self.clients.resource_registry.create_association(resource_id, PRED.hasCommitment, commitment_id)

        self.event_pub.publish_event(event_type=OT.ResourceCommitmentCreatedEvent, origin=org_id, origin_type='Org', sub_type=resource.type_,
            description='The resource has been committed by the Org', resource_id=resource_id, org_name=org.name,
            commitment_id=commitment._id, commitment_type=commitment.commitment.type_)

        return commitment_id

    def release_commitment(self, commitment_id=''):
        """Release the commitment that was created for resources. Throws a NotFound exception if none of the ids are found.
        """
        if not commitment_id:
            raise BadRequest("The commitment_id parameter is missing")

        self.clients.resource_registry.lcs_delete(commitment_id)

        commitment = self.clients.resource_registry.read(commitment_id)

        self.event_pub.publish_event(event_type=OT.ResourceCommitmentReleasedEvent, origin=commitment.provider, origin_type='Org', sub_type='',
            description='The resource has been uncommitted by the Org', resource_id=commitment.commitment.resource_id,
            commitment_id=commitment._id, commitment_type=commitment.commitment.type_ )

        return True

    def is_resource_acquired(self, actor_id='', resource_id=''):
        """Returns True if the specified resource_id has been acquired. The actor_id is optional, as the operation can
        return True if the resource is acquired by any actor or specifically by the specified actor_id, otherwise
        False is returned.
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        try:
            cur_time = int(get_ion_ts())
            commitments, _ = self.clients.resource_registry.find_objects(resource_id, PRED.hasCommitment, RT.Commitment)
            if commitments:
                for com in commitments:
                    # If the expiration is not 0 make sure it has not expired
                    if (actor_id is None or com.consumer == actor_id) and (
                                (int(com.expiration) == 0) or (int(com.expiration) > 0 and cur_time < int(com.expiration))):
                        return True

        except Exception as e:
            log.error('is_resource_acquired: %s for actor_id:%s and resource_id:%s' %  (e.message, actor_id, resource_id))

        return False

    def is_resource_acquired_exclusively(self, actor_id='', resource_id=''):
        """Returns True if the specified resource_id has been acquired exclusively. The actor_id is optional, as the operation can
        return True if the resource is acquired exclusively by any actor or specifically by the specified actor_id,
        otherwise False is returned.
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        try:
            cur_time = int(get_ion_ts())
            commitments,_ = self.clients.resource_registry.find_objects(resource_id,PRED.hasCommitment, RT.Commitment)
            if commitments:
                for com in commitments:
                    # If the expiration is not 0 make sure it has not expired
                    if (actor_id is None or actor_id == com.consumer) and com.commitment.exclusive and \
                       int(com.expiration) > 0 and cur_time < int(com.expiration):
                        return True

        except Exception as e:
            log.error('is_resource_acquired_exclusively: %s for actor_id:%s and resource_id:%s' % (e.message, actor_id, resource_id))

        return False

    def is_in_org(self, container):
        container_list, _ = self.clients.resource_registry.find_subjects(RT.Org, PRED.hasResource, container)
        if container_list:
            return True

        return False

    def find_org_containers(self, org_id=''):
        """Returns a list of containers associated with an Org. Will throw a not NotFound exception
        if the specified id does not exist.
        """
        param_objects = self._validate_parameters(org_id=org_id)
        org = param_objects['org']

        # Containers in the Root ION Org are implied
        if org.org_governance_name == self._get_root_org_name():
            container_list, _ = self.clients.resource_registry.find_resources(RT.CapabilityContainer)
            container_list[:] = [container for container in container_list if not self.is_in_org(container)]
        else:
            container_list, _ = self.clients.resource_registry.find_objects(org, PRED.hasResource, RT.CapabilityContainer)

        return container_list

    def affiliate_org(self, org_id='', affiliate_org_id=''):
        """Creates an association between multiple Orgs as an affiliation
        so that they may coordinate activities between them.
        Throws a NotFound exception if neither id is found.
        """
        param_objects = self._validate_parameters(org_id=org_id, affiliate_org_id=affiliate_org_id)
        org = param_objects['org']
        affiliate_org = param_objects['affiliate_org']

        aid = self.clients.resource_registry.create_association(org, PRED.affiliatedWith, affiliate_org)
        if not aid:
            return False

        return True

    def unaffiliate_org(self, org_id='', affiliate_org_id=''):
        """Removes an association between multiple Orgs as an affiliation.
        Throws a NotFound exception if neither id is found.
        """
        param_objects = self._validate_parameters(org_id=org_id, affiliate_org_id=affiliate_org_id)
        org = param_objects['org']
        affiliate_org = param_objects['affiliate_org']

        aid = self.clients.resource_registry.get_association(org, PRED.affiliatedWith, affiliate_org)
        if not aid:
            raise NotFound("The affiliation association between the specified Orgs is not found")

        self.clients.resource_registry.delete_association(aid)
        return True

    # Local helper functions are below - do not remove them

    def is_enroll_negotiation_open(self, org_id, actor_id):
        try:
            neg_list = self.find_user_negotiations(actor_id, org_id, proposal_type=OT.EnrollmentProposal, negotiation_status=NegotiationStatusEnum.OPEN )

            if neg_list:
                return True

        except Exception, e:
            log.error('is_enroll_negotiation_open: %s for org_id:%s and actor_id:%s' %  (e.message, org_id, actor_id))

        return False
