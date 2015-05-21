#!/usr/bin/env python

"""Define and manage policy and a repository to store and retrieve policy
and templates for policy definitions, aka attribute authority."""

__author__ = 'Stephen P. Henrie, Michael Meisinger'

from pyon.public import PRED, RT, OT, IonObject, NotFound, BadRequest, Inconsistent, log, EventPublisher, ResourceQuery
from pyon.util.containers import is_basic_identifier, create_basic_identifier

from interface.objects import PolicyTypeEnum
from interface.services.core.ipolicy_management_service import BasePolicyManagementService


class PolicyManagementService(BasePolicyManagementService):

    event_pub = None

    def on_start(self):
        self.event_pub = EventPublisher(process=self)

    # -------------------------------------------------------------------------
    # Policy management

    def create_resource_access_policy(self, resource_id='', policy_name='', description='', policy_rule='', ordinal=0):
        """Boilerplate operation for creating an access policy for a specific resource.
        """
        if not resource_id:
            raise BadRequest("The resource_id argument is missing")
        if not policy_name:
            raise BadRequest("The policy_name argument is missing")
        if not description:
            raise BadRequest("The description argument is missing")
        if not policy_rule:
            raise BadRequest("The policy_rule argument is missing")

        policy_obj = IonObject(RT.Policy, name=policy_name, description=description,
                               policy_type=PolicyTypeEnum.RESOURCE_ACCESS,
                               definition=policy_rule, ordinal=ordinal,
                               details=IonObject(OT.ResourceAccessPolicyDetails, resource_id=resource_id))
        policy_id = self.create_policy(policy_obj)
        self._add_resource_policy(resource_id, policy_id, publish_event=False)

        return policy_id

    def create_service_access_policy(self, service_name='', policy_name='', description='', policy_rule='', ordinal=0):
        """Boilerplate operation for creating an access policy for a specific service.
        """
        if not service_name:
            raise BadRequest("The service_name argument is missing")
        if not policy_name:
            raise BadRequest("The policy_name argument is missing")
        if not description:
            raise BadRequest("The description argument is missing")
        if not policy_rule:
            raise BadRequest("The policy_rule argument is missing")

        policy_obj = IonObject(RT.Policy, name=policy_name, description=description,
                               policy_type=PolicyTypeEnum.SERVICE_ACCESS,
                               definition=policy_rule, ordinal=ordinal,
                               details=IonObject(OT.ServiceAccessPolicyDetails, service_name=service_name))
        return self.create_policy(policy_obj)

    def create_common_service_access_policy(self, policy_name='', description='', policy_rule='', ordinal=0):
        """Boilerplate operation for creating a service access policy common to all services.
        """
        if not policy_name:
            raise BadRequest("The policy_name argument is missing")
        if not description:
            raise BadRequest("The description argument is missing")
        if not policy_rule:
            raise BadRequest("The policy_rule argument is missing")

        policy_obj = IonObject(RT.Policy, name=policy_name, description=description,
                               policy_type=PolicyTypeEnum.COMMON_SERVICE_ACCESS,
                               definition=policy_rule, ordinal=ordinal,)
        return self.create_policy(policy_obj)


    def add_process_operation_precondition_policy(self, process_id='', op='', policy_content=''):
        """Boilerplate operation for adding a precondition policy for a specific process operation.
        The precondition method must return a tuple (boolean, string).
        """
        if not process_id:
            raise BadRequest("The process_id argument is missing")
        if not op:
            raise BadRequest("The op argument is missing")
        if not policy_content:
            raise BadRequest("The policy_content argument is missing")

        policy_name = "Process_" + process_id + "_" + op + "_Precondition_Policies"
        policies, _ = self.clients.resource_registry.find_resources(restype=RT.Policy, name=policy_name)
        if policies:
            # Update existing policy by adding to list
            if len(policies) > 1:
                raise Inconsistent('There should only be one Policy object per process and operation')
            policy_obj = policies[0]
            if policy_obj.details.type_ != OT.ProcessOperationPreconditionPolicyDetails or policy_obj.details.op != op:
                raise Inconsistent('The Policy %s does not match the requested process operation %s: %s' % (
                        policy_obj.name, process_id, op))

            policy_obj.details.preconditions.append(policy_content)
            self.update_policy(policy_obj)

            return policy_obj._id

        else:
            policy_obj = IonObject(RT.Policy, name=policy_name,
                                   description='List of operation precondition policies for process ' + process_id,
                                   policy_type=PolicyTypeEnum.PROC_OP_PRECOND,
                                   details=IonObject(OT.ProcessOperationPreconditionPolicyDetails,
                                                     process_id=process_id, op=op, preconditions=[policy_content]))
            return self.create_policy(policy_obj)

    def add_service_operation_precondition_policy(self, service_name='', op='', policy_content=''):
        """Boilerplate operation for adding a precondition policy for a specific service operation.
        The precondition method must return a tuple (boolean, string).
        """
        if not service_name:
            raise BadRequest("The service_name argument is missing")
        if not op:
            raise BadRequest("The op argument is missing")
        if not policy_content:
            raise BadRequest("The policy_content argument is missing")

        policy_name = "Service_" + service_name + "_" + op + "_Precondition_Policies"
        policies, _ = self.clients.resource_registry.find_resources(restype=RT.Policy, name=policy_name)
        if policies:
            # Update existing policy by adding to list
            if len(policies) > 1:
                raise Inconsistent('There should only be one Policy object per service and operation')
            policy_obj = policies[0]
            if policy_obj.details.type_ != OT.ServiceOperationPreconditionPolicyDetails or policy_obj.details.op != op:
                raise Inconsistent('The Policy %s does not match the requested service operation %s: %s' % (
                        policy_obj.name, service_name, op))

            policy_obj.details.preconditions.append(policy_content)
            self.update_policy(policy_obj)

            return policy_obj._id

        else:
            policy_obj = IonObject(RT.Policy, name=policy_name,
                                   description='List of operation precondition policies for service ' + service_name,
                                   policy_type=PolicyTypeEnum.SERVICE_OP_PRECOND,
                                   details=IonObject(OT.ProcessOperationPreconditionPolicyDetails,
                                                     service_name=service_name, op=op, preconditions=[policy_content]))
            return self.create_policy(policy_obj)

    def create_policy(self, policy=None):
        """Persists the provided Policy object. Returns the policy id.
        """
        self._validate_resource_obj("policy", policy, RT.Policy, checks="noid,name")
        if not is_basic_identifier(policy.name):
            raise BadRequest("The policy name '%s' can only contain alphanumeric and underscore characters" % policy.name)

        try:
            # If there is a policy_rule field then try to add the policy name and description to the rule text
            if policy.definition:
                rule_tokens = dict(rule_id=policy.name, description=policy.description)
                policy.definition = policy.definition.format(**rule_tokens)

        except Exception as e:
            raise Inconsistent("Missing the elements in the policy rule to set the description: " + e.message)

        policy_id, _ = self.clients.resource_registry.create(policy)
        policy._id = policy_id

        log.debug('Policy created: ' + policy.name)
        self._publish_policy_event(policy)

        return policy_id

    def update_policy(self, policy=None):
        """Updates the provided Policy object.
        """
        self._validate_resource_obj("policy", policy, RT.Policy, checks="id,name")
        if not is_basic_identifier(policy.name):
            raise BadRequest("The policy name '%s' can only contain alphanumeric and underscore characters" % policy.name)

        self.clients.resource_registry.update(policy)

        self._publish_policy_event(policy)

    def read_policy(self, policy_id=''):
        """Returns the Policy object for the specified policy id.
        Throws exception if id does not match any persisted Policy
        objects.
        """
        policy_obj = self._validate_resource_id("policy_id", policy_id, RT.Policy)

        return policy_obj

    def delete_policy(self, policy_id=''):
        """For now, permanently deletes Policy object with the specified
        id. Throws exception if id does not match any persisted Policy.
        """
        policy_obj = self._validate_resource_id("policy_id", policy_id, RT.Policy)

        res_list = self._find_resources_for_policy(policy_id)
        for res in res_list:
            self._remove_resource_policy(res, policy_obj)

        self.clients.resource_registry.delete(policy_id)

        self._publish_policy_event(policy_obj, delete_policy=True)

    def enable_policy(self, policy_id=''):
        """Sets a flag to enable the use of the policy
        """
        policy_obj = self._validate_resource_id("policy_id", policy_id, RT.Policy)
        policy_obj.enabled = True
        self.update_policy(policy_obj)

    def disable_policy(self, policy_id=''):
        """Resets a flag to disable the use of the policy
        """
        policy_obj = self._validate_resource_id("policy_id", policy_id, RT.Policy)
        policy_obj.enabled = False
        self.update_policy(policy_obj)


    def add_resource_policy(self, resource_id='', policy_id=''):
        """Associates a policy to a specific resource
        """
        resource_obj, policy_obj = self._add_resource_policy(resource_id, policy_id)
        return True

    def _add_resource_policy(self, resource_id, policy_id, publish_event=True):
        """Removing a policy resource association and publish event for containers to update
        """
        resource_obj = self._validate_resource_id("resource_id", resource_id)
        policy_obj = self._validate_resource_id("policy_id", policy_id, RT.Policy)

        self.clients.resource_registry.create_association(resource_obj, PRED.hasPolicy, policy_obj)

        # Publish an event that the resource policy has changed
        if publish_event:
            self._publish_resource_policy_event(policy_obj, resource_obj)

        return resource_obj, policy_obj

    def remove_resource_policy(self, resource_id='', policy_id=''):
        """Removes an association for a policy to a specific resource
        """
        resource_obj = self._validate_resource_id("resource_id", resource_id)
        policy_obj = self._validate_resource_id("policy_id", policy_id, RT.Policy)

        self._remove_resource_policy(resource_obj, policy_obj)
        return True

    def _remove_resource_policy(self, resource, policy):
        aid = self.clients.resource_registry.get_association(resource, PRED.hasPolicy, policy)
        self.clients.resource_registry.delete_association(aid)

        # Publish an event that the resource policy has changed
        self._publish_resource_policy_event(policy, resource)


    def _publish_policy_event(self, policy, delete_policy=False):
        if policy.policy_type == PolicyTypeEnum.COMMON_SERVICE_ACCESS:
            self._publish_service_policy_event(policy, delete_policy)
        elif policy.policy_type == PolicyTypeEnum.SERVICE_ACCESS or policy.policy_type == PolicyTypeEnum.SERVICE_OP_PRECOND:
            self._publish_service_policy_event(policy, delete_policy)
        else:
            # Need to publish an event that a policy has changed for any associated resource
            res_list = self._find_resources_for_policy(policy._id)
            for res in res_list:
                self._publish_resource_policy_event(policy, res, delete_policy)


    def _publish_resource_policy_event(self, policy, resource, delete_policy=False):
        if self.event_pub:
            event_data = dict()
            event_data['origin_type'] = 'Resource_Policy'
            event_data['description'] = 'Updated Resource Policy'
            event_data['resource_id'] = resource._id
            event_data['resource_type'] = resource.type_
            event_data['resource_name'] = resource.name
            event_data['sub_type'] = 'DeletePolicy' if delete_policy else ''

            self.event_pub.publish_event(event_type='ResourcePolicyEvent', origin=policy._id, **event_data)

    def _publish_related_resource_policy_event(self, policy, resource_id, delete_policy=False):
        if self.event_pub:
            event_data = dict()
            event_data['origin_type'] = 'Resource_Policy'
            event_data['description'] = 'Updated Related Resource Policy'
            event_data['resource_id'] = resource_id
            event_data['sub_type'] = 'DeletePolicy' if delete_policy else ''

            self.event_pub.publish_event(event_type='RelatedResourcePolicyEvent', origin=policy._id, **event_data)

    def _publish_service_policy_event(self, policy, delete_policy=False):
        if self.event_pub:
            event_data = dict()
            event_data['origin_type'] = 'Service_Policy'
            event_data['description'] = 'Updated Service Policy'
            event_data['sub_type'] = 'DeletePolicy' if delete_policy else ''
            event_data['service_name'] = getattr(policy.details, 'service_name', "")

            if policy.policy_type == PolicyTypeEnum.SERVICE_OP_PRECOND:
                event_data['op'] = policy.details.op

            self.event_pub.publish_event(event_type='ServicePolicyEvent', origin=policy._id, **event_data)


    def find_resource_policies(self, resource_id=''):
        """Finds all policies associated with a specific resource
        """
        resource_obj = self._validate_resource_id("resource_id", resource_id)
        return self._find_resource_policies(resource_obj)

    def _find_resource_policies(self, resource, policy=None):
        policy_list, _ = self.clients.resource_registry.find_objects(resource, PRED.hasPolicy, policy)
        return policy_list

    def _find_resources_for_policy(self, policy_id=''):
        """Finds all resources associated with a specific policy
        """
        resource_list, _ = self.clients.resource_registry.find_subjects(None, PRED.hasPolicy, policy_id)
        return resource_list


    def get_active_resource_access_policy_rules(self, resource_id='', org_name=''):
        """Generates the set of all enabled access policies for the specified resource within
        the specified Org. If the org_name is not provided, then the root ION Org will be assumed.
        """
        # TODO - extend to handle Org specific service policies at some point.
        policy_list = []
        resource_obj = self._validate_resource_id("resource_id", resource_id)
        resource_id_list = self._get_related_resource_ids(resource_obj)
        if not resource_id_list:
            resource_id_list.append(resource_id)
        log.debug("Retrieving policies for resources: %s", resource_id_list)

        for res_id in resource_id_list:
            policy_set = self._find_resource_policies(res_id)

            for p in policy_set:
                if p.enabled and p.policy_type == PolicyTypeEnum.RESOURCE_ACCESS:
                    log.debug("Including policy: %s", p.name)
                    policy_list.append(p)

        policy_list.sort(key=lambda o: (o.ordinal, o.ts_created))

        return policy_list

    def _get_related_resource_ids(self, resource):
        """For given resource object, find related resources based on type"""
        resource_id_list = []
        # TODO - This could be following associations (parents, children, etc)
        return resource_id_list

    def get_active_service_access_policy_rules(self, service_name='', org_name=''):
        """Generates the set of all enabled access policies for the specified service within
        the specified Org. If the org_name is not provided, then the root ION Org will be assumed.
        """
        # TODO - extend to handle Org specific service policies at some point.
        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.Policy),
                      rq.filter_attribute("enabled", True))
        if service_name:
            rq.add_filter(rq.or_(rq.filter_attribute("policy_type", PolicyTypeEnum.COMMON_SERVICE_ACCESS),
                                 rq.and_(rq.filter_attribute("policy_type", [PolicyTypeEnum.SERVICE_ACCESS, PolicyTypeEnum.SERVICE_OP_PRECOND]),
                                         rq.filter_attribute("details.service_name", service_name))))
        else:
            rq.add_filter(rq.filter_attribute("policy_type", PolicyTypeEnum.COMMON_SERVICE_ACCESS))
        policy_list = self.clients.resource_registry.find_resources_ext(query=rq.get_query(), id_only=False)

        policy_list.sort(key=lambda o: (o.ordinal, o.ts_created))

        return policy_list

    def get_active_service_operation_preconditions(self, service_name='', op='', org_name=''):
        """Generates the set of all enabled precondition policies for the specified service operation
        within the specified Org. If the org_name is not provided, then the root ION Org will be assumed.
        """
        # TODO - extend to handle Org specific service policies at some point.
        if not service_name:
            raise BadRequest("The service_name argument is missing")

        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.Policy),
                      rq.filter_attribute("enabled", True),
                      rq.filter_attribute("policy_type", PolicyTypeEnum.SERVICE_OP_PRECOND),
                      rq.filter_attribute("details.service_name", service_name))
        if op:
            rq.add_filter(rq.filter_attribute("details.op", op))
        policy_list = self.clients.resource_registry.find_resources_ext(query=rq.get_query(), id_only=False)
        policy_list.sort(key=lambda o: (o.ordinal, o.ts_created))

        return policy_list

    def get_active_process_operation_preconditions(self, process_key='', op='', org_name=''):
        """Generates the set of all enabled precondition policies for the specified process operation
        within the specified Org. If the org_name is not provided, then the root ION Org will be assumed.
        """
        # TODO - extend to handle Org specific service policies at some point.
        if not process_key:
            raise BadRequest("The process_key argument is missing")

        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.Policy),
                      rq.filter_attribute("enabled", True),
                      rq.filter_attribute("policy_type", PolicyTypeEnum.PROC_OP_PRECOND),
                      rq.filter_attribute("details.process_key", process_key))
        if op:
            rq.add_filter(rq.filter_attribute("details.op", op))
        policy_list = self.clients.resource_registry.find_resources_ext(query=rq.get_query(), id_only=False)
        policy_list.sort(key=lambda o: (o.ordinal, o.ts_created))

        return policy_list

    # Local helper functions for testing policies - do not remove

    def func1_pass(self, msg, header):
        return True, ''

    def func2_deny(self,  msg, header):
        return False, 'Denied for no reason'

