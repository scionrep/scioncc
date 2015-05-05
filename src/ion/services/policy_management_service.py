#!/usr/bin/env python

"""Define and manage policy and a repository to store and retrieve policy
and templates for policy definitions, aka attribute authority."""

__author__ = 'Stephen P. Henrie, Michael Meisinger'

from pyon.public import PRED, RT, OT, IonObject, NotFound, BadRequest, Inconsistent, log, EventPublisher
from pyon.util.containers import is_basic_identifier, create_basic_identifier

from interface.services.core.ipolicy_management_service import BasePolicyManagementService


class PolicyManagementService(BasePolicyManagementService):

    event_pub = None

    def on_start(self):
        self.event_pub = EventPublisher(process=self)

    # -------------------------------------------------------------------------
    # Policy management

    def create_resource_access_policy(self, resource_id='', policy_name='', description='', policy_rule=''):
        """Boilerplate operation for creating an access policy for a specific resource.
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")
        if not policy_name:
            raise BadRequest("The policy_name parameter is missing")
        if not description:
            raise BadRequest("The description parameter is missing")
        if not policy_rule:
            raise BadRequest("The policy_rule parameter is missing")

        resource_policy_obj = IonObject(OT.ResourceAccessPolicy, policy_rule=policy_rule, resource_id=resource_id)
        policy_obj = IonObject(RT.Policy, name=policy_name, description=description, policy_type=resource_policy_obj)
        policy_id = self.create_policy(policy_obj)
        self._add_resource_policy(resource_id, policy_id, publish_event=False)

        return policy_id

    def create_service_access_policy(self, service_name='', policy_name='', description='', policy_rule=''):
        """Boilerplate operation for creating an access policy for a specific service.
        """
        if not service_name:
            raise BadRequest("The service_name parameter is missing")
        if not policy_name:
            raise BadRequest("The policy_name parameter is missing")
        if not description:
            raise BadRequest("The description parameter is missing")
        if not policy_rule:
            raise BadRequest("The policy_rule parameter is missing")

        service_policy_obj = IonObject(OT.ServiceAccessPolicy, policy_rule=policy_rule, service_name=service_name)
        policy_obj = IonObject(RT.Policy, name=policy_name, description=description, policy_type=service_policy_obj)

        return self.create_policy(policy_obj)

    def create_common_service_access_policy(self, policy_name='', description='', policy_rule=''):
        """Boilerplate operation for creating a service access policy common to all services.
        """
        if not policy_name:
            raise BadRequest("The policy_name parameter is missing")
        if not description:
            raise BadRequest("The description parameter is missing")
        if not policy_rule:
            raise BadRequest("The policy_rule parameter is missing")

        service_policy_obj = IonObject(OT.CommonServiceAccessPolicy, policy_rule=policy_rule)
        policy_obj = IonObject(RT.Policy, name=policy_name, description=description, policy_type=service_policy_obj)

        return self.create_policy(policy_obj)


    def add_process_operation_precondition_policy(self, process_name='', op='', policy_content=''):
        """Boilerplate operation for adding a precondition policy for a specific process operation;
        could be a service or agent. The precondition method must return a tuple (boolean, string).
        """
        if not process_name:
            raise BadRequest("The process_name parameter is missing")
        if not op:
            raise BadRequest("The op parameter is missing")
        if not policy_content:
            raise BadRequest("The policy_content parameter is missing")

        policy_name = process_name + "_" + op + "_Precondition_Policies"
        policies, _ = self.clients.resource_registry.find_resources(restype=RT.Policy, name=policy_name)
        if policies:
            # Update existing policy by adding to list
            if len(policies) > 1:
                raise Inconsistent('There should only be one Policy object per process_name operation')
            if policies[0].policy_type.op != op or policies[0].policy_type.type_ != OT.ProcessOperationPreconditionPolicy:
                raise Inconsistent('There Policy object %s does not match the requested process operation %s: %s' % (
                        policies[0].name, process_name, op ))

            policies[0].policy_type.preconditions.append(policy_content)
            self.update_policy(policies[0])

            return policies[0]._id

        else:
            # Create a new policy object
            op_policy_obj = IonObject(OT.ProcessOperationPreconditionPolicy,  process_name=process_name, op=op)
            op_policy_obj.preconditions.append(policy_content)
            policy_obj = IonObject(RT.Policy, name=policy_name, policy_type=op_policy_obj,
                                   description='List of operation precondition policies for ' + process_name)

            return self.create_policy(policy_obj)


    def create_policy(self, policy=None):
        """Persists the provided Policy object. Returns the policy id.
        """
        if not policy:
            raise BadRequest("The policy parameter is missing")
        if not is_basic_identifier(policy.name):
            raise BadRequest("The policy name '%s' can only contain alphanumeric and underscore characters" % policy.name)

        try:
            # If there is a policy_rule field then try to add the policy name and description to the rule text
            if hasattr(policy.policy_type, 'policy_rule'):
                rule_tokens = dict(rule_id=policy.name, description=policy.description)
                policy.policy_type.policy_rule = policy.policy_type.policy_rule.format(**rule_tokens)

        except Exception as e:
            raise Inconsistent("Missing the elements in the policy rule to set the description: " + e.message)

        policy_id, _ = self.clients.resource_registry.create(policy)
        policy._id = policy_id

        log.debug('Policy created: ' + policy.name)
        self._publish_policy_event(policy)

        return policy_id

    def update_policy(self, policy=None):
        """Updates the provided Policy object.  Throws NotFound exception if
        an existing version of Policy is not found.  Throws Conflict if
        the provided Policy object is not based on the latest persisted
        version of the object.
        """
        if not policy:
            raise BadRequest("The policy parameter is missing")

        if not is_basic_identifier(policy.name):
            raise BadRequest("The policy name '%s' can only contain alphanumeric and underscore characters" % policy.name)

        self.clients.resource_registry.update(policy)

        self._publish_policy_event(policy)

    def read_policy(self, policy_id=''):
        """Returns the Policy object for the specified policy id.
        Throws exception if id does not match any persisted Policy
        objects.
        """
        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)

        return policy

    def delete_policy(self, policy_id=''):
        """For now, permanently deletes Policy object with the specified
        id. Throws exception if id does not match any persisted Policy.
        """
        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)

        res_list = self._find_resources_for_policy(policy_id)
        for res in res_list:
            self._remove_resource_policy(res, policy)

        self.clients.resource_registry.delete(policy_id)

        self._publish_policy_event(policy, delete_policy=True)

    def enable_policy(self, policy_id=''):
        """Sets a flag to enable the use of the policy
        """
        policy = self.read_policy(policy_id)
        policy.enabled = True
        self.update_policy(policy)

    def disable_policy(self, policy_id=''):
        """Resets a flag to disable the use of the policy
        """
        policy = self.read_policy(policy_id)
        policy.enabled = False
        self.update_policy(policy)


    def add_resource_policy(self, resource_id='', policy_id=''):
        """Associates a policy to a specific resource
        """
        resource, policy = self._add_resource_policy(resource_id, policy_id)
        return True

    def _add_resource_policy(self, resource_id, policy_id, publish_event=True):
        """Removing a policy resource association and publish event for containers to update
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)

        aid = self.clients.resource_registry.create_association(resource, PRED.hasPolicy, policy)

        # Publish an event that the resource policy has changed
        if publish_event:
            self._publish_resource_policy_event(policy, resource)

        return resource, policy

    def remove_resource_policy(self, resource_id='', policy_id=''):
        """Removes an association for a policy to a specific resource
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)

        self._remove_resource_policy(resource, policy)

        return True

    def _remove_resource_policy(self, resource, policy):
        aid = self.clients.resource_registry.get_association(resource, PRED.hasPolicy, policy)
        if not aid:
            raise NotFound("The association between the specified Resource %s and Policy %s was not found" % (resource._id, policy._id))

        self.clients.resource_registry.delete_association(aid)

        # Publish an event that the resource policy has changed
        self._publish_resource_policy_event(policy, resource)


    def _publish_policy_event(self, policy, delete_policy=False):

        if policy.policy_type.type_ == OT.CommonServiceAccessPolicy:
            self._publish_service_policy_event(policy, delete_policy)
        elif policy.policy_type.type_ == OT.ServiceAccessPolicy or policy.policy_type.type_ == OT.ProcessOperationPreconditionPolicy:
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

            if policy.policy_type.type_ == OT.ProcessOperationPreconditionPolicy:
                event_data['op'] =  policy.policy_type.op

            if hasattr(policy.policy_type, 'service_name'):
                event_data['service_name'] = policy.policy_type.service_name
            elif hasattr(policy.policy_type, 'process_name'):
                event_data['service_name'] = policy.policy_type.process_name
            else:
                event_data['service_name'] = ''

            self.event_pub.publish_event(event_type='ServicePolicyEvent', origin=policy._id, **event_data)


    def find_resource_policies(self, resource_id=''):
        """Finds all policies associated with a specific resource
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        return self._find_resource_policies(resource)

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
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        # TODO - extend to handle Org specific service policies at some point.

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        rules = ""

        resource_id_list = self._get_related_resource_ids(resource)
        if not resource_id_list:
            resource_id_list.append(resource_id)

        log.debug("Retrieving policies for resources: %s", resource_id_list)

        for res_id in resource_id_list:
            policy_set = self._find_resource_policies(res_id)

            for p in policy_set:
                if p.enabled and p.policy_type.type_ == OT.ResourceAccessPolicy:
                    log.debug("Including policy: %s", p.name)
                    rules += p.policy_type.policy_rule

        return rules

    def _get_related_resource_ids(self, resource):
        """For given resource object, find related resources based on type"""
        resource_id_list = []
        # TODO - This could be following associations
        return resource_id_list


    def get_active_service_access_policy_rules(self, service_name='', org_name=''):
        """Generates the set of all enabled access policies for the specified service within
        the specified Org. If the org_name is not provided, then the root ION Org will be assumed.
        """
        # TODO - extend to handle Org specific service policies at some point.

        rules = ""
        if not service_name:
            policy_set, _ = self.clients.resource_registry.find_resources_ext(restype=RT.Policy,
                                                                              nested_type=OT.CommonServiceAccessPolicy)
            for p in sorted(policy_set, key=lambda o: o.ts_created):
                if p.enabled:
                    rules += p.policy_type.policy_rule

        else:
            policy_set, _ = self.clients.resource_registry.find_resources_ext(restype=RT.Policy,
                                                                              nested_type=OT.ServiceAccessPolicy)
            for p in sorted(policy_set, key=lambda o: o.ts_created):
                if p.enabled and p.policy_type.service_name == service_name:
                    rules += p.policy_type.policy_rule

        return rules

    def get_active_process_operation_preconditions(self, process_name='', op='', org_name=''):
        """Generates the set of all enabled precondition policies for the specified process operation
        within the specified Org; could be a service or resource agent. If the org_name is not provided,
        then the root ION Org will be assumed.
        """
        if not process_name:
            raise BadRequest("The process_name parameter is missing")

        #TODO - extend to handle Org specific service policies at some point.

        preconditions = list()
        policy_set, _ = self.clients.resource_registry.find_resources_ext(restype=RT.Policy,
                                                                          nested_type=OT.ProcessOperationPreconditionPolicy)
        for p in sorted(policy_set, key=lambda o: o.ts_created):
            if op:
                if p.enabled and p.policy_type.process_name == process_name and p.policy_type.op == op:
                    preconditions.append(p.policy_type)
            else:
                if p.enabled and p.policy_type.process_name == process_name:
                    preconditions.append(p.policy_type)

        return preconditions

    # Local helper functions for testing policies - do not remove

    def func1_pass(self, msg, header):
        return True, ''

    def func2_deny(self,  msg, header):
        return False, 'Denied for no reason'

