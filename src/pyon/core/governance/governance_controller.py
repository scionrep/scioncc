#!/usr/bin/env python

"""Essential container component managing policy enforcement and access to container processes"""

__author__ = 'Stephen P. Henrie, Michael Meisinger'

import types

from pyon.core import PROCTYPE_AGENT, PROCTYPE_SERVICE
from pyon.core.bootstrap import CFG, get_service_registry, is_testing
from pyon.core.exception import NotFound, Unauthorized
from pyon.core.governance import get_system_actor_header, get_system_actor
from pyon.core.governance.governance_dispatcher import GovernanceDispatcher
from pyon.core.governance.policy.policy_decision import PolicyDecisionPointManager
from pyon.core.interceptor.interceptor import Invocation
from pyon.ion.event import EventSubscriber
from pyon.ion.resource import RT, OT
from pyon.util.containers import get_ion_ts, named_any
from pyon.util.log import log

from interface.objects import PolicyTypeEnum
from interface.services.core.ipolicy_management_service import PolicyManagementServiceProcessClient
from interface.services.core.iresource_registry_service import ResourceRegistryServiceProcessClient


class GovernanceController(object):
    """
    This is a singleton object which handles governance functionality in the container.
    Registers event callback for PolicyEvent to update local policies on change.
    """

    def __init__(self, container):
        log.debug('GovernanceController.__init__()')
        self.container = container
        self.enabled = False
        self.interceptor_by_name_dict = {}
        self.interceptor_order = []
        self.policy_decision_point_manager = None
        self.governance_dispatcher = None

        # Holds a list per service operation of policy methods to be called before operation is invoked
        self._service_op_preconditions = {}
        # Holds a list per process operation of policy methods to be called before operation is invoked
        self._process_op_preconditions = {}

        self._is_container_org_boundary = False
        self._container_org_name = None
        self._container_org_id = None

        # For policy debugging purposes. Keeps a list of most recent policy updates for later readout
        self._policy_update_log = []
        self._policy_snapshot = None

    def start(self):
        log.debug("GovernanceController starting ...")
        self._CFG = CFG

        self.enabled = CFG.get_safe('interceptor.interceptors.governance.config.enabled', False)
        if not self.enabled:
            log.warn("GovernanceInterceptor disabled by configuration")
        self.policy_event_subscriber = None

        # Containers default to not Org Boundary and ION Root Org
        self._is_container_org_boundary = CFG.get_safe('container.org_boundary', False)
        self._container_org_name = CFG.get_safe('container.org_name', CFG.get_safe('system.root_org', 'ION'))
        self._container_org_id = None
        self._system_root_org_name = CFG.get_safe('system.root_org', 'ION')

        self._is_root_org_container = (self._container_org_name == self._system_root_org_name)

        self.system_actor_id = None
        self.system_actor_user_header = None

        self.rr_client = ResourceRegistryServiceProcessClient(process=self.container)
        self.policy_client = PolicyManagementServiceProcessClient(process=self.container)

        if self.enabled:
            config = CFG.get_safe('interceptor.interceptors.governance.config')
            self.initialize_from_config(config)

            self.policy_event_subscriber = EventSubscriber(event_type=OT.PolicyEvent, callback=self.policy_event_callback)
            self.policy_event_subscriber.start()

            self._policy_snapshot = self._get_policy_snapshot()
            self._log_policy_update("start_governance_ctrl", message="Container start")

    def initialize_from_config(self, config):
        self.governance_dispatcher = GovernanceDispatcher()
        self.policy_decision_point_manager = PolicyDecisionPointManager(self)

        self.interceptor_order = config.get('interceptor_order', None) or []
        gov_ints = config.get('governance_interceptors', None) or {}
        for name in gov_ints:
            interceptor_def = gov_ints[name]
            classobj = named_any(interceptor_def["class"])
            classinst = classobj()
            self.interceptor_by_name_dict[name] = classinst

    def _ensure_system_actor(self):
        """Make sure we have a handle for the system actor"""
        if self.system_actor_id is None:
            system_actor = get_system_actor()
            if system_actor is not None:
                self.system_actor_id = system_actor._id
                self.system_actor_user_header = get_system_actor_header(system_actor)

    def stop(self):
        log.debug("GovernanceController stopping ...")

        if self.policy_event_subscriber is not None:
            self.policy_event_subscriber.stop()

    @property
    def is_container_org_boundary(self):
        return self._is_container_org_boundary

    @property
    def container_org_name(self):
        return self._container_org_name

    @property
    def system_root_org_name(self):
        return self._system_root_org_name

    @property
    def is_root_org_container(self):
        return self._is_root_org_container

    @property
    def CFG(self):
        return self._CFG


    @property
    def rr(self):
        """Returns the active resource registry instance if available in the container or service client.
        """
        if self.container.has_capability('RESOURCE_REGISTRY'):
            return self.container.resource_registry
        return self.rr_client


    def get_container_org_boundary_id(self):
        """Returns the permanent org identifier configured for this container
        """
        if not self._is_container_org_boundary:
            return None

        if self._container_org_id is None:
            org_ids, _ = self.rr.find_resources_ext(restype=RT.Org, attr_name="org_governance_name",
                                                    attr_value=self._container_org_name, id_only=True)
            if org_ids:
                self._container_org_id = org_ids[0]

        return self._container_org_id

    # --- Interceptor management

    def process_incoming_message(self, invocation):
        """The GovernanceController hook into the incoming message interceptor stack
        """
        self.process_message(invocation, self.interceptor_order, Invocation.PATH_IN)
        return self.governance_dispatcher.handle_incoming_message(invocation)

    def process_outgoing_message(self, invocation):
        """The GovernanceController hook into the outgoing message interceptor stack
        """
        self.process_message(invocation, reversed(self.interceptor_order), Invocation.PATH_OUT)
        return self.governance_dispatcher.handle_outgoing_message(invocation)

    def process_message(self, invocation, interceptor_list, method):
        """
        The GovernanceController hook to iterate over the interceptors to call each one and
        evaluate the annotations to see what actions should be done.
        """
        for int_name in interceptor_list:
            interceptor_obj = self.interceptor_by_name_dict[int_name]
            interceptor_func = getattr(interceptor_obj, method)
            # Invoke interceptor function for designated path
            interceptor_func(invocation)

            # Stop processing message if an issue with the message was found by an interceptor
            if invocation.message_annotations.get(GovernanceDispatcher.CONVERSATION__STATUS_ANNOTATION, None) == GovernanceDispatcher.STATUS_REJECT or \
               invocation.message_annotations.get(GovernanceDispatcher.POLICY__STATUS_ANNOTATION, None) == GovernanceDispatcher.STATUS_REJECT:
                break

        return invocation

    # --- Container policy management

    def policy_event_callback(self, policy_event, *args, **kwargs):
        """Generic policy event handler for dispatching policy related events.
        """
        self._ensure_system_actor()

        log.info("Received policy event: %s", policy_event)

        if policy_event.type_ == OT.ResourcePolicyEvent:
            self.resource_policy_event_callback(policy_event, *args, **kwargs)
        elif policy_event.type_ == OT.RelatedResourcePolicyEvent:
            self.resource_policy_event_callback(policy_event, *args, **kwargs)
        elif policy_event.type_ == OT.ServicePolicyEvent:
            self.service_policy_event_callback(policy_event, *args, **kwargs)

        self._log_policy_update("policy_event_callback",
                                message="Event processed",
                                event=policy_event)

    def service_policy_event_callback(self, service_policy_event, *args, **kwargs):
        """The ServicePolicyEvent handler
        """
        log.debug('Service policy event: %s', str(service_policy_event.__dict__))

        policy_id = service_policy_event.origin
        service_name = service_policy_event.service_name
        service_op = service_policy_event.op
        delete_policy = True if service_policy_event.sub_type == 'DeletePolicy' else False

        if service_name:
            if self.container.proc_manager.is_local_service_process(service_name):
                self.update_service_access_policy(service_name, service_op, delete_policy=delete_policy)
            elif self.container.proc_manager.is_local_agent_process(service_name):
                self.update_service_access_policy(service_name, service_op, delete_policy=delete_policy)

        else:
            self.update_common_service_access_policy()

    def resource_policy_event_callback(self, resource_policy_event, *args, **kwargs):
        """The ResourcePolicyEvent handler
        """
        log.debug('Resource policy event: %s', str(resource_policy_event.__dict__))

        policy_id = resource_policy_event.origin
        resource_id = resource_policy_event.resource_id
        delete_policy = True if resource_policy_event.sub_type == 'DeletePolicy' else False

        self.update_resource_access_policy(resource_id, delete_policy)

    def reset_policy_cache(self):
        """Empty and reload the container's policy caches.
        Reload by getting policy for each of the container's processes and common policy.
        """
        log.info('Resetting policy cache')

        # First remove all cached polices and operation precondition functions
        self._clear_container_policy_caches()

        # Load the common service access policies since they are shared across services
        self.update_common_service_access_policy()

        # Iterate over the processes running in the container and reload their policies
        proc_list = self.container.proc_manager.list_local_processes()
        for proc in proc_list:
            self.update_process_policies(proc, force_update=False)

        self._log_policy_update("reset_policy_cache")

    def _clear_container_policy_caches(self):
        self.policy_decision_point_manager.clear_policy_cache()
        self.unregister_all_process_policy_preconditions()

    def update_process_policies(self, process_instance, safe_mode=False, force_update=True):
        """
        Load any applicable process policies for a container process.
        To be called by when spawning a new process, or when policy is reset.
        @param process_instance  The ION process for which to load policy
        @param safe_mode  If True, will not attempt to read policy if Policy MS not available
        """
        # NOTE: During restart, we rely on the bootstrap code to remove registration of Policy MS
        if safe_mode and not self._is_policy_management_service_available():
            if not is_testing() and (process_instance.name not in {"resource_registry", "system_management",
                    "directory", "identity_management"} and process_instance._proc_name != "event_persister"):
                # We are in the early phases of bootstrapping
                log.warn("update_process_policies(%s) - No update. Policy MS not available", process_instance._proc_name)

            self._log_policy_update("update_process_policies",
                                    message="No update. Policy MS not available",
                                    process=process_instance)
            return

        self._ensure_system_actor()

        if process_instance._proc_type == PROCTYPE_SERVICE:
            self.update_service_access_policy(process_instance._proc_listen_name, force_update=force_update)

        elif process_instance._proc_type == PROCTYPE_AGENT:
            # Load any existing policies for this agent with type or name
            if process_instance.resource_type is None:
                self.update_service_access_policy(process_instance.name, force_update=force_update)
            else:
                self.update_service_access_policy(process_instance.resource_type, force_update=force_update)

            if process_instance.resource_id:
                # Load any existing policies for this resource
                self.update_resource_access_policy(process_instance.resource_id, force_update=force_update)

        self._log_policy_update("update_process_policies",
                                message="Checked",
                                process=process_instance)

    def update_common_service_access_policy(self, delete_policy=False):
        """Update policy common to all services"""
        if self.policy_decision_point_manager is None:
            return

        try:
            rules = self.policy_client.get_active_service_access_policy_rules(
                    service_name='', org_name=self._container_org_name,
                    headers=self.system_actor_user_header)
            self.policy_decision_point_manager.set_common_service_policy_rules(rules)

        except Exception as e:
            # If the resource does not exist, just ignore it - but log a warning.
            log.warn("There was an error applying access policy: %s" % e.message)

    def update_service_access_policy(self, service_name, service_op='', delete_policy=False, force_update=True):
        """Update policy for a service"""
        if self.policy_decision_point_manager is None:
            return
        if not force_update and not service_op and self.policy_decision_point_manager.has_service_policy(service_name):
            log.info("Skipping update of service %s policy - already cached", service_name)
            return

        try:
            if service_op:
                policies = self.policy_client.get_active_service_operation_preconditions(
                        service_name=service_name, op=service_op, org_name=self._container_org_name,
                        headers=self.system_actor_user_header)
            else:
                policies = self.policy_client.get_active_service_access_policy_rules(
                        service_name=service_name, org_name=self._container_org_name,
                        headers=self.system_actor_user_header)

            # First update any access policy rules
            svc_access_policy = [p for p in policies
                                 if p.policy_type in (PolicyTypeEnum.COMMON_SERVICE_ACCESS, PolicyTypeEnum.SERVICE_ACCESS)]
            self.policy_decision_point_manager.set_service_policy_rules(service_name, svc_access_policy)

            # Next update any precondition policies
            svc_preconditions = [p for p in policies
                                 if p.policy_type == PolicyTypeEnum.SERVICE_OP_PRECOND]

            # There can be several local processes for a service
            procs = self.container.proc_manager.get_local_service_processes(service_name)
            for proc in procs:
                if svc_preconditions:
                    for op_pre_policy in svc_preconditions:
                        for pre_check in op_pre_policy.preconditions:
                            self.unregister_process_operation_precondition(proc, op_pre_policy.op, pre_check)
                            if not delete_policy:
                                self.register_process_operation_precondition(proc, op_pre_policy.op, pre_check)
                else:
                    # Unregister all, just in case
                    self.unregister_all_process_operation_precondition(proc, service_op)

        except Exception as ex:
            # If the resource does not exist, just ignore it - but log a warning.
            log.warn("Error applying access policy for service %s: %s" % (service_name, ex.message))

    def update_resource_access_policy(self, resource_id, delete_policy=False, force_update=True):
        """Update policy for a resource (such as a device fronted by an agent process)"""
        if self.policy_decision_point_manager is None:
            return
        if self.policy_decision_point_manager.has_resource_policy(resource_id):
            return

        try:
            policy_list = self.policy_client.get_active_resource_access_policy_rules(
                    resource_id, headers=self.system_actor_user_header)
            self.policy_decision_point_manager.set_resource_policy_rules(resource_id, policy_list)

        except Exception as e:
            # If the resource does not exist, just ignore it - but log a warning.
            log.warn("There was an error applying access policy for resource %s: %s", resource_id, e.message)

    def update_process_access_policy(self, process_key, service_op='', delete_policy=False, force_update=True):
        pass
        # procs, op_preconditions = [], None
        # try:
        #     # There can be several local processes for a service all with different names
        #     procs = self.container.proc_manager.get_local_service_processes(service_name)
        #     if procs:
        #         op_preconditions = self.policy_client.get_active_service_operation_preconditions(
        #                 service_name=service_name, op=service_op, org_name=self._container_org_name,
        #                 headers=self.system_actor_user_header)
        # except Exception as ex:
        #     # If the resource does not exist, just ignore it - but log a warning.
        #     log.warn("Error applying precondition access policy for service %s: %s" % (service_name, ex.message))
        #
        # for proc in procs:
        #     try:
        #         if op_preconditions:
        #             for op in op_preconditions:
        #                 for pre in op.preconditions:
        #                     self.unregister_process_operation_precondition(proc, op.op, pre)
        #                     if not delete_policy:
        #                         self.register_process_operation_precondition(proc, op.op, pre)
        #         else:
        #             # Unregister all, just in case
        #             self.unregister_all_process_operation_precondition(proc, service_op)
        #     except Exception as ex:
        #         # If the resource does not exist, just ignore it - but log a warning.
        #         log.warn("Error applying precondition access policy for process %s of service %s: %s" % (proc, service_name, ex.message))


    def get_active_policies(self):
        container_policies = dict()
        container_policies['common_service_access'] = self.policy_decision_point_manager.load_common_service_pdp
        container_policies['service_access'] = {k: v for (k, v) in self.policy_decision_point_manager.service_policy_decision_point.iteritems() if v is not None}
        container_policies['resource_access'] = {k: v for (k, v) in self.policy_decision_point_manager.resource_policy_decision_point.iteritems() if v is not None}
        container_policies['service_operation'] = dict(self._service_op_preconditions)

        #log.info(container_policies)
        return container_policies

    def _is_policy_management_service_available(self):
        """
        Method to verify if the Policy Management Service is running in the system. If the container cannot connect to
        the RR then assume it is remote container so do not try to access Policy Management Service
        """
        policy_service = get_service_registry().is_service_available('policy_management', True)
        if policy_service:
            return True
        return False

    def _get_policy_snapshot(self):
        """Debugging helper that snapshot copies the current container's policy state.
        """
        policy_snap = {}
        policy_snap["snap_ts"] = get_ion_ts()

        policies = self.get_active_policies()
        common_list = []
        policy_snap["common_pdp"] = common_list
        for rule in policies.get("common_service_access", {}).policy.rules:
            rule_dict = dict(id=rule.id, description=rule.description, effect=rule.effect.value)
            common_list.append(rule_dict)

        service_dict = {}
        policy_snap["service_pdp"] = service_dict
        for (svc_name, sp) in policies.get("service_access", {}).iteritems():
            for rule in sp.policy.rules:
                if svc_name not in service_dict:
                    service_dict[svc_name] = []
                rule_dict = dict(id=rule.id, description=rule.description, effect=rule.effect.value)
                service_dict[svc_name].append(rule_dict)

        service_pre_dict = {}
        policy_snap["service_precondition"] = service_pre_dict
        for (svc_name, sp) in policies.get("service_operation", {}).iteritems():
            for op, f in sp.iteritems():
                if svc_name not in service_pre_dict:
                    service_pre_dict[svc_name] = []
                service_pre_dict[svc_name].append(op)

        resource_dict = {}
        policy_snap["resource_pdp"] = resource_dict
        for (res_name, sp) in policies.get("resource_access", {}).iteritems():
            for rule in sp.policy.rules:
                if res_name not in service_dict:
                    resource_dict[res_name] = []
                rule_dict = dict(id=rule.id, description=rule.description, effect=rule.effect.value)
                resource_dict[res_name].append(rule_dict)

        return policy_snap

    def _log_policy_update(self, update_type=None, message=None, event=None, process=None):
        policy_update_dict = {}
        policy_update_dict["update_ts"] = get_ion_ts()
        policy_update_dict["update_type"] = update_type or ""
        policy_update_dict["message"] = message or ""
        if event:
            policy_update_dict["event._id"] = getattr(event, "_id", "")
            policy_update_dict["event.ts_created"] = getattr(event, "ts_created", "")
            policy_update_dict["event.type_"] = getattr(event, "type_", "")
            policy_update_dict["event.sub_type"] = getattr(event, "sub_type", "")
        if process:
            policy_update_dict["proc._proc_name"] = getattr(process, "_proc_name", "")
            policy_update_dict["proc.name"] = getattr(process, "name", "")
            policy_update_dict["proc._proc_listen_name"] = getattr(process, "_proc_listen_name", "")
            policy_update_dict["proc.resource_type"] = getattr(process, "resource_type", "")
            policy_update_dict["proc.resource_id"] = getattr(process, "resource_id", "")
        any_change = False   # Change can only be detected in number/names of policy not content
        snapshot = self._policy_snapshot
        policy_now = self._get_policy_snapshot()
        # Comparison of snapshot to current policy
        try:
            def compare_policy(pol_cur, pol_snap, key, res):
                pol_cur_set = {d["id"] if isinstance(d, dict) else d for d in pol_cur}
                pol_snap_set = {d["id"] if isinstance(d, dict) else d for d in pol_snap}
                if pol_cur_set != pol_snap_set:
                    policy_update_dict["snap.%s.%s.added" % (key, res)] = pol_cur_set - pol_snap_set
                    policy_update_dict["snap.%s.%s.removed" % (key, res)] = pol_snap_set - pol_cur_set
                    log.debug("Policy changed for %s.%s: %s vs %s" % (key, res, pol_cur_set, pol_snap_set))
                    return True
                return False
            policy_update_dict["snap.snap_ts"] = snapshot["snap_ts"]
            for key in ("common_pdp", "service_pdp", "service_precondition", "resource_pdp"):
                pol_snap = snapshot[key]
                pol_cur = policy_now[key]
                if isinstance(pol_cur, dict):
                    for res in pol_cur.keys():
                        pol_list = pol_cur[res]
                        snap_list = pol_snap.get(res, [])
                        any_change = compare_policy(pol_list, snap_list, key, res) or any_change
                elif isinstance(pol_cur, list):
                    any_change = compare_policy(pol_cur, pol_snap, key, "common") or any_change

            policy_update_dict["snap.policy_changed"] = str(any_change)
        except Exception as ex:
            log.warn("Cannot compare current policy to prior snapshot", exc_info=True)

        self._policy_update_log.append(policy_update_dict)
        self._policy_update_log = self._policy_update_log[-100:]
        self._policy_snapshot = policy_now

        if any_change:
            log.debug("Container policy changed. Cause: %s/%s" % (update_type, message))
        else:
            log.debug("Container policy checked but no change. Cause: %s/%s" % (update_type, message))

    # --- Methods for managing operation specific preconditions

    def get_process_operation_dict(self, process_name, auto_add=True):
        if process_name in self._service_op_preconditions:
            return self._service_op_preconditions[process_name]

        if auto_add:
            self._service_op_preconditions[process_name] = dict()
            return self._service_op_preconditions[process_name]

        return None

    def register_process_operation_precondition(self, process, operation, precondition):
        """
        This method is used to register process operation precondition functions
        with the governance controller. The endpoint code will call check_process_operation_preconditions()
        below before calling the business logic operation and if any of
        the precondition functions return False, then the request is denied as Unauthorized.

        At some point, this should be refactored to by another interceptor, but at the operation level.
        """
        if not hasattr(process, operation):
            raise NotFound("The operation %s does not exist for the %s process" % (operation, process.name))

        if type(precondition) == types.MethodType and precondition.im_self != process:
            raise NotFound("The method %s does not exist for the %s process." % (str(precondition), process.name))

        process_op_conditions = self.get_process_operation_dict(process.name)
        if operation in process_op_conditions:
            process_op_conditions[operation].append(precondition)
        else:
            preconditions = list()
            preconditions.append(precondition)
            process_op_conditions[operation] = preconditions

    def unregister_all_process_operation_precondition(self, process, operation):
        """
        This method removes all precondition functions registered with an operation on a process.
        Care should be taken with this call, as it can remove "hard wired" preconditions that are
        directly registered by processes in a container.
        """
        process_op_conditions = self.get_process_operation_dict(process.name, auto_add=False)
        if process_op_conditions is not None and operation in process_op_conditions:
            del process_op_conditions[operation]

    def unregister_process_operation_precondition(self, process, operation, precondition):
        """
        This method removes a specific precondition function registered with an operation on a process.
        Care should be taken with this call, as it can remove "hard wired" preconditions that are
        directly registered by processes in a container.
        """
        #Just skip this if there operation is not passed in.
        if operation is None:
            return

        if not hasattr(process, operation):
            raise NotFound("The operation %s does not exist for the %s service" % (operation, process.name))

        process_op_conditions = self.get_process_operation_dict(process.name, auto_add=False)
        if process_op_conditions is not None and operation in process_op_conditions:
            preconditions = process_op_conditions[operation]
            preconditions[:] = [pre for pre in preconditions if not pre == precondition]
            if not preconditions:
                del process_op_conditions[operation]

    def unregister_all_process_policy_preconditions(self):
        """
        This method removes all precondition functions registered with an operation on a process.
        It will not remove "hard wired" preconditions that are directly registered by processes in a container.
        """
        for proc in self._service_op_preconditions:
            process_op_conditions = self.get_process_operation_dict(proc, auto_add=False)
            if process_op_conditions is not None:
                for op in process_op_conditions:
                    preconditions = process_op_conditions[op]
                    preconditions[:] = [pre for pre in preconditions if type(pre) == types.FunctionType]

    def check_process_operation_preconditions(self, process, msg, headers):
        """
        This method is called by the ION endpoint to execute any process operation preconditions functions before
        allowing the operation to be called.
        """
        operation = headers.get('op', None)
        if operation is None:
            return

        process_op_conditions = self.get_process_operation_dict(process.name, auto_add=False)
        if process_op_conditions is not None and operation in process_op_conditions:
            preconditions = process_op_conditions[operation]
            for precond in reversed(preconditions):
                if type(precond) in (types.MethodType, types.FunctionType):
                    # Handle precondition which are built-in functions
                    try:
                        ret_val, ret_message = precond(msg, headers)
                    except Exception as e:
                        # TODD - Catching all exceptions and logging as errors, don't want to stop processing for this right now
                        log.error('Executing precondition function: %s for operation: %s - %s so it will be ignored.' %
                                  (precond.__name__, operation, e.message))
                        ret_val = True
                        ret_message = ''

                    if not ret_val:
                        raise Unauthorized(ret_message)

                elif isinstance(precond, basestring):
                    try:
                        # See if this is method within the endpoint process, if so call it
                        method = getattr(process, precond, None)
                        if method:
                            ret_val, ret_message = method(msg, headers)
                        else:
                            # It is not a method in the process, so try to execute as a simple python function
                            exec precond
                            pref = locals()["precondition_func"]
                            ret_val, ret_message = pref(process, msg, headers)

                    except Exception as e:
                        # TODD - Catching all exceptions and logging as errors, don't want to stop processing for this right now
                        log.error('Executing precondition function: %s for operation: %s - %s so it will be ignored.' %
                                  (precond, operation, e.message))
                        ret_val = True
                        ret_message = ''

                    if not ret_val:
                        raise Unauthorized(ret_message)
