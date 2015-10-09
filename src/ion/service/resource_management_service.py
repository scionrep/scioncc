#!/usr/bin/env python

__author__ = 'Michael Meisinger, Luke Campbell'

import re

from pyon.agent.agent import ResourceAgentClient
from pyon.core.bootstrap import get_service_registry
from pyon.datastore.datastore_query import QUERY_EXP_KEY, DQ
from pyon.public import log, IonObject, Unauthorized, ResourceQuery, PRED, CFG, RT, log, BadRequest, NotFound
from pyon.util.config import Config
from pyon.util.containers import get_safe, named_any, get_ion_ts, is_basic_identifier

from ion.service.ds_discovery import DatastoreDiscovery

import interface.objects
from interface.objects import AgentCapability, AgentCommandResult, CapabilityType, Resource, View
from interface.services.core.iresource_management_service import BaseResourceManagementService


class ResourceManagementService(BaseResourceManagementService):
    """
    Service that manages resource types and lifecycle workflows. It also provides generic
    operations that manage any kind of resource and their lifecycle.
    Also provides a resource discovery and query capability.
    """

    MAX_SEARCH_RESULTS = CFG.get_safe('service.resource_management.max_search_results', 250)

    def on_init(self):
        self.resource_interface = (Config(["res/config/resource_management.yml"])).data['ResourceInterface']

        self._augment_resource_interface_from_interfaces()

        # Keep a cache of known resource ids
        self.restype_cache = {}

        self.ds_discovery = DatastoreDiscovery(self)

    # -------------------------------------------------------------------------
    # Search and query

    def query(self, query=None, id_only=True, search_args=None):
        """Issue a query provided in structured dict format or internal datastore query format.
        Returns a list of resource or event objects or their IDs only.
        Search_args may contain parameterized values.
        See the query format definition: https://confluence.oceanobservatories.org/display/CIDev/Discovery+Service+Query+Format
        """
        if not query:
            raise BadRequest("Invalid query")

        return self._discovery_request(query, id_only, search_args=search_args, query_params=search_args)

    def query_view(self, view_id='', view_name='', ext_query=None, id_only=True, search_args=None):
        """Execute an existing query as defined within a View resource, providing additional arguments for
        parameterized values.
        If ext_query is provided, it will be combined with the query defined by the View.
        Search_args may contain parameterized values.
        Returns a list of resource or event objects or their IDs only.
        """
        if not view_id and not view_name:
            raise BadRequest("Must provide argument view_id or view_name")
        if view_id and view_name:
            raise BadRequest("Cannot provide both arguments view_id and view_name")
        if view_id:
            view_obj = self.clients.resource_registry.read(view_id)
        else:
            view_obj = self.ds_discovery.get_builtin_view(view_name)
            if not view_obj:
                view_objs, _ = self.clients.resource_registry.find_resources(restype=RT.View, name=view_name)
                if not view_objs:
                    raise NotFound("View with name '%s' not found" % view_name)
                view_obj = view_objs[0]

        if view_obj.type_ != RT.View:
            raise BadRequest("Argument view_id is not a View resource")
        view_query = view_obj.view_definition
        if not QUERY_EXP_KEY in view_query:
            raise BadRequest("Unknown View query format")

        # Get default query params and override them with provided args
        param_defaults = {param.name: param.default for param in view_obj.view_parameters}
        query_params = param_defaults
        if view_obj.param_values:
            query_params.update(view_obj.param_values)
        if search_args:
            query_params.update(search_args)

        # Merge ext_query into query
        if ext_query:
            if ext_query["where"] and view_query["where"]:
                view_query["where"] = [DQ.EXP_AND, [view_query["where"], ext_query["where"]]]
            else:
                view_query["where"] = view_query["where"] or ext_query["where"]
            if ext_query["order_by"]:
                # Override ordering if present
                view_query["where"] = ext_query["order_by"]

            # Other query settings
            view_qargs = view_query["query_args"]
            ext_qargs = ext_query["query_args"]
            view_qargs["id_only"] = ext_qargs.get("id_only", view_qargs["id_only"])
            view_qargs["limit"] = ext_qargs.get("limit", view_qargs["limit"])
            view_qargs["skip"] = ext_qargs.get("skip", view_qargs["skip"])

        return self._discovery_request(view_query, id_only=id_only,
                                       search_args=search_args, query_params=query_params)

    def _discovery_request(self, query=None, id_only=True, search_args=None, query_params=None):
        search_args = search_args or {}
        if not query:
            raise BadRequest('No request query provided')

        if QUERY_EXP_KEY in query and self.ds_discovery:
            query.setdefault("query_args", {})["id_only"] = id_only
            # Query in datastore query format (dict)
            log.debug("Executing datastore query: %s", query)

        elif 'query' not in query:
            raise BadRequest('Unsupported request. %s' % query)

        # if count requested, run id_only query without limit/skip
        count = search_args.get("count", False)
        if count:
            # Only return the count of ID only search
            query.pop("limit", None)
            query.pop("skip", None)
            res = self.ds_discovery.execute_query(query, id_only=True, query_args=search_args, query_params=query_params)
            return [len(res)]

        # TODO: Not all queries are permissible by all users

        # Execute the query
        query_results = self.ds_discovery.execute_query(query, id_only=id_only,
                                                        query_args=search_args, query_params=query_params)

        # Strip out unwanted object attributes for size
        filtered_res = self._strip_query_results(query_results, id_only=id_only, search_args=search_args)

        return filtered_res

    def _strip_query_results(self, query_results, id_only, search_args):
        # Filter the results for smaller result size
        attr_filter = search_args.get("attribute_filter", [])
        if type(attr_filter) not in (list, tuple):
            raise BadRequest("Illegal argument type: attribute_filter")

        if not id_only and attr_filter:
            filtered_res = [dict(__noion__=True, **{k: v for k, v in obj.__dict__.iteritems() if k in attr_filter or k in {"_id", "type_"}}) for obj in query_results]
            return filtered_res
        return query_results


    # -------------------------------------------------------------------------
    # View management (CRUD)

    def create_view(self, view=None):
        if view is None or not isinstance(view, View):
            raise BadRequest("Illegal argument: view")

        # view_objs, _ = self.clients.resource_registry.find_resources(restype=RT.View, name=view.name)
        # if view_objs:
        #     raise BadRequest("View with name '%s' already exists" % view.name)

        view_id, _ = self.clients.resource_registry.create(view)
        return view_id

    def read_view(self, view_id=''):
        view_res = self.clients.resource_registry.read(view_id)
        if not isinstance(view_res, View):
            raise BadRequest("Resource %s is not a View" % view_id)
        return view_res

    def update_view(self, view=None):
        if view is None or not isinstance(view, View):
            raise BadRequest("Illegal argument: view")
        self.clients.resource_registry.update(view)
        return True

    def delete_view(self, view_id=''):
        self.clients.resource_registry.delete(view_id)
        return True

    # -------------------------------------------------------------------------
    # Generic resource interface

    def create_resource(self, resource=None):
        """Creates an arbitrary resource object via its defined create function, so that it
        can successively can be accessed via the agent interface.
        """
        if not isinstance(resource, Resource):
            raise BadRequest("Can only create resources, not type %s" % type(resource))

        res_type = resource._get_type()
        res_interface = self._get_type_interface(res_type)

        if not 'create' in res_interface:
            raise BadRequest("Resource type %s does not support: CREATE" % res_type)

        res = self._call_crud(res_interface['create'], resource, None, res_type)
        if type(res) in (list,tuple):
            res = res[0]
        return res

    def update_resource(self, resource=None):
        """Updates an existing resource via the configured service operation.
        """
        if not isinstance(resource, Resource):
            raise BadRequest("Can only update resources, not type %s" % type(resource))

        res_type = resource._get_type()
        res_interface = self._get_type_interface(res_type)

        if not 'update' in res_interface:
            raise BadRequest("Resource type %s does not support: UPDATE" % res_type)

        self._call_crud(res_interface['update'], resource, None, res_type)

    def read_resource(self, resource_id=''):
        """Returns an existing resource via the configured service operation.
        """
        res_type = self._get_resource_type(resource_id)
        res_interface = self._get_type_interface(res_type)

        if not 'read' in res_interface:
            raise BadRequest("Resource type %s does not support: READ" % res_type)

        res_obj = self._call_crud(res_interface['read'], None, resource_id, res_type)
        return res_obj

    def delete_resource(self, resource_id=''):
        """Deletes an existing resource via the configured service operation.
        """
        res_type = self._get_resource_type(resource_id)
        res_interface = self._get_type_interface(res_type)

        if not 'delete' in res_interface:
            raise BadRequest("Resource type %s does not support: DELETE" % res_type)

        self._call_crud(res_interface['delete'], None, resource_id, res_type)

    CORE_ATTRIBUTES = {"_id", "name", "description", "ts_created", "ts_updated",
                       "lcstate", "availability", "visibility", "alt_resource_type"}

    def get_org_resource_attributes(self, org_id='', order_by='', type_filter=None, limit=0, skip=0):
        """For a given org, return a list of dicts with core resource attributes (_id, type_, name, description,
        ts_created, ts_modified, lcstate, availability, visibility and alt_resource_type).
        The returned list is ordered by name unless otherwise specified.
        Supports pagination and white-list filtering if provided.
        """
        if not org_id:
            raise BadRequest("Must provide org_id")
        res_list = []
        res_objs, _ = self.clients.resource_registry.find_objects(org_id, PRED.hasResource, id_only=False)
        res_list.extend(res_objs)
        # TODO: The following is not correct - this should be all attachments in the Org
        res_objs, _ = self.clients.resource_registry.find_objects(org_id, PRED.hasAttachment, id_only=False)
        res_list.extend(res_objs)
        # TODO: The following should be shared in the Org
        res_objs, _ = self.clients.resource_registry.find_objects(org_id, PRED.hasRole, id_only=False)
        res_list.extend(res_objs)

        def get_core_attrs(resource):
            res_attr = {k:v for k, v in resource.__dict__.iteritems() if k in self.CORE_ATTRIBUTES}
            # HACK: Cannot use type_ because that would treat the dict as IonObject and add back all attributes
            res_attr["type__"] = resource.type_
            return res_attr
        if type_filter:
            type_filter = set(type_filter)
        attr_list = [get_core_attrs(res) for res in res_list if not type_filter or res.type_ in type_filter]

        order_by = order_by or "name"
        attr_list = sorted(attr_list, key=lambda o: o.get(order_by, ""))

        if skip:
            attr_list = attr_list[skip:]
        if limit:
            attr_list = attr_list[:limit]

        # Need to return a similar type than RR.find_objects
        # Major bug in service gateway
        return attr_list, []

    def get_distinct_values(self, restype='', attr_list=None, res_filter=None):
        """Returns a list of distinct values for given resource type and list of attribute names.
        Only supports simple types for the attribute values.
        Returns a sorted list of values or tuples of values.
        """
        if not restype or type(restype) != str:
            raise BadRequest("Illegal value for argument restype")
        if not hasattr(interface.objects, restype):
            raise BadRequest("Given restype is not a resource type")
        if not attr_list or not type(attr_list) in (list, tuple):
            raise BadRequest("Illegal value for argument attr_list")
        type_cls = getattr(interface.objects, restype)
        try:
            if not all(type_cls._schema[an]["type"] in {"str", "int", "float"} for an in attr_list):
                raise BadRequest("Attribute in attr_list if invalid type")
        except KeyError:
            raise BadRequest("Attribute in attr_list unknown")
        if res_filter and type(res_filter) not in (list, tuple):
            raise BadRequest("Illegal value for argument res_filter")

        # NOTE: This can alternatively be implemented as a SELECT DISTINCT query, but this is not
        # supported by the underlying datastore interface.
        rq = ResourceQuery()
        if res_filter:
            rq.set_filter(rq.eq(rq.ATT_TYPE, restype), res_filter)
        else:
            rq.set_filter(rq.eq(rq.ATT_TYPE, restype))
        res_list = self.clients.resource_registry.find_resources_ext(query=rq.get_query(), id_only=False)

        log.debug("Found %s resources of type %s", len(res_list), restype)
        att_values = sorted({tuple(getattr(res, an) for an in attr_list) for res in res_list})

        log.debug("Found %s distinct vales for attribute(s): %s", len(att_values), attr_list)

        return att_values

    def execute_lifecycle_transition(self, resource_id='', transition_event=''):
        """Alter object lifecycle according to given transition event. Throws exception
        if resource object does not exist or given transition_event is unknown/illegal.
        The new life cycle state after applying the transition is returned.
        """
        res_type = self._get_resource_type(resource_id)
        res_interface = self._get_type_interface(res_type)

        if not 'execute_lifecycle_transition' in res_interface:
            raise BadRequest("Resource type %s does not support: execute_lifecycle_transition" % res_type)

        res = self._call_crud(res_interface['execute_lifecycle_transition'], transition_event, resource_id, res_type)
        return res

    def get_lifecycle_events(self, resource_id=''):
        """For a given resource, return a list of possible lifecycle transition events.
        """
        pass

    # -------------------------------------------------------------------------
    # Agent interface

    def negotiate(self, resource_id='', sap_in=None):
        """Initiate a negotiation with this agent. The subject of this negotiation is the given
        ServiceAgreementProposal. The response is either a new ServiceAgreementProposal as counter-offer,
        or the same ServiceAgreementProposal indicating the offer has been accepted.
        """
        pass

    def get_capabilities(self, resource_id='', current_state=True):
        """Introspect for agent capabilities.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.get_capabilities(resource_id=resource_id, current_state=current_state)

        res_interface = self._get_type_interface(res_type)

        cap_list = []
        for param in res_interface['params'].keys():
            cap = AgentCapability(name=param, cap_type=CapabilityType.RES_PAR)
            cap_list.append(cap)

        for cmd in res_interface['commands'].keys():
            cap = AgentCapability(name=cmd, cap_type=CapabilityType.RES_CMD)
            cap_list.append(cap)

        return cap_list

    def execute_resource(self, resource_id='', command=None):
        """Execute command on the resource represented by agent.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.execute_resource(resource_id=resource_id, command=command)

        cmd_res = None
        res_interface = self._get_type_interface(res_type)

        target = get_safe(res_interface, "commands.%s.execute" % command.command, None)
        if target:
            res = self._call_execute(target, resource_id, res_type, command.args, command.kwargs)
            cmd_res = AgentCommandResult(command_id=command.command_id,
                command=command.command,
                ts_execute=get_ion_ts(),
                status=0)
        else:
            log.warn("execute_resource(): command %s not defined", command.command)

        return cmd_res

    def get_resource(self, resource_id='', params=None):
        """Return the value of the given resource parameter.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.get_resource(resource_id=resource_id, params=params)

        res_interface = self._get_type_interface(res_type)

        get_result = {}
        for param in params:
            getter = get_safe(res_interface, "params.%s.get" % param, None)
            if getter:
                get_res = self._call_getter(getter, resource_id, res_type)
                get_result[param] = get_res
            else:
                get_result[param] = None

        return get_result

    def set_resource(self, resource_id='', params=None):
        """Set the value of the given resource parameters.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.set_resource(resource_id=resource_id, params=params)

        res_interface = self._get_type_interface(res_type)

        for param in params:
            setter = get_safe(res_interface, "params.%s.set" % param, None)
            if setter:
                self._call_setter(setter, params[param], resource_id, res_type)
            else:
                log.warn("set_resource(): param %s not defined", param)

    def get_resource_state(self, resource_id=''):
        """Return the current resource specific state, if available.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.get_resource_state(resource_id=resource_id)

        raise BadRequest("Not implemented for resource type %s", res_type)

    def ping_resource(self, resource_id=''):
        """Ping the resource.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.ping_resource(resource_id=resource_id)

        raise BadRequest("Not implemented for resource type %s" % res_type)


    def execute_agent(self, resource_id='', command=None):
        """Execute command on the agent.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.execute_agent(resource_id=resource_id, command=command)

        raise BadRequest("Not implemented for resource type %s" % res_type)

    def get_agent(self, resource_id='', params=None):
        """Return the value of the given agent parameters.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.get_agent(resource_id=resource_id, params=params)

        raise BadRequest("Not implemented for resource type %s" % res_type)

    def set_agent(self, resource_id='', params=None):
        """Set the value of the given agent parameters.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.set_agent(resource_id=resource_id, params=params)

        raise BadRequest("Not implemented for resource type %s" % res_type)

    def get_agent_state(self, resource_id=''):
        """Return the current resource agent common state.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.get_agent_state(resource_id=resource_id)

        raise BadRequest("Not implemented for resource type %s" % res_type)

    def ping_agent(self, resource_id=''):
        """Ping the agent.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.ping_agent(resource_id=resource_id)

        raise BadRequest("Not implemented for resource type %s" % res_type)

    # -----------------------------------------------------------------

    def _augment_resource_interface_from_interfaces(self):
        """
        Add resource type specific entries for CRUD, params and commands based on decorator
        annotations in service interfaces. This enables systematic definition and extension.
        @TODO Implement this so that static definitions are not needed anymore
        """
        pass

    def _get_resource_type(self, resource_id):
        if resource_id in self.restype_cache:
            return self.restype_cache[resource_id]
        res = self.container.resource_registry.read(resource_id)
        res_type = res._get_type()
        self.restype_cache[resource_id] = res_type
        if len(self.restype_cache) > 10000:
            log.warn("Resource type cache exceeds size: %s", len(self.restype_cache))
        return res_type

    def _has_agent(self, res_type):
        type_interface = self.resource_interface.get(res_type, None)
        return type_interface and type_interface.get('agent', False)

    def _get_type_interface(self, res_type):
        """
        Creates a merge of params and commands up the type inheritance chain.
        Note: Entire param and command entries if subtypes replace their super types definition.
        """
        res_interface = dict(params={}, commands={})

        base_types = IonObject(res_type)._get_extends()
        base_types.insert(0, res_type)

        for rt in reversed(base_types):
            type_interface = self.resource_interface.get(rt, None)
            if not type_interface:
                continue
            for tpar, tval in type_interface.iteritems():
                if tpar in res_interface:
                    rval = res_interface[tpar]
                    if isinstance(rval, dict):
                        rval.update(tval)
                    else:
                        res_interface[tpar] = tval
                else:
                    res_interface[tpar] = dict(tval) if isinstance(tval, dict) else tval

        return res_interface

    def _call_getter(self, func_sig, resource_id, res_type):
        return self._call_target(func_sig, resource_id=resource_id, res_type=res_type)

    def _call_setter(self, func_sig, value, resource_id, res_type):
        return self._call_target(func_sig, value=value, resource_id=resource_id, res_type=res_type)

    def _call_execute(self, func_sig, resource_id, res_type, cmd_args, cmd_kwargs):
        return self._call_target(func_sig, resource_id=resource_id, res_type=res_type, cmd_kwargs=cmd_kwargs)

    def _call_crud(self, func_sig, value, resource_id, res_type):
        return self._call_target(func_sig, value=value, resource_id=resource_id, res_type=res_type)

    def _call_target(self, target, value=None, resource_id=None, res_type=None, cmd_args=None, cmd_kwargs=None):
        """
        Makes a call to a specified function. Function specification can be of varying type.
        """
        try:
            if not target:
                return None
            match = re.match("(func|serviceop):([\w.]+)\s*\(\s*([\w,$\s]*)\s*\)\s*(?:->\s*([\w\.]+))?\s*$", target)
            if match:
                func_type, func_name, func_args, res_path = match.groups()
                func = None
                if func_type == "func":
                    if func_name.startswith("self."):
                        func = getattr(self, func_name[5:])
                    else:
                        func = named_any(func_name)
                elif func_type == "serviceop":
                    svc_name, svc_op = func_name.split('.', 1)
                    try:
                        svc_client_cls = get_service_registry().get_service_by_name(svc_name).client
                    except Exception as ex:
                        log.error("No service client found for service: %s", svc_name)
                    else:
                        svc_client = svc_client_cls(process=self)
                        func = getattr(svc_client, svc_op)

                if not func:
                    return None

                args = self._get_call_args(func_args, resource_id, res_type, value, cmd_args)
                kwargs = {} if not cmd_kwargs else cmd_kwargs

                func_res = func(*args, **kwargs)
                log.info("Function %s result: %s", func, func_res)

                if res_path and isinstance(func_res, dict):
                    func_res = get_safe(func_res, res_path, None)

                return func_res

            else:
                log.error("Unknown call target expression: %s", target)

        except Unauthorized as ex:
            # No need to log as this is not an application error, however, need to pass on the exception because
            # when called by the Service Gateway, the error message in the exception is required
            raise ex

        except Exception as ex:
            log.exception("_call_target exception")
            raise ex  #Should to pass it back because when called by the Service Gateway, the error message in the exception is required

    def _get_call_args(self, func_arg_str, resource_id, res_type, value=None, cmd_args=None):
        args = []
        func_args = func_arg_str.split(',')
        if func_args:
            for arg in func_args:
                arg = arg.strip()
                if arg == "$RESOURCE_ID":
                    args.append(resource_id)
                elif arg == "$RESOURCE_TYPE":
                    args.append(res_type)
                elif arg == "$VALUE" or arg == "$RESOURCE":
                    args.append(value)
                elif arg == "$ARGS":
                    if cmd_args is not None:
                        args.extend(cmd_args)
                elif not arg:
                    args.append(None)
                else:
                    args.append(arg)
        return args

    # Callable functions

    def get_resource_size(self, resource_id):
        res_obj = self.container.resource_registry.rr_store.read_doc(resource_id)
        import json
        obj_str = json.dumps(res_obj)
        res_len = len(obj_str)

        log.info("Resource %s length: %s", resource_id, res_len)
        return res_len

    def set_resource_description(self, resource_id, value):
        res_obj = self.container.resource_registry.read(resource_id)
        res_obj.description = value
        self.container.resource_registry.update(res_obj)

        log.info("Resource %s description updated: %s", resource_id, value)

# Helpers

def get_resource_size(resource_id):
    return 10
