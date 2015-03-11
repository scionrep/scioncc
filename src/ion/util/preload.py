#!/usr/bin/env python

"""Utility to bulk load resources into the system, e.g. for initial preload"""

__author__ = 'Michael Meisinger'

import yaml
import re
import os

from pyon.core import MSG_HEADER_ACTOR, MSG_HEADER_ROLES, MSG_HEADER_VALID
from pyon.core.bootstrap import get_service_registry
from pyon.ion.resource import get_restype_lcsm
from pyon.public import CFG, log, BadRequest, Inconsistent, NotFound, IonObject, RT, OT, AS, LCS, named_any, get_safe
from ion.util.parse_utils import get_typed_value

# Well known action config keys
KEY_SCENARIO = "scenario"
KEY_ID = "id"
KEY_OWNER = "owner"
KEY_LCSTATE = "lcstate"
KEY_ORGS = "orgs"

# Well known aliases
ID_ORG_ION = "ORG_ION"
ID_SYSTEM_ACTOR = "USER_SYSTEM"

UUID_RE = '^[0-9a-fA-F]{32}$'


class Preloader(object):

    def initialize_preloader(self, process, preload_cfg):
        log.info("Initialize preloader")

        self.process = process
        self.preload_cfg = preload_cfg
        self._init_preload()
        self.rr = self.process.container.resource_registry

        # Loads internal bootstrapped resource ids that will be referenced during preload
        self._load_system_ids()
        # Load existing resources by preload ID
        self._prepare_incremental()

    def _init_preload(self):
        self.obj_classes = {}           # Cache of class for object types
        self.object_definitions = None  # Dict of preload rows before processing
        self.resource_ids = {}          # Holds a mapping of preload IDs to internal resource ids
        self.resource_objs = {}         # Holds a mapping of preload IDs to the actual resource objects
        self.resource_assocs = {}       # Holds a mapping of existing associations list by predicate

    def preload_master(self, filename):
        """Executes a preload master file"""
        log.info("Preloading from master file: %s", filename)
        with open(filename, "r") as f:
            master_yml = f.read()
        master_cfg = yaml.load(master_yml)
        if not "preload_type" in master_cfg or master_cfg["preload_type"] != "steps":
            raise BadRequest("Invalid preload steps file")

        for step in master_cfg["steps"]:
            step_filename = "%s/%s.yml" % (os.path.dirname(filename), step)
            self._execute_step(step_filename)

    def _execute_step(self, filename):
        """Executes a preload step file"""
        with open(filename, "r") as f:
            step_yml = f.read()
        step_cfg = yaml.safe_load(step_yml)
        if not "preload_type" in step_cfg or step_cfg["preload_type"] != "actions":
            raise BadRequest("Invalid preload actions file")

        for action in step_cfg["actions"]:
            try:
                self._execute_action(action)
            except Exception as ex:
                log.warn("Action failed: " + str(ex))

    def _execute_action(self, action):
        """Executes a preload action"""
        action_type = action["action"]
        #log.debug("Preload action %s id=%s", action_type, action.get("id", ""))
        scope, func_type = action_type.split(":", 1)
        default_funcname = "_load_%s_%s" % (scope, func_type)
        action_func = getattr(self.process, default_funcname, None)
        if not action_func:
            action_funcname = self.preload_cfg["action_plugins"].get(action_type, {})
            if not action_funcname:
                log.warn("Unknown action: %s", action_type)
                return
            action_func = getattr(self.process, action_funcname, None)
            if not action_func:
                log.warn("Action function %s not found for action %s", action_funcname, action_type)
                return
        action_func(action)

    # -------------------------------------------------------------------------

    def _load_system_ids(self):
        """Read some system objects for later reference"""
        org_objs, _ = self.rr.find_resources(name="ION", restype=RT.Org, id_only=False)
        if not org_objs:
            raise BadRequest("ION org not found. Was system force_cleaned since bootstrap?")
        ion_org_id = org_objs[0]._id
        self._register_id(ID_ORG_ION, ion_org_id, org_objs[0])

        system_actor, _ = self.rr.find_resources(
            RT.ActorIdentity, name=CFG.system.system_actor, id_only=False)
        system_actor_id = system_actor[0]._id if system_actor else 'anonymous'
        self._register_id(ID_SYSTEM_ACTOR, system_actor_id, system_actor[0] if system_actor else None)

    def _prepare_incremental(self):
        """
        Look in the resource registry for any resources that have a preload ID on them so that
        they can be referenced under this preload ID during this load run.
        """
        log.debug("Loading prior preloaded resources for reference")

        res_objs, res_keys = self.rr.find_resources_ext(alt_id_ns="PRE", id_only=False)
        res_preload_ids = [key['alt_id'] for key in res_keys]
        res_ids = [obj._id for obj in res_objs]

        log.debug("Found %s previously preloaded resources", len(res_objs))

        res_assocs = self.rr.find_associations(predicate="*", id_only=False)
        [self.resource_assocs.setdefault(assoc["p"], []).append(assoc) for assoc in res_assocs]

        log.debug("Found %s existing associations", len(res_assocs))

        existing_resources = dict(zip(res_preload_ids, res_objs))

        if len(existing_resources) != len(res_objs):
            raise BadRequest("Stored preload IDs are NOT UNIQUE!!! Cannot link to old resources")

        res_id_mapping = dict(zip(res_preload_ids, res_ids))
        self.resource_ids.update(res_id_mapping)
        res_obj_mapping = dict(zip(res_preload_ids, res_objs))
        self.resource_objs.update(res_obj_mapping)

    def create_object_from_cfg(self, cfg, objtype, key="resource", prefix="", existing_obj=None):
        """
        Construct an IonObject of a determined type from given config dict with attributes.
        Convert all attributes according to their schema target type. Supports nested objects.
        Supports edit of objects of same type.
        """
        log.trace("Create object type=%s, prefix=%s", objtype, prefix)
        if objtype == "dict":
            schema = None
        else:
            schema = self._get_object_class(objtype)._schema
        obj_fields = {}         # Attributes for IonObject creation as dict
        nested_done = set()      # Names of attributes with nested objects already created
        obj_cfg = get_safe(cfg, key)
        for subkey, value in obj_cfg.iteritems():
            if subkey.startswith(prefix):
                attr = subkey[len(prefix):]
                if '.' in attr:     # We are a parent entry
                    # TODO: Make sure to not create nested object multiple times
                    slidx = attr.find('.')
                    nested_obj_field = attr[:slidx]
                    parent_field = attr[:slidx+1]
                    nested_prefix = prefix + parent_field    # prefix plus nested object name
                    if '[' in nested_obj_field and nested_obj_field[-1] == ']':
                        sqidx = nested_obj_field.find('[')
                        nested_obj_type = nested_obj_field[sqidx+1:-1]
                        nested_obj_field = nested_obj_field[:sqidx]
                    elif objtype == "dict":
                        nested_obj_type = "dict"
                    else:
                        nested_obj_type = schema[nested_obj_field]['type']

                    # Make sure to not create the same nested object twice
                    if parent_field in nested_done:
                        continue

                    # Support direct indexing in a list
                    list_idx = -1
                    if nested_obj_type.startswith("list/"):
                        _, list_idx, nested_obj_type = nested_obj_type.split("/")
                        list_idx = int(list_idx)

                    log.trace("Get nested object field=%s type=%s, prefix=%s", nested_obj_field, nested_obj_type, prefix)
                    nested_obj = self.create_object_from_cfg(cfg, nested_obj_type, key, nested_prefix)

                    if list_idx >= 0:
                        #print "#####1", prefix, parent_field, nested_obj_field, nested_obj_type, list_idx
                        my_list = obj_fields.setdefault(nested_obj_field, [])
                        if list_idx >= len(my_list):
                            my_list[len(my_list):list_idx] = [None]*(list_idx-len(my_list)+1)
                        my_list[list_idx] = nested_obj
                    else:
                        obj_fields[nested_obj_field] = nested_obj

                    nested_done.add(parent_field)

                elif objtype == "dict":
                    # TODO: What about type?
                    obj_fields[attr] = value

                elif attr in schema:    # We are the leaf attribute
                    try:
                        if value:
                            fieldvalue = get_typed_value(value, schema[attr])
                            obj_fields[attr] = fieldvalue
                    except Exception:
                        log.warn("Object type=%s, prefix=%s, field=%s cannot be converted to type=%s. Value=%s",
                            objtype, prefix, attr, schema[attr]['type'], value, exc_info=True)
                        #fieldvalue = str(fieldvalue)
                else:
                    # warn about unknown fields just once -- not on each row
                    log.warn("Skipping unknown field in %s: %s%s", objtype, prefix, attr)

        if objtype == "dict":
            obj = obj_fields
        else:
            if existing_obj:
                # Edit attributes
                if existing_obj.type_ != objtype:
                    raise Inconsistent("Cannot edit resource. Type mismatch old=%s, new=%s" % (existing_obj.type_, objtype))
                # TODO: Don't edit empty nested attributes
                for attr in list(obj_fields.keys()):
                    if not obj_fields[attr]:
                        del obj_fields[attr]
                for attr in ('alt_ids','_id','_rev','type_'):
                    if attr in obj_fields:
                        del obj_fields[attr]
                existing_obj.__dict__.update(obj_fields)
                log.trace("Update object type %s using field names %s", objtype, obj_fields.keys())
                obj = existing_obj
            else:
                if cfg.get(KEY_ID, None) and 'alt_ids' in schema:
                    if 'alt_ids' in obj_fields:
                        obj_fields['alt_ids'].append("PRE:"+cfg[KEY_ID])
                    else:
                        obj_fields['alt_ids'] = ["PRE:"+cfg[KEY_ID]]

                log.trace("Create object type %s from field names %s", objtype, obj_fields.keys())
                obj = IonObject(objtype, **obj_fields)
        return obj

    def _get_object_class(self, objtype):
        if objtype in self.obj_classes:
            return self.obj_classes[objtype]
        try:
            obj_class = named_any("interface.objects.%s" % objtype)
            self.obj_classes[objtype] = obj_class
            return obj_class
        except Exception:
            log.error('failed to find class for type %s' % objtype)

    def _get_service_client(self, service):
        return get_service_registry().services[service].client(process=self.process)

    def _register_id(self, alias, resid, res_obj=None, is_update=False):
        """Keep preload resource in internal dict for later reference"""
        if not is_update and alias in self.resource_ids:
            raise BadRequest("ID alias %s used twice" % alias)
        self.resource_ids[alias] = resid
        self.resource_objs[alias] = res_obj
        log.trace("Added resource alias=%s to id=%s", alias, resid)

    def _read_resource_id(self, res_id):
        existing_obj = self.rr.read(res_id)
        self.resource_objs[res_id] = existing_obj
        self.resource_ids[res_id] = res_id
        return existing_obj

    def _get_resource_id(self, alias_id):
        """Returns resource ID from preload alias ID, scanning also for real resource IDs to be loaded"""
        if alias_id in self.resource_ids:
            return self.resource_ids[alias_id]
        elif re.match(UUID_RE, alias_id):
            # This is obviously an ID of a real resource - let it fail if not existing
            self._read_resource_id(alias_id)
            log.debug("Referencing existing resource via direct ID: %s", alias_id)
            return alias_id
        else:
            raise KeyError(alias_id)

    def _get_resource_obj(self, res_id, silent=False):
        """Returns a resource object from one of the memory locations for given preload or internal ID"""
        if res_id in self.resource_objs:
            return self.resource_objs[res_id]
        else:
            # Real ID not alias - reverse lookup
            alias_ids = [alias_id for alias_id,int_id in self.resource_ids.iteritems() if int_id==res_id]
            if alias_ids:
                return self.resource_objs[alias_ids[0]]

        if not silent:
            log.debug("_get_resource_obj(): No object found for '%s'", res_id)
        return None

    def _resource_exists(self, res_id):
        if not res_id:
            return None
        res = self._get_resource_obj(res_id, silent=True)
        return res is not None

    def _has_association(self, sub, pred, obj):
        """Returns True if the described associated already exists."""
        for assoc in self.resource_assocs.get(pred, []):
            if assoc.s == sub and assoc.o == obj:
                return True
        return False

    def _update_resource_obj(self, res_id):
        """Updates an existing resource object"""
        res_obj = self._get_resource_obj(res_id)
        self.rr.update(res_obj)
        log.debug("Updating resource %s (pre=%s id=%s): '%s'", res_obj.type_, res_id, res_obj._id, res_obj.name)

    def _get_alt_id(self, res_obj, prefix):
        alt_ids = getattr(res_obj, 'alt_ids', [])
        for alt_id in alt_ids:
            if alt_id.startswith(prefix+":"):
                alt_id_str = alt_id[len(prefix)+1:]
                return alt_id_str

    def _get_op_headers(self, owner_id, force_user=False):
        headers = {}
        if owner_id:
            owner_id = self.resource_ids[owner_id]
            headers[MSG_HEADER_ACTOR] = owner_id
            headers[MSG_HEADER_ROLES] = {'ION': ['SUPERUSER', 'MODERATOR']}
            headers[MSG_HEADER_VALID] = '0'
        elif force_user:
            return self._get_system_actor_headers()
        return headers

    def _get_system_actor_headers(self):
        return {MSG_HEADER_ACTOR: self.resource_ids[ID_SYSTEM_ACTOR],
                MSG_HEADER_ROLES: {'ION': ['SUPERUSER', 'MODERATOR']},
                MSG_HEADER_VALID: '0'}

    def basic_resource_create(self, cfg, restype, svcname, svcop, key="resource",
                              set_attributes=None, **kwargs):
        """
        Orchestration method doing the following:
        - create an object from a row,
        - add any defined constraints,
        - make a service call to create resource for given object,
        - share resource in a given Org
        - store newly created resource id and obj for future reference
        - (optional) support bulk create/update
        """
        res_id_alias = cfg[KEY_ID]
        existing_obj = None
        if res_id_alias in self.resource_ids:
            # TODO: Catch case when ID used twice
            existing_obj = self.resource_objs[res_id_alias]
        elif re.match(UUID_RE, res_id_alias):
            # This is obviously an ID of a real resource
            try:
                existing_obj = self._read_resource_id(res_id_alias)
                log.debug("Updating existing resource via direct ID: %s", res_id_alias)
            except NotFound as nf:
                pass  # Ok it was not there after all

        try:
            res_obj = self.create_object_from_cfg(cfg, restype, key, "", existing_obj=existing_obj)
        except Exception as ex:
            log.exception("Error creating object")
            raise
        if set_attributes:
            for attr, attr_val in set_attributes.iteritems():
                setattr(res_obj, attr, attr_val)

        if existing_obj:
            res_id = self.resource_ids[res_id_alias]
            # TODO: Use the appropriate service call here
            self.rr.update(res_obj)
        else:
            svc_client = self._get_service_client(svcname)
            headers = self._get_op_headers(cfg.get(KEY_OWNER, None), force_user=True)
            res_id = getattr(svc_client, svcop)(res_obj, headers=headers, **kwargs)
            if res_id:
                if svcname == "resource_registry" and svcop == "create":
                    res_id = res_id[0]
                res_obj._id = res_id
            self._register_id(res_id_alias, res_id, res_obj)
            self._resource_assign_org(cfg, res_id)
        return res_id

    def _resource_advance_lcs(self, cfg, res_id):
        """
        Change lifecycle state of object to requested state. Supports bulk.
        """
        res_obj = self._get_resource_obj(res_id)
        restype = res_obj.type_
        lcsm = get_restype_lcsm(restype)
        initial_lcmat = lcsm.initial_state if lcsm else LCS.DEPLOYED
        initial_lcav = lcsm.initial_availability if lcsm else AS.AVAILABLE

        lcstate = cfg.get(KEY_LCSTATE, None)
        if lcstate:
            row_lcmat, row_lcav = lcstate.split("_", 1)
            if row_lcmat != initial_lcmat:    # Vertical transition
                self.rr.set_lifecycle_state(res_id, row_lcmat)
            if row_lcav != initial_lcav:      # Horizontal transition
                self.rr.set_lifecycle_state(res_id, row_lcav)

    def _resource_assign_org(self, cfg, res_id):
        """
        Shares the resource in the given orgs. Supports bulk.
        """
        org_ids = cfg.get(KEY_ORGS, None)
        if org_ids:
            org_ids = get_typed_value(org_ids, targettype="simplelist")
            for org_id in org_ids:
                org_res_id = self.resource_ids[org_id]
                svc_client = self._get_service_client("org_management")
                svc_client.share_resource(org_res_id, res_id, headers=self._get_system_actor_headers())

    def basic_associations_create(self, cfg, res_alias):
        for assoc in cfg.get("associations", []):
            direction, other_id, predicate = assoc.split(",")
            res_id = self.resource_ids[res_alias]
            other_res_id = self.resource_ids[other_id]
            if direction == "TO":
                self.rr.create_association(res_id, predicate, other_res_id)
            elif direction == "FROM":
                self.rr.create_association(other_res_id, predicate, res_id)


