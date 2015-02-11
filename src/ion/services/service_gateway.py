#!/usr/bin/env python

__author__ = "Stephen P. Henrie, Michael Meisinger"

import ast
import inspect
import string
import sys 
import traceback
from flask import Blueprint, request, abort
import flask

from pyon.core.bootstrap import get_service_registry
from pyon.core.object import IonObjectBase
from pyon.core.exception import Unauthorized
from pyon.core.registry import getextends, is_ion_object_dict, issubtype
from pyon.core.governance import DEFAULT_ACTOR_ID, get_role_message_headers, find_roles_by_actor
from pyon.ion.resource import get_object_schema
from pyon.public import IonObject, OT, NotFound, Inconsistent, BadRequest, EventSubscriber, log, CFG
from pyon.public import MSG_HEADER_ACTOR, MSG_HEADER_VALID, MSG_HEADER_ROLES
from pyon.util.lru_cache import LRUCache
from pyon.util.containers import current_time_millis

from ion.util.ui_utils import CONT_TYPE_JSON, json_dumps, json_loads, encode_ion_object, get_auth

from interface.services.core.idirectory_service import DirectoryServiceProcessClient
from interface.services.core.iresource_registry_service import ResourceRegistryServiceProcessClient
from interface.services.core.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.core.iorg_management_service import OrgManagementServiceProcessClient
from interface.objects import Attachment, ProcessDefinition


CFG_PREFIX = "service.service_gateway"
DEFAULT_USER_CACHE_SIZE = 2000
DEFAULT_EXPIRY = "0"

SG_IDENTIFICATION = "service_gateway/ScionCC/1.0"

GATEWAY_RESPONSE = "result"
GATEWAY_ERROR = "error"
GATEWAY_ERROR_EXCEPTION = "exception"
GATEWAY_ERROR_MESSAGE = "message"
GATEWAY_ERROR_TRACE = "trace"

# Stuff for specifying other return types
RETURN_MIMETYPE_PARAM = "return_mimetype"

# Flask blueprint for service gateway routes
sg_blueprint = Blueprint("service_gateway", __name__, static_folder=None)
# Singleton instance of service gateway
sg_instance = None


class ServiceGateway(object):
    """
    The Service Gateway exports service routes for a web server via a Flask blueprint.
    The gateway bridges HTTP requests to ION AMQP RPC calls.
    """

    def __init__(self, process, config, response_class):
        global sg_instance
        sg_instance = self

        self.name = "service_gateway"
        self.process = process
        self.config = config
        self.response_class = response_class

        self.gateway_base_url = process.gateway_base_url
        self.develop_mode = self.config.get_safe(CFG_PREFIX + ".develop_mode") is True

        # Optional list of trusted originators can be specified in config.
        self.trusted_originators = self.config.get_safe(CFG_PREFIX + ".trusted_originators")
        if not self.trusted_originators:
            self.trusted_originators = None
            log.info("Service Gateway will not check requests against trusted originators since none are configured.")

        # Service screening
        self.service_blacklist = self.config.get_safe(CFG_PREFIX + ".service_blacklist") or []
        self.service_whitelist = self.config.get_safe(CFG_PREFIX + ".service_whitelist") or []

        # Get the user_cache_size
        self.user_cache_size = self.config.get_safe(CFG_PREFIX + ".user_cache_size", DEFAULT_USER_CACHE_SIZE)

        # Initialize an LRU Cache to keep user roles cached for performance reasons
        #maxSize = maximum number of elements to keep in cache
        #maxAgeMs = oldest entry to keep
        self.user_role_cache = LRUCache(self.user_cache_size, 0, 0)

        self.log_errors = self.config.get_safe(CFG_PREFIX + ".log_errors", True)

        self.rr_client = ResourceRegistryServiceProcessClient(process=self.process)
        self.idm_client = IdentityManagementServiceProcessClient(process=self.process)
        self.org_client = OrgManagementServiceProcessClient(process=self.process)

    # -------------------------------------------------------------------------
    # Lifecycle management

    def start(self):
        # Configure  subscriptions for user_cache events
        self.user_role_event_subscriber = EventSubscriber(event_type=OT.UserRoleModifiedEvent, origin_type="Org",
                                                          callback=self._user_role_event_callback)
        self.process.add_endpoint(self.user_role_event_subscriber)

        self.user_role_reset_subscriber = EventSubscriber(event_type=OT.UserRoleCacheResetEvent,
                                                          callback=self._user_role_reset_callback)
        self.process.add_endpoint(self.user_role_reset_subscriber)

    def stop(self):
        pass
        # Stop event subscribers - TODO: This hangs
        #self.process.remove_endpoint(self.user_role_event_subscriber)
        #self.process.remove_endpoint(self.user_role_reset_subscriber)

    # -------------------------------------------------------------------------
    # Event subscriber callbacks

    def _user_role_event_callback(self, *args, **kwargs):
        """Callback function for receiving Events when User Roles are modified."""
        user_role_event = args[0]
        org_id = user_role_event.origin
        actor_id = user_role_event.actor_id
        role_name = user_role_event.role_name
        log.debug("User Role modified: %s %s %s" % (org_id, actor_id, role_name))

        # Evict the user and their roles from the cache so that it gets updated with the next call.
        if self.user_role_cache and self.user_role_cache.has_key(actor_id):
            log.debug("Evicting user from the user_role_cache: %s" % actor_id)
            self.user_role_cache.evict(actor_id)

    def _user_role_reset_callback(self, *args, **kwargs):
        """Callback function for when an event is received to clear the user data cache"""
        self.user_role_cache.clear()

    # -------------------------------------------------------------------------
    # Routes

    def sg_index(self):
        return self.gateway_json_response(SG_IDENTIFICATION)

    def process_gateway_request(self, service_name=None, operation=None):
        """
        Makes a secure call to a SciON service via messaging.
        """
        # TODO make this service smarter to respond to the mime type in the request data (ie. json vs text)
        try:
            # TODO: Extract service name and op from request data
            if not service_name:
                if self.develop_mode:
                    # Return a list of available services
                    result = dict(available_services=get_service_registry().services.keys())
                    return self.gateway_json_response(result)
                else:
                    raise BadRequest("Service name missing")
            service_name = str(service_name)

            if not operation:
                if self.develop_mode:
                    # Return a list of available operations
                    result = dict(available_operations=[])
                    return self.gateway_json_response(result)
                else:
                    raise BadRequest("Service operation missing")
            operation = str(operation)

            # Apply service white list and black list for initial protection and get service client
            target_client = self.get_secure_service_client(service_name)

            # Retrieve json data from HTTP Post payload
            json_params = None
            if request.method == "POST":
                payload = request.form["payload"]
                # debug only
                #payload = "{"params": { "restype": "TestInstrument", "name": "", "id_only": false } }"
                json_params = json_loads(str(payload))

            param_list = self.create_parameter_list(service_name, target_client, operation, json_params)

            # Validate requesting user and expiry and add governance headers
            ion_actor_id, expiry = self.get_governance_info_from_request(json_params)
            ion_actor_id, expiry = self.validate_request(ion_actor_id, expiry)
            param_list["headers"] = self.build_message_headers(ion_actor_id, expiry)

            client = target_client(process=self.process)
            method_call = getattr(client, operation)
            result = method_call(**param_list)

            return self.gateway_json_response(result)

        except Exception as ex:
            return self.gateway_error_response(ex)

    def get_resource_schema(self, resource_type):
        try:
            # Validate requesting user and expiry and add governance headers
            ion_actor_id, expiry = self.get_governance_info_from_request()
            ion_actor_id, expiry = self.validate_request(ion_actor_id, expiry)

            return self.gateway_json_response(get_object_schema(resource_type))

        except Exception as ex:
            return self.gateway_error_response(ex)

    def get_attachment(self, attachment_id):

        try:
            # Create client to interface
            attachment = self.rr_client.read_attachment(attachment_id, include_content=True)

            return self.response_class(attachment.content, mimetype=attachment.content_type)

        except Exception as ex:
            return self.gateway_error_response(ex)

    def create_attachment(self):
        try:
            payload = request.form["payload"]
            json_params = json_loads(str(payload))

            ion_actor_id, expiry = self.get_governance_info_from_request(json_params)
            ion_actor_id, expiry = self.validate_request(ion_actor_id, expiry)
            headers = self.build_message_headers(ion_actor_id, expiry)

            data_params = json_params["params"]
            resource_id = str(data_params.get("resource_id", ""))
            fil = request.files["file"]
            content = fil.read()

            keywords = []
            keywords_str = data_params.get("keywords", "")
            if keywords_str.strip():
                keywords = [str(x.strip()) for x in keywords_str.split(",")]

            created_by = data_params.get("attachment_created_by", "unknown user")
            modified_by = data_params.get("attachment_modified_by", "unknown user")

            # build attachment
            attachment = Attachment(name=str(data_params["attachment_name"]),
                                    description=str(data_params["attachment_description"]),
                                    attachment_type=int(data_params["attachment_type"]),
                                    content_type=str(data_params["attachment_content_type"]),
                                    keywords=keywords,
                                    created_by=created_by,
                                    modified_by=modified_by,
                                    content=content)

            ret = self.rr_client.create_attachment(resource_id=resource_id, attachment=attachment, headers=headers)

            return self.gateway_json_response(ret)

        except Exception as ex:
            log.exception("Error creating attachment")
            return self.gateway_error_response(ex)

    def delete_attachment(self, attachment_id):
        try:
            ret = self.rr_client.delete_attachment(attachment_id)
            return self.gateway_json_response(ret)

        except Exception as ex:
            log.exception("Error deleting attachment")
            return self.gateway_error_response(ex)

    def get_version_info(self):
        import pkg_resources
        pkg_list = ["scioncc"]

        version = {}
        for package in pkg_list:
            try:
                version["%s-release" % package] = pkg_resources.require(package)[0].version
                # @TODO git versions for each?
            except pkg_resources.DistributionNotFound:
                pass

        try:
            dir_client = DirectoryServiceProcessClient(process=self.process)
            sys_attrs = dir_client.lookup("/System")
            if sys_attrs and isinstance(sys_attrs, dict):
                version.update({k: v for (k, v) in sys_attrs.iteritems() if "version" in k.lower()})
        except Exception as ex:
            log.exception("Could not determine system directory attributes")

        return self.gateway_json_response(version)

    # =========================================================================
    # Security and governance helpers

    def is_trusted_address(self, requesting_address):
        if self.trusted_originators is None:
            return True

        return requesting_address in self.trusted_originators

    def get_secure_service_client(self, service_name):
        """Checks whether the service indicated by given service_name exists and/or
        is exposed after white and black listing.
        """
        if self.service_whitelist:
            if service_name not in self.service_whitelist:
                raise Unauthorized("Service access not permitted")
        if self.service_blacklist:
            if service_name in self.service_blacklist:
                raise Unauthorized("Service access not permitted")

        # Retrieve service definition
        target_service = get_service_registry().get_service_by_name(service_name)
        if not target_service:
            raise BadRequest("The requested service (%s) is not available" % service_name)
        # Find the concrete client class for making the RPC calls.
        if not target_service.client:
            raise Inconsistent("Cannot find a client class for the specified service: %s" % service_name)
        target_client = target_service.client
        return target_client

    def get_governance_info_from_request(self, json_params=None):
        # Default values for governance headers.
        actor_id = DEFAULT_ACTOR_ID
        expiry = DEFAULT_EXPIRY
        authtoken = ""
        user_session = get_auth()
        if user_session.get("actor_id", None):
            # Get info from current session
            actor_id = user_session["actor_id"]
            expiry = str(int(user_session.get("valid_until", 0)) * 1000)
            log.info("Request associated with session actor_id=%s, expiry=%s", actor_id, expiry)

        # Try to find auth token override
        # Check in headers for OAuth bearer token
        auth_hdr = request.headers.get("authorization", None)
        if auth_hdr:
            token_parts = auth_hdr.split(" ")
            if len(token_parts) == 2 and token_parts[0].lower() == "bearer":
                authtoken = token_parts[1]
        if not authtoken:
            if json_params:
                if "authtoken" in json_params:
                    authtoken = json_params["authtoken"]
            else:
                if "authtoken" in request.args:
                    authtoken = str(request.args["authtoken"])

        # Enable temporary authentication tokens to resolve to actor ids
        if authtoken:
            try:
                token_info = self.idm_client.check_authentication_token(authtoken, headers=self._get_gateway_headers())
                actor_id = token_info.get("actor_id", actor_id)
                expiry = token_info.get("expiry", expiry)
                log.info("Resolved token %s into actor_id=%s expiry=%s", authtoken, actor_id, expiry)
            except NotFound:
                log.info("Provided authentication token not found: %s", authtoken)
            except Unauthorized:
                log.info("Authentication token expired or invalid: %s", authtoken)
            except Exception as ex:
                log.exception("Problem resolving authentication token")

        return actor_id, expiry

    def validate_request(self, ion_actor_id, expiry):
        # There is no point in looking up an anonymous user - so return default values.
        if ion_actor_id == DEFAULT_ACTOR_ID:
            # Since this is an anonymous request, there really is no expiry associated with it
            return DEFAULT_ACTOR_ID, DEFAULT_EXPIRY

        try:
            user = self.idm_client.read_actor_identity(actor_id=ion_actor_id, headers=self._get_gateway_headers())
        except NotFound as e:
            # If the user isn"t found default to anonymous
            return DEFAULT_ACTOR_ID, DEFAULT_EXPIRY

        # Need to convert to int first in order to compare against current time.
        try:
            int_expiry = int(expiry)
        except Exception as ex:
            raise Inconsistent("Unable to read the expiry value in the request '%s' as an int" % expiry)

        # The user has been validated as being known in the system, so not check the expiry and raise exception if
        # the expiry is not set to 0 and less than the current time.
        if 0 < int_expiry < current_time_millis():
            raise Unauthorized("User authentication expired")

        return ion_actor_id, expiry

    # -------------------------------------------------------------------------
    # Service call (messaging) helpers

    def _get_gateway_headers(self):
        """Returns the headers that the service gateway uses to make service calls on behalf of itself
         (not a user passing through), e.g. for identity management purposes"""
        return {MSG_HEADER_ACTOR: self.name,
                MSG_HEADER_VALID: DEFAULT_EXPIRY}

    def build_message_headers(self, actor_id, expiry):
        """Returns the headers that the service gateway uses to make service calls on behalf of a
        user, based on the user session or request arguments"""
        headers = dict()
        headers[MSG_HEADER_ACTOR] = actor_id
        headers[MSG_HEADER_VALID] = expiry

        # If this is an anonymous requester then there are no roles associated with the request
        if actor_id == DEFAULT_ACTOR_ID:
            headers[MSG_HEADER_ROLES] = dict()
            return headers

        try:
            # Check to see if the user's roles are cached already - keyed by user id
            if self.user_role_cache.has_key(actor_id):
                role_header = self.user_role_cache.get(actor_id)
                if role_header is not None:
                    headers[MSG_HEADER_ROLES] = role_header
                    return headers

            # The user's roles were not cached so hit the datastore to find it.
            org_roles = self.org_client.find_all_roles_by_user(actor_id, headers=self._get_gateway_headers())

            role_header = get_role_message_headers(org_roles)

            # Cache the roles by user id
            self.user_role_cache.put(actor_id, role_header)

        except Exception:
            role_header = dict()  # Default to empty dict if there is a problem finding roles for the user

        headers[MSG_HEADER_ROLES] = role_header

        return headers

    def create_parameter_list(self, service_name, target_client, operation, json_params):
        """Build parameter list dynamically from service operation definition, using
        either body json params or URL params"""

        # This is a bit of a hack - should use decorators to indicate which
        # parameter is the dict that acts like kwargs
        optional_args = request.args.to_dict(flat=True)

        param_list = {}
        method_args = inspect.getargspec(getattr(target_client, operation))
        for (arg_index, arg) in enumerate(method_args[0]):
            if arg == "self":
                continue  # skip self
            if not json_params:
                if arg in request.args:
                    # Keep track of which query_string_parms are left after processing
                    del optional_args[arg]

                    # Handle strings differently because of unicode
                    if isinstance(method_args[3][arg_index-1], str):
                        if isinstance(request.args[arg], unicode):
                            param_list[arg] = str(request.args[arg].encode("utf8"))
                        else:
                            param_list[arg] = str(request.args[arg])
                    else:
                        # TODO: Type coercion based on argument expected type
                        param_list[arg] = ast.literal_eval(str(request.args[arg]))
            else:
                if arg in json_params["params"]:
                    object_params = json_params["params"][arg]
                    if is_ion_object_dict(object_params):
                        param_list[arg] = self.create_ion_object(object_params)
                    else:
                        # Not an ION object so handle as a simple type then.
                        if isinstance(json_params["params"][arg], unicode):
                            param_list[arg] = str(json_params["params"][arg].encode("utf8"))
                        else:
                            param_list[arg] = json_params["params"][arg]

        # Send any optional_args if there are any and allowed
        if len(optional_args) > 0 and "optional_args" in method_args[0]:
            # Only support basic strings for these optional params for now
            param_list["optional_args"] = {arg: str(request.args[arg]) for arg in optional_args}

        return param_list

    def create_ion_object(self, object_params):
        """Create and initialize an ION object from a dictionary of parameters coming via HTTP,
        ready to be passed on to services/messaging.
        """
        new_obj = IonObject(object_params["type_"])

        # Iterate over the parameters to add to object; have to do this instead
        # of passing a dict to get around restrictions in object creation on setting _id, _rev params
        for param in object_params:
            self.set_object_field(new_obj, param, object_params.get(param))

        new_obj._validate()  # verify that all of the object fields were set with proper types
        return new_obj

    def set_object_field(self, obj, field, field_val):
        """Recursively set sub object field values.
        TODO: This may be an expensive operation. May also be redundant with object code
        """
        if isinstance(field_val, dict) and field != "kwargs":
            sub_obj = getattr(obj, field)

            if isinstance(sub_obj, IonObjectBase):

                if "type_" in field_val and field_val["type_"] != sub_obj.type_:
                    if issubtype(field_val["type_"], sub_obj.type_):
                        sub_obj = IonObject(field_val["type_"])
                        setattr(obj, field, sub_obj)
                    else:
                        raise Inconsistent("Unable to walk the field %s - types don't match: %s %s" % (
                            field, sub_obj.type_, field_val["type_"]))

                for sub_field in field_val:
                    self.set_object_field(sub_obj, sub_field, field_val.get(sub_field))

            elif isinstance(sub_obj, dict):
                setattr(obj, field, field_val)

            else:
                for sub_field in field_val:
                    self.set_object_field(sub_obj, sub_field, field_val.get(sub_field))
        else:
            # type_ already exists in the class.
            if field != "type_":
                setattr(obj, field, field_val)

    # -------------------------------------------------------------------------
    # Response content helpers

    def json_response(self, response_data):
        """Private implementation of standard flask jsonify to specify the use of an encoder to walk ION objects
        """
        resp_obj = json_dumps(response_data, default=encode_ion_object, indent=None if request.is_xhr else 2)
        return self.response_class(resp_obj, mimetype=CONT_TYPE_JSON)

    def gateway_json_response(self, response_data):
        """Returns the normal service gateway response as JSON"""
        if RETURN_MIMETYPE_PARAM in request.args:
            return_mimetype = str(request.args[RETURN_MIMETYPE_PARAM])
            return self.response_class(response_data, mimetype=return_mimetype)

        result = {
            GATEWAY_RESPONSE: response_data,
            "status": 200,
        }
        return self.json_response(result)

    def gateway_error_response(self, exc):
        """Forms a service gateway error reponse.
        Can extract multiple stacks from a multi-tier RPC service call exception
        """
        if hasattr(exc, "get_stacks"):
            # Process potentially multiple stacks.
            full_error = ""
            for i in range(len(exc.get_stacks())):
                full_error += exc.get_stacks()[i][0] + "\n"
                if i == 0:
                    full_error += string.join(traceback.format_exception(*sys.exc_info()), "")
                else:
                    for ln in exc.get_stacks()[i][1]:
                        full_error += str(ln) + "\n"

            exec_name = exc.__class__.__name__
        else:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            exec_name = exc_type.__name__
            full_error = traceback.format_exception(*sys.exc_info())

        if self.log_errors:
            if self.develop_mode:
                log.error(full_error)
            else:
                log.info(full_error)

        result = {
            GATEWAY_ERROR_EXCEPTION: exec_name,
            GATEWAY_ERROR_MESSAGE: str(exc.message),
        }
        if self.develop_mode:
            result[GATEWAY_ERROR_TRACE] = full_error

        if RETURN_MIMETYPE_PARAM in request.args:
            return_mimetype = str(request.args[RETURN_MIMETYPE_PARAM])
            return self.response_class(result, mimetype=return_mimetype)

        return self.json_response({GATEWAY_ERROR: result})


# -------------------------------------------------------------------------
# Generic route handlers

# Checks to see if the remote_addr in the request is in the list of specified trusted addresses, if any.
@sg_blueprint.before_request
def is_trusted_request():
    if request.remote_addr is not None:
        log.debug("%s from: %s: %s", request.method, request.remote_addr, request.url)

    if not sg_instance.is_trusted_address(request.remote_addr):
        abort(403)


@sg_blueprint.errorhandler(403)
def custom_403(error):
    result = {GATEWAY_ERROR: "The request has been denied since it did not originate from a trusted originator."}
    return sg_instance.json_response(result)


# -------------------------------------------------------------------------
# Service calls

# ROUTE: Get version information about this copy of coi-services
@sg_blueprint.route("/")
def sg_index():
    return sg_instance.sg_index()


# ROUTE: Make a service request
# Accepts arguments passed as query string parameters; like:
#   http://hostname:port/service/request/resource_registry/find_resources?restype=TestInstrument&id_only=False
# Also accepts arguments form encoded and as JSON; example
#   curl --data "payload={"params": { "restype": "TestInstrument", "name": "", "id_only": true } }" http://localhost:4000/service/request/resource_registry/find_resources
@sg_blueprint.route("/request", methods=["GET", "POST"])
@sg_blueprint.route("/request/<service_name>/<operation>", methods=["GET", "POST"])
def process_gateway_request(service_name=None, operation=None):
    return sg_instance.process_gateway_request(service_name, operation)


# ROUTE: Returns a json object for a specified resource type with all default values.
@sg_blueprint.route("/resource_type_schema/<resource_type>")
def get_resource_schema(resource_type):
    return sg_instance.get_resource_schema(resource_type)


# ROUTE: Get attachment for a specific attachment id
@sg_blueprint.route("/attachment/<attachment_id>", methods=["GET"])
def get_attachment(attachment_id):
    return sg_instance.get_attachment(attachment_id)


# ROUTE:
@sg_blueprint.route("/attachment", methods=["POST"])
def create_attachment():
    return sg_instance.create_attachment()


# ROUTE:
@sg_blueprint.route("/attachment/<attachment_id>", methods=["DELETE"])
def delete_attachment(attachment_id):
    return sg_instance.delete_attachment(attachment_id)


# ROUTE: Get version information about this copy of coi-services
@sg_blueprint.route("/version")
def get_version_info():
    return sg_instance.get_version_info()
