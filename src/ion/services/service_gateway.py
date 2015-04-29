#!/usr/bin/env python

__author__ = "Stephen P. Henrie, Michael Meisinger"

import ast
import inspect
import string
import sys
import time
import traceback
from flask import Blueprint, request, abort
import flask

# Create special logging category for service gateway access
import logging
webapi_log = logging.getLogger('webapi')

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

from ion.services.utility.swagger_gen import SwaggerSpecGenerator
from ion.util.ui_utils import CONT_TYPE_JSON, json_dumps, json_loads, encode_ion_object, get_auth, clear_auth

from interface.services.core.idirectory_service import DirectoryServiceProcessClient
from interface.services.core.iresource_registry_service import ResourceRegistryServiceProcessClient
from interface.services.core.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.core.iorg_management_service import OrgManagementServiceProcessClient
from interface.objects import Attachment, ProcessDefinition, MediaResponse


CFG_PREFIX = "service.service_gateway"
DEFAULT_USER_CACHE_SIZE = 2000
DEFAULT_EXPIRY = "0"

SG_IDENTIFICATION = "service_gateway/ScionCC/1.0"

GATEWAY_ARG_PARAMS = "params"
GATEWAY_ARG_JSON = "data"
GATEWAY_RESPONSE = "result"
GATEWAY_STATUS = "status"
GATEWAY_ERROR = "error"
GATEWAY_ERROR_EXCEPTION = "exception"
GATEWAY_ERROR_MESSAGE = "message"
GATEWAY_ERROR_EXCID = "error_id"
GATEWAY_ERROR_TRACE = "trace"

# Stuff for specifying other return types
RETURN_MIMETYPE_PARAM = "return_mimetype"

# Flask blueprint for service gateway routes
sg_blueprint = Blueprint("service_gateway", __name__, static_folder=None)
# Singleton instance of service gateway
sg_instance = None
# Sequence number to identify requests
req_seqnum = 0


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
        self.require_login = self.config.get_safe(CFG_PREFIX + ".require_login") is True

        # Optional list of trusted originators can be specified in config.
        self.trusted_originators = self.config.get_safe(CFG_PREFIX + ".trusted_originators")
        if not self.trusted_originators:
            self.trusted_originators = None
            log.info("Service Gateway will not check requests against trusted originators since none are configured.")

        # Service screening
        self.service_blacklist = self.config.get_safe(CFG_PREFIX + ".service_blacklist") or []
        self.service_whitelist = self.config.get_safe(CFG_PREFIX + ".service_whitelist") or []
        self.no_login_whitelist = set(self.config.get_safe(CFG_PREFIX + ".no_login_whitelist") or [])
        self.set_cors_headers = self.config.get_safe(CFG_PREFIX + ".set_cors") is True

        # Swagger spec generation support
        self.swagger_cfg = self.config.get_safe(CFG_PREFIX + ".swagger_spec") or {}
        self._swagger_gen = None
        if self.swagger_cfg.get("enable", None) is True:
            self._swagger_gen = SwaggerSpecGenerator(config=self.swagger_cfg)

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

    def get_service_spec(self, service_name=None, spec_name=None):
        try:
            if not self._swagger_gen:
                raise NotFound("Spec not available")
            if spec_name != "swagger.json":
                raise NotFound("Unknown spec format")

            swagger_json = self._swagger_gen.get_spec(service_name)

            resp = flask.make_response(flask.jsonify(swagger_json))
            self._add_cors_headers(resp)
            return resp

        except Exception as ex:
            return self.gateway_error_response(ex)


    def process_gateway_request(self, service_name=None, operation=None, id_param=None):
        """
        Makes a secure call to a SciON service operation via messaging.
        """
        # TODO make this service smarter to respond to the mime type in the request data (ie. json vs text)
        self._log_request_start("SVC RPC")
        try:
            result = self._make_service_request(service_name, operation, id_param)
            return self.gateway_json_response(result)

        except Exception as ex:
            return self.gateway_error_response(ex)

        finally:
            self._log_request_end()

    def rest_gateway_request(self, service_name, res_type, id_param=None):
        """
        Makes a REST style call to a SciON service operation via messaging.
        Get with ID returns the resource, POST without ID creates, PUT with ID updates
        and GET without ID returns the collection.
        """
        self._log_request_start("SVC REST")
        try:
            if not service_name:
                raise BadRequest("Service name missing")
            service_name = str(service_name)
            if not res_type:
                raise BadRequest("Resource type missing")
            res_type = str(res_type)

            if request.method == "GET" and id_param:
                operation = "read_" + res_type
                return self.process_gateway_request(service_name, operation, id_param)
            elif request.method == "GET":
                ion_res_type = "".join(x.title() for x in res_type.split('_'))
                res = self._make_service_request("resource_registry", "find_resources", ion_res_type)
                if len(res) == 2:
                    return self.gateway_json_response(res[0])
                raise BadRequest("Unexpected find_resources result")
            elif request.method == "PUT":
                operation = "update_" + res_type
                obj = self._extract_payload_data()
                if not obj:
                    raise BadRequest("Argument object not found")
                if id_param:
                    obj._id = id_param
                return self.process_gateway_request(service_name, operation, obj)
            elif request.method == "POST":
                operation = "create_" + res_type
                obj = self._extract_payload_data()
                if not obj:
                    raise BadRequest("Argument object not found")
                return self.process_gateway_request(service_name, operation, obj)
            else:
                raise BadRequest("Bad REST request")

        except Exception as ex:
            return self.gateway_error_response(ex)

        finally:
            self._log_request_end()

    def _extract_payload_data(self):
        request_obj = None
        if request.headers.get("content-type", "").startswith(CONT_TYPE_JSON):
            if request.data:
                request_obj = json_loads(request.data)
        elif request.form:
            # Form encoded
            if GATEWAY_ARG_JSON in request.form:
                payload = request.form[GATEWAY_ARG_JSON]
                request_obj = json_loads(str(payload))

        if request_obj and is_ion_object_dict(request_obj):
            request_obj = self.create_ion_object(request_obj)

        return request_obj

    def _make_service_request(self, service_name=None, operation=None, id_param=None):
        """
        Executes a secure call to a SciON service operation via messaging.
        """
        if not service_name:
            if self.develop_mode:
                # Return a list of available services
                result = dict(available_services=get_service_registry().services.keys())
                return result
            else:
                raise BadRequest("Service name missing")
        service_name = str(service_name)

        if not operation:
            if self.develop_mode:
                # Return a list of available operations
                result = dict(available_operations=[])
                return result
            else:
                raise BadRequest("Service operation missing")
        operation = str(operation)

        # Apply service white list and black list for initial protection and get service client
        target_client = self.get_secure_service_client(service_name)

        # Get service request arguments and operation parameter values request
        req_args = self._get_request_args()

        param_list = self.create_parameter_list(service_name, target_client, operation, req_args, id_param)

        # Validate requesting user and expiry and add governance headers
        ion_actor_id, expiry = self.get_governance_info_from_request(req_args)
        in_login_whitelist = self.in_login_whitelist("request", service_name, operation)
        ion_actor_id, expiry = self.validate_request(ion_actor_id, expiry, in_whitelist=in_login_whitelist)
        param_list["headers"] = self.build_message_headers(ion_actor_id, expiry)

        # Make service operation call
        client = target_client(process=self.process)
        method_call = getattr(client, operation)
        result = method_call(**param_list)

        return result

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
            payload = request.form[GATEWAY_ARG_JSON]
            json_params = json_loads(str(payload))

            actor_id, expiry = self.get_governance_info_from_request(json_params)
            actor_id, expiry = self.validate_request(actor_id, expiry)
            headers = self.build_message_headers(actor_id, expiry)

            data_params = json_params[GATEWAY_ARG_PARAMS]
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

        # Developer access using api_key
        if self.develop_mode and "api_key" in request.args and request.args["api_key"]:
            actor_id = str(request.args["api_key"])
            expiry = str(int(user_session.get("valid_until", 0)) * 1000)
            if 0 < int(expiry) < current_time_millis():
                expiry = str(current_time_millis() + 10000)
                # flask.session["valid_until"] = int(expiry / 1000)
            log.info("Request associated with actor_id=%s, expiry=%s from developer api_key", actor_id, expiry)

        # Try to find auth token override
        # Check in headers for OAuth bearer token
        auth_hdr = request.headers.get("authorization", None)
        if auth_hdr:
            valid, req = self.process.oauth.verify_request([self.process.oauth_scope])
            if valid:
                actor_id = flask.g.oauth_user.get("actor_id", "")
                if actor_id:
                    log.info("Request associated with actor_id=%s, expiry=%s from OAuth token", actor_id, expiry)
                    return actor_id, DEFAULT_EXPIRY

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

    def in_login_whitelist(self, category, svc, op):
        """Returns True if service op is whitelisted for anonymous access"""
        entry = "%s/%s/%s" % (category, svc, op)
        return entry in self.no_login_whitelist

    def validate_request(self, ion_actor_id, expiry, in_whitelist=False):
        # There is no point in looking up an anonymous user - so return default values.
        if ion_actor_id == DEFAULT_ACTOR_ID:
            # Since this is an anonymous request, there really is no expiry associated with it
            if not in_whitelist and self.require_login:
                raise Unauthorized("Anonymous access not permitted")
            else:
                return DEFAULT_ACTOR_ID, DEFAULT_EXPIRY

        try:
            user = self.idm_client.read_actor_identity(actor_id=ion_actor_id, headers=self._get_gateway_headers())
        except NotFound as e:
            if not in_whitelist and self.require_login:
                # This could be a restart of the system with a new preload.
                # TODO: Invalidate Flask sessions on relaunch/bootstrap with creating new secret
                user_session = get_auth()
                if user_session.get("actor_id", None) == ion_actor_id:
                    clear_auth()
                raise Unauthorized("Invalid identity", exc_id="01.10")
            else:
                # If the user isn't found default to anonymous
                return DEFAULT_ACTOR_ID, DEFAULT_EXPIRY

        # Need to convert to int first in order to compare against current time.
        try:
            int_expiry = int(expiry)
        except Exception as ex:
            raise Inconsistent("Unable to read the expiry value in the request '%s' as an int" % expiry)

        # The user has been validated as being known in the system, so not check the expiry and raise exception if
        # the expiry is not set to 0 and less than the current time.
        if 0 < int_expiry < current_time_millis():
            if not in_whitelist and self.require_login:
                raise Unauthorized("User authentication expired")
            else:
                log.warn("User authentication expired")
                return DEFAULT_ACTOR_ID, DEFAULT_EXPIRY

        return ion_actor_id, expiry

    # -------------------------------------------------------------------------
    # Service call (messaging) helpers

    def _add_cors_headers(self, resp):
        # Set CORS headers so that a Swagger client on a different domain can read spec
        resp.headers["Access-Control-Allow-Headers"] = "Origin, X-Atmosphere-tracking-id, X-Atmosphere-Framework, X-Cache-Date, Content-Type, X-Atmosphere-Transport, *"
        resp.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS , PUT"
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Request-Headers"] = "Origin, X-Atmosphere-tracking-id, X-Atmosphere-Framework, X-Cache-Date, Content-Type, X-Atmosphere-Transport,  *"

    def _log_request_start(self, req_type="SG"):
        global req_seqnum
        req_seqnum += 1
        req_info = dict(request_id=req_seqnum, start_time=time.time(), req_type=req_type)
        flask.g.req_info = req_info
        webapi_log.info("%s REQUEST (%s) - %s", req_type, req_info["request_id"], request.url)

    def _log_request_response(self, content_type, result="", content_length=-1, status_code=200):
        req_info = flask.g.get("req_info", None)
        if req_info:
            req_info["resp_content_type"] = content_type
            req_info["resp_content_length"] = content_length
            req_info["resp_result"] = result
            req_info["resp_status"] = req_info.get("resp_status", status_code)

    def _log_request_error(self, result, status_code):
        req_info = flask.g.get("req_info", None)
        if req_info:
            req_info["resp_error"] = True
            req_info["resp_status"] = status_code
            webapi_log.warn("%s REQUEST (%s) ERROR (%s%s) - %s: %s",
                            req_info["req_type"], req_info["request_id"],
                            status_code,
                            "/id="+result[GATEWAY_ERROR_EXCID] if result[GATEWAY_ERROR_EXCID] else "",
                            result[GATEWAY_ERROR_EXCEPTION],
                            result[GATEWAY_ERROR_MESSAGE])
        else:
            webapi_log.warn("REQUEST ERROR (%s%s) - %s: %s",
                            status_code,
                            "/id="+result[GATEWAY_ERROR_EXCID] if result[GATEWAY_ERROR_EXCID] else "",
                            result[GATEWAY_ERROR_EXCEPTION],
                            result[GATEWAY_ERROR_MESSAGE])

    def _log_request_end(self):
        req_info = flask.g.get("req_info", None)
        if req_info:
            req_info["end_time"] = time.time()
            webapi_log.info("%s REQUEST (%s) RESP (%s) - %.3f s, %s bytes, %s",
                            req_info["req_type"], req_info["request_id"],
                            req_info.get("resp_status", ""),
                            req_info["end_time"] - req_info["start_time"],
                            req_info.get("resp_content_length", ""),
                            req_info.get("resp_content_type", "")
            )
        else:
            webapi_log.warn("REQUEST END - missing start info")


    def _get_request_args(self):
        """Extracts service request arguments from HTTP request. Supports various
        methods and forms of encoding. Separates arguments for special parameters
        from service operation parameters.
        Returns a dict with the service request arguments, containing key params
        with the actual values for the service operation parameters.
        """
        request_args = {}
        if request.method == "POST" or request.method == "PUT":
            # Use only body args and ignore any args from query string
            if request.headers.get("content-type", "").startswith(CONT_TYPE_JSON):
                if request.data:
                    request_args = json_loads(request.data)
                    if GATEWAY_ARG_PARAMS not in request_args:
                        request_args = {GATEWAY_ARG_PARAMS: request_args}
            elif request.form:
                # Form encoded
                if GATEWAY_ARG_JSON in request.form:
                    payload = request.form[GATEWAY_ARG_JSON]
                    request_args = json_loads(str(payload))
                    if GATEWAY_ARG_PARAMS not in request_args:
                        request_args = {GATEWAY_ARG_PARAMS: request_args}
                else:
                    request_args = {GATEWAY_ARG_PARAMS: request.form.to_dict(flat=True)}
            else:
                # No args found in body
                request_args = {GATEWAY_ARG_PARAMS: {}}

        elif request.method == "GET":
            REQ_ARGS_SPECIAL = {"authtoken", "timeout", "headers"}
            args_dict = request.args.to_dict(flat=True)
            request_args = {k: request.args[k] for k in args_dict if k in REQ_ARGS_SPECIAL}
            req_params = {k: request.args[k] for k in args_dict if k not in REQ_ARGS_SPECIAL}
            request_args[GATEWAY_ARG_PARAMS] = req_params

        #log.info("Request args: %s" % request_args)
        return request_args

    def _get_typed_arg_value(self, given_value, arg_default):
        """Returns an argument value, based on a given value and default (type)
        TODO: Type coercion based on argument expected type from schema
        """
        if isinstance(given_value, unicode):
            # Convert all unicode to str in UTF-8
            given_value = given_value.encode("utf8")  # Returns str

        if isinstance(given_value, IonObjectBase) and arg_default is None:
            return given_value
        elif isinstance(arg_default, str):
            return str(given_value)
        elif isinstance(given_value, str):
            # TODO: Better use coercion to expected type here
            return ast.literal_eval(given_value)
        elif is_ion_object_dict(given_value):
            return self.create_ion_object(given_value)
        else:
            return given_value

    def create_parameter_list(self, service_name, target_client, operation, request_args, id_param=None):
        """Build service call parameter list dynamically from service operation definition
        """
        svc_params = {}
        method_args = inspect.getargspec(getattr(target_client, operation))
        svc_op_param_list = method_args[0]
        svc_op_param_defaults = method_args[3]  # Note: this has one less (no self)

        if id_param:
            # Shorthand: if one argument is given, fill the first service argument
            real_params = [param for param in svc_op_param_list if param not in {"self", "headers", "timeout"}]
            if real_params:
                fill_par = real_params[0]
                fill_pos = svc_op_param_list.index(fill_par)
                arg_val = self._get_typed_arg_value(id_param, svc_op_param_defaults[fill_pos-1])
                svc_params[fill_par] = arg_val
                return svc_params

        request_args = request_args or {}
        req_op_args = request_args.get(GATEWAY_ARG_PARAMS, None) or {}
        for (param_idx, param_name) in enumerate(svc_op_param_list):
            if param_name == "self":
                continue
            if param_name in req_op_args:
                svc_params[param_name] = self._get_typed_arg_value(req_op_args[param_name], method_args[3][param_idx-1])
            elif "timeout" in request_args:
                svc_params[param_name] = float(request_args["timeout"])

        optional_args = [param for param in req_op_args if param not in svc_params]
        if optional_args and "optional_args" in svc_op_param_list:
            # Only support basic strings for these optional params for now
            svc_params["optional_args"] = {arg: str(req_op_args[arg]) for arg in optional_args}

        #log.info("Service params: %s" % svc_params)
        return svc_params

    def _get_gateway_headers(self):
        """Returns the headers that the service gateway uses to make service calls on behalf of itself
         (not a user passing through), e.g. for identity management purposes"""
        return {MSG_HEADER_ACTOR: self.name,
                MSG_HEADER_VALID: DEFAULT_EXPIRY}

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
            role_list = self.org_client.list_actor_roles(actor_id, headers=self._get_gateway_headers())
            org_roles = {}
            for role in role_list:
                org_roles.setdefault(role.org_governance_name, []).append(role)

            role_header = get_role_message_headers(org_roles)

            # Cache the roles by user id
            self.user_role_cache.put(actor_id, role_header)

        except Exception:
            role_header = dict()  # Default to empty dict if there is a problem finding roles for the user

        headers[MSG_HEADER_ROLES] = role_header

        return headers

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
        resp = self.response_class(resp_obj, mimetype=CONT_TYPE_JSON)
        if self.develop_mode and (self.set_cors_headers or ("api_key" in request.args and request.args["api_key"])):
            self._add_cors_headers(resp)
        self._log_request_response(CONT_TYPE_JSON, resp_obj, len(resp_obj))
        return resp

    def gateway_json_response(self, response_data):
        """Returns the normal service gateway response as JSON or as media in case the response
        is a media response
        """
        if isinstance(response_data, MediaResponse):
            log.info("Media response. Content mimetype:%s", response_data.media_mimetype)
            content = response_data.body
            if response_data.internal_encoding == "base64":
                import base64
                content = base64.decodestring(content)
            elif response_data.internal_encoding == "utf8":
                pass
            resp = self.response_class(content, response_data.code, mimetype=response_data.media_mimetype)
            self._log_request_response(response_data.media_mimetype, "raw", len(content), response_data.code)
            return resp

        if RETURN_MIMETYPE_PARAM in request.args:
            return_mimetype = str(request.args[RETURN_MIMETYPE_PARAM])
            return self.response_class(response_data, mimetype=return_mimetype)

        result = {
            GATEWAY_RESPONSE: response_data,
            GATEWAY_STATUS: 200,
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
            GATEWAY_ERROR_EXCID: getattr(exc, "exc_id", "") or ""
        }
        if self.develop_mode:
            result[GATEWAY_ERROR_TRACE] = full_error

        if RETURN_MIMETYPE_PARAM in request.args:
            return_mimetype = str(request.args[RETURN_MIMETYPE_PARAM])
            return self.response_class(result, mimetype=return_mimetype)

        status_code = getattr(exc, "status_code", 400)
        self._log_request_error(result, status_code)

        return self.json_response({GATEWAY_ERROR: result, GATEWAY_STATUS: status_code})


# -------------------------------------------------------------------------
# Generic route handlers

# Checks to see if the remote_addr in the request is in the list of specified trusted addresses, if any.
@sg_blueprint.before_request
def is_trusted_request():
    if sg_instance.develop_mode:
        print "----------------------------------------------------------------------------------"
        print "URL:", request.url

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

# ROUTE: Ping with gateway version
@sg_blueprint.route("/")
def sg_index():
    return sg_instance.sg_index()


@sg_blueprint.route("/spec/<spec_name>", methods=["GET"])
@sg_blueprint.route("/spec/<service_name>/<spec_name>", methods=["GET"])
def get_service_spec(service_name=None, spec_name=None):
    return sg_instance.get_service_spec(service_name, spec_name)


# ROUTE: Make a service request
# Accepts arguments passed as query string parameters; like:
#   http://hostname:port/service/request/resource_registry/find_resources?restype=TestInstrument&id_only=False
# Also accepts arguments form encoded and as JSON; example:
#   curl --data "payload={"params": { "restype": "TestInstrument", "name": "", "id_only": true } }" http://localhost:4000/service/request/resource_registry/find_resources
@sg_blueprint.route("/request", methods=["GET", "POST"])
@sg_blueprint.route("/request/<service_name>", methods=["GET", "POST"])
@sg_blueprint.route("/request/<service_name>/<operation>", methods=["GET", "POST"])
@sg_blueprint.route("/request/<service_name>/<operation>/<id_param>", methods=["GET", "POST"])
def process_gateway_request(service_name=None, operation=None, id_param=None):
    return sg_instance.process_gateway_request(service_name, operation, id_param)


# ROUTE: Make a service request in REST style
# Arguments to POST, PUT must be form encoded and as JSON; example:
#   curl --data "payload={"data": { "type_": "TestInstrument", "name": "" } }" http://localhost:4000/service/rest/service/res_type
@sg_blueprint.route("/rest/<service_name>/<res_type>", methods=["GET", "POST"])
@sg_blueprint.route("/rest/<service_name>/<res_type>/<id_param>", methods=["GET", "PUT"])
def rest_gateway_request(service_name, res_type, id_param=None):
    return sg_instance.rest_gateway_request(service_name, res_type, id_param)


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


# ROUTE: Get version information about this copy of ScionCC
@sg_blueprint.route("/version")
def get_version_info():
    return sg_instance.get_version_info()
