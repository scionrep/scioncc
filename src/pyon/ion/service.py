#!/usr/bin/env python

"""Service process base class and service management"""

__author__ = 'Adam R. Smith, Michael Meisinger'

import json
from types import ModuleType
from zope.interface import implementedBy

from pyon.core.exception import BadRequest, ServerError
from pyon.util.log import log
from pyon.util.containers import named_any, itersubclasses
from pyon.util.context import LocalContextMixin


class BaseClients(object):
    """
    Basic object to hold clients for a service. Derived in implementations.
    Placeholder, may not need any functionality.
    """
    pass


class BaseService(LocalContextMixin):
    """
    Base class providing a 'service'. Pure Python class. Not dependent on messaging.
    Such services can be executed by ION processes.
    """

    # The following are set one per implementation (class)
    name = None
    running = False
    dependencies = []
    process_type = "service"

    def __init__(self, *args, **kwargs):
        self.id               = None
        self._proc_name       = None
        self._proc_type       = None
        self._proc_res_id     = None
        self._proc_start_time = None
        self.errcause         = None
        self.org_governance_name = None
        self.container        = None
        self.CFG              = None
        self._process         = None      # reference to IonProcess, internal
        super(BaseService, self).__init__()

    def init(self):
        self._on_init()
        return self.on_init()

    def _on_init(self):
        """Framework hook to initialize"""

    def on_init(self):
        """
        Method to be overridden as necessary by implementing service classes to perform
        initialization actions prior to service start.  Configuration parameters are
        accessible via the self.CFG dict.
        """

    def start(self):
        self._on_start()
        return self.on_start()

    def _on_start(self):
        """Framework hook to start"""
        self.running = True

    def on_start(self):
        """
        Method called at service startup.
        """

    def stop(self):
        res = self.on_stop()
        self._on_stop()
        return res

    def _on_stop(self):
        """Framework hook to stop"""
        self.running = False

    def on_stop(self):
        """
        Method called at service stop. (May not be called if service is terminated immediately).
        """

    def quit(self):
        res = None
        try:
            res = self.on_quit()
        except Exception:
            log.exception("Error while service %s, id: %s quitting" % (self.name, self.id))

        self._on_quit()
        return res

    def _on_quit(self):
        """Framework hook to quit"""
        self.running = False

    def on_quit(self):
        """
        Method called just before service termination.
        """

    def assert_condition(self, condition, errorstr):
        if not condition:
            raise BadRequest(errorstr)

    def _validate_resource_id(self, arg_name, resource_id, res_type=None, optional=False, allow_subtype=True):
        """
        Check that the given argument is a resource id (string), by retrieving the resource from the
        resource registry. Additionally, checks type and returns the result object.
        Supports optional argument and subtypes. res_type can be a list of (sub)types.
        """
        if optional and not resource_id:
            return
        if not resource_id:
            raise BadRequest("Argument '%s': missing" % arg_name)
        resource_obj = self.clients.resource_registry.read(resource_id)
        if res_type:
            type_list = res_type
            if not hasattr(res_type, "__iter__"):
                type_list = [res_type]
            from pyon.core.registry import issubtype
            if allow_subtype and not any(map(lambda check_type: issubtype(resource_obj.type_, check_type), type_list)):
                raise BadRequest("Argument '%s': existing resource is not a '%s' -- SPOOFING ALERT" % (arg_name, res_type))
            elif not allow_subtype and not any(map(lambda check_type: resource_obj.type_ == check_type, type_list)):
                raise BadRequest("Argument '%s': existing resource is not a '%s' -- SPOOFING ALERT" % (arg_name, res_type))
        return resource_obj

    def _validate_resource_obj(self, arg_name, resource_obj, res_type=None, optional=False, checks=""):
        """
        Check that the given argument (object) exists and is a resource object of given type.
        Can be None if optional==True.
        Optional checks in comma separated string:
        - id: resource referenced by ID is compatible and returns it.
        - noid: object contains no id
        - name: object has non empty name
        - unique: name is not used yet in system for given res_type (must be set)
        """
        checks = checks.split(",") if isinstance(checks, basestring) else checks
        if optional and resource_obj is None:
            return
        if not resource_obj:
            raise BadRequest("Argument '%s': missing" % arg_name)
        from interface.objects import Resource
        if not isinstance(resource_obj, Resource):
            raise BadRequest("Argument '%s': not a resource object" % arg_name)
        if "noid" in checks and "_id" in resource_obj:
            raise BadRequest("Argument '%s': resource object has an id" % arg_name)
        if ("name" in checks or "unique" in checks) and not resource_obj.name:
            raise BadRequest("Argument '%s': resource has invalid name" % arg_name)
        if "unique" in checks:
            if not res_type:
                raise BadRequest("Must provide resource type")
            res_list, _ = self.clients.resource_registry.find_resources(restype=res_type, name=resource_obj.name)
            if res_list:
                raise BadRequest("Argument '%s': resource name already exists" % arg_name)
        if res_type and resource_obj.type_ != res_type:
            raise BadRequest("Argument '%s': resource object type is not a '%s' -- SPOOFING ALERT" % (arg_name, res_type))
        if "id" in checks:
            if "_id" not in resource_obj:
                raise BadRequest("Argument '%s': resource object has no id" % arg_name)
            old_resource_obj = self.clients.resource_registry.read(resource_obj._id)
            if res_type and old_resource_obj.type_ != res_type:
                raise BadRequest("Argument '%s': existing resource is not a '%s' -- SPOOFING ALERT" % (arg_name, res_type))
            return old_resource_obj

    def _validate_arg_obj(self, arg_name, arg_obj, obj_type=None, optional=False):
        """
        Check that the given argument exists and is an object of given type
        """
        if optional and arg_obj is None:
            return
        if not arg_obj:
            raise BadRequest("Argument '%s':  missing" % arg_name)
        from interface.objects import IonObjectBase
        if not isinstance(arg_obj, IonObjectBase):
            raise BadRequest("Argument '%s': not an object" % arg_name)
        if obj_type and arg_obj.type_ != obj_type:
            raise BadRequest("Argument '%s': object type is not a '%s'" % (arg_name, obj_type))

    def __str__(self):
        proc_name = 'Unknown proc_name' if self._proc_name is None else self._proc_name
        proc_type = 'Unknown proc_type' if self._proc_type is None else self._proc_type
        return "".join((self.__class__.__name__, "(",
                        "name=", proc_name,
                        ",id=", self.id,
                        ",type=", proc_type,
                        ")"))

    def add_endpoint(self, endpoint):
        """
        Adds a managed listening endpoint to this service/process.

        The service/process must be running inside of an IonProcessThread, or this
        method will raise an error.

        A managed listening endpoint will report failures up to the process, then to
        the container's process manager.
        """
        if self._process is None:
            raise ServerError("No attached IonProcessThread")

        self._process.add_endpoint(endpoint)

    def remove_endpoint(self, endpoint):
        """
        Removes an endpoint from being managed by this service/process.

        The service/process must be running inside of an IonProcessThread, or this
        method will raise an error. It will also raise an error if the endpoint is
        not currently managed.

        Errors raised in the endpoint will no longer be reported to the process or
        process manager.
        """
        if self._process is None:
            raise ServerError("No attached IonProcessThread")

        self._process.remove_endpoint(endpoint)


# -----------------------------------------------------------------------------------------------
# Service management infrastructure

class IonServiceDefinition(object):
    """
    Provides a walkable structure for ION service metadata and object definitions.
    """
    def __init__(self, name, dependencies=None, version=''):
        self.name = name
        self.dependencies = list(dependencies or [])
        self.version = version
        self.operations = []

        # Points to service (Zope) interface
        self.interface = None

        # Points to abstract base class
        self.base = None

        # Points to implementation class
        self.impl = []

        # Points to process client class
        self.client = None

        # Contains a dict schema
        self.schema = None

        # Points to non-process client class
        self.simple_client = None

    def __str__(self):
        return "IonServiceDefinition(name=%s):%s" % (self.name, self.__dict__)

    def __repr__(self):
        return str(self)


class IonServiceOperation(object):
    def __init__(self, name):
        self.name = name
        self.docstring = ''
        self.in_object_type = None
        self.out_object_type = None
        self.throws = []

    def __str__(self):
        return "IonServiceOperation(name=%s):%s" % (self.name, self.__dict__)

    def __repr__(self):
        return str(self)


class IonServiceRegistry(object):
    def __init__(self):
        self.services = {}
        self.services_by_name = {}
        self.classes_loaded = False
        self.operations = None

    def add_servicedef_entry(self, name, key, value, append=False):
        if not name:
            #log.warning("No name for key=%s, value=%s" % (key, value))
            return

        if not name in self.services:
            svc_def = IonServiceDefinition(name)
            self.services[name] = svc_def
        else:
            svc_def = self.services[name]

        oldvalue = getattr(svc_def, key, None)
        if oldvalue is not None:
            if append:
                assert type(oldvalue) is list, "Cannot append to non-list: %s" % oldvalue
                oldvalue.append(value)
            else:
                log.warning("Service %s, key=%s exists. Old=%s, new=%s" % (name, key, getattr(svc_def, key), value))

        if not append:
            setattr(svc_def, key, value)

    @classmethod
    def load_service_mods(cls, path, package=""):
        if isinstance(path, ModuleType):
            for p in path.__path__:
                cls.load_service_mods(p, path.__name__)
            return

        import pkgutil
        for mod_imp, mod_name, is_pkg in pkgutil.iter_modules([path]):
            if is_pkg:
                cls.load_service_mods(path + "/" + mod_name, package + "." + mod_name)
            else:
                mod_qual = "%s.%s" % (package, mod_name)
                try:
                    named_any(mod_qual)
                except Exception as ex:
                    log.warning("Import module '%s' failed: %s" % (mod_qual, ex))

    def build_service_map(self):
        """
        Adds all known service definitions to service registry.
        @todo: May be a bit fragile due to using BaseService.__subclasses__
        """
        for cls in BaseService.__subclasses__():
            assert hasattr(cls, 'name'), 'Service class must define name value. Service class in error: %s' % cls
            if cls.name:
                self.services_by_name[cls.name] = cls
                self.add_servicedef_entry(cls.name, "base", cls)
                try:
                    self.add_servicedef_entry(cls.name, "schema", json.loads(cls.SCHEMA_JSON))
                except Exception as ex:
                    log.exception("Cannot parse service schema " + cls.name)
                interfaces = list(implementedBy(cls))
                if interfaces:
                    self.add_servicedef_entry(cls.name, "interface", interfaces[0])
                if cls.__name__.startswith("Base"):
                    try:
                        client = "%s.%sProcessClient" % (cls.__module__, cls.__name__[4:])
                        self.add_servicedef_entry(cls.name, "client", named_any(client))
                        sclient = "%s.%sClient" % (cls.__module__, cls.__name__[4:])
                        self.add_servicedef_entry(cls.name, "simple_client", named_any(sclient))
                    except Exception, ex:
                        log.warning("Cannot find client for service %s" % (cls.name))

    def discover_service_classes(self):
        """
        Walk implementation directories and find service implementation classes.
        @todo Only works for ion packages and submodules
        """
        IonServiceRegistry.load_service_mods("ion")

        sclasses = [s for s in itersubclasses(BaseService) if not s.__subclasses__()]

        for scls in sclasses:
            self.add_servicedef_entry(scls.name, "impl", scls, append=True)

        self.classes_loaded = True

    def get_service_base(self, name):
        """
        Returns the service base class with interface for the given service name or None.
        """
        if name in self.services:
            return getattr(self.services[name], 'base', None)
        else:
            return None

    def get_service_by_name(self, name):
        """
        Returns the service definition for the given service name or None.
        """
        if name in self.services:
            return self.services[name]
        else:
            return None

    def is_service_available(self, service_name, local_rr_only=False):

        try:
            service_resource = None
            from pyon.core.bootstrap import container_instance
            from interface.objects import ServiceStateEnum
            # Use container direct RR connection if available, otherwise use messaging to the RR service
            if hasattr(container_instance, 'has_capability') and container_instance.has_capability('RESOURCE_REGISTRY'):
                service_resource, _ = container_instance.resource_registry.find_resources(restype='Service', name=service_name)
            elif not local_rr_only:
                from interface.services.core.iresource_registry_service import ResourceRegistryServiceClient
                rr_client = ResourceRegistryServiceClient(container_instance.node)
                service_resource, _ = rr_client.find_resources(restype='Service', name=service_name)
            else:
                log.warn("is_service_available(%s) - No RR connection" % service_name)

            # The service is available only of there is a single RR object for it and it is in one of these states:
            if service_resource and len(service_resource) > 1:
                log.warn("is_service_available(%s) - Found multiple service instances: %s", service_name, service_resource)

            # MM 2013-08-17: Added PENDING, because this means service will be there shortly
            if service_resource and service_resource[0].state in (ServiceStateEnum.READY, ServiceStateEnum.STEADY, ServiceStateEnum.PENDING):
                return True
            elif service_resource:
                log.warn("is_service_available(%s) - Service resource in invalid state", service_resource)

            return False

        except Exception as ex:
            return False
