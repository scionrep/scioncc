#!/usr/bin/env python

"""
Component of the container that manages ION processes etc.

The ProcManager keeps an IonProcessThreadManager as proc_sup (supervisor) to spawn
the ION process threads.
It also instantiates the BaseService instance with the app business logic,
and it registers the process listeners for a new ION process depending on process type.

An ION process is an IonProcessThread instance with a main greenlet, referenced
by the proc attribute, and a BaseService instance, referenced by the service attribute.
It has a control thread that processes all the incoming requests sequentially.
An ION process has a thread manager that spawns PyonThread greenlets for any
listeners and manages their lifecyle and termination.

New processes register in the RR. The first process of a service registers in the RR.
Agents register in the directory. Registration is removed when the process is terminated.
"""

__author__ = 'Michael Meisinger'

from copy import deepcopy
import re
import time
from gevent.lock import RLock

from pyon.agent.agent import ResourceAgent
from pyon.core import (PROCTYPE_SERVICE, PROCTYPE_AGENT, PROCTYPE_IMMEDIATE, PROCTYPE_SIMPLE, PROCTYPE_STANDALONE,
                       PROCTYPE_STREAMPROC)
from pyon.core.bootstrap import CFG, IonObject, get_sys_name
from pyon.core.exception import ContainerConfigError, BadRequest, NotFound
from pyon.ion.endpoint import ProcessRPCServer
from pyon.ion.process import IonProcessThreadManager, IonProcessError
from pyon.ion.resource import RT, PRED
from pyon.ion.service import BaseService
from pyon.ion.stream import StreamPublisher, StreamSubscriber
from pyon.net.channel import RecvChannel
from pyon.net.messaging import IDPool
from pyon.net.transport import NameTrio, TransportError
from pyon.util.containers import DotDict, for_name, named_any, dict_merge, get_safe, is_valid_identifier
from pyon.util.log import log

from interface.objects import ProcessStateEnum, CapabilityContainer, Service, Process, ServiceStateEnum


class ProcManager(object):
    def __init__(self, container):
        self.container = container

        # Define the callables that can be added to Container public API, and add
        self.container_api = [self.spawn_process, self.terminate_process]
        for call in self.container_api:
            setattr(self.container, call.__name__, call)

        self.proc_id_pool = IDPool()

        # Registry of running processes
        self.procs = {}
        self.procs_by_name = {}   # BAD: This is not correct if procs have the same name

        # mapping of greenlets we spawn to process_instances for error handling
        self._spawned_proc_to_process = {}

        # The pyon worker process supervisor
        self.proc_sup = IonProcessThreadManager(heartbeat_secs=CFG.get_safe("container.timeout.heartbeat"),
                                                failure_notify_callback=self._spawned_proc_failed)

        # list of callbacks for process state changes
        self._proc_state_change_callbacks = []

    def start(self):
        log.debug("ProcManager starting ...")
        self.proc_sup.start()

        if self.container.has_capability(self.container.CCAP.RESOURCE_REGISTRY):
            # Register container as resource object
            cc_obj = CapabilityContainer(name=self.container.id, cc_agent=self.container.name)
            self.cc_id, _ = self.container.resource_registry.create(cc_obj)

            #Create an association to an Org object if not the rot ION org and only if found
            if CFG.container.org_name and CFG.container.org_name != CFG.system.root_org:
                org, _ = self.container.resource_registry.find_resources(restype=RT.Org, name=CFG.container.org_name, id_only=True)
                if org:
                    self.container.resource_registry.create_association(org[0], PRED.hasResource, self.cc_id)  # TODO - replace with proper association

        log.debug("ProcManager started, OK.")

    def stop(self):
        log.debug("ProcManager stopping ...")

        # Call quit on procs to give them ability to clean up in reverse order
        procs_list = sorted(self.procs.values(), key=lambda proc: proc._proc_start_time, reverse=True)
        for proc in procs_list:
            try:
                self.terminate_process(proc.id)
            except Exception as ex:
                log.warn("Failed to terminate process (%s): %s", proc.id, ex)

        # TODO: Have a choice of shutdown behaviors for waiting on children, timeouts, etc
        self.proc_sup.shutdown(CFG.get_safe("container.timeout.shutdown"))

        if self.procs:
            log.warn("ProcManager procs not empty: %s", self.procs)
        if self.procs_by_name:
            log.warn("ProcManager procs_by_name not empty: %s", self.procs_by_name)

        # Remove Resource registration
        if self.container.has_capability(self.container.CCAP.RESOURCE_REGISTRY):
            try:
                self.container.resource_registry.delete(self.cc_id, del_associations=True)
            except NotFound:
                # already gone, this is ok
                pass

        log.debug("ProcManager stopped, OK.")

    # -----------------------------------------------------------------

    def spawn_process(self, name=None, module=None, cls=None, config=None, process_id=None):
        """
        Spawn a process within the container. Processes can be of different type.
        """
        if process_id and not is_valid_identifier(process_id, ws_sub='_'):
            raise BadRequest("Given process_id %s is not a valid identifier" % process_id)

        # PROCESS ID. Generate a new process id if not provided
        # TODO: Ensure it is system-wide unique
        process_id = process_id or "%s.%s" % (self.container.id, self.proc_id_pool.get_id())
        log.debug("ProcManager.spawn_process(name=%s, module.cls=%s.%s, config=%s) as pid=%s", name, module, cls, config, process_id)

        # CONFIG
        process_cfg = self._create_process_config(config)

        try:
            service_cls = named_any("%s.%s" % (module, cls))
        except AttributeError as ae:
            # Try to nail down the error
            import importlib
            importlib.import_module(module)
            raise

        # PROCESS TYPE. Determines basic process context (messaging, service interface)
        process_type = get_safe(process_cfg, "process.type") or getattr(service_cls, "process_type", PROCTYPE_SERVICE)

        process_start_mode = get_safe(config, "process.start_mode")

        process_instance = None

        # alert we have a spawning process, but we don't have the instance yet, so give the class instead (more accurate than name)
        self._call_proc_state_changed("%s.%s" % (module, cls), ProcessStateEnum.PENDING)

        try:
            # Additional attributes to set with the process instance
            proc_attr = {"_proc_type": process_type,
                         "_proc_spawn_cfg": config
                         }

            # SPAWN.  Determined by type
            if process_type == PROCTYPE_SERVICE:
                process_instance = self._spawn_service_process(process_id, name, module, cls, process_cfg, proc_attr)

            elif process_type == PROCTYPE_STREAMPROC:
                process_instance = self._spawn_stream_process(process_id, name, module, cls, process_cfg, proc_attr)

            elif process_type == PROCTYPE_AGENT:
                process_instance = self._spawn_agent_process(process_id, name, module, cls, process_cfg, proc_attr)

            elif process_type == PROCTYPE_STANDALONE:
                process_instance = self._spawn_standalone_process(process_id, name, module, cls, process_cfg, proc_attr)

            elif process_type == PROCTYPE_IMMEDIATE:
                process_instance = self._spawn_immediate_process(process_id, name, module, cls, process_cfg, proc_attr)

            elif process_type == PROCTYPE_SIMPLE:
                process_instance = self._spawn_simple_process(process_id, name, module, cls, process_cfg, proc_attr)

            else:
                raise BadRequest("Unknown process type: %s" % process_type)

            # REGISTER.
            self._register_process(process_instance, name)

            process_instance.errcause = "OK"
            log.info("ProcManager.spawn_process: %s.%s -> pid=%s OK", module, cls, process_id)

            if process_type == PROCTYPE_IMMEDIATE:
                log.debug('Terminating immediate process: %s', process_instance.id)
                self.terminate_process(process_instance.id)

                # Terminate process also triggers TERMINATING/TERMINATED
                self._call_proc_state_changed(process_instance, ProcessStateEnum.EXITED)

            else:
                # Update local policies for the new process
                if self.container.has_capability(self.container.CCAP.GOVERNANCE_CONTROLLER):
                    self.container.governance_controller.update_process_policies(
                                process_instance, safe_mode=True, force_update=False)

            return process_instance.id

        except IonProcessError:
            errcause = process_instance.errcause if process_instance else "instantiating process"
            log.exception("Error spawning %s %s process (process_id: %s): %s", name, process_type, process_id, errcause)
            return None

        except Exception:
            errcause = process_instance.errcause if process_instance else "instantiating process"
            log.exception("Error spawning %s %s process (process_id: %s): %s", name, process_type, process_id, errcause)

            # trigger failed notification - catches problems in init/start
            self._call_proc_state_changed(process_instance, ProcessStateEnum.FAILED)

            raise

    def _create_process_config(self, config):
        """ Prepare the config for the new process. Clone system config and apply process overrides.
        Support including config by reference of a resource attribute or object from object store.
        """
        process_cfg = deepcopy(CFG)
        if config:
            # Use provided config. Must be dict or DotDict
            if not isinstance(config, DotDict):
                config = DotDict(config)
            if config.get_safe("process.config_ref"):
                # Use a reference
                config_ref = config.get_safe("process.config_ref")
                log.info("Enhancing new process spawn config from ref=%s" % config_ref)
                matches = re.match(r'^([A-Za-z]+):([A-Za-z0-9_\.]+)/(.*)$', config_ref)
                if matches:
                    ref_type, ref_id, ref_ext = matches.groups()
                    if ref_type == "resources":
                        if self.container.has_capability(self.container.CCAP.RESOURCE_REGISTRY):
                            try:
                                obj = self.container.resource_registry.read(ref_id)
                                if obj and hasattr(obj, ref_ext):
                                    ref_config = getattr(obj, ref_ext)
                                    if isinstance(ref_config, dict):
                                        dict_merge(process_cfg, ref_config, inplace=True)
                                    else:
                                        raise BadRequest("config_ref %s exists but not dict" % config_ref)
                                else:
                                    raise BadRequest("config_ref %s - attribute not found" % config_ref)
                            except NotFound as nf:
                                log.warn("config_ref %s - object not found" % config_ref)
                                raise
                        else:
                            log.error("Container missing RESOURCE_REGISTRY capability to resolve process config ref %s" % config_ref)
                    elif ref_type == "objects":
                        if self.container.has_capability(self.container.CCAP.OBJECT_STORE):
                            try:
                                obj = self.container.object_store.read_doc(ref_id)
                                ref_config = obj
                                if ref_ext:
                                    ref_config = get_safe(obj, ref_ext, None)
                                    if ref_config is None:
                                        raise BadRequest("config_ref %s - attribute not found" % config_ref)

                                if isinstance(ref_config, dict):
                                    dict_merge(process_cfg, ref_config, inplace=True)
                                else:
                                    raise BadRequest("config_ref %s exists but not dict" % config_ref)
                            except NotFound as nf:
                                log.warn("config_ref %s - object not found" % config_ref)
                                raise
                        else:
                            log.error("Container missing OBJECT_STORE capability to resolve process config ref %s" % config_ref)
                    else:
                        raise BadRequest("Unknown reference type in: %s" % config_ref)

            dict_merge(process_cfg, config, inplace=True)
            if self.container.spawn_args:
                # Override config with spawn args
                dict_merge(process_cfg, self.container.spawn_args, inplace=True)

        #log.debug("spawn_process() pid=%s process_cfg=%s", process_id, process_cfg)
        return process_cfg

    def list_local_processes(self, process_type=''):
        """ Returns a list of the running ION processes in the container or filtered by the process_type
        """
        if not process_type:
            return self.procs.values()

        return [p for p in self.procs.itervalues() if p.process_type == process_type]

    def get_a_local_process(self, proc_name=''):
        """ Returns a running ION process in the container for the specified process name
        """
        for p in self.procs.itervalues():
            if p.name == proc_name:
                return p

            if p.process_type == PROCTYPE_AGENT and p.resource_type == proc_name:
                return p

        return None

    def get_local_service_processes(self, service_name=''):
        """ Returns a list of running ION processes in the container for the specified service name
        """
        proc_list = [p for p in self.procs.itervalues() if p.process_type == PROCTYPE_SERVICE and p.name == service_name]
        return proc_list

    def is_local_service_process(self, service_name):
        local_services = self.list_local_processes(PROCTYPE_SERVICE)
        for p in local_services:
            if p.name == service_name:
                return True

        return False

    def is_local_agent_process(self, resource_type):
        local_agents = self.list_local_processes(PROCTYPE_AGENT)
        for p in local_agents:
            if p.resource_type == resource_type:
                return True
        return False

    def _spawned_proc_failed(self, gproc):
        log.error("ProcManager._spawned_proc_failed: %s, %s", gproc, gproc.exception)

        prc = self._spawned_proc_to_process.get(gproc, None)

        # stop the rest of the process
        if prc is not None:
            try:
                self.terminate_process(prc.id, False)
            except Exception as e:
                log.warn("Problem while stopping rest of failed process %s: %s", prc, e)
            finally:
                self._call_proc_state_changed(prc, ProcessStateEnum.FAILED)
        else:
            log.warn("No ION process found for failed proc manager child: %s", gproc)

        # Stop the container if this was the last process
        if not self.procs and CFG.get_safe("container.process.exit_once_empty", False):
            self.container.fail_fast("Terminating container after last process (%s) failed: %s" % (gproc, gproc.exception))

    def add_proc_state_changed_callback(self, cb):
        """
        Adds a callback to be called when a process' state changes.

        The callback should take three parameters: The process, the state, and the container.
        """
        self._proc_state_change_callbacks.append(cb)

    def remove_proc_state_changed_callback(self, cb):
        """
        Removes a callback from the process state change callback list.

        If the callback is not registered, this method does nothing.
        """
        if cb in self._proc_state_change_callbacks:
            self._proc_state_change_callbacks.remove(cb)

    def _call_proc_state_changed(self, svc, state):
        """
        Internal method to call all registered process state change callbacks.
        """
        #log.debug("Proc State Changed (%s): %s", ProcessStateEnum._str_map.get(state, state), svc)
        for cb in self._proc_state_change_callbacks:
            cb(svc, state, self.container)

    def _create_listening_endpoint(self, **kwargs):
        """
        Creates a listening endpoint for spawning processes.

        This method exists to be able to override the type created via configuration.
        In most cases it will create a ProcessRPCServer.
        """
        eptypestr = CFG.get_safe('container.messaging.endpoint.proc_listening_type', None)
        if eptypestr is not None:
            module, cls     = eptypestr.rsplit('.', 1)
            mod             = __import__(module, fromlist=[cls])
            eptype          = getattr(mod, cls)
            ep              = eptype(**kwargs)
        else:
            ep = ProcessRPCServer(**kwargs)
        return ep

    # -----------------------------------------------------------------
    # PROCESS TYPE: service
    def _spawn_service_process(self, process_id, name, module, cls, config, proc_attr):
        """
        Spawn a process acting as a service worker.
        Attach to service queue with service definition, attach to service pid
        """
        process_instance = self._create_app_instance(process_id, name, module, cls, config, proc_attr)

        listen_name = get_safe(config, "process.listen_name") or process_instance.name
        listen_name_xo = self.container.create_xn_service(listen_name)

        log.debug("Service Process (%s) listen_name: %s", name, listen_name)
        process_instance._proc_listen_name = listen_name

        # Service RPC endpoint
        rsvc1 = self._create_listening_endpoint(node=self.container.node,
                                                from_name=listen_name_xo,
                                                process=process_instance)

        # Start an ION process with the right kind of endpoint factory
        proc = self.proc_sup.spawn(name=process_instance.id,
                                   service=process_instance,
                                   listeners=[rsvc1],
                                   proc_name=process_instance._proc_name)
        proc.proc._glname = "ION Proc %s" % process_instance._proc_name
        self.proc_sup.ensure_ready(proc, "_spawn_service_process for %s" % ",".join((str(listen_name), process_instance.id)))

        # map gproc to process_instance
        self._spawned_proc_to_process[proc.proc] = process_instance

        # set service's reference to process
        process_instance._process = proc

        self._process_init(process_instance)
        self._process_start(process_instance)

        try:
            proc.start_listeners()
        except IonProcessError:
            self._process_quit(process_instance)
            self._call_proc_state_changed(process_instance, ProcessStateEnum.FAILED)
            raise

        return process_instance

    # -----------------------------------------------------------------
    # PROCESS TYPE: stream process
    def _spawn_stream_process(self, process_id, name, module, cls, config, proc_attr):
        """
        Spawn a process acting as a data stream process.
        Attach to subscription queue with process function.
        """
        process_instance = self._create_app_instance(process_id, name, module, cls, config, proc_attr)

        listen_name = get_safe(config, "process.listen_name") or name
        log.debug("Stream Process (%s) listen_name: %s", name, listen_name)
        process_instance._proc_listen_name = listen_name

        process_instance.stream_subscriber = StreamSubscriber(process=process_instance, exchange_name=listen_name,
                                                              callback=process_instance.call_process)

        # Add publishers if any...
        publish_streams = get_safe(config, "process.publish_streams")
        pub_names = self._set_publisher_endpoints(process_instance, publish_streams)

        pid_listener_xo = self.container.create_xn_process(process_instance.id)
        rsvc = self._create_listening_endpoint(node=self.container.node,
                                               from_name=pid_listener_xo,
                                               process=process_instance)

        # cleanup method to delete process queue (@TODO: leaks a bit here - should use XOs)
        def cleanup(*args):
            for name in pub_names:
                p = getattr(process_instance, name)
                p.close()

        proc = self.proc_sup.spawn(name=process_instance.id,
                                   service=process_instance,
                                   listeners=[rsvc, process_instance.stream_subscriber],
                                   proc_name=process_instance._proc_name,
                                   cleanup_method=cleanup)
        proc.proc._glname = "ION Proc %s" % process_instance._proc_name
        self.proc_sup.ensure_ready(proc, "_spawn_stream_process for %s" % process_instance._proc_name)

        # map gproc to process_instance
        self._spawned_proc_to_process[proc.proc] = process_instance

        # set service's reference to process
        process_instance._process = proc

        self._process_init(process_instance)
        self._process_start(process_instance)

        try:
            proc.start_listeners()
        except IonProcessError:
            self._process_quit(process_instance)
            self._call_proc_state_changed(process_instance, ProcessStateEnum.FAILED)
            raise

        return process_instance

    # -----------------------------------------------------------------
    # PROCESS TYPE: agent
    def _spawn_agent_process(self, process_id, name, module, cls, config, proc_attr):
        """
        Spawn a process acting as agent process.
        Attach to service pid.
        """
        process_instance = self._create_app_instance(process_id, name, module, cls, config, proc_attr)
        if not isinstance(process_instance, ResourceAgent):
            raise ContainerConfigError("Agent process must extend ResourceAgent")
        listeners = []

        # Set the resource ID if we get it through the config
        resource_id = get_safe(process_instance.CFG, "agent.resource_id")
        if resource_id:
            process_instance.resource_id = resource_id

            resource_id_xo = self.container.create_xn_process(resource_id)

            alistener = self._create_listening_endpoint(node=self.container.node,
                                                        from_name=resource_id_xo,
                                                        process=process_instance)

            listeners.append(alistener)

        pid_listener_xo = self.container.create_xn_process(process_instance.id)
        rsvc = self._create_listening_endpoint(node=self.container.node,
                                               from_name=pid_listener_xo,
                                               process=process_instance)

        listeners.append(rsvc)

        proc = self.proc_sup.spawn(name=process_instance.id,
                                   service=process_instance,
                                   listeners=listeners,
                                   proc_name=process_instance._proc_name)
        proc.proc._glname = "ION Proc %s" % process_instance._proc_name
        self.proc_sup.ensure_ready(proc, "_spawn_agent_process for %s" % process_instance.id)

        # map gproc to process_instance
        self._spawned_proc_to_process[proc.proc] = process_instance

        # set service's reference to process
        process_instance._process = proc

        # Now call the on_init of the agent.
        self._process_init(process_instance)

        if not process_instance.resource_id:
            log.warn("New agent pid=%s has no resource_id set" % process_id)

        self._process_start(process_instance)

        try:
            proc.start_listeners()
        except IonProcessError:
            self._process_quit(process_instance)
            self._call_proc_state_changed(process_instance, ProcessStateEnum.FAILED)
            raise

        if not process_instance.resource_id:
            log.warn("Agent process id=%s does not define resource_id!!" % process_instance.id)

        return process_instance

    # -----------------------------------------------------------------
    # PROCESS TYPE: standalone
    def _spawn_standalone_process(self, process_id, name, module, cls, config, proc_attr):
        """
        Spawn a process acting as standalone process.
        Attach to service pid.
        """
        process_instance = self._create_app_instance(process_id, name, module, cls, config, proc_attr)
        pid_listener_xo = self.container.create_xn_process(process_instance.id)
        rsvc = self._create_listening_endpoint(node=self.container.node,
                                               from_name=pid_listener_xo,
                                               process=process_instance)

        # Add publishers if any...
        publish_streams = get_safe(config, "process.publish_streams")
        pub_names = self._set_publisher_endpoints(process_instance, publish_streams)

        # cleanup method to delete process queue (@TODO: leaks a bit here - should use XOs)
        def cleanup(*args):
            for name in pub_names:
                p = getattr(process_instance, name)
                p.close()

        proc = self.proc_sup.spawn(name=process_instance.id,
                                   service=process_instance,
                                   listeners=[rsvc],
                                   proc_name=process_instance._proc_name,
                                   cleanup_method=cleanup)
        proc.proc._glname = "ION Proc %s" % process_instance._proc_name
        self.proc_sup.ensure_ready(proc, "_spawn_standalone_process for %s" % process_instance.id)

        # map gproc to process_instance
        self._spawned_proc_to_process[proc.proc] = process_instance

        # set service's reference to process
        process_instance._process = proc

        self._process_init(process_instance)
        self._process_start(process_instance)

        try:
            proc.start_listeners()
        except IonProcessError:
            self._process_quit(process_instance)
            self._call_proc_state_changed(process_instance, ProcessStateEnum.FAILED)
            raise

        return process_instance

    # -----------------------------------------------------------------
    # PROCESS TYPE: simple
    def _spawn_simple_process(self, process_id, name, module, cls, config, proc_attr):
        """
        Spawn a process acting as simple process.
        No attachments.
        """
        process_instance = self._create_app_instance(process_id, name, module, cls, config, proc_attr)
        # Add publishers if any...
        publish_streams = get_safe(config, "process.publish_streams")
        pub_names = self._set_publisher_endpoints(process_instance, publish_streams)

        # cleanup method to delete process queue (@TODO: leaks a bit here - should use XOs)
        def cleanup(*args):
            for name in pub_names:
                p = getattr(process_instance, name)
                p.close()

        proc = self.proc_sup.spawn(name=process_instance.id,
                                   service=process_instance,
                                   listeners=[],
                                   proc_name=process_instance._proc_name,
                                   cleanup_method=cleanup)
        proc.proc._glname = "ION Proc %s" % process_instance._proc_name
        self.proc_sup.ensure_ready(proc, "_spawn_simple_process for %s" % process_instance.id)

        # map gproc to process_instance
        self._spawned_proc_to_process[proc.proc] = process_instance

        # set service's reference to process
        process_instance._process = proc

        self._process_init(process_instance)
        self._process_start(process_instance)

        return process_instance

    # -----------------------------------------------------------------
    # PROCESS TYPE: immediate
    def _spawn_immediate_process(self, process_id, name, module, cls, config, proc_attr):
        """
        Spawn a process acting as immediate one off process.
        No messaging attachments.
        """
        process_instance = self._create_app_instance(process_id, name, module, cls, config, proc_attr)
        self._process_init(process_instance)
        self._process_start(process_instance)
        return process_instance

    # -----------------------------------------------------------------

    def _create_app_instance(self, process_id, name, module, cls, config, proc_attr):
        """
        Creates an instance of a BaseService, representing the app logic of a ION process.
        This is independent of the process type service, agent, standalone, etc.
        """
        # APP INSTANCE.
        app_instance = for_name(module, cls)
        if not isinstance(app_instance, BaseService):
            raise ContainerConfigError("Instantiated service not a BaseService %r" % app_instance)

        # Set BaseService instance common attributes
        app_instance.errcause = ""
        app_instance.id = process_id
        app_instance.container = self.container
        app_instance.CFG = config
        app_instance._proc_name = name
        app_instance._proc_start_time = time.time()
        for att, att_val in proc_attr.iteritems():
            setattr(app_instance, att, att_val)

        # Unless the process has been started as part of another Org, default to the container Org or the ION Org
        if 'org_governance_name' in config:
            app_instance.org_governance_name = config['org_governance_name']
        else:
            app_instance.org_governance_name = CFG.get_safe('container.org_name', CFG.get_safe('system.root_org', 'ION'))

        # Add process state management, if applicable
        self._add_process_state(app_instance)

        # Check dependencies (RPC clients)
        self._check_process_dependencies(app_instance)

        return app_instance

    def _add_process_state(self, process_instance):
        """ Add stateful process operations, if applicable
        """
        # Only applies if the process implements stateful interface
        if hasattr(process_instance, "_flush_state"):
            def _flush_state():
                with process_instance._state_lock:
                    state_obj = process_instance.container.state_repository.put_state(process_instance.id, process_instance._proc_state,
                                                                                      state_obj=process_instance._proc_state_obj)
                    state_obj.state = None   # Make sure memory footprint is low for larger states
                    process_instance._proc_state_obj = state_obj
                    process_instance._proc_state_changed = False

            def _load_state():
                if not hasattr(process_instance, "_proc_state"):
                    process_instance._proc_state = {}
                try:
                    with process_instance._state_lock:
                        new_state, state_obj = process_instance.container.state_repository.get_state(process_instance.id)
                        process_instance._proc_state.clear()
                        process_instance._proc_state.update(new_state)
                        process_instance._proc_state_obj = state_obj
                        process_instance._proc_state_changed = False
                except NotFound as nf:
                    log.debug("No persisted state available for process %s", process_instance.id)
                except Exception as ex:
                    log.warn("Process %s load state failed: %s", process_instance.id, str(ex))
            process_instance._flush_state = _flush_state
            process_instance._load_state = _load_state
            process_instance._state_lock = RLock()
            process_instance._proc_state = {}
            process_instance._proc_state_obj = None
            process_instance._proc_state_changed = False

            # PROCESS RESTART: Need to check whether this process had persisted state.
            # Note: This could happen anytime during a system run, not just on RESTART boot
            log.debug("Loading persisted state for process %s", process_instance.id)
            process_instance._load_state()

    def _check_process_dependencies(self, app_instance):
        app_instance.errcause = "setting service dependencies"
        log.debug("spawn_process dependencies: %s", app_instance.dependencies)
        # TODO: Service dependency != process dependency
        for dependency in app_instance.dependencies:
            client = getattr(app_instance.clients, dependency)
            assert client, "Client for dependency not found: %s" % dependency

            # @TODO: should be in a start_client in RPCClient chain
            client.process = app_instance
            client.node = self.container.node

            # Ensure that dep actually exists and is running?

    def _process_init(self, process_instance):
        """ Initialize the process, primarily by calling on_init()
        """
        process_instance.errcause = "initializing service"
        process_instance.init()

    def _process_start(self, process_instance):
        """ Start the process, primarily by calling on_start()
        """
        # Should this be after spawn_process?
        # Should we check for timeout?
        process_instance.errcause = "starting service"
        process_instance.start()

    def _process_quit(self, process_instance):
        """ Common method to handle process stopping.
        """
        process_instance.errcause = "quitting process"

        # Give the process notice to quit doing stuff.
        process_instance.quit()

        # Terminate IonProcessThread (may not have one, i.e. simple process)
        # @TODO: move this into process' on_quit()
        if getattr(process_instance, '_process', None) is not None and process_instance._process:
            process_instance._process.notify_stop()
            process_instance._process.stop()

    def _set_publisher_endpoints(self, process_instance, publisher_streams=None):
        """ Creates and attaches named stream publishers
        """
        publisher_streams = publisher_streams or {}
        names = []

        for name, stream_id in publisher_streams.iteritems():
            # problem is here
            pub = StreamPublisher(process=process_instance, stream_id=stream_id)

            setattr(process_instance, name, pub)
            names.append(name)

        return names

    def _register_process(self, process_instance, name):
        """
        Performs all actions related to registering the new process in the system.
        Also performs process type specific registration, such as for services and agents
        """
        # Add process instance to container's process dict
        if name in self.procs_by_name:
            log.warn("Process name already registered in container: %s" % name)
        self.procs_by_name[name] = process_instance
        self.procs[process_instance.id] = process_instance

        # Add Process to resource registry
        # Note: In general the Process resource should be created by the CEI PD, but not all processes are CEI
        # processes. How to deal with this?
        process_instance.errcause = "registering"

        if process_instance._proc_type != PROCTYPE_IMMEDIATE:
            if self.container.has_capability(self.container.CCAP.RESOURCE_REGISTRY):
                proc_obj = Process(name=process_instance.id, label=name, proctype=process_instance._proc_type)
                proc_id, _ = self.container.resource_registry.create(proc_obj)
                process_instance._proc_res_id = proc_id

                # Associate process with container resource
                self.container.resource_registry.create_association(self.cc_id, PRED.hasProcess, proc_id)
        else:
            process_instance._proc_res_id = None

        # Process type specific registration
        if process_instance._proc_type == PROCTYPE_SERVICE:
            if self.container.has_capability(self.container.CCAP.RESOURCE_REGISTRY):
                # Registration of SERVICE process: in resource registry
                service_list, _ = self.container.resource_registry.find_resources(
                        restype=RT.Service, name=process_instance.name, id_only=True)
                if service_list:
                    process_instance._proc_svc_id = service_list[0]
                    if len(service_list) > 1:
                        log.warn("More than 1 Service resource found with name %s: %s", process_instance.name, service_list)
                else:
                    # We are starting the first process of a service instance
                    # TODO: This should be created by the HA Service agent in the future
                    svc_obj = Service(name=process_instance.name, exchange_name=process_instance._proc_listen_name,
                                      state=ServiceStateEnum.READY)
                    process_instance._proc_svc_id, _ = self.container.resource_registry.create(svc_obj)

                    # Create association to service definition resource
                    svcdef_list, _ = self.container.resource_registry.find_resources(
                            restype=RT.ServiceDefinition, name=process_instance.name, id_only=True)
                    if svcdef_list:
                        if len(svcdef_list) > 1:
                            log.warn("More than 1 ServiceDefinition resource found with name %s: %s", process_instance.name, svcdef_list)
                        self.container.resource_registry.create_association(
                                process_instance._proc_svc_id, PRED.hasServiceDefinition, svcdef_list[0])
                    else:
                        log.error("Cannot find ServiceDefinition resource for %s", process_instance.name)

                self.container.resource_registry.create_association(
                        process_instance._proc_svc_id, PRED.hasProcess, proc_id)

        elif process_instance._proc_type == PROCTYPE_AGENT:
            if self.container.has_capability(self.container.CCAP.DIRECTORY):
                # Registration of AGENT process: in Directory
                caps = process_instance.get_capabilities()
                self.container.directory.register("/Agents", process_instance.id,
                        **dict(name=process_instance._proc_name,
                               container=process_instance.container.id,
                               resource_id=process_instance.resource_id,
                               agent_id=process_instance.agent_id,
                               def_id=process_instance.agent_def_id,
                               capabilities=caps))

        self._call_proc_state_changed(process_instance, ProcessStateEnum.RUNNING)

    def terminate_process(self, process_id, do_notifications=True):
        """
        Terminates a process and all its resources. Termination is graceful with timeout.

        @param  process_id          The id of the process to terminate. Should exist in the container's
                                    list of processes or this will raise.
        @param  do_notifications    If True, emits process state changes for TERMINATING and TERMINATED.
                                    If False, supresses any state changes. Used near EXITED and FAILED.
        """
        process_instance = self.procs.get(process_id, None)
        if not process_instance:
            raise BadRequest("Cannot terminate. Process id='%s' unknown on container id='%s'" % (
                                        process_id, self.container.id))

        log.info("ProcManager.terminate_process: %s -> pid=%s", process_instance._proc_name, process_id)

        if do_notifications:
            self._call_proc_state_changed(process_instance, ProcessStateEnum.TERMINATING)

        self._process_quit(process_instance)

        self._unregister_process(process_id, process_instance)

        if do_notifications:
            self._call_proc_state_changed(process_instance, ProcessStateEnum.TERMINATED)

    def _unregister_process(self, process_id, process_instance):
        # Remove process registration in resource registry
        if process_instance._proc_res_id:
            if self.container.has_capability(self.container.CCAP.RESOURCE_REGISTRY):
                try:
                    self.container.resource_registry.delete(process_instance._proc_res_id, del_associations=True)
                except NotFound:
                    # OK if already gone
                    pass
                except Exception as ex:
                    log.exception(ex)
                    pass

        # Cleanup for specific process types
        if process_instance._proc_type == PROCTYPE_SERVICE:
            if self.container.has_capability(self.container.CCAP.RESOURCE_REGISTRY):
                # Check if this is the last process for this service and do auto delete service resources here
                svcproc_list, _ = self.container.resource_registry.find_objects(
                        process_instance._proc_svc_id, PRED.hasProcess, RT.Process, id_only=True)
                if not svcproc_list:
                    try:
                        self.container.resource_registry.delete(process_instance._proc_svc_id, del_associations=True)
                    except NotFound:
                        # OK if already gone
                        pass
                    except Exception as ex:
                        log.exception(ex)
                        pass

        elif process_instance._proc_type == PROCTYPE_AGENT:
            if self.container.has_capability(self.container.CCAP.DIRECTORY):
                self.container.directory.unregister_safe("/Agents", process_instance.id)

        # Remove internal registration in container
        del self.procs[process_id]
        if process_instance._proc_name in self.procs_by_name:
            del self.procs_by_name[process_instance._proc_name]
        else:
            log.warn("Process name %s not in local registry", process_instance.name)
