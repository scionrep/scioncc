#!/usr/bin/env python

"""Capability Container"""

__author__ = 'Adam R. Smith, Michael Meisinger, Dave Foster <dfoster@asascience.com>'

import atexit
import msgpack
import os
import signal
import traceback
import sys
import gevent
from gevent.event import Event
from contextlib import contextmanager

from pyon.container import ContainerCapability
from pyon.core import bootstrap
from pyon.core.bootstrap import CFG
from pyon.core.exception import ContainerError, BadRequest
from pyon.ion.event import EventPublisher
from pyon.ion.endpoint import ProcessRPCServer
from pyon.net.transport import LocalRouter
from pyon.util.config import Config
from pyon.util.containers import get_default_container_id, DotDict, named_any, dict_merge, get_ion_ts
from pyon.util.log import log
from pyon.util.context import LocalContextMixin
from pyon.util.greenlet_plugin import GreenletLeak
from pyon.util.file_sys import FileSystem

from interface.objects import ContainerStateEnum
from interface.services.icontainer_agent import BaseContainerAgent

# Capability constants for use in:
# if self.container.has_capability(CCAP.RESOURCE_REGISTRY):
CCAP = DotDict()

# Container status
INIT = "INIT"
RUNNING = "RUNNING"
TERMINATING = "TERMINATING"
TERMINATED = "TERMINATED"


class Container(BaseContainerAgent):
    """
    The Capability Container. Its purpose is to spawn/monitor processes and services
    that do the bulk of the work in the ION system. It also manages connections to the Exchange
    and the various forms of datastores in the systems.
    """

    # Class static variables (defaults)
    id          = None
    name        = None
    pidfile     = None
    instance    = None
    version     = None
    start_time  = None

    def __init__(self, *args, **kwargs):
        BaseContainerAgent.__init__(self, *args, **kwargs)

        # Coordinates the container start
        self._status = INIT

        self._is_started = False
        # set container id and cc_agent name (as they are set in base class call)
        self.id = get_default_container_id()
        self.name = "cc_agent_%s" % self.id
        self.start_time = get_ion_ts()

        bootstrap.container_instance = self
        Container.instance = self
        self.container = self  # Make self appear as process to service clients
        self.CCAP = CCAP
        self.CFG = CFG

        log.debug("Container (sysname=%s) initializing ..." % bootstrap.get_sys_name())

        # Keep track of the overrides from the command-line, so they can trump app/rel file data
        self.spawn_args = kwargs

        # Greenlet context-local storage
        self.context = LocalContextMixin()

        # Load general capabilities file and augment with specific profile
        self._load_capabilities()

        # Start the capabilities
        start_order = self.cap_profile['start_order']
        for cap in start_order:
            if cap not in self._cap_definitions:
                raise ContainerError("CC capability %s not defined in profile" % cap)
            if cap in self._capabilities or cap in self._cap_instances:
                raise ContainerError("CC capability %s already initialized" % cap)
            try:
                cap_def = self._cap_definitions[cap]
                log.debug("__init__(): Initializing '%s'" % cap)
                cap_obj = named_any(cap_def['class'])(container=self)
                self._cap_instances[cap] = cap_obj
                if 'depends_on' in cap_def and cap_def['depends_on']:
                    dep_list = cap_def['depends_on'].split(',')
                    for dep in dep_list:
                        dep = dep.strip()
                        if dep not in self._cap_initialized:
                            raise ContainerError("CC capability %s dependent on non-existing capability %s" % (cap, dep))
                if 'field' in cap_def and cap_def['field']:
                    setattr(self, cap_def['field'], cap_obj)
                self._cap_initialized.append(cap)
            except Exception as ex:
                log.error("Container Capability %s init error: %s" % (cap, ex))
                raise

        log.debug("Container initialized, OK.")

    def _load_capabilities(self):
        self._cap_initialized = []  # List of capability constants initialized in container
        self._capabilities = []     # List of capability constants active in container
        self._cap_instances = {}    # Dict mapping capability->manager instance

        self._cap_definitions = Config(["res/config/container_capabilities.yml"]).data['capabilities']

        profile_filename = CFG.get_safe("container.profile", "development")
        if not profile_filename.endswith(".yml"):
            profile_filename = "res/profile/%s.yml" % profile_filename
        log.debug("Loading CC capability profile from file: %s", profile_filename)
        profile_cfg = Config([profile_filename]).data
        if not isinstance(profile_cfg, dict) or profile_cfg['type'] != "profile" or not "profile" in profile_cfg:
            raise ContainerError("Container capability profile invalid: %s" % profile_filename)

        self.cap_profile = profile_cfg['profile']

        if "capabilities" in self.cap_profile and self.cap_profile['capabilities']:
            dict_merge(self._cap_definitions, self.cap_profile['capabilities'], True)

        CCAP.clear()
        cap_list = self._cap_definitions.keys()
        CCAP.update(zip(cap_list, cap_list))

        if "config" in self.cap_profile and self.cap_profile['config']:
            log.info("Container CFG was changed based on profile: %s", profile_filename)
            # Note: The config update actually happens in pycc.py early on

    def start(self):
        log.debug("Container starting...")
        if self._is_started:
            raise ContainerError("Container already started")

        start_order = self.cap_profile['start_order']
        for cap in start_order:
            if cap not in self._cap_instances:
                continue
            # First find the default enabled value if no CFG key exists
            enabled_default = self._cap_definitions.get_safe("%s.enabled_default" % cap, True)
            # Then find CFG key where enabled flag is (default or override)
            enabled_config = self._cap_definitions.get_safe("%s.enabled_config" % cap, "container.%s.enabled" % cap)
            # Then determine the enabled value
            enabled = CFG.get_safe(enabled_config, enabled_default)
            if enabled:
                log.debug("start(): Starting '%s'" % cap)
                try:
                    cap_obj = self._cap_instances[cap]
                    cap_obj.start()
                    self._capabilities.append(cap)
                except Exception as ex:
                    log.error("Container Capability %s start error: %s" % (cap, ex))
                    raise
            else:
                log.debug("start(): Capability '%s' disabled by config '%s'", cap, enabled_config)

        if self.has_capability(CCAP.EVENT_PUBLISHER):
            self.event_pub.publish_event(event_type="ContainerLifecycleEvent",
                                         origin=self.id, origin_type="CapabilityContainer",
                                         sub_type="START",
                                         state=ContainerStateEnum.START)

        self._is_started    = True
        self._status        = RUNNING

        log.info("Container (%s) started, OK.", self.id)

    def has_capability(self, capability):
        """
        Returns True if the given capability is in the list of container capabilities,
        i.e. available in this container.
        """
        return capability in self._capabilities

    @property
    def node(self):
        """
        Returns the active/default Node that should be used for most communication in the system.

        Defers to exchange manager, but only if it has been started, otherwise returns None.
        """
        if self.has_capability(CCAP.EXCHANGE_MANAGER):
            return self.ex_manager.default_node

        return None

    @contextmanager
    def _push_status(self, new_status):
        """
        Temporarily sets the internal status flag.
        Use this as a decorator or in a with-statement before calling a temporary status changing
        method, like start_rel_from_url.
        """
        curstatus = self._status
        self._status = new_status
        try:
            yield
        finally:
            self._status = curstatus

    def serve_forever(self):
        """ Run the container until killed. """
        log.debug("In Container.serve_forever")

        if not self.proc_manager.proc_sup.running:
            self.start()

        # Exit if immediate==True and children len is ok
        num_procs = len(self.proc_manager.proc_sup.children)
        immediate = CFG.system.get('immediate', False)
        if immediate and num_procs == 1:  # only spawned greenlet is the CC-Agent
            log.debug("Container.serve_forever exiting due to CFG.system.immediate")

        else:
            # print a warning just in case
            if immediate and num_procs != 1:
                log.warn("CFG.system.immediate=True but number of spawned processes is not 1 (%d)", num_procs)

            try:
                # This just waits in this Greenlet for all child processes to complete,
                # which is triggered somewhere else.
                self.proc_manager.proc_sup.join_children()
            except (KeyboardInterrupt, SystemExit) as ex:
                if hasattr(self, 'gl_parent_watch') and self.gl_parent_watch is not None:
                    # Remove the greenlet that watches the parent process
                    self.gl_parent_watch.kill()

                # Let the caller handle this
                raise

            except:
                log.exception('Unhandled error! Forcing container shutdown')

    def status(self):
        """
        Returns the internal status.
        """
        return self._status

    def is_running(self):
        """
        Is the container in the process of shutting down or stopped.
        """
        if self._status == RUNNING:
            return True
        return False

    def is_terminating(self):
        """
        Is the container in the process of shutting down or stopped.
        """
        if self._status == TERMINATING or self._status == TERMINATED:
            return True
        return False

    def _cleanup_pid(self):
        if self.pidfile:
            log.debug("Cleanup pidfile: %s", self.pidfile)
            try:
                os.remove(self.pidfile)
            except Exception as e:
                log.warn("Pidfile could not be deleted: %s" % str(e))
            self.pidfile = None

    def stop_container(self):
        log.info("Received request to stop container")
        gl = gevent.spawn_later(0.5, self.stop)

    def stop(self, do_exit=True):
        log.info("=============== Container stopping... ===============")

        self._status = TERMINATING

        if self.has_capability(CCAP.EVENT_PUBLISHER) and self.event_pub is not None:
            try:
                self.event_pub.publish_event(event_type="ContainerLifecycleEvent",
                                             origin=self.id, origin_type="CapabilityContainer",
                                             sub_type="TERMINATE",
                                             state=ContainerStateEnum.TERMINATE)
            except Exception as ex:
                log.exception(ex)

        while self._capabilities:
            capability = self._capabilities.pop()
            #log.debug("stop(): Stopping '%s'" % capability)
            try:
                cap_obj = self._cap_instances[capability]
                cap_obj.stop()
                del self._cap_instances[capability]
            except Exception as ex:
                log.exception("Container stop(): Error stop %s" % capability)

        Container.instance = None
        from pyon.core import bootstrap
        bootstrap.container_instance = None

        self._is_started = False

        self._status = TERMINATED

        log.info("Container stopped (%s).", self.id)
        if do_exit:
            os.kill(os.getpid(), signal.SIGTERM)

    def start_app(self, appdef=None, config=None):
        with self._push_status("START_APP"):
            return self.app_manager.start_app(appdef=appdef, config=config)

    def start_app_from_url(self, app_url=''):
        with self._push_status("START_APP_FROM_URL"):
            return self.app_manager.start_app_from_url(app_url=app_url)

    def start_rel(self, rel=None):
        with self._push_status("START_REL"):
            return self.app_manager.start_rel(rel=rel)

    def start_rel_from_url(self, rel_url='', config=None):
        with self._push_status("START_REL_FROM_URL"):
            return self.app_manager.start_rel_from_url(rel_url=rel_url, config=config)


    def fail_fast(self, err_msg="", skip_stop=False):
        """
        Container needs to shut down and NOW.
        """
        log.error("Fail Fast: %s", err_msg)
        if not skip_stop:
            self.stop()
        log.error("Fail Fast: killing container")

        traceback.print_exc()

        self._kill_fast()

    def _kill_fast(self):
        # The exit code of the terminated process is set to non-zero
        os.kill(os.getpid(), signal.SIGTERM)


class PidfileCapability(ContainerCapability):
    def start(self):
        # Check if this UNIX process already runs a Container.
        self.container.pidfile = "cc-pid-%d" % os.getpid()
        if os.path.exists(self.container.pidfile):
            raise ContainerError("Container.on_start(): Container is a singleton per UNIX process. Existing pid file found: %s" % self.container.pidfile)

        # write out a PID file containing our agent messaging name
        with open(self.container.pidfile, 'w') as f:
            pid_contents = {'messaging': dict(CFG.server.amqp),
                            'container-agent': self.container.name,
                            'container-xp': bootstrap.get_sys_name()}
            f.write(msgpack.dumps(pid_contents))
            atexit.register(self.container._cleanup_pid)

    def stop(self):
        self.container._cleanup_pid()


class SignalHandlerCapability(ContainerCapability):
    def start(self):
        def handle_sigterm():
            """Handles SIGTERM, initiating orderly Python exit handling,
            allowing the container to shutdown.
            Notes:
            - Without this handler, the process is immediately terminated on SIGTERM
            - Gevent has signal handling, so must use gevent version or chain
            """
            try:
                log.info("In TERM signal handler, triggering exit (%s)", self.container.id)
                self.container._cleanup_pid()      # cleanup the pidfile first
            finally:
                # This will raise SystemExit in serve_forever and IPython cores
                # Thereby pycc will be able to shutdown the container
                sys.exit(signal.SIGTERM)

        # Set up SIGTERM handler
        gevent.signal(signal.SIGTERM, handle_sigterm)

        def handle_sigusr2():
            """Handles SIGUSR2, prints debugging greenlet information.
            """
            gls = GreenletLeak.get_greenlets()

            allgls = []

            for gl in gls:
                status = GreenletLeak.format_greenlet(gl)

                # build formatted output:
                # Greenlet at 0xdeadbeef
                #     self: <EndpointUnit at 0x1ffcceef>
                #     func: bound, EndpointUnit.some_func

                status[0].insert(0, "%s at %s:" % (gl.__class__.__name__, hex(id(gl))))
                # indent anything in status a second time
                prefmt = [s.replace("\t", "\t\t") for s in status[0]]
                prefmt.append("traceback:")

                for line in status[1]:
                    for subline in line.split("\n")[0:2]:
                        prefmt.append(subline)

                glstr = "\n\t".join(prefmt)

                allgls.append(glstr)

            # print it out!
            print >>sys.stderr, "\n\n".join(allgls)
            with open("gls-%s" % os.getpid(), "w") as f:
                f.write("\n\n".join(allgls))

        # Set up greenlet debugging signal handler
        gevent.signal(signal.SIGUSR2, handle_sigusr2)


class EventPublisherCapability(ContainerCapability):
    def __init__(self, container):
        ContainerCapability.__init__(self, container)
        self.container.event_pub = None
    def start(self):
        self.container.event_pub = EventPublisher()
    def stop(self):
        self.container.event_pub.close()


class ObjectStoreCapability(ContainerCapability):
    def __init__(self, container):
        ContainerCapability.__init__(self, container)
        self.container.object_store = None
    def start(self):
        from pyon.ion.objstore import ObjectStore
        self.container.object_store = ObjectStore()
    def stop(self):
        self.container.object_store.close()
        self.container.object_store = None


class LocalRouterCapability(ContainerCapability):
    def __init__(self, container):
        ContainerCapability.__init__(self, container)
        self.container.local_router = None
    def start(self):
        # internal router for local transports
        self.container.local_router = LocalRouter(bootstrap.get_sys_name())
        self.container.local_router.start()
        self.container.local_router.ready.wait(timeout=2)
    def stop(self):
        self.container.local_router.stop()


class ContainerAgentCapability(ContainerCapability):
    def start(self):
        # Start the CC-Agent API
        listen_name = self.container.create_process_xn(self.container.name, auto_delete=True)
        rsvc = ProcessRPCServer(from_name=listen_name, service=self.container, process=self.container)

        # Start an ION process with the right kind of endpoint factory
        proc = self.container.proc_manager.proc_sup.spawn(name=self.container.name, listeners=[rsvc], service=self.container)
        self.container.proc_manager.proc_sup.ensure_ready(proc)
        proc.start_listeners()

        # Start a heartbeat
        self.heartbeat_cfg = CFG.get_safe("container.execution_engine.heartbeat") or {}
        self.heartbeat_enabled = self.heartbeat_cfg.get("enabled", False) is True
        if self.heartbeat_enabled:
            self.heartbeater = ContainerHeartbeater(self.container, self.heartbeat_cfg)
            self.heartbeater.start()
    def stop(self):
        if self.heartbeat_enabled:
            self.heartbeater.stop()


class FileSystemCapability(ContainerCapability):
    def __init__(self, container):
        ContainerCapability.__init__(self, container)
        self.container.file_system = FileSystem(CFG)


class ContainerHeartbeater(object):
    """ Utility class that implements the container heartbeat publishing mechanism """
    def __init__(self, container, cfg):
        self.container = container
        self.heartbeat_cfg = cfg
        self.started = False

    def start(self):
        from pyon.net.endpoint import Publisher
        from pyon.util.async import spawn
        self.heartbeat_quit = Event()
        self.heartbeat_interval = float(self.heartbeat_cfg.get("publish_interval", 60))
        self.heartbeat_topic = self.heartbeat_cfg.get("topic", "heartbeat")
        self.heartbeat_pub = Publisher(to_name=self.heartbeat_topic)

        # Directly spawn a greenlet - we don't want this to be a supervised IonProcessThread
        self.heartbeat_gl = spawn(self.heartbeat_loop)
        self.started = True
        log.info("Started container heartbeat (interval=%s, topic=%s)", self.heartbeat_interval, self.heartbeat_topic)

    def stop(self):
        if self.started:
            self.heartbeat_quit.set()
            self.heartbeat_gl.join(timeout=1)
            self.started = False

    def heartbeat_loop(self):
        self.publish_heartbeat()
        while not self.heartbeat_quit.wait(timeout=self.heartbeat_interval):
            self.publish_heartbeat()

    def publish_heartbeat(self):
        try:
            hb_msg = self.get_heartbeat_message()
            headers = dict(expiration=60000)
            self.heartbeat_pub.publish(hb_msg, headers=headers)
        except Exception:
            log.exception("Error publishing heatbeat")

    def get_heartbeat_message(self):
        from interface.objects import ContainerHeartbeat
        hb_msg = ContainerHeartbeat(container_id=self.container.id, ts=get_ion_ts())
        return hb_msg
