""" Process dispatcher registry and aggregator. """

__author__ = 'Michael Meisinger'

import gevent
from gevent.lock import RLock

from pyon.public import BadRequest, EventPublisher, log, NotFound, OT, RT, Subscriber, EventSubscriber, get_ion_ts_millis, get_safe
from pyon.util.async import spawn
from pyon.util.containers import create_valid_identifier
from ion.core.process import EE_STATE_RUNNING, EE_STATE_TERMINATED, EE_STATE_UNKNOWN

from interface.objects import Process, ProcessStateEnum, ContainerHeartbeat, ContainerLifecycleEvent, ProcessLifecycleEvent


class ProcessDispatcherRegistry(object):
    """ PD Registry of containers and processes """

    def __init__(self, pd_core):
        self._pd_core = pd_core
        self.container = self._pd_core.container
        self.rr = self.container.resource_registry

        self._lock = RLock()        # Master lock protecting data structures
        self._containers = {}       # Registry of containers
        self._processes = {}        # Registry of processes

        self.preconditions_true = gevent.event.Event()

    def start(self):
        pass

    def stop(self):
        pass

    # -------------------------------------------------------------------------

    def get_process_info(self, process_id):
        return self._processes.get(process_id, None)

    def list_processes(self):
        return self._processes.values()

    def register_container(self, container_id, ts_event, state, container_info):
        if not container_id or not ts_event or not state:
            raise BadRequest("Invalid container registration")
        ts_event = int(ts_event)
        cc_obj = None
        if container_id not in self._containers:
            try:
                cc_objs, _ = self.rr.find_resources(restype=RT.CapabilityContainer, name=container_id, id_only=False)
                if cc_objs:
                    cc_obj = cc_objs[0]
            except Exception:
                log.exception("Could not retrieve CapabilityContainer resource for %s", container_id)
                return

        with self._lock:
            if container_id not in self._containers:
                self._containers[container_id] = dict(cc_obj=cc_obj, container_id=container_id, ts_event=ts_event)
            container_entry = self._containers[container_id]
            if ts_event >= container_entry["ts_event"] or state == EE_STATE_TERMINATED:
                # This is filled for every new container_entry
                container_entry["ts_update"] = get_ion_ts_millis()
                container_entry["ts_event"] = ts_event
                container_entry["state"] = state
                container_entry["ee_info"] = container_entry["cc_obj"].execution_engine_config
                container_entry["ts_created"] = int(container_entry["cc_obj"].ts_created)
                container_entry["allocation"] = {}
                container_entry["dead_procs"] = {}

        if not self.preconditions_true.is_set():
            self.check_preconditions()

    def get_running_containers(self):
        cont_infos = [c for c in self._containers.values() if c["state"] == EE_STATE_RUNNING]
        return cont_infos

    def get_engine_containers(self):
        """ Returns a dict of engine name to sorted list of container info dicts for running containers """
        cont_list = self.get_running_containers()
        ee_infos = {}
        for c in cont_list:
            cname = c["ee_info"].get("name", "")
            ee_infos.setdefault(cname, []).append(c)
        for eng_cont_list in ee_infos.values():
            eng_cont_list.sort(key=lambda ci: ci["ts_created"])
        return ee_infos

    def register_process(self, container_id, process_id, proc_info, update=True):
        container_entry = self._containers.get(container_id, None)
        if not container_entry:
            log.warn("Container {} not registered".format(container_id))
            return
        was_unknown = False
        new_state = (proc_info or {}).get("state", ProcessStateEnum.RUNNING)
        DEAD_STATES = (ProcessStateEnum.TERMINATING, ProcessStateEnum.TERMINATED, ProcessStateEnum.FAILED,
                       ProcessStateEnum.REJECTED, ProcessStateEnum.EXITED)
        if new_state in DEAD_STATES:
            if process_id not in container_entry["allocation"] and process_id not in container_entry["dead_procs"]:
                proc_entry = dict(process_id=process_id, state=ProcessStateEnum.RUNNING)
                container_entry["dead_procs"][process_id] = proc_entry
                was_unknown = True
            elif process_id in container_entry["allocation"]:
                dead_proc = container_entry["allocation"].pop(process_id)
                container_entry["dead_procs"][process_id] = dead_proc

            if proc_info and (update or was_unknown) and process_id in container_entry["dead_procs"]:
                container_entry["dead_procs"][process_id].update(proc_info)
        else:
            if process_id not in container_entry["allocation"] and process_id not in container_entry["dead_procs"]:
                proc_entry = dict(process_id=process_id, state=ProcessStateEnum.RUNNING)
                container_entry["allocation"][process_id] = proc_entry
                was_unknown = True

            if proc_info and (update or was_unknown) and process_id in container_entry["allocation"]:
                container_entry["allocation"][process_id].update(proc_info)

    def check_preconditions(self):
        if self.preconditions_true.is_set():
            return

        preconds = get_safe(self._pd_core.pd_cfg, "engine.await_preconditions") or {}
        precond_ok = True
        ee_infos = [c for c in self._containers.values() if c["state"] == EE_STATE_RUNNING]
        min_ees = preconds.get("min_engines", 0)
        if min_ees:
            if len(ee_infos) < min_ees:
                precond_ok = False
        engines_exist = preconds.get("engines_exist", None)
        if engines_exist:
            running_engines = {get_safe(e, "ee_info.name", "") for e in ee_infos}
            precond_ok = precond_ok and running_engines.issuperset(set(engines_exist))

        if precond_ok:
            log.info("ProcessDispatcher start preconditions now True")
            self.preconditions_true.set()


class ProcessDispatcherAggregator(object):
    """ PD aggregator for heartbeat input from containers etc. """

    def __init__(self, pd_core):
        self._pd_core = pd_core
        self.container = self._pd_core.container
        self.registry = self._pd_core.registry

    def start(self):
        # Create our own queue for container heartbeats and broadcasts
        topic = get_safe(self._pd_core.pd_cfg, "aggregator.container_topic") or "bx_containers"
        queue_name = "pd_aggregator_%s_%s" % (topic, create_valid_identifier(self.container.id, dot_sub="_"))
        self.sub_cont = Subscriber(binding=topic, from_name=queue_name, auto_delete=True,
                                   callback=self._receive_container_info)
        self.sub_cont_gl = spawn(self.sub_cont.listen)
        self.sub_cont.get_ready_event().wait()

        self.evt_sub = EventSubscriber(event_type=OT.ContainerLifecycleEvent, callback=self._receive_event)
        self.evt_sub.add_event_subscription(event_type=OT.ProcessLifecycleEvent)
        self.evt_sub_gl = spawn(self.evt_sub.listen)
        self.evt_sub.get_ready_event().wait()

        log.info("PD Aggregator - event and heartbeat subscribers started")

    def stop(self):
        # Stop subscribers
        self.sub_cont.close()
        self.evt_sub.close()

        # Wait for subscribers to finish
        self.sub_cont_gl.join(timeout=2)
        self.sub_cont_gl.kill()
        self.sub_cont_gl = None

        self.evt_sub_gl.join(timeout=2)
        self.evt_sub_gl.kill()
        self.evt_sub_gl = None

    # -------------------------------------------------------------------------

    def _receive_container_info(self, msg, headers, *args):
        log.debug("Got container info %s %s %s", msg, headers, args)
        if not isinstance(msg, ContainerHeartbeat):
            log.warn("Unknown container info format")
            return

        self.registry.register_container(msg.container_id, msg.ts, EE_STATE_RUNNING, msg.attributes)

    def _receive_event(self, event, *args, **kwargs):
        log.debug("Got event %s %s %s", event, args, kwargs)
        if isinstance(event, ContainerLifecycleEvent):
            if event.sub_type == "START":
                self.registry.register_container(event.origin, event.ts_created, EE_STATE_RUNNING, {})
            elif event.sub_type == "TERMINATE":
                self.registry.register_container(event.origin, event.ts_created, EE_STATE_TERMINATED, {})

        elif isinstance(event, ProcessLifecycleEvent):
            proc_info = dict(state=event.state, proc_type=event.process_type, proc_name=event.process_name,
                             resource_id=event.process_resource_id, service_name=event.service_name)
            self.registry.register_process(event.container_id, event.origin, proc_info)
