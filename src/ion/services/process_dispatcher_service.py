#!/usr/bin/env python

import uuid
import json
from time import time

import gevent
from gevent import event as gevent_event

from pyon.public import log
from pyon.core.exception import NotFound, BadRequest, ServerError
from pyon.util.containers import create_valid_identifier
from pyon.ion.event import EventPublisher, EventSubscriber


from interface.services.core.iprocess_dispatcher_service import BaseProcessDispatcherService
from interface.objects import ProcessStateEnum, Process


class ProcessStateGate(EventSubscriber):
    """
    Ensure that we get a particular state, now or in the future.

    Usage:
      gate = ProcessStateGate(your_process_dispatcher_client.read_process, process_id, ProcessStateEnum.some_state)
      assert gate.await(timeout_in_seconds)

    This pattern returns True immediately upon reaching the desired state, or False if the timeout is reached.
    This pattern avoids a race condition between read_process and using EventGate.
    """
    def __init__(self, read_process_fn=None, process_id='', desired_state=None, *args, **kwargs):

        if not process_id:
            raise BadRequest("ProcessStateGate trying to wait on invalid process (id = '%s')" % process_id)

        EventSubscriber.__init__(self, *args,
                                 callback=self.trigger_cb,
                                 event_type="ProcessLifecycleEvent",
                                 origin=process_id,
                                 origin_type="DispatchedProcess",
                                 **kwargs)

        self.desired_state = desired_state
        self.process_id = process_id
        self.read_process_fn = read_process_fn
        self.last_chance = None
        self.first_chance = None

        _ = ProcessStateEnum._str_map[self.desired_state] # make sure state exists
        log.info("ProcessStateGate is going to wait on process '%s' for state '%s'",
                self.process_id,
                ProcessStateEnum._str_map[self.desired_state])

    def trigger_cb(self, event, x):
        if event.state == self.desired_state:
            self.gate.set()
        else:
            log.info("ProcessStateGate received an event for state %s, wanted %s",
                     ProcessStateEnum._str_map[event.state],
                     ProcessStateEnum._str_map[self.desired_state])
            log.info("ProcessStateGate received (also) variable x = %s", x)

    def in_desired_state(self):
        # check whether the process we are monitoring is in the desired state as of this moment
        # Once pd creates the process, process_obj is never None
        try:
            process_obj = self.read_process_fn(self.process_id)
            return (process_obj and self.desired_state == process_obj.process_state)
        except NotFound:
            return False

    def await(self, timeout=0):
        #set up the event gate so that we don't miss any events
        start_time = time()
        self.gate = gevent_event.Event()
        self.start()

        #if it's in the desired state, return immediately
        if self.in_desired_state():
            self.first_chance = True
            self.stop()
            log.info("ProcessStateGate found process already %s -- NO WAITING",
                     ProcessStateEnum._str_map[self.desired_state])
            return True

        #if the state was not where we want it, wait for the event.
        ret = self.gate.wait(timeout)
        self.stop()

        if ret:
            # timer is already stopped in this case
            log.info("ProcessStateGate received %s event after %0.2f seconds",
                     ProcessStateEnum._str_map[self.desired_state],
                     time() - start_time)
        else:
            log.info("ProcessStateGate timed out waiting to receive %s event",
                     ProcessStateEnum._str_map[self.desired_state])

            # sanity check for this pattern
            self.last_chance = self.in_desired_state()

            if self.last_chance:
                log.warn("ProcessStateGate was successful reading %s on last_chance; " +
                         "should the state change for '%s' have taken %s seconds exactly?",
                         ProcessStateEnum._str_map[self.desired_state],
                         self.process_id,
                         timeout)

        return ret or self.last_chance

    def _get_last_chance(self):
        return self.last_chance

    def _get_first_chance(self):
        return self.first_chance


class ProcessDispatcherService(BaseProcessDispatcherService):
    #   local container mode - spawn directly in the local container
    #       without going through any external functionality. This is
    #       the default mode.

    def on_init(self):
        self.backend = PDLocalBackend(self.container)

    def on_start(self):
        self.backend.initialize()

    def on_quit(self):
        self.backend.shutdown()

    def create_process_definition(self, process_definition=None, process_definition_id=None):
        """Creates a Process Definition based on given object.

        @param process_definition    ProcessDefinition
        @param process_definition_id desired process definition ID
        @retval process_definition_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        if not (process_definition.module and process_definition.class_name):
            raise BadRequest("process definition must have module and class")
        return self.backend.create_definition(process_definition, process_definition_id)

    def read_process_definition(self, process_definition_id=''):
        """Returns a Process Definition as object.

        @param process_definition_id    str
        @retval process_definition    ProcessDefinition
        @throws NotFound    object with specified id does not exist
        """
        return self.backend.read_definition(process_definition_id)

    def delete_process_definition(self, process_definition_id=''):
        """Deletes/retires a Process Definition.

        @param process_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.backend.delete_definition(process_definition_id)

    def create_process(self, process_definition_id=''):
        """Create a process resource and process id. Does not yet start the process

        @param process_definition_id    str
        @retval process_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not process_definition_id:
            raise NotFound('No process definition was provided')
        process_definition = self.backend.read_definition(process_definition_id)

        # try to get a unique but still descriptive name
        process_id = str(process_definition.name or "process") + uuid.uuid4().hex
        process_id = create_valid_identifier(process_id, ws_sub='_')

        self.backend.create(process_id, process_definition_id)

        try:
            process = Process(process_id=process_id)
            self.container.resource_registry.create(process, object_id=process_id)
        except BadRequest:
            log.debug("Tried to create Process %s, but already exists. This is normally ok.", process_id)
        return process_id

    def schedule_process(self, process_definition_id='', schedule=None, configuration=None, process_id='', name=''):
        """Schedule a process definition for execution on an Execution Engine. If no process id is given,
        a new unique ID is generated.

        @param process_definition_id    str
        @param schedule    ProcessSchedule
        @param configuration    IngestionConfiguration
        @param process_id    str
        @retval process_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        if not process_definition_id:
            raise NotFound('No process definition was provided')
        process_definition = self.backend.read_definition(process_definition_id)

        if configuration is None:
            configuration = {}
        else:
            # push the config through a JSON serializer to ensure that the same
            # config would work with the bridge backend

            try:
                json.dumps(configuration)
            except TypeError, e:
                raise BadRequest("bad configuration: " + str(e))

        # If not provided, create a unique but still descriptive (valid) id
        if not process_id:
            process_id = str(process_definition.name or "process") + uuid.uuid4().hex
            process_id = create_valid_identifier(process_id, ws_sub='_')

        # If not provided, create a unique but still descriptive (valid) name
        if not name:
            name = self._get_process_name(process_definition, configuration)

        try:
            process = Process(process_id=process_id, name=name)
            self.container.resource_registry.create(process, object_id=process_id)
        except BadRequest:
            log.debug("Tried to create Process %s, but already exists. This is normally ok.",
                process_id)

        return self.backend.schedule(process_id, process_definition_id,
            schedule, configuration, name)

    def cancel_process(self, process_id=''):
        """Cancels the execution of the given process id.

        @param process_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        if not process_id:
            raise NotFound('No process was provided')

        cancel_result = self.backend.cancel(process_id)
        return cancel_result

    def read_process(self, process_id=''):
        """Returns a Process as an object.

        @param process_id    str
        @retval process    Process
        @throws NotFound    object with specified id does not exist
        """
        if not process_id:
            raise NotFound('No process was provided')

        return self.backend.read_process(process_id)

    def list_processes(self):
        """Lists managed processes

        @retval processes    list
        """
        return self.backend.list()

    def _get_process_name(self, process_definition, configuration):

        base_name = ""
        name_suffix = ""
        ha_config = configuration.get('highavailability')
        if ha_config:
            if ha_config.get('process_definition_name'):
                base_name = ha_config['process_definition_name']
                name_suffix = "ha"
            elif ha_config.get('process_definition_id'):
                inner_definition = self.backend.read_definition(
                    ha_config['process_definition_id'])
                base_name = inner_definition.name
                name_suffix = "ha"

        name_parts = [str(base_name or process_definition.name or "process")]
        if name_suffix:
            name_parts.append(name_suffix)
        name_parts.append(uuid.uuid4().hex)
        name = '-'.join(name_parts)

        return name


class PDLocalBackend(object):
    """Scheduling backend to PD that manages processes in the local container

    This implementation is the default and is used in single-container
    deployments where there is no CEI launch to leverage.
    """

    # We attempt to make the local backend act a bit more like the real thing.
    # Process spawn requests are asynchronous (not completed by the time the
    # operation returns). Therefore, callers need to listen for events to find
    # the success of failure of the process launch. To make races here more
    # detectable, we introduce an artificial delay between when
    # schedule_process() returns and when the process is actually launched.
    SPAWN_DELAY = 0

    def __init__(self, container):
        self.container = container
        self.event_pub = EventPublisher()
        self._processes = []

        self._spawn_greenlets = set()

        # use the container RR instance -- talks directly to db
        self.rr = container.resource_registry

    def initialize(self):
        pass

    def shutdown(self):
        if self._spawn_greenlets:
            try:
                gevent.killall(list(self._spawn_greenlets), block=True)
            except Exception:
                log.warn("Ignoring error while killing spawn greenlets", exc_info=True)
            self._spawn_greenlets.clear()

    def set_system_boot(self, system_boot):
        pass

    def create_definition(self, definition, definition_id=None):
        pd_id, version = self.rr.create(definition, object_id=definition_id)
        return pd_id

    def read_definition(self, definition_id):
        return self.rr.read(definition_id)

    def read_definition_by_name(self, definition_name):
        raise ServerError("reading process definitions by name not supported by this backend")

    def update_definition(self, definition, definition_id):
        raise ServerError("updating process definitions not supported by this backend")

    def delete_definition(self, definition_id):
        return self.rr.delete(definition_id)

    def create(self, process_id, definition_id):
        if not self._get_process(process_id):
            self._add_process(process_id, {}, ProcessStateEnum.REQUESTED)
        return process_id

    def schedule(self, process_id, definition_id, schedule, configuration, name):

        definition = self.read_definition(definition_id)
        process = self._get_process(process_id)

        # in order for this local backend to behave more like the real thing,
        # we introduce an artificial delay in spawn requests. This helps flush
        # out races where callers try to use a process before it is necessarily
        # running.

        if self.SPAWN_DELAY:
            glet = gevent.spawn_later(self.SPAWN_DELAY, self._inner_spawn,
                process_id, definition, schedule, configuration)
            self._spawn_greenlets.add(glet)

            if process:
                process.process_configuration = configuration
            else:
                self._add_process(process_id, configuration, None)

        else:
            if process:
                process.process_configuration = configuration
            else:
                self._add_process(process_id, configuration, None)
            self._inner_spawn(process_id, name, definition, schedule, configuration)

        return process_id

    def _inner_spawn(self, process_id, process_name, definition, schedule, configuration):

        name = process_name
        module = definition.module
        cls = definition.class_name

        self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
            origin=process_id, origin_type="DispatchedProcess",
            state=ProcessStateEnum.PENDING)

        # Spawn the process
        pid = self.container.spawn_process(name=name, module=module, cls=cls,
            config=configuration, process_id=process_id)
        log.debug('PD: Spawned Process (%s)', pid)

        # update state on the existing process
        process = self._get_process(process_id)
        process.process_state = ProcessStateEnum.RUNNING

        self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
            origin=process_id, origin_type="DispatchedProcess",
            state=ProcessStateEnum.RUNNING)

        if self.SPAWN_DELAY:
            glet = gevent.getcurrent()
            if glet:
                self._spawn_greenlets.discard(glet)

        return pid

    def cancel(self, process_id):
        process = self._get_process(process_id)
        if process:
            try:
                self.container.proc_manager.terminate_process(process_id)
                log.debug('PD: Terminated Process (%s)', process_id)
            except BadRequest, e:
                log.warn("PD: Failed to terminate process %s in container. already dead?: %s",
                    process_id, str(e))
            process.process_state = ProcessStateEnum.TERMINATED

            try:
                self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
                    origin=process_id, origin_type="DispatchedProcess",
                    state=ProcessStateEnum.TERMINATED)
            except BadRequest, e:
                log.warn(e)

        else:
            raise NotFound("process %s unknown" % (process_id,))

        return True

    def read_process(self, process_id):
        process = self._get_process(process_id)
        if process is None:
            raise NotFound("process %s unknown" % process_id)
        return process

    def _add_process(self, pid, config, state):
        proc = Process(process_id=pid, process_state=state,
                process_configuration=config)

        self._processes.append(proc)

    def _remove_process(self, pid):
        self._processes = filter(lambda u: u.process_id != pid, self._processes)

    def _get_process(self, pid):
        wanted_procs = filter(lambda u: u.process_id == pid, self._processes)
        if len(wanted_procs) >= 1:
            return wanted_procs[0]
        else:
            return None

    def list(self):
        return self._processes


# map from internal PD states to external ProcessStateEnum values

_PD_PROCESS_STATE_MAP = {
    "100-UNSCHEDULED": ProcessStateEnum.REQUESTED,
    "150-UNSCHEDULED_PENDING": ProcessStateEnum.REQUESTED,
    "200-REQUESTED": ProcessStateEnum.REQUESTED,
    "250-DIED_REQUESTED": ProcessStateEnum.REQUESTED,
    "300-WAITING": ProcessStateEnum.WAITING,
    "350-ASSIGNED": ProcessStateEnum.PENDING,
    "400-PENDING": ProcessStateEnum.PENDING,
    "500-RUNNING": ProcessStateEnum.RUNNING,
    "600-TERMINATING": ProcessStateEnum.TERMINATING,
    "700-TERMINATED": ProcessStateEnum.TERMINATED,
    "800-EXITED": ProcessStateEnum.EXITED,
    "850-FAILED": ProcessStateEnum.FAILED,
    "900-REJECTED": ProcessStateEnum.REJECTED
}

_PD_PYON_PROCESS_STATE_MAP = {
    ProcessStateEnum.REQUESTED: "200-REQUESTED",
    ProcessStateEnum.WAITING: "300-WAITING",
    ProcessStateEnum.PENDING: "400-PENDING",
    ProcessStateEnum.RUNNING: "500-RUNNING",
    ProcessStateEnum.TERMINATING: "600-TERMINATING",
    ProcessStateEnum.TERMINATED: "700-TERMINATED",
    ProcessStateEnum.EXITED: "800-EXITED",
    ProcessStateEnum.FAILED: "850-FAILED",
    ProcessStateEnum.REJECTED: "900-REJECTED"
}


def process_state_to_pd_core(process_state):
    return _PD_PYON_PROCESS_STATE_MAP[process_state]


def process_state_from_pd_core(core_process_state):
    return _PD_PROCESS_STATE_MAP[core_process_state]


class Notifier(object):
    """Sends Process state notifications via ION events

    This object is fed into the internal PD core classes
    """
    def __init__(self):
        self.event_pub = EventPublisher()

    def notify_process(self, process):
        process_id = process.upid
        state = process.state

        ion_process_state = _PD_PROCESS_STATE_MAP.get(state)
        if not ion_process_state:
            log.debug("Received unknown process state from Process Dispatcher." +
                      " process=%s state=%s", process_id, state)
            return

        log.debug("Emitting event for process state. process=%s state=%s", process_id, ion_process_state)
        try:
            self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
                origin=process_id, origin_type="DispatchedProcess",
                state=ion_process_state)
        except Exception:
            log.exception("Problem emitting event for process state. process=%s state=%s",
                process_id, ion_process_state)


# should be configurable to support multiple process dispatchers?
DEFAULT_HEARTBEAT_QUEUE = "heartbeats"
