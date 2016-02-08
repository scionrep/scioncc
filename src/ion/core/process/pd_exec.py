""" Process dispatcher executor. """

__author__ = 'Michael Meisinger'

import gevent
from gevent.event import AsyncResult, Event
from gevent.pool import Pool
from gevent.queue import Queue

from pyon.public import BadRequest, EventPublisher, log, NotFound, OT, RT, get_safe
from pyon.util.async import spawn

from interface.objects import Process, ProcessStateEnum
from interface.services.agent.icontainer_agent import ContainerAgentClient


class ProcessDispatcherExecutorBase(object):
    """ Base class for PD Executors """

    def __init__(self, pd_core):
        self._pd_core = pd_core
        self.container = self._pd_core.container
        self.queue = Queue()
        self.quit_event = Event()
        self.exec_pool_size = min(int(get_safe(self._pd_core.pd_cfg, "executor.pool_size") or 1), 10)
        self.exec_pool = Pool(size=self.exec_pool_size)

    def start(self):
        self._pool_gl = spawn(self._action_loop)

    def stop(self):
        self.quit_event.set()
        self.queue.put("__QUIT__", None, None)
        self.exec_pool.kill()
        self.exec_pool.join(timeout=2)
        self._pool_gl.join(timeout=2)

    def add_action(self, action_tuple):
        if not action_tuple or len(action_tuple) != 3 or not isinstance(action_tuple[0], basestring) or \
                not isinstance(action_tuple[1], AsyncResult) or not isinstance(action_tuple[2], dict):
            raise BadRequest("Invalid action")
        self.queue.put(action_tuple)

    def execute_action(self, action_tuple):
        self.add_action(action_tuple)
        action_res = action_tuple[1]
        return action_res.get()  # Blocking on AsyncResult

    def _action_loop(self):
        for action in self.queue:
            if self.quit_event.is_set():
                break
            try:
                gl = self.exec_pool.spawn(self._process_action, action)
            except Exception as ex:
                log.exception("Error in PD Executor action")

    def _process_action(self, action):
        log.debug("PD execute action %s", action)
        action_name, action_asyncres, action_kwargs = action

        action_funcname = "_action_%s" % action_name
        action_func = getattr(self, action_funcname, None)
        if not action_func:
            log.warn("Action function not found")
            return
        try:
            action_res = action_func(action_kwargs)
            action_asyncres.set(action_res)
        except Exception as ex:
            log.exception("Error executing action")
            action_asyncres.set_exception(ex)


class ProcessDispatcherAgentExecutor(ProcessDispatcherExecutorBase):
    """ PD Executor using remote calls to CC Agents to manage processes """

    def _action_spawn_process(self, action_kwargs):
        cc_agent_name = action_kwargs["cc_agent"]
        proc_name = action_kwargs["proc_name"]
        module = action_kwargs["module"]
        cls = action_kwargs["cls"]
        config = action_kwargs["config"]

        target_cc_agent = ContainerAgentClient(to_name=cc_agent_name)
        proc_id = target_cc_agent.spawn_process(proc_name, module, cls, config)
        return proc_id

    def _action_terminate_process(self):
        pass



class ProcessDispatcherLocalExecutor(ProcessDispatcherExecutorBase):
    """ PD Executor using local container to manage processes """

    # We attempt to make the local backend act a bit more like the real thing.
    # Process spawn requests are asynchronous (not completed by the time the
    # operation returns). Therefore, callers need to listen for events to find
    # the success of failure of the process launch. To make races here more
    # detectable, we introduce an artificial delay between when
    # schedule_process() returns and when the process is actually launched.
    # SPAWN_DELAY = 0
    #
    # def __init__(self, container):
    #     self.container = container
    #     self.event_pub = EventPublisher()
    #     self._processes = []
    #
    #     self._spawn_greenlets = set()
    #
    #     # use the container RR instance -- talks directly to db
    #     self.rr = container.resource_registry
    #
    # def initialize(self):
    #     pass
    #
    # def shutdown(self):
    #     if self._spawn_greenlets:
    #         try:
    #             gevent.killall(list(self._spawn_greenlets), block=True)
    #         except Exception:
    #             log.warn("Ignoring error while killing spawn greenlets", exc_info=True)
    #         self._spawn_greenlets.clear()
    #
    # def set_system_boot(self, system_boot):
    #     pass
    #
    # def create_definition(self, definition, definition_id=None):
    #     pd_id, version = self.rr.create(definition, object_id=definition_id)
    #     return pd_id
    #
    # def read_definition(self, definition_id):
    #     return self.rr.read(definition_id)
    #
    # def read_definition_by_name(self, definition_name):
    #     raise ServerError("reading process definitions by name not supported by this backend")
    #
    # def update_definition(self, definition, definition_id):
    #     raise ServerError("updating process definitions not supported by this backend")
    #
    # def delete_definition(self, definition_id):
    #     return self.rr.delete(definition_id)
    #
    # def create(self, process_id, definition_id):
    #     if not self._get_process(process_id):
    #         self._add_process(process_id, {}, ProcessStateEnum.REQUESTED)
    #     return process_id
    #
    # def schedule(self, process_id, definition_id, schedule, configuration, name):
    #
    #     definition = self.read_definition(definition_id)
    #     process = self._get_process(process_id)
    #
    #     # in order for this local backend to behave more like the real thing,
    #     # we introduce an artificial delay in spawn requests. This helps flush
    #     # out races where callers try to use a process before it is necessarily
    #     # running.
    #
    #     if self.SPAWN_DELAY:
    #         glet = gevent.spawn_later(self.SPAWN_DELAY, self._inner_spawn,
    #             process_id, definition, schedule, configuration)
    #         self._spawn_greenlets.add(glet)
    #
    #         if process:
    #             process.process_configuration = configuration
    #         else:
    #             self._add_process(process_id, configuration, None)
    #
    #     else:
    #         if process:
    #             process.process_configuration = configuration
    #         else:
    #             self._add_process(process_id, configuration, None)
    #         self._inner_spawn(process_id, name, definition, schedule, configuration)
    #
    #     return process_id
    #
    # def _inner_spawn(self, process_id, process_name, definition, schedule, configuration):
    #
    #     name = process_name
    #     module = definition.module
    #     cls = definition.class_name
    #
    #     self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
    #         origin=process_id, origin_type="DispatchedProcess",
    #         state=ProcessStateEnum.PENDING)
    #
    #     # Spawn the process
    #     pid = self.container.spawn_process(name=name, module=module, cls=cls,
    #         config=configuration, process_id=process_id)
    #     log.debug('PD: Spawned Process (%s)', pid)
    #
    #     # update state on the existing process
    #     process = self._get_process(process_id)
    #     process.process_state = ProcessStateEnum.RUNNING
    #
    #     self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
    #         origin=process_id, origin_type="DispatchedProcess",
    #         state=ProcessStateEnum.RUNNING)
    #
    #     if self.SPAWN_DELAY:
    #         glet = gevent.getcurrent()
    #         if glet:
    #             self._spawn_greenlets.discard(glet)
    #
    #     return pid
    #
    # def cancel(self, process_id):
    #     process = self._get_process(process_id)
    #     if process:
    #         try:
    #             self.container.proc_manager.terminate_process(process_id)
    #             log.debug('PD: Terminated Process (%s)', process_id)
    #         except BadRequest, e:
    #             log.warn("PD: Failed to terminate process %s in container. already dead?: %s",
    #                 process_id, str(e))
    #         process.process_state = ProcessStateEnum.TERMINATED
    #
    #         try:
    #             self.event_pub.publish_event(event_type="ProcessLifecycleEvent",
    #                 origin=process_id, origin_type="DispatchedProcess",
    #                 state=ProcessStateEnum.TERMINATED)
    #         except BadRequest, e:
    #             log.warn(e)
    #
    #     else:
    #         raise NotFound("process %s unknown" % (process_id,))
    #
    #     return True
    #
    # def read_process(self, process_id):
    #     process = self._get_process(process_id)
    #     if process is None:
    #         raise NotFound("process %s unknown" % process_id)
    #     return process
    #
    # def _add_process(self, pid, config, state):
    #     proc = Process(process_id=pid, process_state=state,
    #             process_configuration=config)
    #
    #     self._processes.append(proc)
    #
    # def _remove_process(self, pid):
    #     self._processes = filter(lambda u: u.process_id != pid, self._processes)
    #
    # def _get_process(self, pid):
    #     wanted_procs = filter(lambda u: u.process_id == pid, self._processes)
    #     if len(wanted_procs) >= 1:
    #         return wanted_procs[0]
    #     else:
    #         return None
    #
    # def list(self):
    #     return self._processes


def pd_executor_factory(exec_type, pd_core):
    if exec_type == "local":
        return ProcessDispatcherLocalExecutor(pd_core)
    else:
        return ProcessDispatcherAgentExecutor(pd_core)
