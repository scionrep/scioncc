""" Process dispatcher decision engine. """

__author__ = 'Michael Meisinger'

from copy import deepcopy
import re
import gevent
from gevent.event import AsyncResult
from gevent.queue import Queue

from pyon.public import BadRequest, log, NotFound, OT, RT, Subscriber, Publisher, get_safe, CFG
from pyon.util.async import spawn
from ion.core.process import EE_STATE_RUNNING, EE_STATE_TERMINATED, EE_STATE_UNKNOWN

from interface.objects import Process, ProcessStateEnum, AsyncResultMsg


class ProcessDispatcherDecisionEngine(object):
    """ Base class for PD decision engines """

    def __init__(self, pd_core):
        self._pd_core = pd_core
        self.container = self._pd_core.container
        self.registry = self._pd_core.registry
        self.executor = self._pd_core.executor

        self.sub_cont = None
        self.sub_cont_gl = None
        self.sub_active = False

        self._pd_core.leader_manager.add_leader_callback(self._leader_callback)

        self._load_rules()

    def start(self):
        queue_name = get_safe(self._pd_core.pd_cfg, "command_queue") or "pd_command"
        self.sub_cont = Subscriber(binding=queue_name, from_name=queue_name, callback=self._receive_command)
        self.sub_cont_gl = spawn(self.sub_cont.listen, activate=False)
        self.sub_cont.get_ready_event().wait()

        self.pub_result = Publisher()

    def stop(self):
        if self.sub_cont:
            self.sub_cont.close()
            self.sub_cont_gl.join(timeout=2)
            self.sub_cont_gl.kill()
            self.sub_cont_gl = None
            self.sub_cont = None

    def _leader_callback(self, leader_info):
        if leader_info["action"] == "acquire_leader":
            def start_sub():
                if not self.registry.preconditions_true.is_set():
                    log.info("PD is leader - awaiting PD preconditions")
                    # Await preconditions
                    self.registry.preconditions_true.wait()

                if self._pd_core.is_leader() and self.sub_cont is not None and not self.sub_active:
                    # Are we still leader? Not activated?
                    num_msg, num_cons = self.sub_cont.get_stats()
                    log.info("PD is leader - starting to consume (%s pending commands, %s consumers)", num_msg, num_cons)
                    self.sub_cont.activate()
                    self.sub_active = True
            start_sub_gl = spawn(start_sub)
        elif leader_info["action"] == "release_leader":
            if self.sub_cont is not None and self.sub_active:
                self.sub_cont.deactivate()
                self.sub_active = False

    def _receive_command(self, command, headers, *args):
        log.info("PD execute command %s", command["command"])

        cmd_funcname = "_cmd_{}".format(command["command"])
        cmd_func = getattr(self, cmd_funcname, None)
        cmd_res, cmd_complete = None, False
        if not cmd_func:
            log.warn("Command function not found")
            self._send_command_reply(command, dict(message="ERROR"), status=400)
            return
        try:
            cmd_res = cmd_func(command)
            cmd_complete = True
        except Exception as ex:
            log.exception("Error executing command")
            self._send_command_reply(command, dict(message=str(ex)), status=400)

        if cmd_complete:
            self._send_command_reply(command, cmd_res)

    def _send_command_reply(self, command, result=None, status=200):
        reply_to = command.get("reply_to", None)
        if not reply_to:
            return
        res_msg = AsyncResultMsg(request_id=command["command_id"], result=result, status=status)
        self.pub_result.publish(to_name=reply_to, msg=res_msg)

    # -------------------------------------------------------------------------

    def _cmd_start_rel(self, command):
        log.debug("Start rel")
        rel = command["rel_def"]

        max_proc_replicas = int(CFG.get_safe("container.process.max_replicas", 0))

        for rel_app_cfg in rel["apps"]:
            app_name = rel_app_cfg["name"]
            log.debug("app definition in rel: %s", str(rel_app_cfg))

            # Decide where process should go
            container_name = self._determine_target_container(rel_app_cfg)
            log.debug("Dispatch app %s to container %s", app_name, container_name)

            if 'processapp' in rel_app_cfg:
                name, module, cls = rel_app_cfg["processapp"]

                rel_cfg = None
                if 'config' in rel_app_cfg:
                    rel_cfg = deepcopy(rel_app_cfg["config"])

                if 'replicas' in rel_app_cfg:
                    proc_replicas = int(rel_app_cfg["replicas"])
                    if max_proc_replicas > 0:
                        if proc_replicas > max_proc_replicas:
                            log.info("Limiting number of proc replicas to %s from %s", max_proc_replicas, proc_replicas)
                        proc_replicas = min(proc_replicas, max_proc_replicas)
                    if proc_replicas < 1 or proc_replicas > 100:
                        log.warn("Invalid number of process replicas: %s", proc_replicas)
                        proc_replicas = 1
                    for i in xrange(proc_replicas):
                        proc_name = "%s.%s" % (name, i) if i else name
                        action_res = self._add_spawn_process_action(cc_agent=container_name, proc_name=proc_name,
                                             module=module, cls=cls, config=rel_cfg)
                        proc_id = action_res.wait()
                else:
                    action_res = self._add_spawn_process_action(cc_agent=container_name, proc_name=name,
                                         module=module, cls=cls, config=rel_cfg)
                    proc_id = action_res.wait()

            else:
                log.warn("App file not supported")

    def _add_spawn_process_action(self, cc_agent, proc_name, module, cls, config):
        action_res = AsyncResult()
        action_kwargs = dict(cc_agent=cc_agent, proc_name=proc_name, module=module, cls=cls, config=config)
        action = ("spawn_process", action_res, action_kwargs)
        self.executor.add_action(action)
        return action_res

    # -------------------------------------------------------------------------

    def _load_rules(self):
        self.rules_cfg = get_safe(self._pd_core.pd_cfg, "engine.dispatch_rules") or []
        self.default_engine = get_safe(self._pd_core.pd_cfg, "engine.default_engine") or "default"

    def _determine_target_container(self, app_cfg):
        # Determine engine
        target_engine = self.default_engine
        app_name = app_cfg["name"]
        for rule in self.rules_cfg:
            if "appname_pattern" in rule:
                if re.match(rule["appname_pattern"], app_name):
                    target_engine = rule["engine"]
                    break

        # Determine container
        ee_containers = self.registry.get_engine_containers()
        ee_conts = ee_containers.get(target_engine, None) or ee_containers.get(self.default_engine, None) or ee_containers.get("", None)
        if ee_conts is None:
            raise BadRequest("Could not determine engine for app {}".format(app_name))
        elif not ee_conts:
            raise BadRequest("No running containers for app {}".format(app_name))
        cont_info = ee_conts[0]
        container_name = cont_info["cc_obj"].cc_agent
        return container_name
