""" Process dispatcher decision engine. """

__author__ = 'Michael Meisinger'

import gevent
from gevent.event import AsyncResult
from gevent.queue import Queue

from pyon.public import BadRequest, EventPublisher, log, NotFound, OT, RT, Subscriber, Publisher, get_safe
from pyon.util.async import spawn

from interface.objects import Process, ProcessStateEnum, AsyncResultMsg
from interface.services.icontainer_agent import ContainerAgentClient


class ProcessDispatcherDecisionEngine(object):
    """ Base class for PD decision engines """

    def __init__(self, pd_core):
        self._pd_core = pd_core
        self.container = self._pd_core.container
        self.registry = self._pd_core.registry
        self.executor = self._pd_core.executor

    def start(self):
        queue_name = get_safe(self._pd_core._pd_cfg, "command_queue") or "pd_command"
        self.sub_cont = Subscriber(binding=queue_name, from_name=queue_name, callback=self._receive_command)
        self.sub_cont_gl = spawn(self.sub_cont.listen, activate=True)
        self.sub_cont.get_ready_event().wait()
        #self.sub_cont.activate()
        # TODO: Only activate if we are leader and preconditions true

        self.pub_result = Publisher()

    def stop(self):
        self.sub_cont.close()
        self.sub_cont_gl.join(timeout=2)
        self.sub_cont_gl.kill()
        self.sub_cont_gl = None

    def _receive_command(self, command, headers, *args):
        # Await preconditions - this is a big ugly given that we receive and then wait
        self.registry.preconditions_true.wait()

        log.info("PD execute command %s", command)

        cmd_funcname = "_cmd_%s" % command["command"]
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
        log.info("START REL")
        rel = command["rel_def"]

        for rel_app_cfg in rel.apps:
            name = rel_app_cfg.name
            log.debug("app definition in rel: %s" % str(rel_app_cfg))

            # Decide where process should go
            target_cc_agent = ContainerAgentClient(to_name="")

            if 'processapp' in rel_app_cfg:

                name, module, cls = rel_app_cfg.processapp

                if 'replicas' in rel_app_cfg:
                    proc_replicas = int(rel_app_cfg["replicas"])
                    if self.max_proc_replicas > 0:
                        if proc_replicas > self.max_proc_replicas:
                            log.info("Limiting number of proc replicas to %s from %s", self.max_proc_replicas, proc_replicas)
                        proc_replicas = min(proc_replicas, self.max_proc_replicas)
                    if proc_replicas < 1 or proc_replicas > 100:
                        log.warn("Invalid number of process replicas: %s", proc_replicas)
                        proc_replicas = 1
                    for i in xrange(proc_replicas):
                        proc_name = "%s.%s" % (name, i) if i else name
                        target_cc_agent.spawn_process(proc_name, module, cls, rel_cfg)
                else:
                    target_cc_agent.spawn_process(name, module, cls, rel_cfg)

            else:
                log.warn("App file not supported")

        return dict(foo="bar")