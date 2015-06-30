"""
Process dispatcher core logic.

Based in parts on OOI process dispatcher, https://github.com/scion-network/epu
(C) University of Chicago, 2013. Open source under Apache 2.0.
Simplified to work without IaaS resource management (EPUM) and directly
integrated with ScionCC.

The process dispatcher is a background process that manages executing processes in the
system. It provides a command set to launch and terminate processes, and monitors
running containers and processes through CC Agents.

There is at most ever one leader, which maintains a registry of containers and processes,
and executes requested actions.
The process_management service is high available with short response time, placing
requests into the PD queue as needed. Results need to be waited on.

CC Agents provide a command set to spawn and terminate processes. They also send
heartbeats to a queue monitored by the process dispatcher.
"""

__author__ = 'Michael Meisinger'

from gevent.event import AsyncResult

from pyon.ion.identifier import create_simple_unique_id
from pyon.public import BadRequest, log, get_safe, Publisher

from ion.core.process.leader import LeaderManager
from ion.core.process.pd_engine import ProcessDispatcherDecisionEngine
from ion.core.process.pd_exec import pd_executor_factory
from ion.core.process.pd_registry import ProcessDispatcherRegistry, ProcessDispatcherAggregator
from ion.core.process.proc_util import AsyncResultWaiter

PD_LOCK_SCOPE = "PD"


class ProcessDispatcher(object):

    def __init__(self, container, config):
        self.container = container
        self._pd_cfg = config or {}
        self._enabled = False

        # Component that determines one leader in the distributed system
        self.leader_manager = LeaderManager(PD_LOCK_SCOPE, container=self.container)

        # The authoritative process registry
        self.registry = ProcessDispatcherRegistry(pd_core=self)

        # Component that listens to external input such as heartbeats
        self.aggregator = ProcessDispatcherAggregator(pd_core=self)

        # Component that executes actions
        self.executor = pd_executor_factory("global", pd_core=self)

        # The decision engine
        self.engine = ProcessDispatcherDecisionEngine(pd_core=self)

        self.pd_client = ProcessDispatcherClient(self.container, self._pd_cfg)

    def start(self):
        log.info("PD starting...")
        self.leader_manager.start()
        self.leader_manager.await_leader()
        self.registry.start()
        self.executor.start()
        self.engine.start()
        self.aggregator.start()
        self._enabled = True
        log.info("PD started.")

    def stop(self):
        log.info("PD stopping...")
        self._enabled = False
        self.aggregator.stop()
        self.engine.stop()
        self.executor.stop()
        self.registry.stop()
        self.leader_manager.stop()
        log.info("PD stopped.")


class ProcessDispatcherClient(object):
    def __init__(self, container, config):
        self.container = container
        self._pd_cfg = config or {}

        self._cmd_queue_name = get_safe(self._pd_cfg, "command_queue", "pd_command")
        self.cmd_pub = Publisher(to_name=self._cmd_queue_name)

    # -------------------------------------------------------------------------
    # Public API (callable by process management service)

    def start_rel(self, rel_def, reply_to=None):
        command_id = create_simple_unique_id()
        action_cmd = dict(command="start_rel", command_id=command_id, rel_def=rel_def,
                          reply_to=reply_to)

        self.cmd_pub.publish(action_cmd)
        return command_id

    def start_rel_blocking(self, rel_def, timeout=None):
        waiter = AsyncResultWaiter()
        reply_to = waiter.activate()
        command_id = self.start_rel(rel_def, reply_to)
        cmd_res = waiter.await(timeout=timeout, request_id=command_id)   # This could take a long time!
        return cmd_res

    def schedule(self, process_id, process_definition, schedule, configuration, name):
        command_id = create_simple_unique_id()
        action_cmd = dict(command="schedule", command_id=command_id,
                          process_id=process_id, process_definition=process_definition,
                          schedule=schedule, configuration=configuration, name=name)

        self.cmd_pub.publish(action_cmd)
        return command_id

    def cancel(self, process_id):
        command_id = create_simple_unique_id()
        action_cmd = dict(command="cancel", command_id=command_id,
                          process_id=process_id)
        self.cmd_pub.publish(action_cmd)
        return command_id

    def list(self):
        command_id = create_simple_unique_id()
        action_cmd = dict(command="list", command_id=command_id)
        self.cmd_pub.publish(action_cmd)
        return command_id
