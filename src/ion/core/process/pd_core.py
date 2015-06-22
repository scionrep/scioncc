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

from pyon.public import BadRequest, log

from ion.core.process.leader import LeaderManager
from ion.core.process.pd_exec import pd_executor_factory
from ion.core.process.pd_registry import ProcessDispatcherRegistry, ProcessDispatcherAggregator

PD_LOCK_SCOPE = "PD"


class ProcessDispatcher(object):

    def __init__(self, process):
        self.process = process
        self.container = self.process.container
        self.CFG = self.process.CFG
        self._enabled = False

        # Component that determines one leader in the distributed system
        self.leader_manager = LeaderManager(PD_LOCK_SCOPE, self.process)

        # The authoritative process registry
        self.registry = ProcessDispatcherRegistry(pd_core=self)

        # Component that listens to external input such as heartbeats
        self.aggregator = ProcessDispatcherAggregator(pd_core=self)

        # The decision engine
        self.engine = None

        # Component that executes actions
        self.executor = pd_executor_factory("global", pd_core=self)

    def start(self):
        log.info("PD starting...")
        self.leader_manager.start()
        self.leader_manager.await_leader()
        self.registry.start()
        self.executor.start()
        self.aggregator.start()
        self._enabled = True
        log.info("PD started.")

    def stop(self):
        log.info("PD stopping...")
        self._enabled = False
        self.aggregator.stop()
        self.executor.stop()
        self.registry.stop()
        self.leader_manager.stop()
        log.info("PD stopped.")

    # -------------------------------------------------------------------------
    # Public API (callable by process management service)

    def schedule(self, process_id, process_definition, schedule, configuration, name):
        if not self._enabled:
            raise BadRequest("PD API not enabled")
        action_res = AsyncResult()
        action_args = dict(process_id=process_id, process_definition=process_definition,
                           schedule=schedule, configuration=configuration, name=name)
        action = ("schedule", action_res, action_args)
        self.executor.add_action(action)
        return action_res

    def cancel(self, process_id):
        if not self._enabled:
            raise BadRequest("PD API not enabled")
        action_res = AsyncResult()
        action_args = dict(process_id=process_id)
        action = ("cancel", action_res, action_args)
        self.executor.add_action(action)
        return action_res

    def read_process(self, process_id):
        if not self._enabled:
            raise BadRequest("PD API not enabled")
        return self.registry.get_process_info(process_id)

    def list(self):
        if not self._enabled:
            raise BadRequest("PD API not enabled")
        return self.registry.list_processes()
