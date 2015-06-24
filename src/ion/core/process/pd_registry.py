""" Process dispatcher registry and aggregator. """

__author__ = 'Michael Meisinger'

import gevent
from gevent.lock import RLock

from pyon.public import BadRequest, EventPublisher, log, NotFound, OT, RT, ProcessSubscriber, ProcessEventSubscriber
from pyon.util.containers import create_valid_identifier

from interface.objects import Process, ProcessStateEnum, ContainerHeartbeat


class ProcessDispatcherRegistry(object):
    """ PD Registry of containers and processes """

    def __init__(self, pd_core):
        self._pd_core = pd_core
        self.container = self._pd_core.container
        self.process = self._pd_core.process

        self._lock = RLock()        # Master lock protecting data structures
        self._containers = {}       # Registry of containers
        self._processes = {}        # Registry of processes

    def start(self):
        pass

    def stop(self):
        pass

    # -------------------------------------------------------------------------

    def get_process_info(self, process_id):
        return self._processes.get(process_id, None)

    def list_processes(self):
        return self._processes.values()

    def register_container(self, container_info):
        if isinstance(container_info, ContainerHeartbeat):
            pass
        else:
            raise BadRequest("Unknown container info format")


class ProcessDispatcherAggregator(object):
    """ PD aggregator for heartbeat input from containers etc. """

    def __init__(self, pd_core):
        self._pd_core = pd_core
        self.container = self._pd_core.container
        self.process = self._pd_core.process
        self.registry = self._pd_core.registry

    def start(self):
        # Create our own queue for container heartbeats and broadcasts
        topic = "bx_containers"
        queue_name = "pd_aggregator_%s_%s" % (topic, create_valid_identifier(self.process.id, dot_sub="_"))
        self.sub_cont = ProcessSubscriber(process=self.process, binding=topic, from_name=queue_name,
                                          callback=self._receive_container_info, auto_delete=True)
        self.process.add_endpoint(self.sub_cont)

        self.evt_sub_c = ProcessEventSubscriber(process=self.process, event_type=OT.ContainerLifecycleEvent,
                                                callback=self._receive_event)
        self.evt_sub_c.add_event_subscription(event_type=OT.ProcessLifecycleEvent, origin_type="PD",)
        self.process.add_endpoint(self.evt_sub_c)

        log.info("PD Aggregator - event and heartbeat subscribers started")

    def stop(self):
        pass

    # -------------------------------------------------------------------------

    def _receive_container_info(self, msg, headers, *args):
        #print "!!! Got container info", msg, headers, args
        pass

    def _receive_event(self, event, *args, **kwargs):
        print "!!! Got  event", event
