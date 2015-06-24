#!/usr/bin/env python

import time
from gevent import Timeout
from gevent.event import Event, AsyncResult

from pyon.ion.identifier import create_simple_unique_id
from pyon.net.endpoint import Subscriber, Publisher
from pyon.public import log, NotFound, BadRequest, EventSubscriber, OT, ProcessPublisher, get_ion_ts
from pyon.util.async import spawn

from interface.objects import ProcessStateEnum, AsyncResultMsg


class AsyncResultWaiter(object):
    """
    Class that makes waiting for an async result notification easy.
    Creates a subscriber for a generated token name, which can be handed to the async provider.
    The provider then publishes the result to the token name when ready.
    The caller can wait for the result or timeout.
    """
    def __init__(self, process=None):
        self.process = process

        self.async_res = AsyncResult()
        self.wait_name = "asyncresult_" + create_simple_unique_id()
        if self.process:
            self.wait_name = self.wait_name + "_" + self.process.id
        self.wait_sub = Subscriber(from_name=self.wait_name, callback=self._result_callback)
        self.activated = False

    def activate(self):
        if self.activated:
            raise BadRequest("Already active")
        self.listen_gl = spawn(self.wait_sub.listen)    # This initializes and activates the listener
        self.wait_sub.get_ready_event().wait(timeout=1)
        self.activated = True

        return self.wait_name

    def _result_callback(self, msg, headers):
        log.debug("AsyncResultWaiter: received message")
        self.async_res.set(msg)

    def await(self, timeout=None, request_id=None):
        try:
            result = self.async_res.get(timeout=timeout)
            if request_id and isinstance(result, AsyncResultMsg) and result.request_id != request_id:
                log.warn("Received result for different request: %s", result)
                result = None

        except Timeout:
            result = None

        self.wait_sub.deactivate()
        self.wait_sub.close()
        self.listen_gl.join(timeout=1)
        self.activated = False

        return result


class AsyncResultPublisher(object):
    """
    Class that helps sending async results.
    """
    def __init__(self, process=None, wait_name=None):
        self.process = process
        if not wait_name.startswith("asyncresult_"):
            raise BadRequest("Not a valid wait_name")
        self.wait_name = wait_name
        if self.process:
            self.pub = ProcessPublisher(process=self.process, to_name=wait_name)
        else:
            self.pub = Publisher(to_name=wait_name)

    def publish_result(self, request_id, result):
        async_res = AsyncResultMsg(result=result, request_id=request_id, ts=get_ion_ts())
        self.pub.publish(async_res)
        self.pub.close()

    def publish_error(self, request_id, error, error_code):
        async_res = AsyncResultMsg(result=error, request_id=request_id, ts=get_ion_ts(), status=error_code)
        self.pub.publish(async_res)
        self.pub.close()


class ProcessStateGate(EventSubscriber):
    """
    Ensure that a process gets to a particular state, now or in the future.

    Usage:
      gate = ProcessStateGate(process_management_client.read_process, process_id, ProcessStateEnum.some_state)
      assert gate.await(timeout_in_seconds)

    This pattern returns True immediately upon reaching the desired state, or False if the timeout is reached.
    This pattern avoids a race condition between read_process and using EventGate.
    """
    def __init__(self, read_process_fn=None, process_id='', desired_state=None, *args, **kwargs):

        if not process_id:
            raise BadRequest("ProcessStateGate trying to wait on invalid process (id = '%s')" % process_id)

        EventSubscriber.__init__(self, *args,
                                 callback=self._trigger_cb,
                                 event_type=OT.ProcessLifecycleEvent,
                                 origin=process_id,
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

    def _trigger_cb(self, event, x):
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
            return process_obj and self.desired_state == process_obj.process_state
        except NotFound:
            return False

    def await(self, timeout=0):
        # Set up the event gate so that we don't miss any events
        start_time = time.time()
        self.gate = Event()
        self.start()

        # If it's in the desired state, return immediately
        if self.in_desired_state():
            self.first_chance = True
            self.stop()
            log.info("ProcessStateGate found process already %s -- NO WAITING",
                     ProcessStateEnum._str_map[self.desired_state])
            return True

        # If the state was not where we want it, wait for the event.
        ret = self.gate.wait(timeout)
        self.stop()

        if ret:
            # timer is already stopped in this case
            log.info("ProcessStateGate received %s event after %0.2f seconds",
                     ProcessStateEnum._str_map[self.desired_state],
                     time.time() - start_time)
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
