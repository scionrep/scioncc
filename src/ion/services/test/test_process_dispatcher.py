#!/usr/bin/env python

import uuid
from gevent import queue
from datetime import datetime, timedelta

from unittest import SkipTest
from mock import Mock, patch, DEFAULT
from nose.plugins.attrib import attr

from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.net.endpoint import RPCClient
from pyon.util.containers import DotDict, get_safe
from pyon.public import log, CFG, BaseService, NotFound, BadRequest, Conflict, IonException, EventSubscriber
from pyon.core import bootstrap

from interface.services.core.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget,\
    ProcessStateEnum, ProcessQueueingMode
from interface.services.core.iresource_registry_service import ResourceRegistryServiceClient

from ion.services.process_dispatcher_service import ProcessDispatcherService, PDLocalBackend, Notifier


# NOTE: much of the Process Dispatcher functionality is tested directly in the
# epu repository where the code resides. This file only attempts to test the
# Pyon interface itself as well as some integration testing to validate
# communication.

class ProcessStateWaiter(object):
    def __init__(self):
        self.event_queue = queue.Queue()
        self.event_sub = None

    def start(self, process_id=None):
        assert self.event_sub is None
        self.event_sub = EventSubscriber(event_type="ProcessLifecycleEvent",
            callback=self._event_callback, origin=process_id, origin_type="DispatchedProcess")
        self.event_sub.start()

    def stop(self):
        if self.event_sub:
            self.event_sub.stop()
            self.event_sub = None

    def _event_callback(self, event, *args, **kwargs):
        self.event_queue.put(event)

    def await_state_event(self, pid=None, state=None, timeout=30, strict=False):
        """Wait for a state event for a process.
        if strict is False, allow intermediary events
        """

        start_time = datetime.now()

        assert state in ProcessStateEnum._str_map, "process state %s unknown!" % state
        state_str = ProcessStateEnum._str_map.get(state)

        # stick the pid into a container if it is only one
        if pid is not None and not isinstance(pid, (list, tuple)):
            pid = (pid,)

        while 1:
            if datetime.now() - start_time > timedelta(seconds=timeout):
                raise AssertionError("Waiter timeout! Waited %s seconds for process %s state %s" % (timeout, pid, state_str))
            try:
                event = self.event_queue.get(timeout=timeout)
            except queue.Empty:
                raise AssertionError("Event timeout! Waited %s seconds for process %s state %s" % (timeout, pid, state_str))
            log.debug("Got event: %s", event)

            if (pid is None or event.origin in pid) and (state is None or event.state == state):
                return event

            elif strict:
                raise AssertionError("Got unexpected event %s. Expected state %s for process %s" % (event, state_str, pid))

    def await_many_state_events(self, pids, state=None, timeout=30, strict=False):
        pid_set = set(pids)
        while pid_set:
            event = self.await_state_event(tuple(pid_set), state, timeout=timeout, strict=strict)
            pid_set.remove(event.origin)

    def await_nothing(self, pid=None, timeout=10):
        start_time = datetime.now()

        # stick the pid into a container if it is only one
        if pid is not None and not isinstance(pid, (list, tuple)):
            pid = (pid,)

        while 1:
            timeleft = timedelta(seconds=timeout) - (datetime.now() - start_time)
            timeleft_seconds = timeleft.total_seconds()
            if timeleft_seconds <= 0:
                return
            try:
                event = self.event_queue.get(timeout=timeleft_seconds)
                if pid is None or event.origin in pid:
                    state_str = ProcessStateEnum._str_map.get(event.state, str(event.state))
                    raise AssertionError("Expected no event, but got state %s for process %s" % (state_str, event.origin))

            except queue.Empty:
                return


class TestProcess(BaseService):
    """Test process to deploy via PD
    """
    name = __name__ + "test"

    def on_init(self):
        self.i = 0
        self.response = self.CFG.test_response
        self.restart = get_safe(self.CFG, "process.start_mode") == "RESTART"

    def count(self):
        self.i += 1
        return self.i

    def query(self):
        return self.response

    def is_restart(self):
        return self.restart

    def get_process_name(self, pid=None):
        if pid is None:
            return
        proc = self.container.proc_manager.procs.get(pid)
        if proc is None:
            return
        return proc._proc_name


# a copy to use in process definitions for testing process->engine map functionality
TestProcessForProcessEngineMap = TestProcess


class TestProcessThatCrashes(BaseService):
    """Test process to deploy via PD
    """
    name = __name__ + "test"

    def on_init(self):
        raise Exception("I died :(")


class TestClient(RPCClient):
    def __init__(self, to_name=None, node=None, **kwargs):
        to_name = to_name or __name__ + "test"
        RPCClient.__init__(self, to_name=to_name, node=node, **kwargs)

    def count(self, headers=None, timeout=None):
        return self.request({}, op='count', headers=headers, timeout=timeout)

    def query(self, headers=None, timeout=None):
        return self.request({}, op='query', headers=headers, timeout=timeout)

    def get_process_name(self, pid=None, headers=None, timeout=None):
        return self.request({'pid': pid}, op='get_process_name', headers=headers, timeout=timeout)

    def is_restart(self, headers=None, timeout=None):
        return self.request({}, op='is_restart', headers=headers, timeout=timeout)


@attr('INT', group='cei')
class ProcessDispatcherServiceIntTest(IonIntegrationTestCase):

    def setUp(self):
        raise SkipTest("Process dispatcher currently not supported")
        self._start_container()
        self.container.start_rel_from_url('res/deploy/basic.yml')

        self.rr_cli = ResourceRegistryServiceClient()
        self.pd_cli = ProcessDispatcherServiceClient()


        self.process_definition = ProcessDefinition(name='test_process')
        self.process_definition.executable = {'module': 'ion.services.test.test_process_dispatcher',
                                              'class': 'TestProcess'}
        self.process_definition_id = self.pd_cli.create_process_definition(self.process_definition)

        self.waiter = ProcessStateWaiter()

    def tearDown(self):
        self.waiter.stop()

    def test_create_schedule_cancel(self):
        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.ALWAYS

        proc_name = 'myreallygoodname'
        pid = self.pd_cli.create_process(self.process_definition_id)
        self.waiter.start(pid)

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration={}, process_id=pid, name=proc_name)
        self.assertEqual(pid, pid2)

        # verifies L4-CI-CEI-RQ141 and L4-CI-CEI-RQ142
        self.waiter.await_state_event(pid, ProcessStateEnum.RUNNING)

        proc = self.pd_cli.read_process(pid)
        self.assertEqual(proc.process_id, pid)
        self.assertEqual(proc.process_configuration, {})
        self.assertEqual(proc.process_state, ProcessStateEnum.RUNNING)

        # make sure process is readable directly from RR (mirrored)
        # verifies L4-CI-CEI-RQ63
        # verifies L4-CI-CEI-RQ64
        proc = self.rr_cli.read(pid)
        self.assertEqual(proc.process_id, pid)

        # now try communicating with the process to make sure it is really running
        test_client = TestClient()
        for i in range(5):
            self.assertEqual(i + 1, test_client.count(timeout=10))

        # verifies L4-CI-CEI-RQ147

        # check the process name was set in container
        got_proc_name = test_client.get_process_name(pid=pid2)
        self.assertEqual(proc_name, got_proc_name)

        # kill the process and start it again
        self.pd_cli.cancel_process(pid)

        self.waiter.await_state_event(pid, ProcessStateEnum.TERMINATED)

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration={}, process_id=pid)
        self.assertEqual(pid, pid2)

        self.waiter.await_state_event(pid, ProcessStateEnum.RUNNING)

        for i in range(5):
            self.assertEqual(i + 1, test_client.count(timeout=10))

        # kill the process for good
        self.pd_cli.cancel_process(pid)
        self.waiter.await_state_event(pid, ProcessStateEnum.TERMINATED)

    def test_schedule_with_config(self):

        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.ALWAYS

        pid = self.pd_cli.create_process(self.process_definition_id)
        self.waiter.start(pid)

        # verifies L4-CI-CEI-RQ66

        # feed in a string that the process will return -- verifies that
        # configuration actually makes it to the instantiated process
        test_response = uuid.uuid4().hex
        configuration = {"test_response": test_response}

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration=configuration, process_id=pid)
        self.assertEqual(pid, pid2)

        self.waiter.await_state_event(pid, ProcessStateEnum.RUNNING)

        test_client = TestClient()

        # verifies L4-CI-CEI-RQ139
        # assure that configuration block (which can contain inputs, outputs,
        # and arbitrary config) 1) makes it to the process and 2) is returned
        # in process queries

        self.assertEqual(test_client.query(), test_response)

        proc = self.pd_cli.read_process(pid)
        self.assertEqual(proc.process_id, pid)
        self.assertEqual(proc.process_configuration, configuration)

        # kill the process for good
        self.pd_cli.cancel_process(pid)
        self.waiter.await_state_event(pid, ProcessStateEnum.TERMINATED)

    def test_schedule_bad_config(self):

        process_schedule = ProcessSchedule()

        # a non-JSON-serializable IonObject
        o = ProcessTarget()

        with self.assertRaises(BadRequest) as ar:
            self.pd_cli.schedule_process(self.process_definition_id,
                process_schedule, configuration={"bad": o})
        self.assertTrue(ar.exception.message.startswith("bad configuration"))

    def test_cancel_notfound(self):
        with self.assertRaises(NotFound):
            self.pd_cli.cancel_process("not-a-real-process-id")

    def test_create_invalid_definition(self):
        # create process definition missing module and class
        # verifies L4-CI-CEI-RQ137
        executable = dict(url="http://somewhere.com/something.py")
        definition = ProcessDefinition(name="test_process", executable=executable)
        with self.assertRaises(BadRequest):
            self.pd_cli.create_process_definition(definition)



pd_config = {
    'processdispatcher': {
        'backend': "native",
        'static_resources': True,
        'heartbeat_queue': "hbeatq",
        'dashi_uri': "amqp://guest:guest@localhost/",
        'dashi_exchange': "%s.pdtests" % bootstrap.get_sys_name(),
        'default_engine': "engine1",
        'dispatch_retry_seconds': 10,
        "engines": {
            "engine1": {
                "slots": 100,
                "base_need": 1
            },
            "engine2": {
                "slots": 100,
                "base_need": 1
            },
            "engine3": {
                "slots": 100,
                "base_need": 1,
                "heartbeat_period": 2,
                "heartbeat_warning": 4,
                "heartbeat_missing": 6
            }
        },
        "process_engines": {
            "ion.services.test.test_process_dispatcher.TestProcessForProcessEngineMap": "engine2"
        }
    }
}