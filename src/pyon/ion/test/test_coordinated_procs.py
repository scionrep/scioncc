#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from unittest import SkipTest
from mock import Mock, patch, ANY, sentinel, call
from nose.plugins.attrib import attr
import random
from gevent.event import AsyncResult, Event
import gevent

from pyon.util.int_test import IntegrationTestCase

from pyon.agent.agent import ResourceAgent
from pyon.container.procs import ProcManager
from pyon.core.exception import BadRequest, NotFound
from pyon.ion.endpoint import ProcessRPCServer
from pyon.ion.process import IonProcessError
from pyon.public import PRED, CCAP, IonObject, log, get_ion_ts_millis, EventSubscriber, EventPublisher, OT
from pyon.ion.service import BaseService

from interface.objects import ProcessStateEnum
from interface.services.examples.ihello_service import BaseHelloService, HelloServiceClient


class CoordinatedProcess(BaseHelloService):

    evt_count = dict(total=0)

    def on_start(self):
        self.terminate_loop = Event()
        self.has_lock = False
        self.lock_expires = None
        CoordinatedProcess.evt_count[self.id] = 0

        self.bg_loop = gevent.spawn(self._bg_loop)

        self.evt_sub = EventSubscriber(event_type=OT.ResourceCommandEvent,
                                       callback=self._on_event)
        self.add_endpoint(self.evt_sub)

    def _bg_loop(self):
        """Background loop that acquires a master lock"""
        # Delayed initialization to create some non-determinism
        delay = random.random() * 0.2
        gevent.sleep(delay)
        log.info("Starting %s %s with id=%s", self._proc_type, self._proc_name, self.id)

        counter = 0
        while not self.terminate_loop.wait(timeout=0.4):
            counter += 1
            try:
                self._check_lock()
                if self.has_lock and counter % 3 == 0:
                    gevent.sleep(0.5)  # Simulates a lock up - lose lock
            except Exception:
                log.exception("Exception in bg_loop")

    def _check_lock(self):
        LOCK_TIMEOUT = 0.5
        cur_time = get_ion_ts_millis()
        has_lock = self._is_master()
        self.has_lock = self.container.directory.acquire_lock("coord", LOCK_TIMEOUT, self.id)
        if self.has_lock:
            # We are the master
            if not has_lock:
                log.info("Process %s is now the master", self._proc_name)
            self.lock_expires = cur_time + int(1000*LOCK_TIMEOUT)

    def _is_master(self):
        return self.has_lock and get_ion_ts_millis() < self.lock_expires

    def _on_event(self, event, *args, **kwargs):
        CoordinatedProcess.evt_count["total"] += 1
        if self._is_master():
            CoordinatedProcess.evt_count[self.id] += 1

    def on_quit(self):
        log.info("Quitting process %s", self._proc_name)
        # tell the trigger greenlet we're done
        self.terminate_loop.set()

        # wait on the greenlets to finish cleanly
        self.bg_loop.join(timeout=2)

    def hello(self, text=''):
        if self._is_master():
            return "MASTER"
        else:
            return ""

    def noop(self, text=''):
        pass


class CoordinatedProcessTest(IntegrationTestCase):

    def setUp(self):
        self._start_container()

    def test_procs(self):
        # Start procs
        config = {}
        pid1 = self.container.spawn_process("hello1", __name__, "CoordinatedProcess", config=config)
        pid2 = self.container.spawn_process("hello2", __name__, "CoordinatedProcess", config=config)
        pid3 = self.container.spawn_process("hello3", __name__, "CoordinatedProcess", config=config)

        # Wait for them to be ready
        gevent.sleep(0.3)

        # Call service
        evt_pub = EventPublisher(event_type=OT.ResourceCommandEvent)
        hc = HelloServiceClient()
        end_time = get_ion_ts_millis() + 4000
        counter = 0
        while get_ion_ts_millis() < end_time:
            counter += 1
            res = hc.hello("foo")
            evt_pub.publish_event(origin=str(counter))
            gevent.sleep(random.random()*0.1)

        # Wait for them to be finish
        gevent.sleep(0.3)

        # Wrap up
        self.container.terminate_process(pid1)
        self.container.terminate_process(pid2)
        self.container.terminate_process(pid3)

        evt_count = CoordinatedProcess.evt_count
        log.info("Counters: %s", evt_count)

        self.assertEqual(evt_count["total"], 3*counter)
        master_evts = evt_count[pid1] + evt_count[pid2] + evt_count[pid3]
        self.assertLessEqual(master_evts, counter)
        if master_evts < counter:
            log.info("Lost %s events - no functioning master", counter - master_evts)
