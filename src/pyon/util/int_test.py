#!/usr/bin/env python

""" Integration test base class and utils """

from mock import patch
from unittest import SkipTest
import unittest
from copy import deepcopy
import os
from gevent import greenlet, spawn

from pyon.container.cc import Container
from pyon.core import bootstrap
from pyon.core.bootstrap import bootstrap_pyon, CFG
from pyon.core.interfaces.interfaces import InterfaceAdmin
from pyon.util.containers import DotDict, dict_merge
from pyon.util.log import log


def pre_initialize_ion():
    # Do necessary system initialization
    # Make sure this happens only once
    iadm = InterfaceAdmin(bootstrap.get_sys_name(), config=CFG)
    iadm.create_core_datastores()
    #iadm.store_config(CFG)
    iadm.store_interfaces(idempotent=True)
    iadm.close()

# STATIC INITIALIZATION (called from below)
# This code is executed once before any integration test is run.
def initialize_ion_int_tests():
    # Bootstrap pyon CFG, logging and object/resource interfaces
    bootstrap_pyon()
    if bootstrap.is_testing():
        IonIntegrationTestCase._force_clean(False, initial=True)
        pre_initialize_ion()


class IntegrationTestCase(unittest.TestCase):
    """
    Base test class to allow operations such as starting the container
    """
    SkipTest = SkipTest

    def shortDescription(self):
        # @see http://www.saltycrane.com/blog/2012/07/how-prevent-nose-unittest-using-docstring-when-verbosity-2/
        return None

    # override __str__ and __repr__ behavior to show a copy-pastable nosetest name for tests
    #  pack.module:TestClassName.test_function_name
    def __repr__(self):
        name = self.id()
        name = name.split('.')
        return "%s ( %s )" % (name[-1], '.'.join(name[:-2]) + ":" + '.'.join(name[-2:]))
    __str__ = __repr__

    def run(self, result=None):
        unittest.TestCase.run(self, result)

    def _start_container(self):
        self._patch_out_fail_fast_kill()

        # Tests cannot run without pre-initialized datastores and resource objects
        pre_initialize_ion()

        self.container = None
        self.addCleanup(self._stop_container)
        self.container = Container()
        self.container.start()

    def _stop_container(self):
        if self.container:
            try:
                self.container.stop(do_exit=False)
            except Exception:
                log.exception("Error stopping container")
            try:
                if self.container and self.container.pidfile and os.path.exists(self.container.pidfile):
                    os.remove(self.container.pidfile)
            except Exception:
                log.exception("Error removing container pidfile")

            self.container = None

        self._force_clean()

    def _patch_out_fail_fast_kill(self):
        patcher = patch('pyon.container.cc.Container._kill_fast')
        patcher.start()
        self.addCleanup(patcher.stop)

    @classmethod
    def _force_clean(cls, recreate=False, initial=False):
        # Database resources
        from pyon.core.bootstrap import get_sys_name, CFG
        from pyon.datastore.datastore_common import DatastoreFactory
        datastore = DatastoreFactory.get_datastore(config=CFG, variant=DatastoreFactory.DS_BASE, scope=get_sys_name())
        if initial:
            datastore._init_database(datastore.database)

        dbs = datastore.list_datastores()
        clean_prefix = '%s_' % get_sys_name().lower()
        things_to_clean = [x for x in dbs if x.startswith(clean_prefix)]
        try:
            for thing in things_to_clean:
                datastore.delete_datastore(datastore_name=thing)
                if recreate:
                    datastore.create_datastore(datastore_name=thing)

        finally:
            datastore.close()

        # Broker resources
        from putil.rabbitmq.rabbit_util import RabbitManagementUtil
        rabbit_util = RabbitManagementUtil(CFG, sysname=bootstrap.get_sys_name())
        deleted_exchanges, deleted_queues = rabbit_util.clean_by_sysname()
        log.info("Deleted %s exchanges, %s queues" % (len(deleted_exchanges), len(deleted_queues)))

        # File system
        from pyon.util.file_sys import FileSystem
        FileSystem._clean(CFG)

    # -------------------------------------------------------------------------

    def _start_tracer_log(self, config=None):
        """Temporarily enables tracer log and configures it until end of test (cleanUp)"""
        if not self.container:
            return
        from pyon.util import tracer
        if not tracer.trace_data["config"].get("log_trace", False):
            tracer_cfg_old = tracer.trace_data["config"]
            tracer.trace_data["config"] = tracer.trace_data["config"].copy()
            tracer.trace_data["config"]["log_trace"] = True
            if config:
                tracer.trace_data["config"].update(config)

            def cleanup_tracer():
                tracer.trace_data["config"] = tracer_cfg_old
                log.info("--------------- Stopping Tracer Logging ---------------")
            self.addCleanup(cleanup_tracer)
            log.info("--------------- Starting Tracer Logging ---------------")

    def _breakpoint(self, scope=None, global_scope=None):
        from pyon.util.breakpoint import breakpoint
        breakpoint(scope=scope, global_scope=global_scope)

    @staticmethod
    def _get_alt_cfg(cfg_merge):
        cfg_clone = deepcopy(CFG)
        dict_merge(cfg_clone, cfg_merge, inplace=True)
        return DotDict(**cfg_clone)

    def patch_alt_cfg(self, cfg_obj_or_str, cfg_merge):
        """ Patches given CFG (DotDict) based on system CFG with given dict merged """
        alt_cfg = self._get_alt_cfg(cfg_merge)
        self.patch_cfg(cfg_obj_or_str, alt_cfg)

    def patch_cfg(self, cfg_obj_or_str, *args, **kwargs):
        """
        Helper method for patching the CFG (or any dict, but useful for patching CFG).

        This method exists because the decorator versions of patch/patch.dict do not function
        until the test_ method is called - ie, when setUp is run, the patch hasn't occurred yet.
        Use this in your setUp method if you need to patch CFG and have stuff in setUp respect it.

        @param  cfg_obj_or_str  An actual ref to CFG or a string defining where to find it ie 'pyon.ion.exchange.CFG'
        @param  *args           *args to pass to patch.dict
        @param  **kwargs        **kwargs to pass to patch.dict
        """
        patcher = patch.dict(cfg_obj_or_str, *args, **kwargs)
        patcher.start()
        self.addCleanup(patcher.stop)

# Name alias
IonIntegrationTestCase = IntegrationTestCase

initialize_ion_int_tests()
