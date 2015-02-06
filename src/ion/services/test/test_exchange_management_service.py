#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Dave Foster <dfoster@asascience.com>'

import os
from mock import Mock, patch, sentinel
from nose.plugins.attrib import attr

from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase

from pyon.ion import exchange
from pyon.net.transport import BaseTransport
from pyon.public import PRED, RT, CFG, log, BadRequest, Conflict, Inconsistent, NotFound
from pyon.util.containers import DotDict
from ion.services.exchange_management_service import ExchangeManagementService

from interface.objects import ExchangeSpace, ExchangePoint, ExchangeName
from interface.services.core.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.core.iexchange_management_service import ExchangeManagementServiceClient


@attr('INT', group='coi')
@patch.dict('pyon.ion.exchange.CFG', container=DotDict(CFG.container, exchange=DotDict(auto_register=True)))
class TestExchangeManagementServiceInt(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/basic.yml')

        self.ems = ExchangeManagementServiceClient()
        self.rr = ResourceRegistryServiceClient()

        orglist, _ = self.rr.find_resources(RT.Org)
        self.org_id = orglist[0]._id

        # we test actual exchange interaction in pyon, so it's fine to mock the broker interaction here
        self.container.ex_manager.create_xs = Mock()
        self.container.ex_manager.delete_xs = Mock()
        self.container.ex_manager.create_xp = Mock()
        self.container.ex_manager.delete_xp = Mock()

    def test_xs_create_delete(self):
        exchange_space = ExchangeSpace(name="bobo")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        # should be able to pull from RR an exchange space
        es2 = self.rr.read(esid)
        self.assertEquals(exchange_space.name, es2.name)

        # should have an exchange declared on the broker
        self.container.ex_manager.create_xs.assert_called_once_with('bobo', use_ems=False)

        # should have an assoc to an org
        orglist, _ = self.rr.find_subjects(RT.Org, PRED.hasExchangeSpace, esid, id_only=True)
        self.assertEquals(len(orglist), 1)
        self.assertEquals(orglist[0], self.org_id)

        self.ems.delete_exchange_space(esid)

        # should no longer have that id in the RR
        self.assertRaises(NotFound, self.rr.read, esid)

        # should no longer have an assoc to an org
        orglist2, _ = self.rr.find_subjects(RT.Org, PRED.hasExchangeSpace, esid, id_only=True)
        self.assertEquals(len(orglist2), 0)

        # should no longer have that exchange declared
        self.assertEquals(self.container.ex_manager.delete_xs.call_count, 1)
        self.assertIn('bobo', self.container.ex_manager.delete_xs.call_args[0][0].exchange) # prefixed with sysname

    def test_xs_delete_without_create(self):
        self.assertRaises(NotFound, self.ems.delete_exchange_space, '123')

    def test_xp_create_delete(self):

        # xp needs an xs first
        exchange_space = ExchangeSpace(name="doink")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        exchange_point = ExchangePoint(name="hammer")
        epid = self.ems.create_exchange_point(exchange_point, esid)

        # should be in RR
        ep2 = self.rr.read(epid)
        self.assertEquals(exchange_point.name, ep2.name)

        # should be associated to the XS as well
        xslist, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangePoint, epid, id_only=True)
        self.assertEquals(len(xslist), 1)
        self.assertEquals(xslist[0], esid)

        # should exist on broker (both xp and xs)
        self.container.ex_manager.create_xs.assert_called_once_with('doink', use_ems=False)
        self.assertIn('hammer', self.container.ex_manager.create_xp.call_args[0])

        self.ems.delete_exchange_point(epid)
        self.ems.delete_exchange_space(esid)

        # should no longer be in RR
        self.assertRaises(NotFound, self.rr.read, epid)

        # should no longer be associated
        xslist2, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangePoint, epid, id_only=True)
        self.assertEquals(len(xslist2), 0)

        # should no longer exist on broker (both xp and xs)

    def test_xp_create_then_delete_xs(self):

        # xp needs an xs first
        exchange_space = ExchangeSpace(name="doink")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        exchange_point = ExchangePoint(name="hammer")
        epid = self.ems.create_exchange_point(exchange_point, esid)

        # delete xs
        self.ems.delete_exchange_space(esid)

        # should no longer have an association
        xslist2, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangePoint, epid, id_only=True)
        self.assertEquals(len(xslist2), 0)

        # TEST ONLY: have to clean up the xp or we leave junk on the broker
        # we have to do it manually because the xs is gone
        #self.ems.delete_exchange_point(epid)
        # @TODO: reaching into ex manager for transport is clunky
        xs = exchange.ExchangeSpace(self.container.ex_manager, self.container.ex_manager._priviledged_transport, exchange_space.name)
        xp = exchange.ExchangePoint(self.container.ex_manager, self.container.ex_manager._priviledged_transport, exchange_point.name, xs, 'ttree')
        self.container.ex_manager.delete_xp(xp, use_ems=False)

    def test_xn_declare_and_undeclare(self):

        # xn needs an xs first
        exchange_space = ExchangeSpace(name="bozo")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        exchange_name = ExchangeName(name='shoes', xn_type="XN_PROCESS")
        enid = self.ems.declare_exchange_name(exchange_name, esid)

        # should be in RR
        en2 = self.rr.read(enid)
        self.assertEquals(exchange_name.name, en2.name)

        # should have an assoc from XN to XS
        xnlist, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangeName, enid, id_only=True)
        self.assertEquals(len(xnlist), 1)
        self.assertEquals(xnlist[0], esid)

        # container API got called, will have declared a queue
        self.ems.undeclare_exchange_name(enid)      # canonical name = xn id in current impl

    def test_xn_declare_no_xs(self):
        exchange_name = ExchangeName(name="shoez", xn_type='XN_PROCESS')
        self.assertRaises(NotFound, self.ems.declare_exchange_name, exchange_name, '11')

    def test_xn_undeclare_without_declare(self):
        self.assertRaises(NotFound, self.ems.undeclare_exchange_name, 'some_non_id')

    def test_xn_declare_then_delete_xs(self):

        # xn needs an xs first
        exchange_space = ExchangeSpace(name="bozo")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        exchange_name = ExchangeName(name='shnoz', xn_type="XN_PROCESS")
        enid = self.ems.declare_exchange_name(exchange_name, esid)

        # delete the XS
        self.ems.delete_exchange_space(esid)

        # no longer should have assoc from XS to XN
        xnlist, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangeName, enid, id_only=True)
        self.assertEquals(len(xnlist), 0)

        # cleanup: delete the XN (assoc already removed, so we reach into the implementation here)
        # @TODO: reaching into ex manager for transport is clunky
        self.rr.delete(enid)
        xs = exchange.ExchangeSpace(self.container.ex_manager, self.container.ex_manager._priviledged_transport, exchange_space.name)
        xn = exchange.ExchangeName(self.container.ex_manager, self.container.ex_manager._priviledged_transport, exchange_name.name, xs)
        self.container.ex_manager.delete_xn(xn, use_ems=False)


@attr('INT', group='coi')
class TestContainerExchangeToEms(IonIntegrationTestCase):
    # these tests should auto contact the EMS to do the work
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/basic.yml')

        self.ems = ExchangeManagementServiceClient()
        self.rr = ResourceRegistryServiceClient()

        # we want the ex manager to do its thing, but without actual calls to broker
        # just mock out the transport
        self.container.ex_manager._priviledged_transport = Mock(BaseTransport)

    def test_create_xs_talks_to_ems(self):
        self.patch_cfg('pyon.ion.exchange.CFG', container=DotDict(CFG.container, exchange=DotDict(auto_register=True)))

        xs = self.container.ex_manager.create_xs('house')
        self.addCleanup(xs.delete)

        # should have called EMS and set RR items
        res, _ = self.rr.find_resources(RT.ExchangeSpace, name='house')
        self.assertEquals(res[0].name, 'house')

        # should have tried to call broker as well
        self.assertEquals(self.container.ex_manager._priviledged_transport.declare_exchange_impl.call_count, 1)
        self.assertIn('house', self.container.ex_manager._priviledged_transport.declare_exchange_impl.call_args[0][0])

    @patch.dict('pyon.ion.exchange.CFG', container=DotDict(CFG.container, exchange=DotDict(auto_register=False)))
    def test_create_xs_with_no_flag_only_uses_ex_manager(self):
        self.patch_cfg('pyon.ion.exchange.CFG', container=DotDict(CFG.container, exchange=DotDict(auto_register=False)))

        xs = self.container.ex_manager.create_xs('house')
        self.addCleanup(xs.delete)

        e1,e2 = self.rr.find_resources(RT.ExchangeSpace, name='house')
        self.assertEquals(e1, [])
        self.assertEquals(e2, [])
        self.assertEquals(self.container.ex_manager._priviledged_transport.declare_exchange_impl.call_count, 1)
        self.assertIn('house', self.container.ex_manager._priviledged_transport.declare_exchange_impl.call_args[0][0])

