#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Dave Foster <dfoster@asascience.com>'

import os
from mock import Mock, patch, sentinel
from nose.plugins.attrib import attr

from pyon.util.int_test import IonIntegrationTestCase

from pyon.ion import exchange
from pyon.net.transport import BaseTransport
from pyon.public import PRED, RT, CFG, log, BadRequest, Conflict, Inconsistent, NotFound
from pyon.util.containers import DotDict
from ion.service.exchange_management_service import ExchangeManagementService

from interface.objects import ExchangeSpace, ExchangePoint, ExchangeName
from interface.services.core.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.core.iexchange_management_service import ExchangeManagementServiceClient


@attr('INT', group='coi')
@patch.dict('pyon.ion.exchange.CFG', IonIntegrationTestCase._get_alt_cfg({'container':{'messaging':{'auto_register': True}}}))
class TestExchangeManagementServiceInt(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/basic.yml')

        self.ems = ExchangeManagementServiceClient()
        self.rr = ResourceRegistryServiceClient()

        orglist, _ = self.rr.find_resources(RT.Org)
        self.org_id = orglist[0]._id

        # we test actual exchange interaction in pyon, so it's fine to mock the broker interaction here
        self._clear_mocks()

    def _clear_mocks(self):
        self.container.ex_manager.create_xs = Mock()
        self.container.ex_manager.delete_xs = Mock()
        self.container.ex_manager.create_xp = Mock()
        self.container.ex_manager.delete_xp = Mock()
        self.container.ex_manager._create_xn = Mock()
        self.container.ex_manager.delete_xn = Mock()

    def test_exchange_management(self):
        # TEST: XS management
        self._do_test_xs()

        # TEST: XP management
        self._do_test_xp()

        # TEST: XN management
        self._do_test_xn()

    def _do_test_xs(self):
        exchange_space = ExchangeSpace(name="bobo")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        # should have an exchange declared on the broker
        self.container.ex_manager.create_xs.assert_called_once_with('bobo')

        # should be able to pull from RR an exchange space
        es2 = self.rr.read(esid)
        self.assertEquals(exchange_space.name, es2.name)

        es3 = self.ems.read_exchange_space(esid)
        self.assertEquals(es3.name, es2.name)

        # should have an assoc to an org
        orglist, _ = self.rr.find_subjects(RT.Org, PRED.hasExchangeSpace, esid, id_only=True)
        self.assertEquals(len(orglist), 1)
        self.assertEquals(orglist[0], self.org_id)

        self.container.ex_manager.create_xs.return_value = "xs1"
        self.ems.delete_exchange_space(esid)

        # should no longer have that id in the RR
        with self.assertRaises(NotFound):
            self.rr.read(esid)

        # should no longer have an assoc to an org
        orglist2, _ = self.rr.find_subjects(RT.Org, PRED.hasExchangeSpace, esid, id_only=True)
        self.assertEquals(len(orglist2), 0)

        # should no longer have that exchange declared
        self.assertEquals(self.container.ex_manager.delete_xs.call_count, 1)
        self.assertEquals("xs1", self.container.ex_manager.delete_xs.call_args[0][0])

        with self.assertRaises(NotFound):
            self.ems.delete_exchange_space('123')

    def _do_test_xp(self):
        self._clear_mocks()

        # xp needs an xs first
        exchange_space = ExchangeSpace(name="doink")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        exchange_point = ExchangePoint(name="hammer")
        epid = self.ems.create_exchange_point(exchange_point, esid)

        # should be in RR
        ep2 = self.rr.read(epid)
        self.assertEquals(exchange_point.name, ep2.name)

        ep3 = self.ems.read_exchange_point(epid)
        self.assertEquals(ep3.name, ep2.name)

        # should be associated to the XS as well
        xslist, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangePoint, epid, id_only=True)
        self.assertEquals(len(xslist), 1)
        self.assertEquals(xslist[0], esid)

        # should exist on broker (both xp and xs)
        self.assertEquals(self.container.ex_manager.create_xs.call_count, 2)
        self.assertEquals(self.container.ex_manager.create_xs.call_args[0][0], 'doink')
        # TODO: Weird mock reaction here - code bug?
        # self.assertEquals(self.container.ex_manager.create_xs.call_args[1][0], 'doink')
        self.assertIn('hammer', self.container.ex_manager.create_xp.call_args[0])

        self.ems.delete_exchange_point(epid)
        self.ems.delete_exchange_space(esid)

        # should no longer be in RR
        with self.assertRaises(NotFound):
            self.rr.read(epid)

        # should no longer be associated
        xslist2, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangePoint, epid, id_only=True)
        self.assertEquals(len(xslist2), 0)

        # should no longer exist on broker (both xp and xs)

        # TEST: xp create then delete xs

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

        self.ems.delete_exchange_point(epid)

    def _do_test_xn(self):
        self._clear_mocks()

        # xn needs an xs first
        exchange_space = ExchangeSpace(name="bozo")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        exchange_name = ExchangeName(name='shoes', xn_type="process")
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

        # TEST: xn_declare_no_xs(self):
        exchange_name = ExchangeName(name="shoez", xn_type='process')
        self.assertRaises(NotFound, self.ems.declare_exchange_name, exchange_name, '11')

        # TEST: xn_undeclare_without_declare(self):
        self.assertRaises(NotFound, self.ems.undeclare_exchange_name, 'some_non_id')

        # TEST: xn_declare_then_delete_xs(self):

        # xn needs an xs first
        exchange_space = ExchangeSpace(name="bozo")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        exchange_name = ExchangeName(name='shnoz', xn_type="process")
        enid = self.ems.declare_exchange_name(exchange_name, esid)

        # delete the XS
        self.ems.delete_exchange_space(esid)

        # no longer should have assoc from XS to XN
        xnlist, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangeName, enid, id_only=True)
        self.assertEquals(len(xnlist), 0)

        self.ems.undeclare_exchange_name(enid)
