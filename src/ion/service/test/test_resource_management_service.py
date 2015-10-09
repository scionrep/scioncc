#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisinger'


from mock import Mock, patch
from nose.plugins.attrib import attr

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import PRED, RT, IonObject, OT, log, BadRequest, Conflict, Inconsistent, NotFound
from pyon.util.context import LocalContextMixin

from ion.service.resource_management_service import ResourceManagementService
from ion.util.testing_utils import create_dummy_resources, create_dummy_events

from interface.services.core.iresource_management_service import ResourceManagementServiceClient


@attr('INT', group='coi')
class TestResourceManagementService(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/basic.yml')
        self.rms = ResourceManagementServiceClient()

    def xtest_ui_ops(self):
        pass
