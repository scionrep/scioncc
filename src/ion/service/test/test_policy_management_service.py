#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisinger'

import unittest
from mock import Mock, patch
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT, IonObject, OT

from interface.objects import PolicyTypeEnum
from ion.service.policy_management_service import PolicyManagementService

from interface.services.core.ipolicy_management_service import PolicyManagementServiceClient


@attr('INT', group='coi')
@patch.dict('pyon.core.governance.governance_controller.CFG', IonIntegrationTestCase._get_alt_cfg({'interceptor': {'interceptors': {'governance': {'config': {'enabled': False}}}}}))
class TestPolicyManagementServiceInt(IonIntegrationTestCase):

    def setUp(self):

        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/basic.yml')
        self.container.governance_controller.policy_event_callback = Mock()

        self.policy_management_service = PolicyManagementServiceClient()

    def test_policy(self):
        self._do_test_policy_crud()

    def _do_test_policy_crud(self):
        policy_rule = '<Rule id="{rule_id}"> <description>{description}</description></Rule>'

        policy_obj = IonObject(RT.Policy, name='Test_Policy', description='This is a test policy',
                               policy_type=PolicyTypeEnum.RESOURCE_ACCESS,
                               definition=policy_rule)

        policy_obj.name = ' '
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_policy(policy_obj)

        policy_obj.name = 'Test_Policy'
        policy_id = self.policy_management_service.create_policy(policy_obj)
        self.assertNotEqual(policy_id, None)

        with self.assertRaises(BadRequest):
            self.policy_management_service.read_policy()
        policy = None
        policy = self.policy_management_service.read_policy(policy_id)
        self.assertNotEqual(policy, None)

        policy.name = ' '
        with self.assertRaises(BadRequest):
            self.policy_management_service.update_policy(policy)
        policy.name = 'Updated_Test_Policy'
        self.policy_management_service.update_policy(policy)

        policy = None
        policy = self.policy_management_service.read_policy(policy_id)
        self.assertNotEqual(policy, None)
        self.assertEqual(policy.name, 'Updated_Test_Policy')

        with self.assertRaises(BadRequest):
            self.policy_management_service.create_resource_access_policy()
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_resource_access_policy(policy_id)
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_resource_access_policy(policy_id, policy.name)
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_resource_access_policy(policy_id, policy.name, "description")
        #p_id =  self.policy_management_service.create_resource_access_policy(policy_id, "Resource_access_name", "Policy Description", "Test_Rule")
        #self.assertNotEqual(p_id, None)

        with self.assertRaises(BadRequest):
            self.policy_management_service.create_service_access_policy()
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_service_access_policy(service_name="service_name")
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_service_access_policy(service_name="service_name", policy_name="policy_name")
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_service_access_policy(service_name="service_name", policy_name="policy_name", description="description")
        #p_obj = self.policy_management_service.create_service_access_policy("service_name", "policy_name", "description", "policy_rule")
        #self.assertNotEqual(p_obj, None)

        with self.assertRaises(BadRequest):
            self.policy_management_service.create_common_service_access_policy()
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_common_service_access_policy(policy_name="policy_name")
        with self.assertRaises(BadRequest):
            self.policy_management_service.create_common_service_access_policy(policy_name="policy_name",description="description")
        #p_id = self.policy_management_service.create_common_service_access_policy(policy_name="policy_name",description="description", policy_rule="test_rule")
        #self.assertNotEqual(p_id, None)

        with self.assertRaises(BadRequest):
            self.policy_management_service.add_process_operation_precondition_policy()
        with self.assertRaises(BadRequest):
            self.policy_management_service.add_process_operation_precondition_policy(process_id="process_id")
        with self.assertRaises(BadRequest):
            self.policy_management_service.add_process_operation_precondition_policy(process_id="process_id", op="op")

        self.policy_management_service.enable_policy(policy_id)
        self.policy_management_service.enable_policy(policy_id)
        with self.assertRaises(BadRequest):
            self.policy_management_service.delete_policy()
        self.policy_management_service.delete_policy(policy_id)

        with self.assertRaises(NotFound) as cm:
            self.policy_management_service.read_policy(policy_id)

        with self.assertRaises(NotFound) as cm:
            self.policy_management_service.delete_policy(policy_id)

    def _do_test_policy_finds(self):
        pass
