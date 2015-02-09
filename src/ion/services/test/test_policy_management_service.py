#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisinger'

import unittest
from mock import Mock, patch
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT, IonObject, OT
from ion.services.policy_management_service import PolicyManagementService

from interface.services.core.ipolicy_management_service import PolicyManagementServiceClient


@attr('INT', group='coi')
class TestPolicyManagementServiceInt(IonIntegrationTestCase):

    def setUp(self):

        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/basic.yml')

        self.policy_management_service = PolicyManagementServiceClient()

    def test_policy_crud(self):

        res_policy_obj = IonObject(OT.ResourceAccessPolicy, policy_rule='<Rule id="%s"> <description>%s</description></Rule>')

        policy_obj = IonObject(RT.Policy, name='Test_Policy',
            description='This is a test policy',
            policy_type=res_policy_obj)

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
            self.policy_management_service.add_process_operation_precondition_policy(process_name="process_name")
        with self.assertRaises(BadRequest):
            self.policy_management_service.add_process_operation_precondition_policy(process_name="process_name", op="op")

        self.policy_management_service.enable_policy(policy_id)
        self.policy_management_service.enable_policy(policy_id)
        with self.assertRaises(BadRequest):
            self.policy_management_service.delete_policy()
        self.policy_management_service.delete_policy(policy_id)

        with self.assertRaises(NotFound) as cm:
            self.policy_management_service.read_policy(policy_id)
        self.assertIn("does not exist", cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.policy_management_service.delete_policy(policy_id)
        self.assertIn("does not exist", cm.exception.message)
