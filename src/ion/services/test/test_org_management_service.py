#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisiger'

from nose.plugins.attrib import attr

from pyon.util.int_test import IonIntegrationTestCase

from pyon.core.governance import MODERATOR_ROLE
from pyon.public import PRED, RT, BadRequest, NotFound

from interface.services.core.iorg_management_service import OrgManagementServiceClient
from interface.services.core.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import Org, UserRole


@attr('INT', group='coi')
class TestOrgManagementServiceInt(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/basic.yml')

        self.resource_registry = ResourceRegistryServiceClient(node=self.container.node)
        self.org_management_service = OrgManagementServiceClient(node=self.container.node)

    def test_org_management(self):
        # CRUD
        with self.assertRaises(BadRequest) as br:
            self.org_management_service.create_org(Org(name="Test Facility", org_governance_name="Test Facility"))
        self.assertTrue("contains invalid characters" in br.exception.message)

        with self.assertRaises(BadRequest):
            self.org_management_service.create_org()

        org_obj = Org(name="Test Facility")
        org_id = self.org_management_service.create_org(org_obj)
        self.assertNotEqual(org_id, None)

        org = None
        org = self.org_management_service.read_org(org_id)
        self.assertNotEqual(org, None)
        self.assertEqual(org.org_governance_name, 'Test_Facility')

        # Check that the roles got associated to them
        role_list = self.org_management_service.find_org_roles(org_id)
        self.assertEqual(len(role_list), 3)

        with self.assertRaises(BadRequest):
            self.org_management_service.update_org()
        org.name = 'Updated Test Facility'
        self.org_management_service.update_org(org)

        org = None
        org = self.org_management_service.read_org(org_id)
        self.assertNotEqual(org, None)
        self.assertEqual(org.name, 'Updated Test Facility')
        self.assertEqual(org.org_governance_name, 'Test_Facility')

        user_role = self.org_management_service.find_org_role_by_name(org_id, MODERATOR_ROLE)
        self.assertNotEqual(user_role, None)

        self.org_management_service.remove_user_role(org_id, MODERATOR_ROLE)
        with self.assertRaises(BadRequest) as cm:
            user_role = self.org_management_service.find_org_role_by_name(org_id, MODERATOR_ROLE)
        self.assertIn("The User Role 'MODERATOR' does not exist for this Org", cm.exception.message)

        # Org affiliation
        root_org = self.org_management_service.find_org()
        self.assertNotEqual(root_org, None)

        ret = self.org_management_service.affiliate_org(root_org._id, org_id)
        self.assertTrue(ret)

        ret = self.org_management_service.unaffiliate_org(root_org._id, org_id)
        self.assertTrue(ret)

        # Org containers
        containers = self.org_management_service.find_org_containers(root_org._id)

        all_containers, _ = self.resource_registry.find_resources(restype=RT.CapabilityContainer, id_only=True)

        self.assertEqual(len(containers), len(all_containers))

        # Org deletion
        with self.assertRaises(BadRequest):
            self.org_management_service.delete_org()
        self.org_management_service.delete_org(org_id)

        with self.assertRaises(NotFound) as cm:
            self.org_management_service.read_org(org_id)
        self.assertIn("does not exist", cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.org_management_service.delete_org(org_id)
        self.assertIn("does not exist", cm.exception.message)
