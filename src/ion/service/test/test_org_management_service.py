#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisiger'

from nose.plugins.attrib import attr

from pyon.util.int_test import IonIntegrationTestCase

from pyon.core.governance import MODERATOR_ROLE, OPERATOR_ROLE, MEMBER_ROLE
from pyon.public import PRED, RT, BadRequest, NotFound, get_ion_ts_millis

from interface.services.core.iorg_management_service import OrgManagementServiceClient
from interface.services.core.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import Org, UserRole, TestInstrument, ActorIdentity


@attr('INT', group='coi')
class TestOrgManagementServiceInt(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/basic.yml')

        self.resource_registry = ResourceRegistryServiceClient()
        self.org_management_service = OrgManagementServiceClient()

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

        org = self.org_management_service.read_org(org_id)
        self.assertNotEqual(org, None)
        self.assertEqual(org.org_governance_name, 'Test_Facility')

        # Check that the roles got associated to them
        role_list = self.org_management_service.list_org_roles(org_id)
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

        self.org_management_service.remove_org_role(org_id, MODERATOR_ROLE)
        with self.assertRaises(NotFound) as cm:
            user_role = self.org_management_service.find_org_role_by_name(org_id, MODERATOR_ROLE)
        self.assertIn("Role MODERATOR not found in Org", cm.exception.message)

        self._do_test_membership(org_id)

        self._do_test_share_and_commitments(org_id)

        self._do_test_containers()

        self._do_test_affiliation(org_id)

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

    def _do_test_membership(self, org_id):
        root_org = self.org_management_service.find_org()
        self.assertNotEqual(root_org, None)

        actor_obj = ActorIdentity(name="Test user")
        actor_id, _ = self.resource_registry.create(actor_obj)

        self.assertTrue(self.org_management_service.is_registered(actor_id))
        self.assertFalse(self.org_management_service.is_enrolled(org_id, actor_id))

        self.assertFalse(self.org_management_service.is_registered(org_id))
        self.assertFalse(self.org_management_service.is_registered("FOOBAR"))
        self.assertTrue(self.org_management_service.is_enrolled(root_org._id, actor_id))
        actor_objs = self.org_management_service.list_enrolled_actors(org_id)
        self.assertEquals(0, len(actor_objs))
        org_objs = self.org_management_service.list_orgs_for_actor(actor_id)
        self.assertEquals(1, len(org_objs))

        role_objs = self.org_management_service.list_actor_roles(actor_id, org_id)
        self.assertEquals(0, len(role_objs))
        self.assertFalse(self.org_management_service.has_role(org_id, actor_id, MEMBER_ROLE))

        self.org_management_service.enroll_member(org_id, actor_id)
        res_ids, _ = self.resource_registry.find_objects(org_id, PRED.hasMember, RT.ActorIdentity, id_only=True)
        self.assertEquals(1, len(res_ids))

        self.assertTrue(self.org_management_service.is_enrolled(org_id, actor_id))
        self.assertTrue(self.org_management_service.has_role(org_id, actor_id, MEMBER_ROLE))
        self.assertFalse(self.org_management_service.has_role(org_id, actor_id, OPERATOR_ROLE))

        actor_objs = self.org_management_service.list_enrolled_actors(org_id)
        self.assertEquals(1, len(actor_objs))
        org_objs = self.org_management_service.list_orgs_for_actor(actor_id)
        self.assertEquals(2, len(org_objs))

        role_objs = self.org_management_service.list_actor_roles(actor_id, org_id)
        self.assertEquals(1, len(role_objs))

        self.org_management_service.grant_role(org_id, actor_id, OPERATOR_ROLE)

        role_objs = self.org_management_service.list_actor_roles(actor_id, org_id)
        self.assertEquals(2, len(role_objs))
        self.assertTrue(self.org_management_service.has_role(org_id, actor_id, OPERATOR_ROLE))

        self.org_management_service.revoke_role(org_id, actor_id, OPERATOR_ROLE)
        role_objs = self.org_management_service.list_actor_roles(actor_id, org_id)
        self.assertEquals(1, len(role_objs))

        self.org_management_service.cancel_member_enrollment(org_id, actor_id)
        res_ids, _ = self.resource_registry.find_objects(org_id, PRED.hasMember, RT.ActorIdentity, id_only=True)
        self.assertEquals(0, len(res_ids))

        self.assertFalse(self.org_management_service.is_enrolled(org_id, actor_id))

        self.resource_registry.delete(actor_id)

    def _do_test_share_and_commitments(self, org_id):
        root_org = self.org_management_service.find_org()
        self.assertNotEqual(root_org, None)

        actor_obj = ActorIdentity(name="Test user")
        actor_id, _ = self.resource_registry.create(actor_obj)

        self.org_management_service.enroll_member(org_id, actor_id)

        inst_obj = TestInstrument(name="Test instrument")
        inst_id, _ = self.resource_registry.create(inst_obj)

        self.assertFalse(self.org_management_service.is_resource_acquired(resource_id=inst_id))

        self.org_management_service.share_resource(org_id, inst_id)
        res_ids, _ = self.resource_registry.find_objects(org_id, PRED.hasResource, id_only=True)
        self.assertEquals(1, len(res_ids))

        cmt_id = self.org_management_service.create_resource_commitment(org_id, actor_id, inst_id)

        self.assertTrue(self.org_management_service.is_resource_acquired(resource_id=inst_id))
        self.assertFalse(self.org_management_service.is_resource_acquired_exclusively(resource_id=inst_id))

        cmt_objs = self.org_management_service.find_commitments(org_id=org_id)
        self.assertEquals(1, len(cmt_objs))
        cmt_objs = self.org_management_service.find_commitments(resource_id=inst_id)
        self.assertEquals(1, len(cmt_objs))
        cmt_objs = self.org_management_service.find_commitments(actor_id=actor_id)
        self.assertEquals(1, len(cmt_objs))

        res_objs = self.org_management_service.find_acquired_resources(org_id=org_id)
        self.assertEquals(1, len(res_objs))
        res_objs = self.org_management_service.find_acquired_resources(actor_id=actor_id)
        self.assertEquals(1, len(res_objs))

        cmt_id = self.org_management_service.create_resource_commitment(org_id, actor_id, inst_id, exclusive=True,
                                                                        expiration=get_ion_ts_millis()+1000)

        self.assertTrue(self.org_management_service.is_resource_acquired(resource_id=inst_id))
        self.assertTrue(self.org_management_service.is_resource_acquired_exclusively(resource_id=inst_id))

        cmt_objs = self.org_management_service.find_commitments(org_id=org_id, exclusive=True)
        self.assertEquals(1, len(cmt_objs))
        cmt_objs = self.org_management_service.find_commitments(resource_id=inst_id, exclusive=True)
        self.assertEquals(1, len(cmt_objs))
        cmt_objs = self.org_management_service.find_commitments(actor_id=actor_id, exclusive=True)
        self.assertEquals(1, len(cmt_objs))

        self.org_management_service.unshare_resource(org_id, inst_id)
        res_ids, _ = self.resource_registry.find_objects(org_id, PRED.hasResource, id_only=True)
        self.assertEquals(0, len(res_ids))

        self.resource_registry.delete(inst_id)
        self.resource_registry.delete(actor_id)

    def _do_test_containers(self):
        # Org containers
        root_org = self.org_management_service.find_org()
        self.assertNotEqual(root_org, None)

        containers = self.org_management_service.find_org_containers(root_org._id)

        all_containers, _ = self.resource_registry.find_resources(restype=RT.CapabilityContainer, id_only=True)

        self.assertEqual(len(containers), len(all_containers))

    def _do_test_affiliation(self, org_id):
        # Org affiliation
        root_org = self.org_management_service.find_org()
        self.assertNotEqual(root_org, None)

        self.org_management_service.affiliate_org(root_org._id, org_id)

        self.org_management_service.unaffiliate_org(root_org._id, org_id)
