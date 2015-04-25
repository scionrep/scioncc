#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from nose.plugins.attrib import attr
import gevent

from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.exception import Unauthorized
from pyon.public import BadRequest, NotFound, get_ion_ts_millis, Inconsistent, Unauthorized

from interface.services.core.iidentity_management_service import IdentityManagementServiceClient
from interface.services.core.iorg_management_service import OrgManagementServiceClient
from interface.services.core.iresource_registry_service import ResourceRegistryServiceClient

from interface.objects import ActorIdentity, AuthStatusEnum, UserIdentityDetails, Credentials
from interface.objects import TokenTypeEnum


@attr('INT', group='coi')
class TestIdentityManagementServiceInt(IonIntegrationTestCase):
    
    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/basic.yml')

        self.resource_registry = ResourceRegistryServiceClient()
        self.identity_management_service = IdentityManagementServiceClient()
        self.org_client = OrgManagementServiceClient()

    def test_actor_identity(self):
        # TEST: Create, Update, Read
        actor_identity_obj = ActorIdentity(name="John Doe")
        actor_id = self.identity_management_service.create_actor_identity(actor_identity_obj)

        actor_identity = self.identity_management_service.read_actor_identity(actor_id)
        self.assertEquals(actor_identity_obj.name, actor_identity.name)

        actor_identity.name = 'Updated name'
        self.identity_management_service.update_actor_identity(actor_identity)

        ai = self.identity_management_service.find_actor_identity_by_name(actor_identity.name)
        self.assertEquals(ai.name, actor_identity.name)
        with self.assertRaises(NotFound):
            ai = self.identity_management_service.find_actor_identity_by_name("##FOO USER##")

        self.assertEquals(ai.name, actor_identity.name)

        # TEST: Actor credentials
        self._do_test_credentials(actor_id)

        # TEST: Identity details (user profile)
        self._do_test_profile(actor_id)

        # TEST: Password reset
        self._do_test_password_reset(actor_id)

        # TEST: Auth tokens
        self._do_test_auth_tokens(actor_id)

        # TEST: Delete
        self.identity_management_service.delete_actor_identity(actor_id)
 
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.read_actor_identity(actor_id)
        self.assertTrue("does not exist" in cm.exception.message)
 
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.delete_actor_identity(actor_id)
        self.assertTrue("does not exist" in cm.exception.message)

    def _do_test_credentials(self, actor_id):
        actor_identity = self.identity_management_service.read_actor_identity(actor_id)
        self.assertEquals(len(actor_identity.credentials), 0)

        actor_cred = Credentials(username="jdoe", password_hash="123", password_salt="foo")
        self.identity_management_service.register_credentials(actor_id, actor_cred)

        actor_identity = self.identity_management_service.read_actor_identity(actor_id)
        self.assertEquals(len(actor_identity.credentials), 1)
        self.assertEquals(actor_identity.credentials[0].username, "jdoe")

        actor_id1 = self.identity_management_service.find_actor_identity_by_username("jdoe")
        self.assertEquals(actor_id1, actor_id)
        with self.assertRaises(NotFound):
            self.identity_management_service.find_actor_identity_by_username("##FOO USER##")

        self.identity_management_service.unregister_credentials(actor_id, "jdoe")
        actor_identity = self.identity_management_service.read_actor_identity(actor_id)
        self.assertEquals(len(actor_identity.credentials), 0)

        self.identity_management_service.set_actor_credentials(actor_id, "jdoe1", "mypasswd")
        actor_identity = self.identity_management_service.read_actor_identity(actor_id)
        self.assertEquals(len(actor_identity.credentials), 1)
        self.assertEquals(actor_identity.credentials[0].username, "jdoe1")
        self.assertNotEquals(actor_identity.credentials[0].password_hash, "mypasswd")

        actor_id1 = self.identity_management_service.check_actor_credentials("jdoe1", "mypasswd")
        self.assertEquals(actor_id1, actor_id)

        with self.assertRaises(NotFound):
            self.identity_management_service.check_actor_credentials("jdoe1", "mypasswd1")

        self.identity_management_service.set_user_password("jdoe1", "mypasswd1")
        actor_id1 = self.identity_management_service.check_actor_credentials("jdoe1", "mypasswd1")
        self.assertEquals(actor_id1, actor_id)

        for i in range(6):
            with self.assertRaises(NotFound):
                self.identity_management_service.check_actor_credentials("jdoe1", "mypasswd0")

        with self.assertRaises(NotFound):
            self.identity_management_service.check_actor_credentials("jdoe1", "mypasswd1")

        self.identity_management_service.set_actor_auth_status(actor_id, AuthStatusEnum.ENABLED)
        actor_id1 = self.identity_management_service.check_actor_credentials("jdoe1", "mypasswd1")
        self.assertEquals(actor_id1, actor_id)


    def _do_test_profile(self, actor_id):
        actor_details1 = self.identity_management_service.read_identity_details(actor_id)
        self.assertEquals(actor_details1, None)

        actor_details = UserIdentityDetails()
        actor_details.contact.individual_names_given = "John"
        actor_details.contact.individual_name_family = "Doe"
        self.identity_management_service.define_identity_details(actor_id, actor_details)

        actor_details1 = self.identity_management_service.read_identity_details(actor_id)
        self.assertEquals(actor_details1.contact.individual_names_given, actor_details.contact.individual_names_given)

    def _do_test_password_reset(self, actor_id=''):
        idm = self.identity_management_service
        actor_obj = idm.read_actor_identity(actor_id)
        username = actor_obj.credentials[0].username
        reset_token = idm.request_password_reset(username=username)

        actor_obj = idm.read_actor_identity(actor_id)

        self.assertEquals(actor_obj.passwd_reset_token.token_type, TokenTypeEnum.ACTOR_RESET_PASSWD)
        self.assertTrue(actor_obj.passwd_reset_token.token_string)
        self.assertEquals(reset_token, actor_obj.passwd_reset_token.token_string)

        self.assertRaises(Unauthorized, idm.reset_password,
                          username=username,
                          token_string='xxx', new_password='passwd')

        idm.reset_password(username=username, token_string=reset_token, new_password='xyddd')

        actor_obj = idm.read_actor_identity(actor_id)
        self.assertEquals(actor_obj.passwd_reset_token, None)

        self.assertRaises(Unauthorized, idm.reset_password,
                          username=username,
                          token_string=reset_token, new_password='passwd')

    def _do_test_auth_tokens(self, actor_id):
        # Note: test of service gateway token functionality is in SGS test

        token_str = self.identity_management_service.create_authentication_token(actor_id, validity=10000)
        self.assertIsInstance(token_str, str)
        self.assertGreaterEqual(len(token_str), 25)

        token_info = self.identity_management_service.check_authentication_token(token_str)
        self.assertEquals(token_info["actor_id"], actor_id)

        token_info = self.identity_management_service.check_authentication_token(token_str)
        self.assertGreaterEqual(int(token_info["expiry"]), get_ion_ts_millis())

        with self.assertRaises(BadRequest):
            self.identity_management_service.create_authentication_token(actor_id="", validity=10000)

        with self.assertRaises(BadRequest):
            self.identity_management_service.create_authentication_token(actor_id, validity="FOO")

        with self.assertRaises(BadRequest):
            self.identity_management_service.create_authentication_token(actor_id, validity=-200)

        cur_time = get_ion_ts_millis()

        with self.assertRaises(BadRequest):
            self.identity_management_service.create_authentication_token(actor_id, start_time=str(cur_time-100000), validity=50)

        with self.assertRaises(BadRequest):
            self.identity_management_service.create_authentication_token(actor_id, validity=35000000)

        with self.assertRaises(NotFound):
            self.identity_management_service.check_authentication_token("UNKNOWN")

        token_str2 = self.identity_management_service.create_authentication_token(actor_id, validity=1)
        token_info = self.identity_management_service.check_authentication_token(token_str2)

        gevent.sleep(1.1)

        with self.assertRaises(Unauthorized):
            self.identity_management_service.check_authentication_token(token_str2)

        token = self.identity_management_service.read_authentication_token(token_str2)

        token.expires = str(cur_time + 5000)
        self.identity_management_service.update_authentication_token(token)
        token_info = self.identity_management_service.check_authentication_token(token_str2)

        token_str3 = self.identity_management_service.create_authentication_token(actor_id, validity=2)
        token_info = self.identity_management_service.check_authentication_token(token_str3)

        self.identity_management_service.invalidate_authentication_token(token_str3)

        with self.assertRaises(Unauthorized):
            self.identity_management_service.check_authentication_token(token_str3)

        token = self.identity_management_service.read_authentication_token(token_str3)
        self.assertEquals(token.token_string, token_str3)
        self.assertIn(token_str3, token._id)

        token.status = "OPEN"
        self.identity_management_service.update_authentication_token(token)

        token_info = self.identity_management_service.check_authentication_token(token_str3)
