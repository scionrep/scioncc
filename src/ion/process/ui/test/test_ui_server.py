#!/usr/bin/env python

__author__ = 'Michael Meisiger'

from mock import Mock, patch
from nose.plugins.attrib import attr
import gevent
import json
import requests

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import PRED, RT, BadRequest, NotFound, CFG, log
from ion.util.ui_utils import CONT_TYPE_JSON

from interface.services.core.iidentity_management_service import IdentityManagementServiceClient
from interface.services.core.iorg_management_service import OrgManagementServiceClient
from interface.services.core.iresource_registry_service import ResourceRegistryServiceClient

from interface.objects import Org, UserRole, ActorIdentity, UserIdentityDetails, OAuthClientIdentityDetails


@attr('INT', group='coi')
class TestUIServer(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/basic.yml')

        self.resource_registry = ResourceRegistryServiceClient()
        self.org_client = OrgManagementServiceClient()
        self.idm_client = IdentityManagementServiceClient()

        self.ui_server_proc = self.container.proc_manager.procs_by_name["ui_server"]
        self.ui_base_url = self.ui_server_proc.base_url
        self.sg_base_url = self.ui_server_proc.gateway_base_url

    def test_ui_server(self):
        actor_identity_obj = ActorIdentity(name="John Doe")
        actor_id = self.idm_client.create_actor_identity(actor_identity_obj)
        self.idm_client.set_actor_credentials(actor_id, "jdoe", "mypasswd")

        actor_details = UserIdentityDetails()
        actor_details.contact.individual_names_given = "John"
        actor_details.contact.individual_name_family = "Doe"
        self.idm_client.define_identity_details(actor_id, actor_details)

        # TEST: Authentication
        self._do_test_authentication()

        # TEST: Service gateway
        self._do_test_service_gateway(actor_id)

        self.idm_client.delete_actor_identity(actor_id)

    def _do_test_authentication(self):
        session = requests.session()

        # TEST: Login
        resp = session.get(self.ui_base_url + "/auth/session")
        resp_json = self._assert_json_response(resp, None)
        self.assertEquals("", resp_json["result"]["username"])

        resp = session.post(self.ui_base_url + "/auth/login", data=dict(username="jdoe", password="foo"))
        self._assert_json_response(resp, None, status=404)

        resp = session.get(self.ui_base_url + "/auth/session")
        resp_json = self._assert_json_response(resp, None)
        self.assertEquals("", resp_json["result"]["username"])

        resp = session.post(self.ui_base_url + "/auth/login", data=dict(username="jdoe", password="mypasswd"))
        resp_json = self._assert_json_response(resp, None)
        self.assertIn("actor_id", resp_json["result"])
        self.assertIn("username", resp_json["result"])
        self.assertIn("full_name", resp_json["result"])
        self.assertEquals("jdoe", resp_json["result"]["username"])

        resp = session.get(self.ui_base_url + "/auth/session")
        resp_json = self._assert_json_response(resp, None)
        self.assertEquals("jdoe", resp_json["result"]["username"])

        resp = session.get(self.ui_base_url + "/auth/logout")
        self._assert_json_response(resp, "OK")

        resp = session.get(self.ui_base_url + "/auth/session")
        resp_json = self._assert_json_response(resp, None)
        self.assertEquals("", resp_json["result"]["username"])

    def _do_test_service_gateway(self, actor_id):
        # We don't want to modify import order during testing, so import here
        from ion.service.service_gateway import SG_IDENTIFICATION

        session = requests.session()

        # TEST: Service gateway available
        resp = session.get(self.sg_base_url + "/")
        self._assert_json_response(resp, SG_IDENTIFICATION)

        # TEST: Login
        resp = session.post(self.ui_base_url + "/auth/login", data=dict(username="jdoe", password="mypasswd"))
        resp_json = self._assert_json_response(resp, None)
        self.assertEquals("jdoe", resp_json["result"]["username"])

        # TEST: Service access
        resp = session.get(self.sg_base_url + "/request/resource_registry/find_resources?restype=ActorIdentity&id_only=True")
        resp_json = self._assert_json_response(resp, None)
        self.assertIn(actor_id, resp_json["result"][0])

        # Request as POST with JSON data
        payload = dict(data=json.dumps(dict(params=dict(restype="ActorIdentity", id_only=True))))
        resp = session.post(self.sg_base_url + "/request/resource_registry/find_resources", data=payload)
        resp_json = self._assert_json_response(resp, None)
        self.assertIn(actor_id, resp_json["result"][0])

        # Request as POST with JSON data and no params
        payload = dict(data=json.dumps(dict(restype="ActorIdentity", id_only=True)))
        resp = session.post(self.sg_base_url + "/request/resource_registry/find_resources", data=payload)
        resp_json = self._assert_json_response(resp, None)
        self.assertIn(actor_id, resp_json["result"][0])

        # Request as POST with params directly as form data
        payload = dict(restype="ActorIdentity", id_only=True)
        resp = session.post(self.sg_base_url + "/request/resource_registry/find_resources", data=payload)
        resp_json = self._assert_json_response(resp, None)
        self.assertIn(actor_id, resp_json["result"][0])

        resp = session.get(self.sg_base_url + "/request/resource_registry/read/" + actor_id)
        resp_json = self._assert_json_response(resp, None)
        self.assertIn("type_", resp_json["result"])

        # TEST: REST API
        resp = session.get(self.sg_base_url + "/rest/identity_management/actor_identity")
        resp_json = self._assert_json_response(resp, None)
        self.assertIn(actor_id, [o["_id"] for o in resp_json["result"]])
        num_actors = len(resp_json["result"])

        resp = session.get(self.sg_base_url + "/rest/identity_management/actor_identity/" + actor_id)
        resp_json = self._assert_json_response(resp, None)
        self.assertIn("type_", resp_json["result"])

        # Form encoded create request
        other_actor_obj = ActorIdentity(name="Jane Foo")
        other_actor_obj.details = None
        other_actor_obj_dict = other_actor_obj.__dict__.copy()
        # Remove unseralizable attributes.
        del other_actor_obj_dict['passwd_reset_token']
        payload = dict(data=json.dumps(other_actor_obj_dict))
        resp = session.post(self.sg_base_url + "/rest/identity_management/actor_identity", data=payload)
        resp_json = self._assert_json_response(resp, None)
        other_actor_id = resp_json["result"]

        resp = session.get(self.sg_base_url + "/rest/identity_management/actor_identity")
        resp_json = self._assert_json_response(resp, None)
        self.assertEquals(len(resp_json["result"]), num_actors + 1)

        resp = session.get(self.sg_base_url + "/rest/identity_management/actor_identity/" + other_actor_id)
        resp_json = self._assert_json_response(resp, None)
        self.assertIn("type_", resp_json["result"])
        self.assertEquals(other_actor_id, resp_json["result"]["_id"])

        # Form encoded update request
        resp_json["result"]["name"] = "Jane Long"
        payload = dict(data=json.dumps(resp_json["result"]))
        resp = session.put(self.sg_base_url + "/rest/identity_management/actor_identity/" + other_actor_id, data=payload)
        resp_json = self._assert_json_response(resp, None)

        resp = session.get(self.sg_base_url + "/rest/identity_management/actor_identity/" + other_actor_id)
        resp_json = self._assert_json_response(resp, None)
        self.assertIn("type_", resp_json["result"])
        self.assertEquals("Jane Long", resp_json["result"]["name"])

        # JSON enconded request
        resp_json["result"]["name"] = "Jane Dunn"
        payload = json.dumps(resp_json["result"])
        resp = session.put(self.sg_base_url + "/rest/identity_management/actor_identity/" + other_actor_id, data=payload,
                           headers={'Content-Type': CONT_TYPE_JSON})
        resp_json = self._assert_json_response(resp, None)

        resp = session.get(self.sg_base_url + "/rest/identity_management/actor_identity/" + other_actor_id)
        resp_json = self._assert_json_response(resp, None)
        self.assertIn("type_", resp_json["result"])
        self.assertEquals("Jane Dunn", resp_json["result"]["name"])


    def _assert_json_response(self, resp, result, status=200):
        self.assertIn("application/json", resp.headers["content-type"])
        if status:
            self.assertEquals(status, resp.status_code)
        resp_json = resp.json()
        if result is not None:
            self.assertIn("result", resp_json)
            if type(result) in (str, unicode, int, long, float, bool):
                self.assertEquals(result, resp_json["result"])
            else:
                self.fail("Unsupported result type")
        return resp_json

    def test_ui_oauth2(self):
        actor_identity_obj = ActorIdentity(name="John Doe")
        actor_id = self.idm_client.create_actor_identity(actor_identity_obj)
        self.idm_client.set_actor_credentials(actor_id, "jdoe", "mypasswd")

        actor_details = UserIdentityDetails()
        actor_details.contact.individual_names_given = "John"
        actor_details.contact.individual_name_family = "Doe"
        self.idm_client.define_identity_details(actor_id, actor_details)

        client_obj = ActorIdentity(name="UI Client", details=OAuthClientIdentityDetails(default_scopes="scioncc"))
        client_actor_id = self.idm_client.create_actor_identity(client_obj)
        client_id = "ui"
        self.idm_client.set_actor_credentials(client_actor_id, "client:"+client_id, "client_secret")

        session = requests.session()

        # TEST: OAuth2 authorize
        log.info("------------ Get token #1")
        auth_params = {"client_id": client_id, "grant_type": "password", "username": "jdoe", "password": "mypasswd"}
        resp = session.post(self.ui_base_url + "/oauth/token", data=auth_params)
        access_token = resp.json()

        # TEST: Service access
        log.info("------------ Access with token")
        resp = session.get(self.sg_base_url + "/request/resource_registry/find_resources?restype=ActorIdentity&id_only=True",
                           headers={"Authorization": "Bearer %s" % access_token["access_token"]})
        resp_json = self._assert_json_response(resp, None)
        #self.assertIn(actor_id, resp_json["result"][0])

        log.info("------------ Access with bad token")
        resp = session.get(self.sg_base_url + "/request/resource_registry/find_resources?restype=ActorIdentity&id_only=True",
                           headers={"Authorization": "Bearer FOOBAR"})
        resp_json = self._assert_json_response(resp, None, status=401)

        # TEST: Get session using token
        log.info("------------ Get session using token")
        resp = session.get(self.ui_base_url + "/auth/session",
                           headers={"Authorization": "Bearer %s" % access_token["access_token"]})
        resp_json = self._assert_json_response(resp, None)
        self.assertEqual(actor_id, resp_json["result"]["actor_id"])

        # TEST: Get new access token
        log.info("------------ Refresh token")
        auth_params = {"client_id": client_id, "grant_type": "refresh_token", "refresh_token": access_token["refresh_token"]}
        resp = session.post(self.ui_base_url + "/oauth/token", data=auth_params)
        access_token1 = resp.json()


        with patch('ion.process.ui.server.ui_instance.session_timeout', 2):
            log.info("Patched server.session_timeout to %s", self.ui_server_proc.session_timeout)

            session = requests.session()

            log.info("------------ Get token #2 (with short expiration)")
            auth_params = {"client_id": client_id, "grant_type": "password", "username": "jdoe", "password": "mypasswd"}
            resp = session.post(self.ui_base_url + "/oauth/token", data=auth_params)
            access_token = resp.json()

            # TEST: Service access
            log.info("------------ Access before expired")
            resp = session.get(self.sg_base_url + "/request/resource_registry/find_resources?restype=ActorIdentity&id_only=True",
                               headers={"Authorization": "Bearer %s" % access_token["access_token"]})
            resp_json = self._assert_json_response(resp, None)

            gevent.sleep(2)

            # TEST: Service access fails after expiration
            log.info("------------ Access after token expiration")
            resp = session.get(self.sg_base_url + "/request/resource_registry/find_resources?restype=ActorIdentity&id_only=True",
                               headers={"Authorization": "Bearer %s" % access_token["access_token"]})
            resp_json = self._assert_json_response(resp, None, status=401)


        with patch('ion.process.ui.server.ui_instance.session_timeout', 2), \
             patch('ion.process.ui.server.ui_instance.extend_session_timeout', True), \
             patch('ion.process.ui.server.ui_instance.max_session_validity', 3):

            session = requests.session()

            log.info("------------ Get token #3 (with short expiration)")
            auth_params = {"client_id": client_id, "grant_type": "password", "username": "jdoe", "password": "mypasswd"}
            resp = session.post(self.ui_base_url + "/oauth/token", data=auth_params)
            access_token = resp.json()

            gevent.sleep(1)

            # TEST: Service access extends expiration
            log.info("------------ Access before expired should extend expiration")
            resp = session.get(self.sg_base_url + "/request/resource_registry/find_resources?restype=ActorIdentity&id_only=True",
                               headers={"Authorization": "Bearer %s" % access_token["access_token"]})
            resp_json = self._assert_json_response(resp, None)

            gevent.sleep(1.1)

            # TEST: Service access will fail unless was extended
            log.info("------------ Access before expired after extension")
            resp = session.get(self.sg_base_url + "/request/resource_registry/find_resources?restype=ActorIdentity&id_only=True",
                               headers={"Authorization": "Bearer %s" % access_token["access_token"]})
            resp_json = self._assert_json_response(resp, None)

            gevent.sleep(1.5)

            # TEST: Service access fails after max validity
            log.info("------------ Access after token expiration")
            resp = session.get(self.sg_base_url + "/request/resource_registry/find_resources?restype=ActorIdentity&id_only=True",
                               headers={"Authorization": "Bearer %s" % access_token["access_token"]})
            resp_json = self._assert_json_response(resp, None, status=401)


        # TEST: Remember user from within session
        session = requests.session()
        log.info("------------ Get token #4")
        auth_params = {"client_id": client_id, "grant_type": "password", "username": "jdoe", "password": "mypasswd"}
        resp = session.post(self.ui_base_url + "/oauth/token", data=auth_params)
        access_token = resp.json()

        # TEST: Get session without token tests remember user
        log.info("------------ Get session without token")
        resp = session.get(self.ui_base_url + "/auth/session")
        resp_json = self._assert_json_response(resp, None)
        self.assertEqual(actor_id, resp_json["result"]["actor_id"])

        # TEST: Logout
        log.info("------------ Logout")
        resp = session.get(self.ui_base_url + "/auth/logout",
                           headers={"Authorization": "Bearer %s" % access_token["access_token"]})
        resp_json = self._assert_json_response(resp, None)
        self.assertEqual(resp_json["result"], "OK")

        # TEST: Get session without token after logout
        log.info("------------ Get session without token")
        resp = session.get(self.ui_base_url + "/auth/session")
        resp_json = self._assert_json_response(resp, None)
        self.assertEqual(resp_json["result"]["actor_id"], "")

        # TEST: Service access after logout
        log.info("------------ Access with token after logout")
        resp = session.get(self.sg_base_url + "/request/resource_registry/find_resources?restype=ActorIdentity&id_only=True",
                           headers={"Authorization": "Bearer %s" % access_token["access_token"]})
        resp_json = self._assert_json_response(resp, None, 401)


        with patch('ion.process.ui.server.ui_instance.remember_user', False):
            session = requests.session()
            log.info("------------ Get token #5")
            auth_params = {"client_id": client_id, "grant_type": "password", "username": "jdoe", "password": "mypasswd"}
            resp = session.post(self.ui_base_url + "/oauth/token", data=auth_params)
            access_token = resp.json()

            # TEST: Get session without token fails if remember user is False
            log.info("------------ Get session without token")
            resp = session.get(self.ui_base_url + "/auth/session")
            resp_json = self._assert_json_response(resp, None)
            self.assertEqual(resp_json["result"]["actor_id"], "")