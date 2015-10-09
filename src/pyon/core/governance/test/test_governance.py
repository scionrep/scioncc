#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'

from mock import Mock
from nose.plugins.attrib import attr

from pyon.util.unit_test import PyonTestCase

from pyon.core.bootstrap import IonObject
from pyon.core.exception import Unauthorized, BadRequest, Inconsistent
from pyon.core.governance.governance_controller import GovernanceController
from pyon.core.governance import MODERATOR_ROLE, MEMBER_ROLE, SUPERUSER_ROLE, GovernanceHeaderValues
from pyon.core.governance import find_roles_by_actor, get_actor_header, get_system_actor_header, get_role_message_headers, get_valid_resource_commitments, get_valid_principal_commitments
from pyon.ion.resource import PRED, RT
from pyon.ion.service import BaseService
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin

from interface.services.examples.ihello_service  import HelloServiceProcessClient


class UnitTestService(BaseService):
    name = 'UnitTestService'

    def test_op(self):
        pass

    def func1(self, msg,  header):
        return True, ''

    def func2(self, msg,  header):
        return False, 'No reason'

    def func3(self, msg,  header):
        return True, ''

    #This invalid test function does not have the proper signature
    def bad_signature(self, msg):
        return True, ''

    #This invalid test function does not have the proper return tuple
    def bad_return(self, msg, header):
        return True


@attr('UNIT')
class GovernanceUnitTest(PyonTestCase):

    governance_controller = None

    def setUp(self):
        FakeContainer = Mock()
        FakeContainer.id = "containerid"
        FakeContainer.node = Mock()
        self.governance_controller = GovernanceController(FakeContainer())

        self.pre_func1 =\
        """def precondition_func(process, msg, headers):
            if headers['op'] != 'test_op':
                return False, 'Cannot call the test_op operation'
            else:
                return True, ''

        """

        self.pre_func2 =\
        """def precondition_func(process, msg, headers):
            if headers['op'] == 'test_op':
                return False, 'Cannot call the test_op operation'
            else:
                return True, ''

        """

        #This invalid test function does not have the proper signature
        self.bad_pre_func1 =\
        """def precondition_func(msg, headers):
            if headers['op'] == 'test_op':
                return False, 'Cannot call the test_op operation'
            else:
                return True, ''

        """

        #This invalid test function does not return the proper tuple
        self.bad_pre_func2 =\
        """def precondition_func(process, msg, headers):
            if headers['op'] == 'test_op':
                return False
            else:
                return True

        """

    def test_initialize_from_config(self):

        intlist = {'policy'}
        config = {'interceptor_order': intlist,
                  'governance_interceptors':
                  {'policy': {'class': 'pyon.core.governance.policy.policy_interceptor.PolicyInterceptor'}}}

        self.governance_controller.initialize_from_config(config)

        self.assertEquals(self.governance_controller.interceptor_order, intlist)
        self.assertEquals(len(self.governance_controller.interceptor_by_name_dict),
                          len(config['governance_interceptors']))

    # TODO - Need to fill this method out
    def test_process_message(self):
        pass

    def test_register_process_operation_precondition(self):

        bs = UnitTestService()

        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)), 0)
        self.governance_controller.register_process_operation_precondition(bs, 'test_op', bs.func1)
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)), 1)
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)['test_op']), 1)

        self.governance_controller.register_process_operation_precondition(bs, 'test_op', 'func2')
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)['test_op']), 2)

        #Its possible to register invalid functions
        self.governance_controller.register_process_operation_precondition(bs, 'test_op', 'func4')
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)['test_op']), 3)

        self.governance_controller.register_process_operation_precondition(bs, 'test_op', 'self.pre_func1')
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)['test_op']), 4)

        #Its possible to register invalid functions
        self.governance_controller.register_process_operation_precondition(bs, 'test_op', bs.bad_signature)
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)['test_op']), 5)

    def test_unregister_process_operation_precondition(self):

        bs = UnitTestService()

        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)), 0)
        self.governance_controller.register_process_operation_precondition(bs, 'test_op', bs.func1)
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)), 1)
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)['test_op']), 1)

        self.governance_controller.register_process_operation_precondition(bs, 'test_op', 'func2')
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)['test_op']), 2)

        self.governance_controller.unregister_process_operation_precondition(bs, 'test_op', 'func1')
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)['test_op']), 2)

        self.governance_controller.unregister_process_operation_precondition(bs, 'test_op', bs.func1)
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)['test_op']), 1)

        self.governance_controller.register_process_operation_precondition(bs, 'test_op', self.pre_func1)
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)['test_op']), 2)

        self.governance_controller.unregister_process_operation_precondition(bs, 'test_op', self.pre_func1)
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)['test_op']), 1)

        self.governance_controller.unregister_process_operation_precondition(bs, 'test_op',  'func2')
        self.assertEqual(len(self.governance_controller.get_process_operation_dict(bs.name)), 0)

    def test_check_process_operation_preconditions(self):

        bs = UnitTestService()

        self.governance_controller.register_process_operation_precondition(bs, 'test_op', bs.func1)
        self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})

        self.governance_controller.register_process_operation_precondition(bs, 'test_op', 'func2')
        with self.assertRaises(Unauthorized) as cm:
            self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})
        self.assertIn('No reason', cm.exception.message)

        self.governance_controller.unregister_process_operation_precondition(bs, 'test_op', 'func2')
        self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})

        self.governance_controller.register_process_operation_precondition(bs, 'test_op', bs.func3)
        self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})

        self.governance_controller.register_process_operation_precondition(bs, 'test_op', bs.func2)
        with self.assertRaises(Unauthorized) as cm:
            self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})
        self.assertIn('No reason', cm.exception.message)

        self.governance_controller.unregister_process_operation_precondition(bs, 'test_op', bs.func2)
        self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})

        self.governance_controller.register_process_operation_precondition(bs, 'test_op', self.pre_func1)
        self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})

        self.governance_controller.register_process_operation_precondition(bs, 'test_op', self.pre_func2)
        with self.assertRaises(Unauthorized) as cm:
            self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})
        self.assertIn('Cannot call the test_op operation', cm.exception.message)

        self.governance_controller.unregister_process_operation_precondition(bs, 'test_op', self.pre_func2)
        self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})

        #Its possible to register invalid functions - but it should get ignored when checked
        self.governance_controller.register_process_operation_precondition(bs, 'test_op', 'func4')
        self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})
        self.governance_controller.unregister_process_operation_precondition(bs, 'test_op', 'func4')

        #Its possible to register invalid functions - but it should get ignored when checked
        self.governance_controller.register_process_operation_precondition(bs, 'test_op', bs.bad_signature)
        self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})
        self.governance_controller.unregister_process_operation_precondition(bs, 'test_op', bs.bad_signature)

        #Its possible to register invalid functions - but it should get ignored when checked
        self.governance_controller.register_process_operation_precondition(bs, 'test_op', bs.bad_return)
        self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})
        self.governance_controller.unregister_process_operation_precondition(bs, 'test_op', bs.bad_return)

        #Its possible to register invalid functions - but they it get ignored when checked
        self.governance_controller.register_process_operation_precondition(bs, 'test_op', self.bad_pre_func1)
        self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})
        self.governance_controller.unregister_process_operation_precondition(bs, 'test_op', self.bad_pre_func1)

        #Its possible to register invalid functions - but it should get ignored when checked
        self.governance_controller.register_process_operation_precondition(bs, 'test_op', self.bad_pre_func2)
        self.governance_controller.check_process_operation_preconditions(bs, {}, {'op': 'test_op'})
        self.governance_controller.unregister_process_operation_precondition(bs, 'test_op', self.bad_pre_func2)

    def test_governance_header_values(self):
        process = Mock()
        process.name = 'test_process'

        headers = {'op': 'test_op', 'process': process, 'request': 'request', 'ion-actor-id': 'ionsystem', 'receiver': 'resource-registry',
                                   'sender-type': 'sender-type', 'resource-id': '123xyz' ,'sender-service': 'sender-service',
                                   'ion-actor-roles': {'ION': [SUPERUSER_ROLE, MODERATOR_ROLE, MEMBER_ROLE]}}

        gov_values = GovernanceHeaderValues(headers)
        self.assertEqual(gov_values.op, 'test_op')
        self.assertEqual(gov_values.process_name, 'test_process')
        self.assertEqual(gov_values.actor_id, 'ionsystem')
        self.assertEqual(gov_values.actor_roles, {'ION': [SUPERUSER_ROLE, MODERATOR_ROLE, MEMBER_ROLE]})
        self.assertEqual(gov_values.resource_id,'123xyz')

        self.assertRaises(BadRequest, GovernanceHeaderValues, {})

        headers = {'op': 'test_op', 'request': 'request', 'ion-actor-id': 'ionsystem', 'receiver': 'resource-registry',
                   'sender-type': 'sender-type', 'resource-id': '123xyz' ,'sender-service': 'sender-service',
                   'ion-actor-roles': {'ION': [SUPERUSER_ROLE, MODERATOR_ROLE, MEMBER_ROLE]}}

        gov_values = GovernanceHeaderValues(headers)
        self.assertEqual(gov_values.op, 'test_op')
        self.assertEqual(gov_values.process_name, 'Unknown-Process')
        self.assertEqual(gov_values.actor_id, 'ionsystem')
        self.assertEqual(gov_values.actor_roles, {'ION': [SUPERUSER_ROLE, MODERATOR_ROLE, MEMBER_ROLE]})
        self.assertEqual(gov_values.resource_id,'123xyz')

        headers = {'op': 'test_op', 'request': 'request', 'receiver': 'resource-registry',
                   'sender-type': 'sender-type', 'resource-id': '123xyz' ,'sender-service': 'sender-service',
                   'ion-actor-roles': {'ION': [SUPERUSER_ROLE, MODERATOR_ROLE, MEMBER_ROLE]}}

        self.assertRaises(Inconsistent, GovernanceHeaderValues, headers)

        headers = {'op': 'test_op', 'request': 'request', 'ion-actor-id': 'ionsystem', 'receiver': 'resource-registry',
                   'sender-type': 'sender-type', 'resource-id': '123xyz' ,'sender-service': 'sender-service',
                   'ion-actor-123-roles': {'ION': [SUPERUSER_ROLE, MODERATOR_ROLE, MEMBER_ROLE]}}

        self.assertRaises(Inconsistent, GovernanceHeaderValues, headers)

        headers = {'op': 'test_op', 'request': 'request', 'ion-actor-id': 'ionsystem', 'receiver': 'resource-registry',
                   'sender-type': 'sender-type','sender-service': 'sender-service',
                   'ion-actor-roles': {'ION': [SUPERUSER_ROLE, MODERATOR_ROLE, MEMBER_ROLE]}}

        self.assertRaises(Inconsistent, GovernanceHeaderValues, headers)

        gov_values = GovernanceHeaderValues(headers, resource_id_required=False)
        self.assertEqual(gov_values.op, 'test_op')
        self.assertEqual(gov_values.process_name, 'Unknown-Process')
        self.assertEqual(gov_values.actor_id, 'ionsystem')
        self.assertEqual(gov_values.actor_roles, {'ION': [SUPERUSER_ROLE, MODERATOR_ROLE, MEMBER_ROLE]})
        self.assertEqual(gov_values.resource_id,'')


class GovernanceTestProcess(LocalContextMixin):
    name = 'gov_test'
    id='gov_client'
    process_type = 'simple'


@attr('INT')
class GovernanceIntTest(IonIntegrationTestCase):


    def setUp(self):

        self._start_container()

        #Instantiate a process to represent the test
        self.gov_client = GovernanceTestProcess()

        self.rr = self.container.resource_registry

    def add_org_role(self, org='', user_role=None):
        """Adds a UserRole to an Org. Will call Policy Management Service to actually
        create the role object that is passed in, if the role by the specified
        name does not exist. Throws exception if either id does not exist.
        """
        user_role.org_governance_name = org.org_governance_name
        user_role_id, _ = self.rr.create(user_role)

        aid = self.rr.create_association(org._id, PRED.hasRole, user_role_id)

        return user_role_id

    def test_get_actor_header(self):

        #Setup data
        actor = IonObject(RT.ActorIdentity, name='actor1')
        actor_id, _ = self.rr.create(actor)

        ion_org = IonObject(RT.Org, name='ION', org_governance_name='ION')
        ion_org_id, _ = self.rr.create(ion_org)
        ion_org._id = ion_org_id

        manager_role = IonObject(RT.UserRole, name='Org Manager', governance_name=MODERATOR_ROLE, description='Org Manager')
        manager_role_id = self.add_org_role(ion_org, manager_role)

        member_role = IonObject(RT.UserRole, name='Org Member', governance_name=MEMBER_ROLE, description='Org Member')


        # all actors have a defaul MEMBER_ROLE
        actor_roles = find_roles_by_actor(actor_id)
        self.assertDictEqual(actor_roles, {'ION': [MEMBER_ROLE]})

        actor_header = get_actor_header(actor_id)
        self.assertDictEqual(actor_header, {'ion-actor-id': actor_id, 'ion-actor-roles': {'ION': [MEMBER_ROLE]}})

        #Add Org Manager Role
        self.rr.create_association(actor_id, PRED.hasRole, manager_role_id)

        actor_roles = find_roles_by_actor(actor_id)
        role_header = get_role_message_headers({'ION': [manager_role, member_role]})
        self.assertDictEqual(actor_roles, role_header)

        org2 = IonObject(RT.Org, name='Org 2', org_governance_name='Second_Org')

        org2_id, _ = self.rr.create(org2)
        org2._id = org2_id


        member2_role = IonObject(RT.UserRole, governance_name=MEMBER_ROLE, name='Org Member', description='Org Member')
        member2_role_id = self.add_org_role(org2, member2_role)

        operator2_role = IonObject(RT.UserRole, governance_name='OPERATOR', name='Instrument Operator',
                                   description='Instrument Operator')
        operator2_role_id = self.add_org_role(org2, operator2_role)

        self.rr.create_association(actor_id, PRED.hasRole, member2_role_id)

        self.rr.create_association(actor_id, PRED.hasRole, operator2_role_id)

        actor_roles = find_roles_by_actor(actor_id)

        role_header = get_role_message_headers({'ION': [manager_role, member_role], 'Second_Org': [operator2_role, member2_role]})

        self.assertEqual(len(actor_roles), 2)
        self.assertEqual(len(role_header), 2)
        self.assertIn('Second_Org', actor_roles)
        self.assertIn('Second_Org', role_header)
        self.assertEqual(len(actor_roles['Second_Org']), 2)
        self.assertEqual(len(role_header['Second_Org']), 2)
        self.assertIn('OPERATOR', actor_roles['Second_Org'])
        self.assertIn('OPERATOR', role_header['Second_Org'])
        self.assertIn(MEMBER_ROLE, actor_roles['Second_Org'])
        self.assertIn(MEMBER_ROLE, role_header['Second_Org'])
        self.assertIn('ION', actor_roles)
        self.assertIn('ION', role_header)
        self.assertIn(MODERATOR_ROLE, actor_roles['ION'])
        self.assertIn(MEMBER_ROLE, actor_roles['ION'])
        self.assertIn(MODERATOR_ROLE, role_header['ION'])
        self.assertIn(MEMBER_ROLE, role_header['ION'])

        actor_header = get_actor_header(actor_id)

        self.assertEqual(actor_header['ion-actor-id'], actor_id)
        self.assertEqual(actor_header['ion-actor-roles'], actor_roles)

        #Now make sure we can change the name of the Org and not affect the headers
        org2 = self.rr.read(org2_id)
        org2.name = 'Updated Org 2'
        org2_id, _ = self.rr.update(org2)

        actor_roles = find_roles_by_actor(actor_id)

        self.assertEqual(len(actor_roles), 2)
        self.assertEqual(len(role_header), 2)
        self.assertIn('Second_Org', actor_roles)
        self.assertIn('Second_Org', role_header)
        self.assertEqual(len(actor_roles['Second_Org']), 2)
        self.assertEqual(len(role_header['Second_Org']), 2)
        self.assertIn('OPERATOR', actor_roles['Second_Org'])
        self.assertIn('OPERATOR', role_header['Second_Org'])
        self.assertIn(MEMBER_ROLE, actor_roles['Second_Org'])
        self.assertIn(MEMBER_ROLE, role_header['Second_Org'])
        self.assertIn('ION', actor_roles)
        self.assertIn('ION', role_header)
        self.assertIn(MODERATOR_ROLE, actor_roles['ION'])
        self.assertIn(MEMBER_ROLE, actor_roles['ION'])
        self.assertIn(MODERATOR_ROLE, role_header['ION'])
        self.assertIn(MEMBER_ROLE, role_header['ION'])

        actor_header = get_actor_header(actor_id)

        self.assertEqual(actor_header['ion-actor-id'], actor_id)
        self.assertEqual(actor_header['ion-actor-roles'], actor_roles)


    def test_get_sytsem_actor_header(self):
        actor = IonObject(RT.ActorIdentity, name='ionsystem')

        actor_id, _ = self.rr.create(actor)

        system_actor_header = get_system_actor_header()
        self.assertDictEqual(system_actor_header['ion-actor-roles'],{'ION': [MEMBER_ROLE]})

    def test_get_valid_org_commitment(self):
        from pyon.util.containers import get_ion_ts_millis

        # create ION org and an actor
        ion_org = IonObject(RT.Org, name='ION')
        ion_org_id, _ = self.rr.create(ion_org)
        ion_org._id = ion_org_id
        actor = IonObject(RT.ActorIdentity, name='actor1')
        actor_id, _ = self.rr.create(actor)
        device = IonObject(RT.TestDevice, name="device1")
        device_id, _ = self.rr.create(device)

        # create an expired commitment in the org
        ts = get_ion_ts_millis() - 50000
        com_obj = IonObject(RT.Commitment, provider=ion_org_id, consumer=actor_id, commitment=True, expiration=ts)
        com_id, _ = self.rr.create(com_obj)
        self.rr.create_association(ion_org_id, PRED.hasCommitment, com_id)
        c = get_valid_principal_commitments(ion_org_id, actor_id)
        # verify that the commitment is not returned
        self.assertIsNone(c)

        self.rr.create_association(com_id, PRED.hasTarget, device_id)
        c = get_valid_resource_commitments(device_id, actor_id)
        # verify that the commitment is not returned
        self.assertIsNone(c)

        # create a commitment that has not expired yet
        ts = get_ion_ts_millis() + 50000
        com_obj = IonObject(RT.Commitment, provider=ion_org_id, consumer=actor_id, commitment=True, expiration=ts)
        com_id, _ = self.rr.create(com_obj)
        self.rr.create_association(ion_org_id, PRED.hasCommitment, com_id)
        c = get_valid_principal_commitments(ion_org_id, actor_id)
        # verify that the commitment is returned
        self.assertIsNotNone(c)

        self.rr.create_association(com_id, PRED.hasTarget, device_id)
        c = get_valid_resource_commitments(device_id, actor_id)
        # verify that the commitment is not returned
        self.assertIsNotNone(c)



    ### THIS TEST USES THE HELLO SERVICE EXAMPLE

    @attr('PRECONDITION1')
    def test_multiple_process_pre_conditions(self):
        hello1 = self.container.spawn_process('hello_service1','ion.service.examples.hello_service','HelloService' )
        self.addCleanup(self.container.terminate_process, hello1)

        hello2 = self.container.spawn_process('hello_service2','ion.service.examples.hello_service','HelloService' )
        self.addCleanup(self.container.terminate_process, hello2)

        hello3 = self.container.spawn_process('hello_service3','ion.service.examples.hello_service','HelloService' )
        #self.addCleanup(self.container.terminate_process, hello3)

        client = HelloServiceProcessClient(process=self.gov_client)

        actor_id='anonymous'
        text='mytext 123'

        actor_headers = get_actor_header(actor_id)
        ret = client.hello(text, headers=actor_headers)
        self.assertIn(text, ret)

        with self.assertRaises(Unauthorized) as cm:
            ret = client.noop(text=text)
        self.assertIn('The noop operation has been denied', cm.exception.message)

        self.container.terminate_process(hello3)

        with self.assertRaises(Unauthorized) as cm:
            ret = client.noop(text=text)
        self.assertIn('The noop operation has been denied', cm.exception.message)
