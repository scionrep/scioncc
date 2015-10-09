#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'


import unittest, os, gevent, platform, simplejson
from mock import Mock, patch

from pyon.util.int_test import IonIntegrationTestCase

from pyon.util.containers import get_ion_ts
from nose.plugins.attrib import attr
from pyon.util.context import LocalContextMixin

from pyon.datastore.datastore import DatastoreManager
from pyon.ion.event import EventRepository

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound, Unauthorized
from pyon.public import PRED, RT, IonObject, CFG, log, OT, LCS, LCE, AS

from pyon.ion.resregistry import ResourceRegistryServiceWrapper
from pyon.core.governance.negotiation import Negotiation
from ion.process.bootstrap.load_system_policy import LoadSystemPolicy
from pyon.core.governance import MODERATOR_ROLE, MEMBER_ROLE, SUPERUSER_ROLE, OPERATOR_ROLE, get_system_actor, get_system_actor_header
from pyon.core.governance import get_actor_header
from pyon.net.endpoint import RPCClient, BidirClientChannel


from interface.services.core.iresource_registry_service import ResourceRegistryServiceProcessClient
from interface.services.core.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.core.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.core.iexchange_management_service import ExchangeManagementServiceProcessClient
from interface.services.core.ipolicy_management_service import PolicyManagementServiceProcessClient
from interface.services.core.isystem_management_service import SystemManagementServiceProcessClient
from interface.objects import AgentCommand, ProposalOriginatorEnum, ProposalStatusEnum, NegotiationStatusEnum, ComputedValueAvailability


ORG2 = 'Org 2'

DENY_EXCHANGE_TEXT = '''
        <Rule RuleId="%s" Effect="Deny">
            <Description>
                %s
            </Description>

            <Target>
                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">exchange_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

            </Target>

        </Rule>
        '''


TEST_POLICY_TEXT = '''
        <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">anonymous</AttributeValue>
                            <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>


                <Actions>
                    <Action>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_exchange_space</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>


                    </Action>
                </Actions>

            </Target>

        </Rule>
        '''


TEST_BOUNDARY_POLICY_TEXT = '''
        <Rule RuleId="%s" Effect="Deny">
            <Description>
                %s
            </Description>

            <Target>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">anonymous</AttributeValue>
                            <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>


            </Target>

        </Rule>
        '''

###########


DENY_PARAM_50_RULE = '''
            <Rule RuleId="%s:" Effect="Permit">
                <Description>
                    %s
                </Description>

                <Target>
                    <Resources>
                        <Resource>
                            <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">.*$</AttributeValue>
                                <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ResourceMatch>
                        </Resource>
                        <Resource>
                            <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">agent</AttributeValue>
                                <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:receiver-type" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ResourceMatch>
                        </Resource>
                    </Resources>

                    <Actions>
                        <Action>
                            <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">set_resource</AttributeValue>
                                <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ActionMatch>
                        </Action>
                    </Actions>

                </Target>

              <Condition>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-code">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string"><![CDATA[def policy_func(process, message, headers):
                            params = message['params']
                            if params['INTERVAL'] <= 50:
                                return True, ''
                            return False, 'The value for SBE37Parameter.INTERVAL cannot be greater than 50'
                        ]]>
                        </AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                    </Apply>
                </Condition>

            </Rule>
            '''


DENY_PARAM_30_RULE = '''
            <Rule RuleId="%s:" Effect="Permit">
                <Description>
                    %s
                </Description>

                <Target>
                    <Resources>
                        <Resource>
                            <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">.*$</AttributeValue>
                                <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ResourceMatch>
                        </Resource>
                        <Resource>
                            <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">agent</AttributeValue>
                                <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:receiver-type" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ResourceMatch>
                        </Resource>
                    </Resources>

                    <Actions>
                        <Action>
                            <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">set_resource</AttributeValue>
                                <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ActionMatch>
                        </Action>
                    </Actions>

                </Target>

              <Condition>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-code">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string"><![CDATA[def policy_func(process, message, headers):
                            params = message['params']
                            if params['INTERVAL'] <= 30:
                                return True, ''
                            return False, 'The value for SBE37Parameter.INTERVAL cannot be greater than 30'
                        ]]>
                        </AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                    </Apply>
                </Condition>

            </Rule>
            '''

DENY_PARAM_10_RULE = '''
            <Rule RuleId="%s:" Effect="Permit">
                <Description>
                    %s
                </Description>

                <Target>
                    <Resources>
                        <Resource>
                            <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">.*$</AttributeValue>
                                <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ResourceMatch>
                        </Resource>
                        <Resource>
                            <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">agent</AttributeValue>
                                <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:receiver-type" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ResourceMatch>
                        </Resource>
                    </Resources>

                    <Actions>
                        <Action>
                            <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">set_resource</AttributeValue>
                                <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ActionMatch>
                        </Action>
                    </Actions>

                </Target>

              <Condition>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-code">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string"><![CDATA[def policy_func(process, message, headers):
                            params = message['params']
                            if params['INTERVAL'] <= 10:
                                return True, ''
                            return False, 'The value for SBE37Parameter.INTERVAL cannot be greater than 10'
                        ]]>
                        </AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                    </Apply>
                </Condition>

            </Rule>
            '''



@attr('INT', group='coi')
class TestGovernanceHeaders(IonIntegrationTestCase):
    def setUp(self):
        # Start container and services
        self._start_container()
        self.container.start_rel_from_url('res/deploy/basic.yml')

        #Instantiate a process to represent the test
        process = GovernanceTestProcess()

        self.rr_client = ResourceRegistryServiceProcessClient(process=process)

        #Get info on the ION System Actor
        self.system_actor = get_system_actor()
        log.info('system actor:' + self.system_actor._id)

        self.system_actor_header = get_system_actor_header()
        self.resource_id_header_value = ''

    def test_governance_message_headers(self):
        '''
        This test is used to make sure the ION endpoint code is properly setting the
        '''

        #Get function pointer to send function
        old_send = BidirClientChannel._send

        # make new send to patch on that duplicates send
        def patched_send(*args, **kwargs):

            #Only duplicate the message send from the initial client call
            msg_headers = kwargs['headers']

            if (self.resource_id_header_value == '') and 'resource-id' in msg_headers:
                self.resource_id_header_value = msg_headers['resource-id']

            return old_send(*args, **kwargs)

        # patch it into place with auto-cleanup to try to interogate the message headers
        patcher = patch('pyon.net.endpoint.BidirClientChannel._send', patched_send)
        patcher.start()
        self.addCleanup(patcher.stop)

        # Instantiate an object
        obj = IonObject("ActorIdentity", name="name")

        # Can't call update with object that hasn't been persisted
        with self.assertRaises(BadRequest) as cm:
            self.rr_client.update(obj)
       # self.assertTrue(cm.exception.message.startswith("Object does not have required '_id' or '_rev' attribute"))

        self.resource_id_header_value = ''

        # Persist object and read it back
        obj_id, obj_rev = self.rr_client.create(obj)
        log.debug('The id of the created object is %s', obj_id)
        self.assertEqual(self.resource_id_header_value, '' )

        self.resource_id_header_value = ''
        read_obj = self.rr_client.read(obj_id)
        self.assertEqual(self.resource_id_header_value, obj_id )

        # Cannot create object with _id and _rev fields pre-set
        self.resource_id_header_value = ''
        with self.assertRaises(BadRequest) as cm:
            self.rr_client.create(read_obj)
        #self.assertTrue(cm.exception.message.startswith("Doc must not have '_id'"))
        self.assertEqual(self.resource_id_header_value, '' )

        # Update object
        read_obj.name = "John Doe"
        self.resource_id_header_value = ''
        self.rr_client.update(read_obj)
        self.assertEqual(self.resource_id_header_value, obj_id )

        # Update should fail with revision mismatch
        self.resource_id_header_value = ''
        with self.assertRaises(Conflict) as cm:
            self.rr_client.update(read_obj)
        #self.assertTrue(cm.exception.message.startswith("Object not based on most current version"))
        self.assertEqual(self.resource_id_header_value, obj_id )

        # Re-read and update object
        self.resource_id_header_value = ''
        read_obj = self.rr_client.read(obj_id)
        self.assertEqual(self.resource_id_header_value, obj_id )

        self.resource_id_header_value = ''
        self.rr_client.update(read_obj)
        self.assertEqual(self.resource_id_header_value, obj_id )

        #Create second object
        obj = IonObject("ActorIdentity", name="Babs Smith")

        self.resource_id_header_value = ''

        # Persist object and read it back
        obj2_id, obj2_rev = self.rr_client.create(obj)
        log.debug('The id of the created object is %s', obj_id)
        self.assertEqual(self.resource_id_header_value, '' )

        #Test for multi-read
        self.resource_id_header_value = ''
        objs = self.rr_client.read_mult([obj_id, obj2_id])

        self.assertAlmostEquals(self.resource_id_header_value, [obj_id, obj2_id])
        self.assertEqual(len(objs),2)

        # Delete object
        self.resource_id_header_value = ''
        self.rr_client.delete(obj_id)
        self.assertEqual(self.resource_id_header_value, obj_id )


        # Delete object
        self.resource_id_header_value = ''
        self.rr_client.delete(obj2_id)
        self.assertEqual(self.resource_id_header_value, obj2_id )


class GovernanceTestProcess(LocalContextMixin):
    name = 'gov_test'
    id='gov_client'
    process_type = 'simple'

@attr('INT', group='coi')
class TestGovernanceInt(IonIntegrationTestCase):

    def setUp(self):
        from unittest import SkipTest
        raise SkipTest("Need to rework governance tests")

        # Start container
        self._start_container()

        #Load a deploy file
        self.container.start_rel_from_url('res/deploy/basic.yml')

        #Instantiate a process to represent the test
        process=GovernanceTestProcess()

        #Load system policies after container has started all of the services
        policy_loaded = CFG.get_safe('system.load_policy', False)

        if not policy_loaded:
            log.debug('Loading policy')
            LoadSystemPolicy.op_load_system_policies(process)

        gevent.sleep(self.SLEEP_TIME*2)  # Wait for events to be fired and policy updated

        self.rr_msg_client = ResourceRegistryServiceProcessClient(process=process)
        self.rr_client = ResourceRegistryServiceWrapper(self.container.resource_registry, process)
        self.id_client = IdentityManagementServiceProcessClient(process=process)
        self.pol_client = PolicyManagementServiceProcessClient(process=process)
        self.org_client = OrgManagementServiceProcessClient(process=process)
        self.ems_client = ExchangeManagementServiceProcessClient(process=process)
        self.sys_management = SystemManagementServiceProcessClient(process=process)

        #Get info on the ION System Actor
        self.system_actor = get_system_actor()
        log.info('system actor:' + self.system_actor._id)
        self.system_actor_header = get_system_actor_header()
        self.anonymous_actor_headers = {'ion-actor-id':'anonymous'}
        self.ion_org = self.org_client.find_org()

        # Setup access to event repository
        dsm = DatastoreManager()
        ds = dsm.get_datastore("events")

        self.event_repo = EventRepository(dsm)

    def tearDown(self):
        policy_list, _ = self.rr_client.find_resources(restype=RT.Policy)

        # Must remove the policies in the reverse order they were added
        for policy in sorted(policy_list, key=lambda p: p.ts_created, reverse=True):
            self.pol_client.delete_policy(policy._id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be fired and policy updated

    def test_basic_policy_operations(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy, id_only=True)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")

        log.debug('Begin testing with policies')

        #First check existing policies to see if they are in place to keep an anonymous user from creating things
        with self.assertRaises(Unauthorized) as cm:
            test_org_id = self.org_client.create_org(org=IonObject(RT.Org, name='test_org', description='A test Org'))
        self.assertIn( 'org_management(create_org) has been denied',cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            test_org = self.org_client.find_org(name='test_org')

        #Add a new policy to deny all operations to the exchange_management by default .
        test_policy_id = self.pol_client.create_service_access_policy('exchange_management', 'Exchange_Management_Deny_Policy',
            'Deny all operations in  Exchange Management Service by default',
            DENY_EXCHANGE_TEXT, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be fired and policy updated

        #Attempt to access an operation in service which does not have specific policies set
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        #Add a new policy to allow the the above service call.
        test_policy_id = self.pol_client.create_service_access_policy('exchange_management', 'Exchange_Management_Test_Policy',
            'Allow specific operations in the Exchange Management Service for anonymous user',
            TEST_POLICY_TEXT, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be fired and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)

        #disable the test policy to try again
        self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        #now enable the test policy to try again
        self.pol_client.enable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)


        #Now test service operation specific policies - specifically that there can be more than one on the same operation.

        pol1_id = self.pol_client.add_process_operation_precondition_policy(process_name='policy_management', op='disable_policy', policy_content='func1_pass', headers=self.system_actor_header )

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #try to disable the test policy  again
        self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        #now enable the test policy to try again
        self.pol_client.enable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)


        pol2_id = self.pol_client.add_process_operation_precondition_policy(process_name='policy_management', op='disable_policy', policy_content='func2_deny', headers=self.system_actor_header )

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #try to disable the test policy again
        with self.assertRaises(Unauthorized) as cm:
            self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)
        self.assertIn( 'Denied for no reason',cm.exception.message)


        self.pol_client.delete_policy(pol2_id,  headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #try to disable the test policy  again
        self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)


        #try to enable the test policy  again
        self.pol_client.enable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated


        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)


        pre_func1 =\
        """def precondition_func(process, msg, headers):
            if headers['op'] == 'disable_policy':
                return False, 'Denied for no reason again'
            else:
                return True, ''

        """
        #Create a dynamic precondition function to deny calls to disable policy
        pre_func1_id = self.pol_client.add_process_operation_precondition_policy(process_name='policy_management', op='disable_policy', policy_content=pre_func1, headers=self.system_actor_header )

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #try to disable the test policy again
        with self.assertRaises(Unauthorized) as cm:
            self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)
        self.assertIn( 'Denied for no reason again',cm.exception.message)

        #Now delete the most recent precondition policy
        self.pol_client.delete_policy(pre_func1_id,  headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated


        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)


        #Now test that a precondition function can be enabled and disabled
        pre_func2 =\
        """def precondition_func(process, msg, headers):
            if headers['op'] == 'create_exchange_space':
                return False, 'Denied for from a operation precondition function'
            else:
                return True, ''

        """
        #Create a dynamic precondition function to deny calls to disable policy
        pre_func2_id = self.pol_client.add_process_operation_precondition_policy(process_name='exchange_management', op='create_exchange_space', policy_content=pre_func2, headers=self.system_actor_header )

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Denied for from a operation precondition function',cm.exception.message)


        #try to enable the precondition policy
        self.pol_client.disable_policy(pre_func2_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated


        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)

        #try to enable the precondition policy
        self.pol_client.enable_policy(pre_func2_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Denied for from a operation precondition function',cm.exception.message)

        #Delete the precondition policy
        self.pol_client.delete_policy(pre_func2_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)


        self.pol_client.delete_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        ###########
        ### Now test access to service create* operations based on roles...

        #Anonymous users should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            id = self.ssclient.create_interval_timer(start_time="now", event_origin="Interval_Timer_233", headers=self.anonymous_actor_headers)
        self.assertIn( 'scheduler(create_interval_timer) has been denied',cm.exception.message)

        #now try creating a new user with a valid actor
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "actor id=" + actor_id)
        actor_header = get_actor_header(actor_id)

        #User without OPERATOR or MANAGER role should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            id = self.ssclient.create_interval_timer(start_time="now", event_origin="Interval_Timer_233", headers=actor_header)
        self.assertIn( 'scheduler(create_interval_timer) has been denied',cm.exception.message)

        #Remove the OPERATOR_ROLE from the user.
        self.org_client.grant_role(self.ion_org._id, actor_id, MODERATOR_ROLE,  headers=self.system_actor_header)

        #Refresh headers with new role
        actor_header = get_actor_header(actor_id)

        #User with proper role should now be allowed to access this service operation.
        id = self.ssclient.create_interval_timer(start_time="now", end_time="-1", event_origin="Interval_Timer_233", headers=actor_header)


    @patch.dict(CFG, {'container':{'org_boundary':True}})
    def test_policy_cache_reset(self):

        before_policy_set = self.container.governance_controller.get_active_policies()


        #First clear all of the policies to test that failures will be caught due to missing policies
        self.container.governance_controller._clear_container_policy_caches()

        empty_policy_set = self.container.governance_controller.get_active_policies()
        self.assertEqual(len(empty_policy_set['service_access'].keys()), 0)
        self.assertEqual(len(empty_policy_set['resource_access'].keys()), 0)

        #With policies gone, an anonymous user should be able to create an object
        test_org_id = self.org_client.create_org(org=IonObject(RT.Org, name='test_org1', description='A test Org'))
        test_org = self.org_client.find_org(name='test_org1')
        self.assertEqual(test_org._id, test_org_id)

        #Trigger the event to reset the policy caches
        self.sys_management.reset_policy_cache()

        gevent.sleep(20)  # Wait for events to be published and policy reloaded for all running processes

        after_policy_set = self.container.governance_controller.get_active_policies()

        #With policies refreshed, an anonymous user should NOT be able to create an object
        with self.assertRaises(Unauthorized) as cm:
            test_org_id = self.org_client.create_org(org=IonObject(RT.Org, name='test_org2', description='A test Org'))
        self.assertIn( 'org_management(create_org) has been denied',cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            test_org = self.org_client.find_org(name='test_org2')

        self.assertEqual(len(before_policy_set.keys()), len(after_policy_set.keys()))
        self.assertEqual(len(before_policy_set['service_access'].keys()), len(after_policy_set['service_access'].keys()))
        self.assertEqual(len(before_policy_set['resource_access'].keys()), len(after_policy_set['resource_access'].keys()))
        self.assertEqual(len(before_policy_set['service_operation'].keys()), len(after_policy_set['service_operation'].keys()))

        #If the number of keys for service operations were equal, then check each set of operation precondition functions
        for key in before_policy_set['service_operation']:
            self.assertEqual(len(before_policy_set['service_operation'][key]), len(after_policy_set['service_operation'][key]))


    @patch.dict(CFG, {'container':{'org_boundary':True}})
    def test_org_boundary(self):

        with self.assertRaises(NotFound) as nf:
            org2 = self.org_client.find_org(ORG2)
        self.assertIn('The Org with name Org 2 does not exist',nf.exception.message)

        #Create a second Org
        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        #First try to get a list of Users by hitting the RR anonymously - should be allowed.
        actors,_ = self.rr_msg_client.find_resources(restype=RT.ActorIdentity)
        self.assertEqual(len(actors),2) #Should include the ION System Actor, Web auth actor.

        log.debug('Begin testing with policies')

        #Create a new actor - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_actor_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)


        #now try creating a new actors with a valid actor
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "actor id=" + actor_id)
        actor_header = get_actor_header(actor_id)

        #First try to get a list of Users by hitting the RR anonymously - should be allowed.
        actors,_ = self.rr_msg_client.find_resources(restype=RT.ActorIdentity)
        self.assertEqual(len(actors),3) #Should include the ION System Actor and web auth actor as well.

        #Now enroll the actor as a member of the Second Org
        self.org_client.enroll_member(org2_id,actor_id, headers=self.system_actor_header)
        actor_header = get_actor_header(actor_id)

        #Add a new Org boundary policy which deny's all anonymous access
        test_policy_id = self.pol_client.create_resource_access_policy( org2_id, 'Org_Test_Policy',
            'Deny all access for anonymous actor',
            TEST_BOUNDARY_POLICY_TEXT, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be fired and policy updated

        #Hack to force container into an Org Boundary for second Org
        self.container.governance_controller._container_org_name = org2.org_governance_name
        self.container.governance_controller._is_container_org_boundary = True

        #First try to get a list of Users by hitting the RR anonymously - should be denied.
        with self.assertRaises(Unauthorized) as cm:
            actors,_ = self.rr_msg_client.find_resources(restype=RT.ActorIdentity, headers=self.anonymous_actor_headers)
        self.assertIn( 'resource_registry(find_resources) has been denied',cm.exception.message)


        #Now try to hit the RR with a real user and should now be allowed
        actors,_ = self.rr_msg_client.find_resources(restype=RT.ActorIdentity, headers=actor_header)
        self.assertEqual(len(actors),3) #Should include the ION System Actor and web auth actor as well.

        #TODO - figure out how to right a XACML rule to be a member of the specific Org as well

        #Hack to force container back to default values
        self.container.governance_controller._container_org_name = 'ION'
        self.container.governance_controller._is_container_org_boundary = False
        self.container.governance_controller._container_org_id = None

        self.pol_client.delete_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated



    def test_org_enroll_negotiation(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")


        with self.assertRaises(BadRequest) as cm:
            myorg = self.org_client.read_org()
        self.assertTrue(cm.exception.message == 'The org_id parameter is missing')

        log.debug('Begin testing with policies')

        #Create a new user - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_actor_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)

        #Now create user with proper credentials
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "actor id=" + actor_id)

        #Build the message headers used with this user
        actor_header = get_actor_header(actor_id)

        #Get the associated user id
        user_info = IonObject(RT.UserInfo, name='Test User')
        actor_user_id = self.id_client.create_user_info(actor_id=actor_id, user_info=user_info, headers=actor_header)

        #Attempt to enroll a user anonymously - should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            self.org_client.enroll_member(self.ion_org._id,actor_id, headers=self.anonymous_actor_headers)
        self.assertIn( 'org_management(enroll_member) has been denied',cm.exception.message)

        #Attempt to let a user enroll themselves - should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            self.org_client.enroll_member(self.ion_org._id,actor_id, headers=actor_header)
        self.assertIn( 'org_management(enroll_member) has been denied',cm.exception.message)

        #Attept to enroll the user in the ION Root org as a manager - should not be allowed since
        #registration with the system implies membership in the ROOT Org.
        with self.assertRaises(BadRequest) as cm:
            self.org_client.enroll_member(self.ion_org._id,actor_id, headers=self.system_actor_header)
        self.assertTrue(cm.exception.message == 'A request to enroll in the root ION Org is not allowed')

        #Verify that anonymous user cannot find a list of enrolled users in an Org
        with self.assertRaises(Unauthorized) as cm:
            actors = self.org_client.list_enrolled_actors(self.ion_org._id, headers=self.anonymous_actor_headers)
        self.assertIn('org_management(list_enrolled_actors) has been denied',cm.exception.message)

        #Verify that a user without the proper Org Manager cannot find a list of enrolled users in an Org
        with self.assertRaises(Unauthorized) as cm:
            actors = self.org_client.list_enrolled_actors(self.ion_org._id, headers=actor_header)
        self.assertIn( 'org_management(list_enrolled_actors) has been denied',cm.exception.message)

        actors = self.org_client.list_enrolled_actors(self.ion_org._id, headers=self.system_actor_header)
        self.assertEqual(len(actors),3)  # WIll include the ION system actor

        #Create a second Org
        with self.assertRaises(NotFound) as nf:
            org2 = self.org_client.find_org(ORG2)
        self.assertIn('The Org with name Org 2 does not exist',nf.exception.message)

        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),0)

        #Build the Service Agreement Proposal for enrollment request
        sap = IonObject(OT.EnrollmentProposal,consumer=actor_id, provider=org2_id )

        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),1)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        #Build the Service Agreement Proposal for enrollment request
        sap2 = IonObject(OT.EnrollmentProposal,consumer=actor_id, provider=org2_id )

        #User tried proposing an enrollment again - this should fail
        with self.assertRaises(BadRequest) as cm:
            self.org_client.negotiate(sap2, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: not is_enroll_negotiation_open',cm.exception.message)

        #Manager trys to reject the proposal but incorrectly
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.EnrollmentProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.REJECTED, ProposalOriginatorEnum.PROVIDER)
        sap_response.sequence_num -= 1

        #Should fail because the proposal sequence was not incremented
        with self.assertRaises(Inconsistent) as cm:
            self.org_client.negotiate(sap_response, headers=actor_header )
        self.assertIn('The Service Agreement Proposal does not have the correct sequence_num value (0) for this negotiation (1)',cm.exception.message)

        #Manager now trys to reject the proposal but with the correct proposal sequence
        sap_response.sequence_num += 1

        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),1)

        self.assertEqual(negotiations[0].negotiation_status, NegotiationStatusEnum.REJECTED)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.EnrollmentNegotiationStatusEvent)
        self.assertEquals(len(events_r), 2)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.REJECTED])

        #Create a new enrollment proposal

        #Build the Service Agreement Proposal to enroll
        sap = IonObject(OT.EnrollmentProposal,consumer=actor_id, provider=org2_id, description='Enrollment request for test user' )

        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=actor_header)
        self.assertEqual(len(negotiations),2)

        actors = self.org_client.list_enrolled_actors(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(actors),0)

        #Check the get extended marine facility to check on the open and closed negotiations when called by normal user
        ext_mf = self.obs_client.get_marine_facility_extension(org_id=org2_id,user_id=actor_user_id, headers=actor_header)
        self.assertEqual(len(ext_mf.closed_requests), 0)
        self.assertEqual(len(ext_mf.open_requests), 0)

        #Check the get extended marine facility to check on the open and closed negotiations when called by privledged user
        ext_mf = self.obs_client.get_marine_facility_extension(org_id=org2_id,user_id=self.system_actor._id, headers=self.system_actor_header)
        self.assertEqual(len(ext_mf.closed_requests), 1)
        self.assertEqual(len(ext_mf.open_requests), 1)


        #Manager approves proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.EnrollmentProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        #Make sure the Negotiation object has the proper description set from the initial SAP
        self.assertEqual(negotiations[0].description, sap.description)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        actors = self.org_client.list_enrolled_actors(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(actors),1)

        #User tried requesting enrollment again - this should fail
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.EnrollmentProposal,consumer=actor_id, provider=org2_id )
            neg_id = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: not is_enrolled',cm.exception.message)


        #Check the get extended marine facility to check on the open and closed negotiations when called by normal user
        ext_mf = self.obs_client.get_marine_facility_extension(org_id=org2_id,user_id=actor_user_id, headers=actor_header)
        self.assertEqual(len(ext_mf.closed_requests), 0)
        self.assertEqual(len(ext_mf.open_requests), 0)

        #Check the get extended marine facility to check on the open and closed negotiations when called by privledged user
        ext_mf = self.obs_client.get_marine_facility_extension(org_id=org2_id,user_id=self.system_actor._id, headers=self.system_actor_header)
        self.assertEqual(len(ext_mf.closed_requests), 2)
        self.assertEqual(len(ext_mf.open_requests), 0)


        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.EnrollmentNegotiationStatusEvent)
        self.assertEquals(len(events_r), 4)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.GRANTED])

        events_c = self.event_repo.find_events(origin=org2_id, event_type=OT.OrgMembershipGrantedEvent)
        self.assertEquals(len(events_c), 1)

        events_i = self.event_repo.find_events(origin=org2_id, event_type=OT.OrgNegotiationInitiatedEvent)
        self.assertEquals(len(events_i), 2)

        ret = self.org_client.is_enrolled(org_id=org2_id, actor_id=actor_id, headers=self.system_actor_header)
        self.assertEquals(ret, True)

        self.org_client.cancel_member_enrollment(org_id=org2_id, actor_id=actor_id, headers=self.system_actor_header)

        ret = self.org_client.is_enrolled(org_id=org2_id, actor_id=actor_id, headers=self.system_actor_header)
        self.assertEquals(ret, False)

    def test_org_role_negotiation(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")


        with self.assertRaises(BadRequest) as cm:
            myorg = self.org_client.read_org()
        self.assertTrue(cm.exception.message == 'The org_id parameter is missing')

        log.debug('Begin testing with policies')

        #Create a new user - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_actor_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)

        #Now create user with proper credentials
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "actor id=" + actor_id)

        #Build the message headers used with this user
        actor_header = get_actor_header(actor_id)

        actors = self.org_client.list_enrolled_actors(self.ion_org._id, headers=self.system_actor_header)
        self.assertEqual(len(actors),3)  # WIll include the ION system actor and the non user actor from setup

        ## test_org_roles and policies

        roles = self.org_client.list_org_roles(self.ion_org._id)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.governance_name for r in roles], [MODERATOR_ROLE, MEMBER_ROLE, SUPERUSER_ROLE])

        roles = self.org_client.list_enrolled_actors(self.ion_org._id, self.system_actor._id, headers=self.system_actor_header)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.governance_name for r in roles], [MEMBER_ROLE, MODERATOR_ROLE, SUPERUSER_ROLE])

        roles = self.org_client.list_enrolled_actors(self.ion_org._id, actor_id, headers=self.system_actor_header)
        self.assertEqual(len(roles),1)
        self.assertItemsEqual([r.governance_name for r in roles], [MEMBER_ROLE])


        #Create a second Org
        with self.assertRaises(NotFound) as nf:
            org2 = self.org_client.find_org(ORG2)
        self.assertIn('The Org with name Org 2 does not exist',nf.exception.message)

        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        roles = self.org_client.list_org_roles(org2_id)
        self.assertEqual(len(roles),2)
        self.assertItemsEqual([r.governance_name for r in roles], [MODERATOR_ROLE, MEMBER_ROLE])

        #Create the Instrument Operator Role
        operator_role = IonObject(RT.UserRole, governance_name=OPERATOR_ROLE,name='Instrument Operator', description='Instrument Operator')

        #First try to add the user role anonymously
        with self.assertRaises(Unauthorized) as cm:
            self.org_client.add_org_role(org2_id, operator_role, headers=self.anonymous_actor_headers)
        self.assertIn('org_management(add_org_role) has been denied',cm.exception.message)

        self.org_client.add_org_role(org2_id, operator_role, headers=self.system_actor_header)

        roles = self.org_client.list_org_roles(org2_id)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.governance_name for r in roles], [MODERATOR_ROLE, MEMBER_ROLE,  OPERATOR_ROLE])

        #Add the same role to the first Org as well
        self.org_client.add_org_role(self.ion_org._id, operator_role, headers=self.system_actor_header)

        # test proposals roles.

        #First try to find user requests anonymously
        with self.assertRaises(Unauthorized) as cm:
            requests = self.org_client.find_org_negotiations(org2_id, headers=self.anonymous_actor_headers)
        self.assertIn('org_management(find_org_negotiations) has been denied',cm.exception.message)

        #Next try to find user requests as as a basic member
        with self.assertRaises(Unauthorized) as cm:
            requests = self.org_client.find_org_negotiations(org2_id, headers=actor_header)
        self.assertIn('org_management(find_org_negotiations) has been denied',cm.exception.message)

        #Should not be denied for user with Org Manager role or ION System manager role
        requests = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(requests),0)

        #Build the Service Agreement Proposal for assigning  a role to a user
        sap = IonObject(OT.RequestRoleProposal,consumer=actor_id, provider=org2_id, role_name=OPERATOR_ROLE )

        # First try to request a role anonymously
        with self.assertRaises(Unauthorized) as cm:
            sap_response = self.org_client.negotiate(sap, headers=self.anonymous_actor_headers)
        self.assertIn('org_management(negotiate) has been denied',cm.exception.message)

        # Next try to propose to assign a role without being a member
        with self.assertRaises(BadRequest) as cm:
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_enrolled',cm.exception.message)

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),0)

        #Build the Service Agreement Proposal to enroll
        sap = IonObject(OT.EnrollmentProposal,consumer=actor_id, provider=org2_id )

        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),1)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        actors = self.org_client.list_enrolled_actors(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(actors),0)

        #Manager approves proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.EnrollmentProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        actors = self.org_client.list_enrolled_actors(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(actors),1)

        #Create a proposal to add a role to a user
        sap = IonObject(OT.RequestRoleProposal,consumer=actor_id, provider=org2_id, role_name=OPERATOR_ROLE )
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        ret = self.org_client.has_role(org2_id, actor_id,OPERATOR_ROLE, headers=actor_header )
        self.assertEqual(ret, False)

        #Run through a series of differet finds to ensure the various parameter filters are working.
        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_org_negotiations(org2_id,negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),1)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, proposal_type=OT.RequestRoleProposal, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        #Manager  rejects the initial role proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.RequestRoleProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.REJECTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_org_negotiations(org2_id,negotiation_status=NegotiationStatusEnum.REJECTED, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),1)

        self.assertEqual(negotiations[0].negotiation_status, NegotiationStatusEnum.REJECTED)

        #Make sure the user still does not have the requested role
        ret = self.org_client.has_role(org2_id, actor_id,OPERATOR_ROLE, headers=actor_header )
        self.assertEqual(ret, False)


        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.RequestRoleNegotiationStatusEvent)
        self.assertEquals(len(events_r), 2)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.REJECTED])


        #Create a second proposal to add a role to a user
        sap = IonObject(OT.RequestRoleProposal,consumer=actor_id, provider=org2_id, role_name=OPERATOR_ROLE )
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),3)

        closed_negotiations = self.org_client.find_org_closed_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(closed_negotiations),2)

        #Create an instrument resource
        ia_list,_ = self.rr_client.find_resources(restype=RT.InstrumentAgent)

        self.assertEqual(len(ia_list),0)

        ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent1', description='The first Instrument Agent')

        #Intruments should not be able to be created by anoymous users
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.create_instrument_agent(ia_obj, headers=self.anonymous_actor_headers)
        self.assertIn('instrument_management(create_instrument_agent) has been denied',cm.exception.message)

        #Intruments should not be able to be created by users that are not Instrument Operators
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.create_instrument_agent(ia_obj, headers=actor_header)
        self.assertIn('instrument_management(create_instrument_agent) has been denied',cm.exception.message)

        #Manager approves proposal for role request
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.RequestRoleProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        #mke sure there are no more open negotiations
        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)
        self.assertEqual(len(negotiations),0)

        #Verify the user has been assigned the requested role in the second Org
        ret = self.org_client.has_role(org2_id, actor_id,OPERATOR_ROLE, headers=actor_header )
        self.assertEqual(ret, True)

        #Verify the user has only been assigned the requested role in the second Org and not in the first Org
        ret = self.org_client.has_role(self.ion_org._id, actor_id,OPERATOR_ROLE, headers=actor_header )
        self.assertEqual(ret, False)

        #Refresh headers with new role
        actor_header = get_actor_header(actor_id)

        #now try to request the same role for the same user - should be denied
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.RequestRoleProposal,consumer=actor_id, provider=org2_id, role_name=OPERATOR_ROLE )
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: not has_role',cm.exception.message)

        #Now the user with the proper role should be able to create an instrument.
        self.ims_client.create_instrument_agent(ia_obj, headers=actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.RequestRoleNegotiationStatusEvent)
        self.assertEquals(len(events_r), 4)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.GRANTED])
        self.assertEqual(events_r[-1][2].role_name, sap_response2.role_name)

        events_c = self.event_repo.find_events(origin=org2_id, event_type=OT.UserRoleGrantedEvent)
        self.assertEquals(len(events_c), 2)

        events_i = self.event_repo.find_events(origin=org2_id, event_type=OT.OrgNegotiationInitiatedEvent)
        self.assertEquals(len(events_i), 3)

    def test_org_acquire_resource_negotiation(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")

        with self.assertRaises(BadRequest) as cm:
            myorg = self.org_client.read_org()
        self.assertTrue(cm.exception.message == 'The org_id parameter is missing')

        log.debug('Begin testing with policies')

        #Create a new user - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_actor_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)

        #Now create user with proper credentials
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "actor id=" + actor_id)

        #Create a second Org
        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        roles = self.org_client.list_org_roles(org2_id)
        self.assertEqual(len(roles),2)
        self.assertItemsEqual([r.governance_name for r in roles], [MODERATOR_ROLE, MEMBER_ROLE])

        #Create the Instrument Operator Role
        operator_role = IonObject(RT.UserRole, governance_name=OPERATOR_ROLE,name='Instrument Operator', description='Instrument Operator')

        #And add it to all Orgs
        self.org_client.add_org_role(self.ion_org._id, operator_role, headers=self.system_actor_header)
        self.org_client.add_org_role(org2_id, operator_role, headers=self.system_actor_header)

        #Add the OPERATOR_ROLE to the User for the ION Org
        self.org_client.grant_role(self.ion_org._id, actor_id, OPERATOR_ROLE, headers=self.system_actor_header)

        #Enroll the user in the second Org - do without Negotiation for test
        self.org_client.enroll_member(org2_id, actor_id,headers=self.system_actor_header )

        #Build the message headers used with this user
        actor_header = get_actor_header(actor_id)

        #Test the invitation process

        #Create a invitation proposal to add a role to a user
        sap = IonObject(OT.RequestRoleProposal,consumer=actor_id, provider=org2_id, role_name=OPERATOR_ROLE,
            originator=ProposalOriginatorEnum.PROVIDER )
        sap_response = self.org_client.negotiate(sap, headers=self.system_actor_header )

        ret = self.org_client.has_role(org2_id, actor_id,OPERATOR_ROLE, headers=actor_header )
        self.assertEqual(ret, False)

        #User creates proposal to approve
        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, proposal_type=OT.RequestRoleProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED)
        sap_response2 = self.org_client.negotiate(sap_response, headers=actor_header )

        #Verify the user has been assigned the requested role in the second Org
        ret = self.org_client.has_role(org2_id, actor_id,OPERATOR_ROLE, headers=actor_header )
        self.assertEqual(ret, True)

        #Build the message headers used with this user
        actor_header = get_actor_header(actor_id)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.RequestRoleNegotiationStatusEvent)
        self.assertEquals(len(events_r), 4)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.GRANTED])

        #Create the instrument agent with the user that has the proper role
        ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent1', description='The Instrument Agent')
        self.ims_client.create_instrument_agent(ia_obj, headers=actor_header)

        #Ensure the instrument agent has been created
        ia_list,_ = self.rr_client.find_resources(restype=RT.InstrumentAgent)
        self.assertEqual(len(ia_list),1)
        self.assertEquals(ia_list[0].lcstate, LCS.DRAFT)
        self.assertEquals(ia_list[0].availability, AS.PRIVATE)

        #Advance the Life cycle to planned. Must be OPERATOR so anonymous user should fail
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_agent_lifecycle(ia_list[0]._id, LCE.PLAN, headers=self.anonymous_actor_headers)
        self.assertIn( 'instrument_management(execute_instrument_agent_lifecycle) has been denied',cm.exception.message)

        #Advance the Life cycle to planned. Must be OPERATOR
        self.ims_client.execute_instrument_agent_lifecycle(ia_list[0]._id, LCE.PLAN, headers=actor_header)
        ia = self.rr_client.read(ia_list[0]._id)
        self.assertEquals(ia.lcstate, LCS.PLANNED)


        #First make a acquire resource request with an non-enrolled user.
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=self.system_actor._id, provider=org2_id, resource_id=ia_list[0]._id )
            sap_response = self.org_client.negotiate(sap, headers=self.system_actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_enrolled',cm.exception.message)


        #Make a proposal to acquire a resource with an enrolled user that has the right role but the resource is not shared the Org
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_resource_shared',cm.exception.message)

        #So share the resource
        self.org_client.share_resource(org_id=org2_id, resource_id=ia_list[0]._id, headers=self.system_actor_header  )

        #Verify the resource is shared
        res_list,_ = self.rr_client.find_objects(org2,PRED.hasResource)
        self.assertEqual(len(res_list), 1)
        self.assertEqual(res_list[0]._id, ia_list[0]._id)


        #First try to acquire the resource exclusively but it should fail since the user cannot do this without first
        #having had acquired the resource
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_resource_acquired',cm.exception.message)


        #Make a proposal to acquire a resource with an enrolled user that has the right role and is now shared
        sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id)
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, proposal_type=OT.AcquireResourceProposal, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        self.assertEqual(negotiations[0]._id, sap_response.negotiation_id)


        #Manager Creates a counter proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.AcquireResourceProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        #Counter proposals for demonstration only
        #Calculate one week from now in milliseconds
        cur_time = int(get_ion_ts())
        week_expiration = cur_time +  ( 7 * 24 * 60 * 60 * 1000 )

        sap_response = Negotiation.create_counter_proposal(negotiations[0], originator=ProposalOriginatorEnum.PROVIDER)
        sap_response.expiration = str(week_expiration)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        #User Creates a counter proposal
        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, proposal_type=OT.AcquireResourceProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)

        cur_time = int(get_ion_ts())
        month_expiration = cur_time +  ( 30 * 24 * 60 * 60 * 1000 )

        sap_response = Negotiation.create_counter_proposal(negotiations[0])
        sap_response.expiration = str(month_expiration)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )


        gevent.sleep(self.SLEEP_TIME+1)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.AcquireResourceNegotiationStatusEvent)
        self.assertEquals(len(events_r), 3)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.COUNTER])
        self.assertEqual(events_r[-1][2].resource_id, ia_list[0]._id)


        #Manager approves Instrument resource proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.AcquireResourceProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)

        self.assertEqual(len(negotiations),0) #Should be no more open negotiations for a user because auto-accept is enabled

        #The following are no longer needed with auto-accept enabled for acquiring a resource
        '''
        self.assertEqual(len(negotiations),1)

        #User accepts proposal in return
        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, proposal_type=OT.AcquireResourceProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED)
        sap_response2 = self.org_client.negotiate(sap_response, headers=actor_header )

        '''

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)
        self.assertEqual(len(negotiations),0)

        #Check commitment to be active
        commitments, _ = self.rr_client.find_objects(ia_list[0]._id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),1)

        resource_commitment, _ = self.rr_client.find_objects(actor_id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(resource_commitment),1)
        self.assertNotEqual(resource_commitment[0].lcstate, LCS.DELETED)


        subjects, _ = self.rr_client.find_subjects(None,PRED.hasCommitment, commitments[0]._id)
        self.assertEqual(len(subjects),3)

        contracts, _ = self.rr_client.find_subjects(RT.Negotiation,PRED.hasContract, commitments[0]._id)
        self.assertEqual(len(contracts),1)

        cur_time = int(get_ion_ts())
        invalid_expiration = cur_time +  ( 13 * 60 * 60 * 1000 ) # 12 hours from now

        #Now try to acquire the resource exclusively for longer than 12 hours
        sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id,
                    expiration=str(invalid_expiration))
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        #make sure the negotiation was rejected for being too long.
        negotiation = self.rr_client.read(sap_response.negotiation_id)
        self.assertEqual(negotiation.negotiation_status, NegotiationStatusEnum.REJECTED)

        #Now try to acquire the resource exclusively for 20 minutes
        cur_time = int(get_ion_ts())
        valid_expiration = cur_time +  ( 20 * 60 * 1000 ) # 12 hours from now

        sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id,
                    expiration=str(valid_expiration))
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        #Check commitment to be active
        commitments, _ = self.rr_client.find_objects(ia_list[0]._id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),2)

        exclusive_contract, _ = self.rr_client.find_objects(sap_response.negotiation_id,PRED.hasContract, RT.Commitment)
        self.assertEqual(len(contracts),1)

        #Now try to acquire the resource exclusively again - should fail
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: not is_resource_acquired_exclusively',cm.exception.message)

        #Release the exclusive commitment to the resource
        self.org_client.release_commitment(exclusive_contract[0]._id, headers=actor_header)


        #Check exclusive commitment to be inactive
        commitments, _ = self.rr_client.find_resources(restype=RT.Commitment, lcstate=LCS.DELETED)
        self.assertEqual(len(commitments),1)
        self.assertEqual(commitments[0].commitment.exclusive, True)

        #Shared commitment is still actove
        commitments, _ = self.rr_client.find_objects(ia_list[0],PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),1)
        self.assertNotEqual(commitments[0].lcstate, LCS.DELETED)

        #Now release the shared commitment
        self.org_client.release_commitment(resource_commitment[0]._id, headers=actor_header)

        #Check for both commitments to be inactive
        commitments, _ = self.rr_client.find_resources(restype=RT.Commitment, lcstate=LCS.DELETED)
        self.assertEqual(len(commitments),2)

        commitments, _ = self.rr_client.find_objects(ia_list[0],PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),0)


        #Now check some negative cases...

        #Attempt to acquire the same resource from the ION Org which is not sharing it - should fail
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=self.ion_org._id, resource_id=ia_list[0]._id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_resource_shared',cm.exception.message)


        #Remove the OPERATOR_ROLE from the user.
        self.org_client.revoke_role(org2_id, actor_id, OPERATOR_ROLE,  headers=self.system_actor_header)

        #Refresh headers with new role
        actor_header = get_actor_header(actor_id)

        #Make a proposal to acquire a resource with an enrolled user that does not have the right role
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id )
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: has_role',cm.exception.message)

        gevent.sleep(self.SLEEP_TIME+1)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.AcquireResourceNegotiationStatusEvent)
        self.assertEquals(len(events_r), 6)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.GRANTED])
        self.assertEqual(events_r[-1][2].resource_id, ia_list[0]._id)

        events_c = self.event_repo.find_events(origin=org2_id, event_type=OT.ResourceCommitmentCreatedEvent)
        self.assertEquals(len(events_c), 2)

        events_i = self.event_repo.find_events(origin=org2_id, event_type=OT.OrgNegotiationInitiatedEvent)
        self.assertEquals(len(events_i), 4)

        ret = self.org_client.is_resource_shared(org_id=org2_id, resource_id=ia_list[0]._id, headers=self.system_actor_header )
        self.assertEquals(ret, True)

        #So unshare the resource
        self.org_client.unshare_resource(org_id=org2_id, resource_id=ia_list[0]._id, headers=self.system_actor_header  )

        ret = self.org_client.is_resource_shared(org_id=org2_id, resource_id=ia_list[0]._id, headers=self.system_actor_header )
        self.assertEquals(ret, False)
