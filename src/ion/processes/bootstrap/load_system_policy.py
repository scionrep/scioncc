#!/usr/bin/env python

"""Process that loads the system policy"""

__author__ = 'Stephen P. Henrie'

from pyon.core.governance import get_system_actor, get_system_actor_header
from pyon.public import CFG, log, ImmediateProcess, iex, Container, IonObject, RT, OT

from interface.services.core.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.core.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.core.ipolicy_management_service import PolicyManagementServiceProcessClient


class LoadSystemPolicy(ImmediateProcess):
    """
    bin/pycc -x ion.processes.bootstrap.load_system_policy.LoadSystemPolicy op=load
    """
    def on_init(self):
        pass

    def on_start(self):
        op = self.CFG.get("op", None)
        log.info("LoadSystemPolicy: {op=%s}" % op)
        if op:
            if op == "load":
                self.op_load_system_policies(self)
            else:
                raise iex.BadRequest("Operation unknown")
        else:
            raise iex.BadRequest("No operation specified")

    def on_quit(self):
        pass

    #
    # Create the initial set of policy rules for the ION system. To make the rules easier to write, start by
    # denying all anonymous access to Org services and then add rules which Permit access to specific operations
    # based on conditions.
    #
    @classmethod
    def op_load_system_policies(cls, calling_process):

        org_client = OrgManagementServiceProcessClient(node=Container.instance.node, process=calling_process)
        ion_org = org_client.find_org()

        id_client = IdentityManagementServiceProcessClient(node=Container.instance.node, process=calling_process )

        system_actor = get_system_actor()
        log.info('system actor:' + system_actor._id)

        sa_user_header = get_system_actor_header()

        policy_client = PolicyManagementServiceProcessClient(node=Container.instance.node, process=calling_process)


        timeout = 20



##############
        '''
        This rule must be loaded before the Deny_Everything rule
        '''

        policy_client = PolicyManagementServiceProcessClient(node=Container.instance.node, process=calling_process)

        policy_text = '''
        <Rule RuleId="%s:" Effect="Permit">
            <Description>
                %s
            </Description>


        <Target>
            <Subjects>
                <Subject>
                    <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">SUPERUSER</AttributeValue>
                        <SubjectAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </SubjectMatch>
                </Subject>
            </Subjects>
        </Target>


        </Rule>
        '''

        policy_id = policy_client.create_common_service_access_policy( 'SUPERUSER_Permit_Everything',
            'A global policy rule that permits access to everything with the ION Manager role',
            policy_text, headers=sa_user_header)




##############


        '''
        This rule must be loaded before the Deny_Everything rule
        '''

        policy_text = '''
        <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">service</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:receiver-type" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">read_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">READ</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">FIND</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">get_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">GET</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">list_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">LIST</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">

                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">list_enrolled_actors</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">list_orgs_for_actor</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">list_shared_resources</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">list_orgs_for_resource</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find_org_negotiations</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find_org_closed_negotiations</AttributeValue>
                        </Apply>
                        <ActionAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                </Apply>
            </Condition>

        </Rule>
        '''

        policy_id = policy_client.create_common_service_access_policy( 'Allowed_Anonymous_Service_Operations',
            'A global policy rule which specifies operations that are allowed with anonymous access',
            policy_text, headers=sa_user_header)

  ##############

        #This rule has been modified specifically for 2.0 to Deny for only specific services and agents. Everything else will be allowed.

        policy_text = '''
        <Rule RuleId="%s:" Effect="Deny">
            <Description>
                %s
            </Description>

            <Target>
                    <!-- REMOVE THE RESOURCE TARGETS BELOW AFTER 2.0 TO TIGHTEN POLICY -->

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">org_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">policy_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">identity_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">resource_registry</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

            </Target>

        </Rule>
        '''

        policy_id = policy_client.create_common_service_access_policy( 'Deny_Everything', 'A global policy rule that denies access to everything by default',
            policy_text, headers=sa_user_header)


        ##############

        ###THIS POLICY HAS BEEN COMMENTED OUT FOR THE INITIAL R2 RELEASE

        """
        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

               <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">service</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:receiver-type" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">CREATE</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">update_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">UPDATE</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">delete_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">RETIRE</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">MODERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

        </Rule> '''

        policy_id = policy_client.create_common_service_access_policy( 'Allowed_CUD_Service_Operations_for_Roles',
            'A global policy rule which specifies operations that are allowed with for OPERATOR AND MANAGER roles',
            policy_text, headers=sa_user_header)
        """

        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">resource_registry</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>


                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">MODERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:and">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DELETE</AttributeValue>
                            </Apply>
                            <ActionAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">UPDATE</AttributeValue>
                            </Apply>
                            <ActionAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_attachment</AttributeValue>
                            </Apply>
                            <ActionAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">LCS-CHANGE</AttributeValue>
                            </Apply>
                            <ActionAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('resource_registry', 'RR_Permitted_Operations',
            'Permit these operations in the Resource Registry Service for the proper roles',
            policy_text, headers=sa_user_header)


        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">resource_registry</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>
                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_attachment</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">MODERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">MEMBER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>
            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-function">
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">check_attachment_policy</AttributeValue>
                    <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                </Apply>
            </Condition>
        </Rule> '''

        policy_id = policy_client.create_service_access_policy('resource_registry', 'RR_create_attachment_Operation',
            'Permit create attachment operation only in the context of the org to which the user belongs',
            policy_text, headers=sa_user_header)

        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">resource_registry</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>
                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">LCS-CHANGE</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>
            </Target>
            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-function">
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">check_lifecycle_policy</AttributeValue>
                    <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                </Apply>
            </Condition>
        </Rule> '''

        policy_id = policy_client.create_service_access_policy('resource_registry', 'RR_LCS_Operation',
            'Permit lifecycle change operation only in the context of the org to which the user belongs',
            policy_text, headers=sa_user_header)

        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">resource_registry</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>
                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">UPDATE</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>
            </Target>
            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-function">
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">check_edit_policy</AttributeValue>
                    <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                </Apply>
            </Condition>
        </Rule> '''

        policy_id = policy_client.create_service_access_policy('resource_registry', 'RR_edit_Operation',
            'Permit edit operation only in the context of the org to which the user belongs',
            policy_text, headers=sa_user_header)

        ##############
        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">identity_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">signon</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">check_authentication_token</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>
            </Target>


            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">anonymous</AttributeValue>
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-one-and-only">
                        <SubjectAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('identity_management', 'IDS_Permitted_Non_Anonymous',
            'Permit these operations in the Identity Management Service if the user is not anonymous',
            policy_text, headers=sa_user_header)



        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">org_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>



                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">MODERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">

                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DELETE</AttributeValue>
                        </Apply>
                        <ActionAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                </Apply>
            </Condition>



        </Rule> '''

        policy_id = policy_client.create_service_access_policy('org_management', 'OrgMS_MODERATOR_ROLE_Permitted',
            'Permit these operations in the Org Management Service for the role of Org Manager',
            policy_text, headers=sa_user_header)


        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">policy_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>



                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">MODERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">

                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DELETE</AttributeValue>
                        </Apply>
                        <ActionAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                </Apply>
            </Condition>



        </Rule> '''

        policy_id = policy_client.create_service_access_policy('policy_management', 'PolicyMS_MODERATOR_ROLE_Permitted',
            'Permit these operations in the Policy Management Service for the role of Org Manager',
            policy_text, headers=sa_user_header)


        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">org_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>


                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">release_commitment</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('org_management', 'OrgMS_Role_Permitted',
            'Permit these operations in the Org Management Service for the operator roles',
            policy_text, headers=sa_user_header)

##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">org_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">negotiate</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>


                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">MEMBER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>


        </Rule> '''

        policy_id = policy_client.create_service_access_policy('org_management', 'OMS_MEMBER_ROLE_Permitted',
            'Permit these operations in the Org Management Service for any user that is a simple Member of the Org',
            policy_text, headers=sa_user_header)

