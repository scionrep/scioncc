#!/usr/bin/env python

"""Process that loads the system policy"""

__author__ = 'Stephen P. Henrie, Michael Meisinger'

import os
import yaml

from pyon.core.exception import ContainerConfigError
from pyon.core.governance import get_system_actor, get_system_actor_header
from pyon.public import CFG, log, ImmediateProcess, IonObject, RT, OT, BadRequest

from interface.services.core.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.core.ipolicy_management_service import PolicyManagementServiceProcessClient


class LoadSystemPolicy(ImmediateProcess):
    """
    bin/pycc -x ion.process.bootstrap.load_system_policy.LoadSystemPolicy op=load
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
                raise BadRequest("Operation unknown")
        else:
            raise BadRequest("No operation specified")

    def on_quit(self):
        pass

    @classmethod
    def op_load_system_policies(cls, calling_process):
        """
        Create the initial set of policy rules for the system.
        To establish clear rule precedence, denying all anonymous access to Org services first
        and then add rules which Permit access to specific operations based on conditions.
        """
        orgms_client = OrgManagementServiceProcessClient(process=calling_process)
        policyms_client = PolicyManagementServiceProcessClient(process=calling_process)

        ion_org = orgms_client.find_org()
        system_actor = get_system_actor()
        log.info('System actor: %s', system_actor._id)

        sa_user_header = get_system_actor_header(system_actor)

        policy_rules_filename = calling_process.CFG.get_safe("bootstrap.initial_policy_rules")
        if not policy_rules_filename:
            raise ContainerConfigError("Policy rules file not configured")
        if not os.path.exists(policy_rules_filename):
            raise ContainerConfigError("Policy rules file does not exist")

        with open(policy_rules_filename, "r") as f:
            policy_rules_yml = f.read()
        policy_rules_cfg = yaml.safe_load(policy_rules_yml)
        if "type" not in policy_rules_cfg or policy_rules_cfg["type"] != "scioncc_policy_rules":
            raise ContainerConfigError("Invalid policy rules file content")

        log.info("Loading %s policy rules", len(policy_rules_cfg["rules"]))
        for rule_cfg in policy_rules_cfg["rules"]:
            rule_name, policy_type, rule_desc = rule_cfg["name"], rule_cfg["policy_type"], rule_cfg.get("description", "")
            if rule_cfg.get("enable") is False:
                log.info("Policy rule %s disabled", rule_name)
                continue
            log.info("Loading policy rule %s (%s)", rule_name, policy_type)
            rule_filename = rule_cfg["rule_def"]
            if not os.path.isabs(rule_filename):
                rule_filename = os.path.join(os.path.dirname(policy_rules_filename), rule_filename)
            with open(rule_filename, "r") as f:
                rule_def = f.read()
            ordinal = rule_cfg.get("ordinal", 0)

            # Create the policy
            if policy_type == "common_service_access":
                policyms_client.create_common_service_access_policy(rule_name, rule_desc, rule_def, ordinal=ordinal,
                                                                    headers=sa_user_header)
            elif policy_type == "service_access":
                service_name = rule_cfg["service_name"]
                policyms_client.create_service_access_policy(service_name, rule_name, rule_desc, rule_def,
                                                             ordinal=ordinal, headers=sa_user_header)
            elif policy_type == "resource_access":
                resource_type, resource_name = rule_cfg["resource_type"], rule_cfg["resource_name"]
                res_ids, _ = calling_process.container.resource_registry.find_resources(
                        restype=resource_type, name=resource_name, id_only=True)
                if res_ids:
                    resource_id = res_ids[0]
                    policyms_client.create_resource_access_policy(resource_id, rule_name, rule_desc, rule_def,
                                                                   ordinal=ordinal, headers=sa_user_header)
            else:
                raise ContainerConfigError("Rule %s has invalid policy type: %s" % (rule_name, policy_type))
