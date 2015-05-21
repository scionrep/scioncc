#!/usr/bin/env python

"""Bootstrap process for org related resources"""

__author__ = 'Michael Meisinger, Stephen Henrie'

from pyon.core.governance import MODERATOR_ROLE, SUPERUSER_ROLE, get_system_actor
from pyon.public import IonObject, RT
from ion.core.bootstrap_process import BootstrapPlugin, AbortBootstrap

from interface.objects import Org, UserRole, ExchangeSpace
from interface.services.core.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.core.iexchange_management_service import ExchangeManagementServiceProcessClient


class BootstrapOrg(BootstrapPlugin):
    """
    Bootstrap process for Org and related resources
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        org_ms_client = OrgManagementServiceProcessClient(process=process)
        ex_ms_client = ExchangeManagementServiceProcessClient(process=process)

        system_actor = get_system_actor()
        if not system_actor:
            raise AbortBootstrap("Cannot find system actor")
        system_actor_id = system_actor._id

        # Create root Org: ION
        root_orgname = config.system.root_org
        org = Org(name=root_orgname, description="ION Root Org")
        self.org_id = org_ms_client.create_org(org)

        # Instantiate initial set of User Roles for this Org
        superuser_role = UserRole(governance_name=SUPERUSER_ROLE, name='Superuser role',
                                  description='Has all permissions system wide')
        org_ms_client.add_org_role(self.org_id, superuser_role)
        org_ms_client.grant_role(self.org_id, system_actor_id, SUPERUSER_ROLE)

        # Make the ION system agent a manager for the ION Org
        org_ms_client.grant_role(self.org_id, system_actor_id, MODERATOR_ROLE)

        # Create root ExchangeSpace
        system_xs_name = process.container.ex_manager.system_xs_name
        xs = ExchangeSpace(name=system_xs_name, description="ION service XS")
        self.xs_id = ex_ms_client.create_exchange_space(xs, self.org_id)
