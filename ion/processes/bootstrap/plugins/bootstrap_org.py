#!/usr/bin/env python

"""Bootstrap process for org related resources"""

__author__ = 'Michael Meisinger, Stephen Henrie'

from ion.core.bootstrap_process import BootstrapPlugin, AbortBootstrap
from pyon.core.governance import MODERATOR_ROLE, SUPERUSER_ROLE
from pyon.ion.exchange import ION_ROOT_XS
from pyon.public import IonObject, RT

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

        system_actor, _ = process.container.resource_registry.find_resources(
            restype=RT.ActorIdentity, name=config.system.system_actor, id_only=True)
        if not system_actor:
            raise AbortBootstrap("Cannot find system actor")
        system_actor_id = system_actor[0]

        # Create root Org: ION
        root_orgname = config.system.root_org
        org = Org(name=root_orgname, description="ION Root Org")
        self.org_id = org_ms_client.create_org(org)

        # Instantiate initial set of User Roles for this Org
        SUPERUSER_ROLE = UserRole(governance_name=SUPERUSER_ROLE, name='ION Manager', description='ION Manager')
        org_ms_client.add_user_role(self.org_id, SUPERUSER_ROLE)
        org_ms_client.grant_role(self.org_id, system_actor_id, SUPERUSER_ROLE )

        # Make the ION system agent a manager for the ION Org
        org_ms_client.grant_role(self.org_id, system_actor_id, MODERATOR_ROLE )

        # Create root ExchangeSpace
        xs = ExchangeSpace(name=ION_ROOT_XS, description="ION service XS")
        self.xs_id = ex_ms_client.create_exchange_space(xs, self.org_id)

