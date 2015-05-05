#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.core.governance import get_system_actor
from ion.core.bootstrap_process import BootstrapPlugin, AbortBootstrap
from pyon.public import IonObject, RT, log, ResourceQuery, PRED
from pyon.util.containers import get_safe

from interface.objects import ActorIdentity, Org


class BootstrapCore(BootstrapPlugin):
    """
    Bootstrap plugin for core system resources.
    No service dependency
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        # Detect if system has been started before by the presence of the ION system actor
        system_actor = get_system_actor()
        if system_actor:
            raise AbortBootstrap("System already initialized. Start with bootmode=restart or force_clean (-fc)!")

        # Create ION actor
        actor_name = get_safe(config, "system.system_actor", "ionsystem")
        sys_actor = ActorIdentity(name=actor_name, description="ION System Agent")
        process.container.resource_registry.create(sys_actor)

    def on_restart(self, process, config, **kwargs):
        # Delete leftover Service and associated Process resources
        svc_ids, _ = process.container.resource_registry.find_resources(restype=RT.Service, id_only=True)

        if svc_ids:
            rq = ResourceQuery()
            rq.set_filter(rq.filter_type(RT.Process),
                          rq.filter_associated_from_subject(svc_ids, predicate=PRED.hasProcess))
            proc_ids = process.container.resource_registry.find_resources_ext(query=rq.get_query(), id_only=True)

            log.info("Deleting %s Service resources", len(svc_ids))
            process.container.resource_registry.rr_store.delete_mult(svc_ids)

            if proc_ids:
                log.info("Deleting %s Procvess resources", len(proc_ids))
                process.container.resource_registry.rr_store.delete_mult(proc_ids)
