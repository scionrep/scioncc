    #!/usr/bin/env python

"""Bootstrap actions for exchange"""

__author__ = 'Dave Foster <dfoster@asascience.com>, Michael Meisinger'

from pyon.public import log, get_sys_name, RT, PRED, CFG
from pyon.ion.exchange import ExchangeSpace, ExchangePoint, ExchangeName, DEFAULT_SYSTEM_XS, DEFAULT_EVENTS_XP

from ion.core.bootstrap_process import BootstrapPlugin

from interface.services.core.iexchange_management_service import ExchangeManagementServiceProcessClient
from interface.objects import ExchangeBroker as ResExchangeBroker
from interface.objects import ExchangeSpace as ResExchangeSpace
from interface.objects import ExchangePoint as ResExchangePoint


class BootstrapExchange(BootstrapPlugin):
    """
    Bootstrap plugin for exchange/management
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        """
        Bootstraps initial objects in the system from configuration (pyon.yml) via
        EMS calls.
        """
        rr = process.container.resource_registry
        ems_client = ExchangeManagementServiceProcessClient(process=process)

        # Get ION Org
        root_org_name = config.get_safe('system.root_org', "ION")
        org_ids, _ = rr.find_resources(restype=RT.Org, name=root_org_name, id_only=True)
        if not org_ids or len(org_ids) > 1:
            raise StandardError("Could not determine root Org")
        org_id = org_ids[0]

        # Create XSs and XPs resource objects
        xs_by_name = {}   # Name to resource ID mapping for ExchangeSpace
        xs_defs = config.get_safe("exchange.exchange_spaces", {})
        for xsname, xsdict in xs_defs.iteritems():
            xso = ResExchangeSpace(name=xsname, description=xsdict.get("description", ""))
            xso_id = ems_client.create_exchange_space(xso, org_id)
            xs_by_name[xsname] = xso_id

            log.info("ExchangeSpace %s, id %s", xsname, xso_id)

            for xpname, xpopts in xsdict.get("exchange_points", {}).iteritems():
                xpo = ResExchangePoint(name=xpname, description=xpopts.get("description", ""),
                                       topology_type=xpopts.get('type', 'ttree'))
                xpo_id = ems_client.create_exchange_point(xpo, xso_id)

                log.info("\tExchangePoint %s, id %s", xpname, xpo_id)

        # Create XSs and XPs resource objects
        for brokername, bdict in config.get_safe("exchange.exchange_brokers", {}).iteritems():
            xbo = ResExchangeBroker(name=brokername, description=bdict.get("description", ""))
            xbo_id = ems_client.create_exchange_broker(xbo)
            log.info("\tExchangeBroker %s, id %s", brokername, xbo_id)

            for xs_name in bdict.get("join_xs", None) or []:
                if xs_name in xs_by_name:
                    xs_id = xs_by_name[xs_name]
                    ems_client.add_exchange_space_to_exchange_broker(xs_id, xbo_id)
                else:
                    log.warn("ExchangeSpace %s unknown. Broker %s cannot join", xs_name, brokername)

            for xp_name in bdict.get("join_xp", None) or []:
                pass

    def on_restart(self, process, config, **kwargs):
        """
        Handles bootstrapping of system restart for exchange resources and broker state.

        - Ensures ExchangePoint and ExchangeSpace resources in system have a properly
          declared AMQP exchange
        - Ensures ExchangeName resources in system have a properly declared queue
        - Logs all exchanges/queues it didn't understand
        - Purges all service queues as long as no consumers are attached, or can be
          overridden with force=True on pycc command line
        """
        rr = process.container.resource_registry
        ex_manager = process.container.ex_manager
        sys_name = get_sys_name()

        # get list of queues from broker with full props that have to do with our sysname
        all_queues = ex_manager._list_queues()
        queues = {q['name']: q for q in all_queues if q['name'].startswith(sys_name)}

        # get list of exchanges from broker with full props
        all_exchanges = ex_manager._list_exchanges()
        exchanges = {e['name']: e for e in all_exchanges if e['name'].startswith(sys_name)}

        # now get list of XOs from RR
        xs_objs, _ = rr.find_resources(RT.ExchangeSpace)
        xp_objs, _ = rr.find_resources(RT.ExchangePoint)
        xn_objs, _ = rr.find_resources(RT.ExchangeName)

        xs_by_xp = {}
        assocs = rr.find_associations(predicate=PRED.hasExchangePoint, id_only=False)
        for assoc in assocs:
            if assoc.st == RT.ExchangeSpace and assoc.ot == RT.ExchangePoint:
                xs_by_xp[assoc.o] = assoc.s

        sys_xs_name = CFG.get_safe("exchange.core.system_xs", DEFAULT_SYSTEM_XS)
        sys_node_name, sys_node = ex_manager._get_node_for_xs(sys_xs_name)

        #
        # VERIFY XSs have a declared exchange
        #
        rem_exchanges = set(exchanges)

        xs_by_id = {}
        for rrxs in xs_objs:
            xs = ExchangeSpace(ex_manager, ex_manager._get_priv_transport(sys_node_name), sys_node, rrxs.name)
            xs_by_id[rrxs._id] = xs

            if xs.exchange in rem_exchanges:
                rem_exchanges.remove(xs.exchange)
            else:
                log.warn("BootstrapExchange restart: RR XS %s, id=%s NOT FOUND in exchanges", rrxs.name, rrxs._id)

        for rrxp in xp_objs:
            xs_id = xs_by_xp.get(rrxp._id, None)
            if not xs_id or xs_id not in xs_by_id:
                log.warn("Inconsistent!! XS for XP %s not found", rrxp.name)
                continue
            xs = xs_by_id[xs_id]
            xp = ExchangePoint(ex_manager, ex_manager._get_priv_transport(sys_node_name), sys_node, rrxp.name, xs)

            if xp.exchange in rem_exchanges:
                rem_exchanges.remove(xp.exchange)
            else:
                log.warn("BootstrapExchange restart: RR XP %s, id=%s NOT FOUND in exchanges", rrxp.name, rrxp._id)

        # # events and main service exchange should be left
        system_rpc_ex = "%s.%s" % (sys_name, sys_xs_name)
        event_ex = "%s.%s.%s" % (sys_name, sys_xs_name, CFG.get_safe("exchange.core.events", DEFAULT_EVENTS_XP))
        data_ex = "%s.%s.%s" % (sys_name, sys_xs_name, CFG.get_safe("exchange.core.data_streams", "data"))

        if system_rpc_ex in rem_exchanges:
            rem_exchanges.remove(system_rpc_ex)
        if event_ex in rem_exchanges:
            rem_exchanges.remove(event_ex)
        if data_ex in rem_exchanges:
            rem_exchanges.remove(data_ex)

        # what is left?
        for exchange in rem_exchanges:
            log.warn("BootstrapExchange restart: unknown exchange on broker %s", exchange)

        #
        # VERIFY XNs have a declared queue
        #
        rem_queues = set(queues)

        for rrxn in xn_objs:
            # can instantiate ExchangeNames, don't need specific types

            # @TODO: most queue types have a name instead of anon
            """
            # @TODO: except queue type, which needs to be fixed to record declared name type
            if rrxn.xn_type == "QUEUE":
                log.info("TODO: queue type XNs, %s", rrxn.name)
                continue
            """

            exchange_space_list, _ = rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangeName, rrxn._id)
            if not len(exchange_space_list) == 1:
                raise StandardError("Association from ExchangeSpace to ExchangeName %s does not exist" % rrxn._id)

            rrxs = exchange_space_list[0]

            xs = ExchangeSpace(ex_manager, ex_manager._get_priv_transport(sys_node_name), sys_node, rrxs.name)
            xn = ExchangeName(ex_manager, ex_manager._get_priv_transport(sys_node_name), sys_node, rrxn.name, xs)

            if xn.queue in rem_queues:
                rem_queues.remove(xn.queue)
            else:
                log.warn("BootstrapExchange restart: RR XN %s, type %s NOT FOUND in queues", xn.queue, xn.xn_type)

        # get list of service name possibilities
        svc_objs, _ = rr.find_resources(RT.ServiceDefinition)
        svc_names = [s.name for s in svc_objs]

        proc_objs, _ = rr.find_resources(RT.Process, id_only=False)
        current_proc_names = [p.name for p in proc_objs]
        cont_objs, _ = rr.find_resources(RT.CapabilityContainer, id_only=False)
        current_containers = [c.name for c in cont_objs]

        from pyon.ion.event import local_event_queues

        # PROCESS QUEUES + SERVICE QUEUES - not yet represented by resource
        proc_queues = set()
        svc_queues = set()
        event_queues = set()

        for queue in list(rem_queues):
            pieces = queue.split(".")

            # EVENT QUEUES
            if queue.startswith(event_ex) and pieces[-1] in local_event_queues:
                event_queues.add(queue)
                rem_queues.remove(queue)
                continue

            # CC AGENT QUEUES
            if pieces[-1].startswith("cc_agent_") and pieces[-1][9:] in current_containers:
                proc_queues.add(queue)
                rem_queues.remove(queue)
                continue

            # PROCESS QUEUES: proc manager spawned
            # pattern "<sysname>.<root_xs>.<containerid>.<pid>"
            if len(pieces) > 3 and pieces[-1].isdigit():
                if "%s.%s" % (pieces[-2], pieces[-1]) in current_proc_names:
                    proc_queues.add(queue)
                    rem_queues.remove(queue)
                continue

            # SERVICE QUEUES
            # pattern "<sysname>.<root_xs>.<service name>"
            if len(pieces) == 3:
                if pieces[-1] in svc_names:
                    svc_queues.add(queue)
                    rem_queues.remove(queue)

            # LOCAL RPC QUEUES
            # pattern "<sysname>.<root_xs>.rpc_<uuid>"
            if len(pieces) == 3:
                if pieces[-1].startswith("rpc_"):
                    rem_queues.remove(queue)


        # EMPTY LEFTOVER QUEUES - they are unaccounted for
        # TODO - current container used queues, e.g. process_dispatcher

        for qn in rem_queues:
            if int(queues[qn]['consumers']) == 0:
                ex_manager.delete_queue(qn)
                log.debug("Deleted unused queue: %s (%s messages)", qn, queues[qn]['messages'])

        #
        # EMPTY SERVICE QUEUES
        #
        for queue in svc_queues:
            if int(queues[queue]['messages']) > 0:
                ex_manager.purge_queue(queue)
                log.info("Purged service queue %s (%s messages)", queue, queues[queue]['messages'])
