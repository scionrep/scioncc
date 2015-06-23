#!/usr/bin/env python

"""Exchange management classes."""

__author__ = 'Michael Meisinger, Dave Foster'

import gevent
import requests
import simplejson as json
import socket
import time

from pyon.core import bootstrap
from pyon.core.bootstrap import CFG, get_service_registry
from pyon.core.exception import Timeout, ServiceUnavailable, ServerError
from pyon.ion.endpoint import ProcessEndpointUnitMixin
from pyon.ion.identifier import create_simple_unique_id
from pyon.ion.resource import RT
from pyon.net import messaging
from pyon.net.transport import NameTrio, TransportError, XOTransport
from pyon.util.containers import get_safe
from pyon.util.log import log

from interface.services.core.iresource_registry_service import ResourceRegistryServiceProcessClient


ION_DEFAULT_BROKER = "system_broker"
DEFAULT_SYSTEM_XS = "system"
DEFAULT_EVENTS_XP = "events"


class ExchangeManagerError(StandardError):
    pass


class ExchangeManager(object):
    """
    Manager object for the CC to manage Exchange related resources.
    """

    def __init__(self, container):
        log.debug("ExchangeManager initializing ...")
        self.container = container
        self._rr_client = None

        # Define the callables that can be added to Container public API
        self.container_api = [self.create_xs,
                              self.create_xp,
                              self.create_service_xn,
                              self.create_process_xn,
                              self.create_queue_xn,
                              self.create_event_xn]

        # Add the public callables to Container
        for call in self.container_api:
            setattr(self.container, call.__name__, call)

        self.system_xs_name         = CFG.get_safe("exchange.core.system_xs", DEFAULT_SYSTEM_XS)
        self.default_xs             = None
        self._xs_cache              = {}              # caching of xs names to RR objects
        self._default_xs_obj        = None      # default XS registry object
        self.org_id                 = None
        self._default_xs_declared   = False

        # mappings
        self.xs_by_name = {}                    # friendly named XS to XSO
        self.xn_by_name = {}                    # friendly named XN to XNO
        # xn by xs is a property

        # mapping of node/ioloop runner by connection name (in config, named via container.messaging.server keys)
        # also privileged connections to those same nodes, if existing.
        self._nodes           = {}
        self._ioloops         = {}
        self._priv_nodes      = {}
        self._priv_ioloops    = {}

        # cache of privileged transports
        # they may not be created by privileged connections, as privileged connections are optional.
        self._priv_transports = {}

    def start(self):
        log.debug("ExchangeManager.start")

        total_count = 0

        def handle_failure(name, node, priv):
            log.warn("Node %s (privileged: %s) could not be started", priv, name)
            node.ready.set()        # let it fall out below

        # read broker config to get nodes to connect to
        brokers = []
        for broker_name, broker_cfg in CFG.get_safe('exchange.exchange_brokers').iteritems():
            cfg_key = broker_cfg.get('server', None)
            if not cfg_key:
                continue

            brokers.append((broker_name, cfg_key, False))

            priv_key = broker_cfg.get('server_priv', None)
            if priv_key is not None:
                brokers.append((broker_name, priv_key, True))

        # connect to all known brokers
        for b in brokers:
            broker_name, cfgkey, is_priv = b

            if cfgkey not in CFG.server:
                raise ExchangeManagerError("Config key %s (name: %s) (from CFG.container.messaging.server) not in CFG.server" % (cfgkey, broker_name))

            total_count += 1
            log.debug("Starting connection: %s", broker_name)

            try:
                cfg_params = CFG.server[cfgkey]

                if cfg_params['type'] == 'local':
                    node, ioloop = messaging.make_local_node(0, self.container.local_router)
                else:
                    # start it with a zero timeout so it comes right back to us
                    node, ioloop = messaging.make_node(cfg_params, broker_name, 0)

                # install a finished handler directly on the ioloop just for this startup period
                fail_handle = lambda _: handle_failure(broker_name, node, is_priv)
                ioloop.link(fail_handle)

                # wait for the node ready event, with a large timeout just in case
                node_ready = node.ready.wait(timeout=15)

                # remove the finished handler, we don't care about it here
                ioloop.unlink(fail_handle)

                # only add to our list if we started successfully
                if not node.running:
                    ioloop.kill()      # make sure ioloop dead
                else:
                    if is_priv:
                        self._priv_nodes[broker_name]   = node
                        self._priv_ioloops[broker_name] = ioloop
                    else:
                        self._nodes[broker_name]        = node
                        self._ioloops[broker_name]      = ioloop

            except socket.error as e:
                log.warn("Could not start connection %s due to socket error, continuing", broker_name)

        fail_count = total_count - len(self._nodes) - len(self._priv_nodes)
        if fail_count > 0 or total_count == 0:
            if fail_count == total_count:
                raise ExchangeManagerError("No node connection was able to start (%d nodes attempted, %d nodes failed)" % (total_count, fail_count))

            log.warn("Some nodes could not be started, ignoring for now")   # @TODO change when ready

        # load interceptors into each
        map(lambda x: x.setup_interceptors(CFG.interceptor), self._nodes.itervalues())
        map(lambda x: x.setup_interceptors(CFG.interceptor), self._priv_nodes.itervalues())

        # prepare privileged transports
        for name in self._nodes:
            node = self._priv_nodes.get(name, self._nodes[name])
            transport = self.get_transport(node)
            transport.lock = True    # prevent any attempt to close
            transport.add_on_close_callback(lambda *a, **kw: self._privileged_transport_closed(name, *a, **kw))
            self._priv_transports[name] = transport

        # create default Exchange Space
        self.default_xs = self._create_root_xs()

        log.debug("Started %d connections (%s)", len(self._nodes) + len(self._priv_nodes), ",".join(self._nodes.keys() + self._priv_nodes.keys()))

    def stop(self, *args, **kwargs):
        # ##############
        # HACK
        #
        # It appears during shutdown that when a channel is closed, it's not FULLY closed by the pika connection
        # until the next round of _handle_events. We have to yield here to let that happen, in order to have close
        # work fine without blowing up.
        # ##############
        time.sleep(0.1)
        # ##############

        log.debug("ExchangeManager.stopping (%d connections)", len(self._nodes) + len(self._priv_nodes))

        for name in self._nodes:
            self._nodes[name].stop_node()
            self._ioloops[name].kill()
            #self._nodes[name].client.ioloop.start()     # loop until connection closes

        for name in self._priv_nodes:
            self._priv_nodes[name].stop_node()
            self._priv_ioloops[name].kill()
            #self._priv_nodes[name].client.ioloop.start()

        # @TODO: does this do anything? node's already gone by this point
        for transport in self._priv_transports.itervalues():
            transport.lock = False
            transport.close()

        # @TODO undeclare root xs??  need to know if last container
        #self.default_xs.delete()

    @property
    def default_node(self):
        """
        Returns the default node connection.
        """
        if ION_DEFAULT_BROKER in self._nodes:
            return self._nodes[ION_DEFAULT_BROKER]
        elif len(self._nodes):
            log.warn("No default connection, returning first available")
            return self._nodes.values()[0]

        return None

    @property
    def xn_by_xs(self):
        """
        Get a list of XNs associated by XS (friendly name).
        """
        ret = {}
        for xnname, xn in self.xn_by_name.iteritems():
            xsn = xn._xs._exchange
            if not xsn in ret:
                ret[xsn] = []
            ret[xsn].append(xn)

        return ret

    def _privileged_transport_closed(self, name, transport, code, text):
        """
        Callback for when the privileged transport is closed.

        If it's an error close, this is bad and will fail fast the container.
        """
        if not (code == 0 or code == 200):
            log.error("The privileged transport has failed (%s: %s)", code, text)
            self.container.fail_fast("ExManager privileged transport (broker %s) has failed (%s: %s)" % (name, code, text), True)

    def cleanup_xos(self):
        """
        Iterates the list of Exchange Objects and deletes them.

        Typically used for test cleanup.
        """

        xns = self.xn_by_name.values()  # copy as we're removing as we go

        for xn in xns:
            if isinstance(xn, ExchangePoint):   # @TODO ugh
                self.delete_xp(xn)
            else:
                self.delete_xn(xn)

        xss = self.xs_by_name.values()

        for xs in xss:
            if not (xs == self.default_xs and not self._default_xs_declared):
                self.delete_xs(xs)

        # reset xs map to initial state
        self._default_xs_declared = False
        self.xs_by_name = {self.system_xs_name: self.default_xs }      # friendly named XS to XSO

    def _get_xs_obj(self, name=None):
        """
        Gets a resource-registry represented XS, either via cache or RR request.
        """
        name = name or self.system_xs_name
        if name in self._xs_cache:
            return self._xs_cache[name]

        return None

    def _bootstrap_default_org(self):
        """
        Finds an Org resource to be used by create_xs.

        @TODO: create_xs is being removed, so this will not be needed
        """
        if not self.org_id:
            # find the default Org
            root_orgname = CFG.get_safe("system.root_org", "ION")
            org_ids, _ = self._rr.find_resources(RT.Org, name=root_orgname, id_only=True)
            if not org_ids or len(org_ids) != 1:
                log.warn("Could not find ION root Org")
                return None

            self.org_id = org_ids[0]
            log.debug("Bootstrapped Container exchange manager with org id: %s", self.org_id)

        return self.org_id

    def _create_root_xs(self):
        """
        The ROOT/default XS needs a special creation - simulate EMS here.
        """
        node_name, node = self._get_node_for_xs(self.system_xs_name)
        transport = self._get_priv_transport(node_name)

        xs = ExchangeSpace(self,
                           transport,
                           node,
                           self.system_xs_name,
                           exchange_type='topic',
                           durable=False,            # @TODO: configurable?
                           auto_delete=True)

        # ensure_default_declared will take care of any declaration we need to do
        return xs

    @property
    def _rr(self):
        """
        Returns the active resource registry instance or client.

        Used to directly contact the resource registry via the container if available,
        otherwise the messaging client to the RR service is returned.
        """
        if self.container.has_capability('RESOURCE_REGISTRY'):
            return self.container.resource_registry

        if self._rr_client is None:
            self._rr_client = ResourceRegistryServiceProcessClient(process=self.container)

        return self._rr_client

    def get_transport(self, node):
        """
        Get a transport to be used by operations here.
        """
        assert self.container

        with node._lock:
            transport = node._new_transport()
            return transport

    def _get_priv_transport(self, node_name):
        """
        Returns the privileged transport corresponding to the node name.
        Does basic error checking.
        """
        if not node_name in self._priv_transports:
            raise ExchangeManagerError("No transport available for node %s", node_name)

        return self._priv_transports[node_name]

    def _build_security_headers(self):
        """
        Builds additional security headers to be passed through to EMS.
        """
        # pull context from container
        ctx = self.container.context.get_context()

        if isinstance(ctx, dict):
            return ProcessEndpointUnitMixin.build_security_headers(ctx)

        return None

    def _get_node_for_xs(self, xs_name):
        """
        Finds a node to be used by an ExchangeSpace.

        Looks up the given exchange space in CFG under the exchange.exchange_brokers section.
        Will return the default node if none found.
        Returns a 2-tuple of name, node.
        """
        for broker_name, broker_cfg in CFG.get_safe('exchange.exchange_brokers', {}).iteritems():
            # Bug in DotList, contains not implemented correctly
            if xs_name in list(broker_cfg['join_xs']):
                return broker_name, self._priv_nodes.get(broker_name, self._nodes.get(broker_name, None))

        # return default node, have to look up the name
        default_node = self.default_node
        for name, node in self._nodes.iteritems():
            if node == default_node:
                return name, node

        # couldn't find a default, raise
        raise ExchangeManagerError("Could not find a node or default for XS %s", xs_name)

    def _get_node_for_xp(self, xp_name, xs_name):
        """
        Finds a node to be used by an ExchangePoint, falling back to an ExchangeSpace if none found.

        Similar to _get_node_for_xs.
        Returns a 2-tuple of name, node.
        """
        for broker_name, broker_cfg in CFG.get_safe('exchange.exchange_brokers', {}).iteritems():
            # Bug in DotList, contains not implemented correctly
            if xp_name in list(broker_cfg['join_xp']):
                return broker_name, self._priv_nodes.get(broker_name, self._nodes.get(broker_name, None))

        # @TODO: iterate exchange.exchange_spaces.<item>.exchange_points?

        return self._get_node_for_xs(xs_name)

    def create_xs(self, name, exchange_type='topic', durable=False, auto_delete=True, declare=True):
        log.debug("ExchangeManager.create_xs: %s", name)

        node_name, node = self._get_node_for_xs(name)
        transport = self._get_priv_transport(node_name)

        xs = ExchangeSpace(self, transport, node, name,
                           exchange_type=exchange_type,
                           durable=durable,
                           auto_delete=auto_delete)

        if declare:
            # declare default but only if we're not making default!
            if self.default_xs is not None and name != self.system_xs_name:
                self._ensure_default_declared()

            xs.declare()

        self.xs_by_name[name] = xs

        return xs

    def delete_xs(self, xs):
        """
        @type xs    ExchangeSpace
        """
        log.debug("ExchangeManager.delete_xs: %s", xs)

        name = xs._exchange     # @TODO this feels wrong
        self.xs_by_name.pop(name, None)   # EMS may be running on the same container, which touches this same dict
                                          # so delete in the safest way possible
                                          # @TODO: does this mean we need to sync xs_by_name and friends in the datastore?

        try:
            xs.delete()
        except TransportError as ex:
            log.warn("Could not delete XS (%s): %s", name, ex)

    def create_xp(self, name, xs=None, declare=True, **kwargs):
        log.debug("ExchangeManager.create_xp: %s", name)

        xs              = xs or self.default_xs

        node_name, node = self._get_node_for_xp(name, xs._exchange)
        transport       = self._get_priv_transport(node_name)

        xp = ExchangePoint(self, transport, node, name, xs, **kwargs)

        # put in xn_by_name anyway
        self.xn_by_name[name] = xp

        # is the exchange object for the XS available?
        xso = self._get_xs_obj(xs._exchange)        # @TODO: _exchange is wrong

        if declare:
            self._ensure_default_declared()
            xp.declare()

        return xp

    def delete_xp(self, xp):
        log.debug("ExchangeManager.delete_xp: name=%s", 'TODO')   # xp.build_xname())

        name = xp._exchange              # @TODO: not right
        self.xn_by_name.pop(name, None)  # EMS may be running on the same container, which touches this same dict
                                         # so delete in the safest way possible
                                         # @TODO: does this mean we need to sync xs_by_name and friends in the datastore?

        try:
            xp.delete()
        except TransportError as ex:
            log.warn("Could not delete XP (%s): %s", name, ex)

    def _create_xn(self, xn_type, name, xs=None, declare=True, **kwargs):
        xs = xs or self.default_xs
        log.debug("ExchangeManager._create_xn: type: %s, name=%s, xs=%s, kwargs=%s", xn_type, name, xs, kwargs)

        # @TODO: based on xs/xp
        node_name, node = self._get_node_for_xs(xs._exchange)   # feels wrong
        transport       = self._get_priv_transport(node_name)

        if xn_type == "service":
            xn = ServiceExchangeName(self, transport, node, name, xs, **kwargs)
        elif xn_type == "process":
            xn = ProcessExchangeName(self, transport, node, name, xs, **kwargs)
        elif xn_type == "queue":
            xn = QueueExchangeName(self, transport, node, name, xs, **kwargs)
        else:
            raise StandardError("Unknown XN type: %s" % xn_type)

        self._register_xn(name, xn, xs, declare=declare)
        return xn

    def _register_xn(self, name, xn, xs, declare=True):
        """
        Helper method to register an XN with EMS/RR.
        """
        self.xn_by_name[name] = xn

        xso = self._get_xs_obj(xs._exchange)

        if declare:
            self._ensure_default_declared()
            xn.declare()

        return xn

    def create_service_xn(self, name, xs=None, **kwargs):
        return self._create_xn('service', name, xs=xs, **kwargs)

    def create_process_xn(self, name, xs=None, **kwargs):
        return self._create_xn('process', name, xs=xs, **kwargs)

    def create_queue_xn(self, name, xs=None, **kwargs):
        return self._create_xn('queue', name, xs=xs, **kwargs)

    def create_event_xn(self, name, event_type=None, origin=None, sub_type=None, origin_type=None, pattern=None,
                        xp=None, auto_delete=None, **kwargs):
        """
        Creates an EventExchangeName suitable for listening with an EventSubscriber.
        
        Pass None for the name to have one automatically generated.
        If you pass a pattern, it takes precedence over making a new one from event_type/origin/sub_type/origin_type.
        """
        # make a name if no name exists
        name = name or create_simple_unique_id()

        # get event xp for the xs if not set
        if not xp:
            # pull from configuration
            eventxp = CFG.get_safe('exchange.core.events', DEFAULT_EVENTS_XP)
            xp = self.create_xp(eventxp)

        node = xp.node
        transport = xp._transports[0]

        xn = EventExchangeName(self, transport, node, name, xp,
                               event_type=event_type,
                               sub_type=sub_type,
                               origin=origin,
                               origin_type=origin_type,
                               pattern=pattern,
                               auto_delete=auto_delete,
                               **kwargs)

        self._register_xn(name, xn, xp)

        return xn

    def delete_xn(self, xn):
        log.debug("ExchangeManager.delete_xn: name=%s", "TODO")  # xn.build_xlname())

        name = xn._queue                 # @TODO feels wrong
        self.xn_by_name.pop(name, None)  # EMS may be running on the same container, which touches this same dict
                                         # so delete in the safest way possible
                                         # @TODO: does this mean we need to sync xs_by_name and friends in the datastore?

        try:
            xn.delete()
        except TransportError as ex:
            log.warn("Could not delete XN (%s): %s", name, ex)

    def _ensure_default_declared(self):
        """
        Ensures we declared the default exchange space.
        Needed by most exchange object calls, so each one calls here.
        """
        if not self._default_xs_declared:
            log.debug("ExchangeManager._ensure_default_declared, declaring default xs")
            self._default_xs_declared = True
            self.default_xs.declare()

    def get_definitions(self):
        """
        Rabbit HTTP management API call to get all defined objects on a broker.

        Returns users, vhosts, queues, exchanges, bindings, rabbit_version, and permissions.
        """
        url = self._get_management_url("definitions")
        raw_defs = self._call_management(url)

        return raw_defs

    def list_nodes(self):
        """
        Rabbit HTTP management API call to get all nodes in a cluster.
        """
        url = self._get_management_url("nodes")
        nodes = self._call_management(url)

        return nodes

    def list_connections(self):
        """
        Rabbit HTTP management API call to get all connections to a broker.
        """
        url = self._get_management_url("connections")
        conns = self._call_management(url)

        return conns

    def list_channels(self):
        """
        Rabbit HTTP management API call to get channels opened on the broker.
        """
        url = self._get_management_url("channels")
        chans = self._call_management(url)

        return chans

    def list_exchanges(self):
        """
        Rabbit HTTP management API call to list exchanges on the broker.

        Returns a list of exchange names. If you want the full set of properties for each,
        use _list_exchanges.
        """
        raw_exchanges = self._list_exchanges()
        exchanges = [x['name'] for x in raw_exchanges]

        return exchanges

    def _list_exchanges(self):
        """
        Rabbit HTTP management API call to list exchanges with full properties.

        This is used by list_exchanges to get a list of names, but does not filter anything.
        """
        url = self._get_management_url("exchanges", "%2f")
        raw_exchanges = self._call_management(url)

        return raw_exchanges

    def list_queues(self, name=None, return_columns=None):
        """
        Rabbit HTTP management API call to list names of queues on the broker.

        Returns a list of queue names.  Can specify an optional list of
        column names to filter the data returned from the API query.  If you want
        the full properties for each, use _list_queues.

        @param  name    If set, filters the list by only including queues with name in them.
        """
        raw_queues = self._list_queues(return_columns=return_columns)

        nl = lambda x: (name is None) or (name is not None and name in x)

        if return_columns is None:
            queues = [x['name'] for x in raw_queues if nl(x['name'])]
        else:
            queues = [x for x in raw_queues if nl(x['name'])]

        return queues

    def _list_queues(self, return_columns=None):
        """
        Rabbit HTTP management API call to list queues with full properties. Can specify an optional list of
        column names to filter the data returned from the API query.

        This is used by list_queues to get a list of names, but does not filter anything.
        """
        feats = "%2f"
        if isinstance(return_columns, list):
            feats += "?columns=" + ','.join(return_columns)
        url = self._get_management_url("queues", feats)
        raw_queues = self._call_management(url)

        return raw_queues

    def get_queue_info(self, queue):
        """
        Rabbit HTTP management API call to get full properties of a single queue.
        """
        url = self._get_management_url("queues", "%2f", queue)
        queue_info = self._call_management(url)

        return queue_info

    def list_bindings(self, exchange=None, queue=None):
        """
        Rabbit HTTP management API call to list bindings.

        Returns a list of tuples formatted as (exchange, queue, routing_key, properties_key aka binding id).
        This method can optionally filter queues or exchanges (or both) by specifying strings to
        exchange/queue keyword arguments. If you want the full list of properties unfiltered, call
        _list_bindings instead.

        The properties_key is used to delete a binding.

        If you want to get the bindings on a specific queue or exchange, don't use the filters here, but
        call the specific list_bindings_for_queue or list_bindings_for_exchange, as they will not result
        in a large result from the management API.

        @param  exchange    If set, filters the list by only including bindings with exchanges that have the
                            passed value in them.
        @param  queue       If set, filters the list by only including bindings with queues that have the
                            passed value in them.
        """
        raw_binds = self._list_bindings()

        ql = lambda x: (queue is None) or (queue is not None and queue in x)
        el = lambda x: (exchange is None) or (exchange is not None and exchange in x)

        binds = [(x['source'], x['destination'], x['routing_key'], x['properties_key']) for x in raw_binds if x['destination_type'] == 'queue' and x['source'] != '' and ql(x['destination']) and el(x['source'])]
        return binds

    def _list_bindings(self):
        """
        Rabbit HTTP management API call to list bindings with full properties.

        This is used by list_bindings to get a list of binding tuples, but does not filter anything.
        """
        url = self._get_management_url("bindings", "%2f")
        raw_binds = self._call_management(url)

        return raw_binds

    def list_bindings_for_queue(self, queue):
        """
        Rabbit HTTP management API call to list bindings for a queue.

        Returns a list of tuples formatted as (exchange, queue, routing_key, properties_key aka binding id).
        If you want the full list of properties for all the bindings, call _list_bindings_for_queue instead.

        This method is much more efficient than calling list_bindings with a filter.

        @param  queue   The name of the queue you want bindings for.
        """
        raw_binds = self._list_bindings_for_queue(queue)

        binds = [(x['source'], x['destination'], x['routing_key'], x['properties_key']) for x in raw_binds if x['source'] != '']
        return binds

    def _list_bindings_for_queue(self, queue):
        """
        Rabbit HTTP management API call to list bindings on a queue with full properties.

        This is used by list_bindings_for_queue to get a list of binding tuples, but does not filter
        anything.
        """
        url = self._get_management_url("queues", "%2f", queue, "bindings")
        raw_binds = self._call_management(url)

        return raw_binds

    def list_bindings_for_exchange(self, exchange):
        """
        Rabbit HTTP management API call to list bindings for an exchange.

        Returns a list of tuples formatted as (exchange, queue, routing_key, properties_key aka binding id).
        If you want the full list of properties for all the bindings, call _list_bindings_for_exchange instead.

        This method is much more efficient than calling list_bindings with a filter.

        @param  exchange    The name of the exchange you want bindings for.
        """
        raw_binds = self._list_bindings_for_exchange(exchange)

        binds = [(x['source'], x['destination'], x['routing_key'], x['properties_key']) for x in raw_binds if x['source'] != '']
        return binds

    def _list_bindings_for_exchange(self, exchange):
        """
        Rabbit HTTP management API call to list bindings for an exchange with full properties.

        This is used by list_bindings_for_exchange to get a list of binding tuples, but does not filter
        anything.
        """
        url = self._get_management_url("exchanges", "%2f", exchange, "bindings", "source")
        raw_binds = self._call_management(url)

        return raw_binds

    def delete_binding(self, exchange, queue, binding_prop_key):
        """
        Rabbit HTTP management API call to delete a binding.

        You may also use delete_binding_tuple to directly pass the tuples returned by
        any of the list binding calls.
        """

        # have to urlencode the %, even though it is already urlencoded - rabbit needs this.
        url = self._get_management_url("bindings", "%2f", "e", exchange, "q", queue, binding_prop_key.replace("%", "%25"))
        self._call_management_delete(url)

        return True

    def delete_binding_tuple(self, binding_tuple):
        """
        Rabbit HTTP management API call to delete a binding using a tuple from our list binding methods.
        """
        return self.delete_binding(binding_tuple[0], binding_tuple[1], binding_tuple[3])

    def delete_queue(self, queue):
        """
        Rabbit HTTP management API call to delete a queue.
        """
        url = self._get_management_url("queues", "%2f", queue)
        self._call_management_delete(url)

    def purge_queue(self, queue):
        """
        Rabbit HTTP management API call to purge a queue.
        """
        url = self._get_management_url("queues", "%2f", queue, "contents")
        self._call_management_delete(url)

        return True

    def _get_management_url(self, *feats):
        """
        Builds a URL to be used with the Rabbit HTTP management API.
        """
        node = self._priv_nodes.get(ION_DEFAULT_BROKER, self.default_node)
        host = node.client.parameters.host

        mgmt_cfg_key = CFG.get_safe("container.messaging.management.server", "rabbit_manage")
        mgmt_cfg = CFG.get_safe("server." + mgmt_cfg_key)
        mgmt_port = get_safe(mgmt_cfg, "port") or "15672"
        url = "http://%s:%s/api/%s" % (host, mgmt_port, "/".join(feats))

        return url

    def _call_management(self, url):
        """
        Makes a GET HTTP request to the Rabbit HTTP management API.

        This method will raise an exception if a non-200 HTTP status code is returned.

        @param  url     A URL to be used, build one with _get_management_url.
        """
        return self._make_management_call(url)

    def _call_management_delete(self, url):
        """
        Makes an HTTP DELETE request to the Rabbit HTTP management API.

        This method will raise an exception if a non-200 HTTP status code is returned.

        @param  url     A URL to be used, build one with _get_management_url.
        """
        return self._make_management_call(url, method="delete")

    def _make_management_call(self, url, method="get", data=None):
        """
        Makes a call to the Rabbit HTTP management API using the passed in HTTP method.
        """
        log.debug("Calling rabbit API management (%s): %s", method, url)

        meth = getattr(requests, method)

        try:
            mgmt_cfg_key = CFG.get_safe("container.messaging.management.server", "rabbit_manage")
            mgmt_cfg = CFG.get_safe("server." + mgmt_cfg_key)
            username = get_safe(mgmt_cfg, "username") or "guest"
            password = get_safe(mgmt_cfg, "password") or "guest"

            with gevent.timeout.Timeout(10):
                r = meth(url, auth=(username, password), data=data)
            r.raise_for_status()

            if not r.content == "":
                content = json.loads(r.content)
            else:
                content = None

        except gevent.timeout.Timeout as ex:
            raise Timeout(str(ex))
        except requests.exceptions.Timeout as ex:
            raise Timeout(str(ex))
        except (requests.exceptions.ConnectionError, socket.error) as ex:
            raise ServiceUnavailable(str(ex))
        except requests.exceptions.RequestException as ex:
            # the generic base exception all requests' exceptions inherit from, raise our
            # general server error too.
            raise ServerError(str(ex))

        return content

##############################################################################
##############################################################################
##############################################################################

class ExchangeSpace(XOTransport, NameTrio):

    def __init__(self, exchange_manager, privileged_transport, node, exchange, exchange_type='topic', durable=False, auto_delete=True):
        XOTransport.__init__(self,
                             exchange_manager=exchange_manager,
                             privileged_transport=privileged_transport,
                             node=node)
        NameTrio.__init__(self, exchange=exchange)

        self._xs_exchange_type = exchange_type
        self._xs_durable       = durable
        self._xs_auto_delete   = auto_delete

    @property
    def exchange_durable(self):
        # Added because exchanges get deleted on broker restart
        if CFG.get_safe('container.messaging.names.durable', False):
            self._xs_durable = True
            return True

        return self._xs_durable

    @property
    def exchange_auto_delete(self):
        # Added because exchanges get deleted on broker restart
        if CFG.get_safe('container.messaging.names.durable', False):
            self._xs_auto_delete = False
            return False

        return self._xs_auto_delete

    @property
    def exchange(self):
        return "%s.%s" % (bootstrap.get_sys_name(), self._exchange)

    def declare(self):
        self.declare_exchange_impl(self.exchange,
                                   exchange_type=self._xs_exchange_type,
                                   durable=self.exchange_durable,
                                   auto_delete=self.exchange_auto_delete)

    def delete(self):
        self.delete_exchange_impl(self.exchange)


class ExchangeName(XOTransport, NameTrio):

    xn_type         = "XN_BASE"
    _xn_durable     = None
    _xn_auto_delete = None
    _declared_queue = None

    def __init__(self, exchange_manager, privileged_transport, node, name, xs, durable=None, auto_delete=None):
        XOTransport.__init__(self,
                             exchange_manager=exchange_manager,
                             privileged_transport=privileged_transport,
                             node=node)
        NameTrio.__init__(self, exchange=None, queue=name)

        self._xs = xs

        if durable is not None:
            self._xn_durable = durable
        if auto_delete is not None:
            self._xn_auto_delete = auto_delete

    @property
    def queue_durable(self):
        if self._xn_durable is not None:
            return self._xn_durable

        if CFG.get_safe('container.messaging.names.durable', False):
            return True

        return False

    @queue_durable.setter
    def queue_durable(self, value):
        self._xn_durable = value

    @property
    def queue_auto_delete(self):
        if self._xn_auto_delete is not None:
            return self._xn_auto_delete

        return False

    @queue_auto_delete.setter
    def queue_auto_delete(self, value):
        self._xn_auto_delete = value

    @property
    def exchange(self):
        return self._xs.exchange

    @property
    def queue(self):
        # make sure prefixed with sysname?
        queue = self._queue
        if self._queue and not self.exchange in self._queue:
            queue = ".".join([self.exchange, self._queue])

        return queue

    def declare(self):
        self._declared_queue = self.declare_queue_impl(self.queue,
                                                       durable=self.queue_durable,
                                                       auto_delete=self.queue_auto_delete)
        return self._declared_queue

    def delete(self):
        self.delete_queue_impl(self.queue)
        self._declared_queue = None

    def bind(self, binding_key, xs_or_xp=None):
        exchange = self.exchange
        if xs_or_xp is not None:
            exchange = xs_or_xp.exchange

        self.bind_impl(exchange, self.queue, binding_key)

    def unbind(self, binding_key, xs_or_xp=None):
        exchange = self.exchange
        if xs_or_xp is not None:
            exchange = xs_or_xp.exchange

        self.unbind_impl(exchange, self.queue, binding_key)

    def setup_listener(self, binding, default_cb):
        log.debug("ExchangeName.setup_listener: B %s", binding)

        # make sure we've bound (idempotent action)
        self.bind(binding)

    def get_stats(self):
        return self.get_stats_impl(self.queue)

    def purge(self):
        return self.purge_impl(self.queue)

    def __str__(self):
        return self.xn_type + "-" + NameTrio.__str__(self)


class ExchangePoint(ExchangeName):
    """
    @TODO is this really an ExchangeName? seems more inline with XS
    @TODO a nameable ExchangePoint - to be able to create a named queue that receives routed
            messages from the XP.
    """
    XPTYPES = {
        'basic': 'basic',
        'ttree': 'ttree',
        }

    xn_type = "XN_XP"

    def __init__(self, exchange_manager, privileged_transport, node, name, xs, xptype=None):
        xptype = xptype or 'ttree'

        XOTransport.__init__(self,
                             exchange_manager=exchange_manager,
                             privileged_transport=privileged_transport,
                             node=node)
        NameTrio.__init__(self, exchange=name)

        self._xs        = xs
        self._xptype    = xptype

    @property
    def exchange(self):
        return "%s.%s" % (self._xs.exchange, self._exchange)

    @property
    def queue(self):
        if self._queue:
            return self._queue
        return None     # @TODO: correct?

    def declare(self):
        param_kwargs = {}
        # Added because exchanges get deleted on broker restart
        if CFG.get_safe('container.messaging.names.durable', False):
            param_kwargs["durable"] = True
            param_kwargs["auto_delete"] = False

        self.declare_exchange_impl(self.exchange, **param_kwargs)

    def delete(self):
        self.delete_exchange_impl(self.exchange)

    def create_route(self, name):
        """
        Returns an ExchangePointRoute used for sending messages to an exchange point.
        """
        return ExchangePointRoute(self._exchange_manager, self._transports[0], self.node, name, self)

    def get_stats(self):
        raise NotImplementedError("get_stats not implemented for XP")

    def purge(self):
        raise NotImplementedError("purge not implemented for XP")


class ExchangePointRoute(ExchangeName):
    """
    Used for sending messages to an exchange point via a Publisher.

    This object is created via ExchangePoint.create_route
    """

    def __init__(self, exchange_manager, privileged_transport, node, name, xp):
        ExchangeName.__init__(self, exchange_manager, privileged_transport, node, name, xp)     # xp goes to xs param

    @property
    def queue_durable(self):
        return self._xs.queue_durable    # self._xs -> owning xp

    def declare(self):
        raise StandardError("ExchangePointRoute does not support declare")

    def delete(self):
        raise StandardError("ExchangePointRoute does not support delete")


class ProcessExchangeName(ExchangeName):
    xn_type = "XN_PROCESS"
    pass


class ServiceExchangeName(ExchangeName):
    xn_type = "XN_SERVICE"

    @ExchangeName.queue_durable.getter
    def queue_durable(self):
        """
        Queue durable override for service names.

        Ignores the CFG setting, as these are supposed to be non-durable, unless you specifically
        ask for it to be.

        See: http://stackoverflow.com/questions/7019643/overriding-properties-in-python
        """
        if self._xn_durable is not None:
            return self._xn_durable

        return False


class QueueExchangeName(ExchangeName):
    xn_type = "XN_QUEUE"

    @property
    def queue(self):
        if self._declared_queue:
            return self._declared_queue
        return ExchangeName.queue.fget(self)

    def setup_listener(self, binding, default_cb):
        log.debug("ExchangeQueue.setup_listener: passing on binding")


class EventExchangeName(ExchangeName):
    """
    Listening ExchangeName for Event subscribers.
    """
    xn_type = "XN_EVENT"

    def __init__(self, exchange_manager, privileged_transport, node, name, xp, event_type=None, origin=None,
                 sub_type=None, origin_type=None, pattern=None, auto_delete=None):
        # xp goes to xs param
        ExchangeName.__init__(self, exchange_manager, privileged_transport, node, name, xp, auto_delete=auto_delete)

        self._queue = name

        if not pattern:
            from pyon.ion.event import BaseEventSubscriberMixin
            self._binding = BaseEventSubscriberMixin._topic(event_type,
                                                            origin,
                                                            sub_type=sub_type,
                                                            origin_type=origin_type)
        else:
            self._binding = pattern

    @property
    def queue_durable(self):
        # TODO: Is this correct? This overshadows the base class.
        return self._xs.queue_durable
