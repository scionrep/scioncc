#!/usr/bin/python

import shlex
import simplejson

from putil.rabbitmq.rabbitmqadmin import Management, make_parser, LISTABLE, DELETABLE




class RabbitManagementUtil(object):
    def __init__(self, config, options=None, sysname=None):
        """
        Given a config object (system CFG or rabbit mgmt config), extracts the correct config
        and prepares util for subsequent calls to RabbitMQ via management plugin REST API.
        """
        self.mgmt_cfg = self.get_mgmt_config(config, sysname)
        self.connect_str = self.build_connect_str(self.mgmt_cfg)
        self.options = options
        self.sysname = sysname
        self.call_args = self.connect_str
        if self.options:
            self.call_args += "_" + self.options
        self.parser = make_parser()

    @staticmethod
    def get_mgmt_config(config, sysname=None):
        """ Returns the RabbitMq management config dict from indirect reference in container CFG
        or from given config dict. """
        if not config:
            raise RuntimeError("Bad config argument")
        if "container" in config and hasattr(config, "get_safe"):
            mgmt_cfg_key = config.get_safe("container.messaging.management.server", "rabbit_manage")
            mgmt_cfg = config.get_safe("server." + mgmt_cfg_key)
        elif "host" in config:
            mgmt_cfg = config
        else:
            raise RuntimeError("Bad RabbitMQ management config")
        sysname = sysname or "scioncc"

        mgmt_cfg = mgmt_cfg.copy()
        mgmt_cfg["host"] = mgmt_cfg.get("host", None) or "localhost"
        mgmt_cfg["port"] = mgmt_cfg.get("port", None) or "15672"
        mgmt_cfg["username"] = mgmt_cfg.get("username", None) or "guest"
        mgmt_cfg["password"] = mgmt_cfg.get("password", None) or "guest"
        mgmt_cfg["vhost"] = mgmt_cfg.get("vhost", None) or "/"

        mgmt_cfg["system_exchange"] = mgmt_cfg.get("system_exchange", None)
        if not mgmt_cfg["system_exchange"] and "exchange" in config and hasattr(config, "get_safe"):
            mgmt_cfg["system_exchange"] = "%s.%s" % (sysname, config.get_safe('exchange.core.system_xs', 'system'))

        mgmt_cfg["events_xp"] = mgmt_cfg.get("events_xp", None)
        if not mgmt_cfg["events_xp"] and "exchange" in config and hasattr(config, "get_safe"):
            mgmt_cfg["events_xp"] = "%s.%s" % (mgmt_cfg["system_exchange"], config.get_safe('exchange.core.events', 'events'))

        return mgmt_cfg

    @staticmethod
    def build_connect_str(mgmt_cfg):
        connect_str = "-q -H {0} -P {1} -u {2} -p {3} -V {4}".format(
                mgmt_cfg["host"], mgmt_cfg["port"], mgmt_cfg["username"], mgmt_cfg["password"], mgmt_cfg["vhost"])
        return connect_str

    @staticmethod
    def get_mgmt_url(config, feats=None):
        mgmt_cfg = RabbitManagementUtil.get_mgmt_config(config)
        feats = feats or []

        url = "http://%s:%s/api/%s" % (mgmt_cfg["host"], mgmt_cfg["port"], "/".join(feats))
        return url


    # -------------------------------------------------------------------------
    # Util methods

    def clean_by_prefix(self, prefix):
        """
        Utility method to clean (sysname) prefixed exchanges and queues on a broker.

        @param  prefix  The sysname / prefix to use to select exchanges and queues to delete.
                        Must be the prefix to the exchange or queue or this will not be deleted.
        @returns        A 2-tuple of (list of exchanges deleted, list of queues deleted).
        """
        exchanges         = self.list_names('exchanges')
        deleted_exchanges = self.delete_names_with_prefix('exchange', exchanges, prefix)

        queues            = self.list_names('queues')
        deleted_queues    = self.delete_names_with_prefix('queue', queues, prefix)

        return deleted_exchanges, deleted_queues

    def clean_by_sysname(self, sysname=None):
        sysname = sysname or self.sysname
        if not sysname:
            raise RuntimeError("Must provide sysname")
        return self.clean_by_prefix(sysname or self.sysname)

    def declare_exchange(self, xp):
        if xp == "events":
            ex_name = self.mgmt_cfg["events_xp"]
        else:
            ex_name = self.mgmt_cfg["system_exchange"]
        cmd_str = '{0} declare exchange name="{1}" durable=false auto_delete=true type=topic'.format(self.call_args, ex_name)
        (options, args) = self.parser.parse_args(shlex.split(cmd_str))
        mgmt = Management(options, args[1:])
        mgmt.invoke_declare()

    def declare_queue(self, xp, queue_name):
        if xp == "events":
            ex_name = self.mgmt_cfg["events_xp"]
        else:
            ex_name = self.mgmt_cfg["system_exchange"]

        if queue_name.startswith(self.sysname):
            qqueue_name = queue_name
        else:
            qqueue_name = ".".join([ex_name, queue_name])

        cmd_str = '{0} declare queue name="{1}" durable=false auto_delete=false'.format(self.call_args, qqueue_name)
        (options, args) = self.parser.parse_args(shlex.split(cmd_str))
        mgmt = Management(options, args[1:])
        mgmt.invoke_declare()

    def bind_queue(self, xp, queue_name, binding):
        if xp == "events":
            ex_name = self.mgmt_cfg["events_xp"]
        else:
            ex_name = self.mgmt_cfg["system_exchange"]

        if queue_name.startswith(self.sysname):
            qqueue_name = queue_name
        else:
            qqueue_name = ".".join([ex_name, queue_name])

        cmd_str = '{0} declare binding source="{1}" destination="{2}" destination_type=queue routing_key="{3}"'.format(
                self.call_args, ex_name, qqueue_name, binding)
        (options, args) = self.parser.parse_args(shlex.split(cmd_str))
        mgmt = Management(options, args[1:])
        mgmt.invoke_declare()

    # TODO: Move the management calls from pyon.ion.exchange here

    # -------------------------------------------------------------------------
    # Helpers

    def list_names(self, listable_type):
        list_str = '%s list %s name' % (self.call_args, listable_type)
        (options, args) = self.parser.parse_args(shlex.split(list_str))
        mgmt = Management(options, args[1:])
        uri = mgmt.list_show_uri(LISTABLE, 'list', mgmt.args[1:])
        output_json = mgmt.get(uri)
        listables = simplejson.loads(output_json)
        return listables

    def list_names_with_prefix(self, listables, name_prefix):
        return [l['name'] for l in listables if l['name'].startswith(name_prefix)]

    # This function works on exchange, queue, vhost, user
    def delete_names_with_prefix(self, deletable_type, deleteable,  name_prefix):
        deleted = []
        for d in deleteable:
            try:
                if d['name'].startswith(name_prefix):
                    delete_cmd = '%s delete %s name="%s"' % (self.call_args, deletable_type, d['name'])
                    (options, args) = self.parser.parse_args(shlex.split(delete_cmd))
                    mgmt = Management(options, args[1:])
                    mgmt.invoke_delete()
                    deleted.append(d['name'])
            except KeyError:
                # Some has no key 'name'
                pass
        return deleted
