#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject, log, RT, PRED, Conflict, Inconsistent, NotFound, BadRequest

from interface.services.core.iexchange_management_service import BaseExchangeManagementService


class ExchangeManagementService(BaseExchangeManagementService):
    """
    The Exchange Management Service is the service that manages the Exchange and its associated
    resources, such as ExchangeSpaces, Names, Points and Brokers.
    """
    EX_NAME_TYPES = {'service', 'process', 'queue'}

    def on_init(self):
        self.rr = self.clients.resource_registry

    # -------------------------------------------------------------------------
    # ExchangeSpace management

    def create_exchange_space(self, exchange_space=None, org_id=''):
        """Creates an Exchange Space distributed resource from the parameter exchange_space object.
        """
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        self._validate_resource_obj("exchange_space", exchange_space, RT.ExchangeSpace, checks="noid,name")

        xs_id = self._get_org_exchange_space(org_id, exchange_space.name)
        if xs_id:
            return xs_id

        exchange_space_id, _ = self.rr.create(exchange_space)
        self.rr.create_association(org_id, PRED.hasExchangeSpace, exchange_space_id)

        self.container.ex_manager.create_xs(exchange_space.name)
        
        return exchange_space_id
    
    def _get_org_exchange_space(self, org_id, exchange_space_name):
        """Returns the XS id of an XS with given name, associated with an Org"""
        xs_objs, _ = self.rr.find_objects(subject=org_id, predicate=PRED.hasExchangeSpace, id_only=False)
        for xs in xs_objs:
            if xs.name == exchange_space_name:
                return xs._id
        
    def update_exchange_space(self, exchange_space=None):
        """Updates an existing Exchange Space resource with data passed in as a parameter.
        """
        self._validate_resource_obj("exchange_space", exchange_space, RT.ExchangeSpace, checks="id,name")

        self.rr.update(exchange_space)

    def read_exchange_space(self, exchange_space_id=''):
        """Returns an Exchange Space resource for the provided exchange space id.
        """
        exchange_space_obj = self._validate_resource_id("exchange_space_id", exchange_space_id, RT.ExchangeSpace)

        return exchange_space_obj

    def delete_exchange_space(self, exchange_space_id=''):
        """Deletes an existing exchange space resource for the provided id.
        """
        exchange_space_obj = self._validate_resource_id("exchange_space_id", exchange_space_id, RT.ExchangeSpace)

        # delete XS now
        self.rr.delete(exchange_space_id)

        # call container API to delete
        xs = self.container.ex_manager.create_xs(exchange_space_obj.name, declare=False)
        self.container.ex_manager.delete_xs(xs)

    # -------------------------------------------------------------------------
    # ExchangeName management

    def declare_exchange_name(self, exchange_name=None, exchange_space_id=''):
        """Create an Exchange Name resource resource
        """
        self._validate_resource_obj("exchange_name", exchange_name, RT.ExchangeName, checks="noid,name")
        exchange_space_obj = self._validate_resource_id("exchange_space_id", exchange_space_id, RT.ExchangeSpace)

        if exchange_name.xn_type not in self.EX_NAME_TYPES:
            raise BadRequest("Unknown exchange name type: %s" % exchange_name.xn_type)

        xns, _ = self.rr.find_objects(exchange_space_id, PRED.hasExchangeName, id_only=False)
        exchange_name_id = None
        for xn in xns:
            if xn.name == exchange_name.name and xn.xn_type == exchange_name.xn_type:
                exchange_name_id = xn._id

        exchange_space = self.read_exchange_space(exchange_space_id)
        if not exchange_name_id:
            exchange_name_id, _ = self.rr.create(exchange_name)
            self.rr.create_association(exchange_space_id, PRED.hasExchangeName, exchange_name_id)

        # Call container API
        xs = self.container.ex_manager.create_xs(exchange_space.name)
        self.container.ex_manager._create_xn(exchange_name.xn_type, exchange_name.name, xs)

        return exchange_name_id  #QUestion - is this the correct canonical name?

    def undeclare_exchange_name(self, canonical_name='', exchange_space_id=''):
        """Remove an exhange name resource
        """
        exchange_space_obj = self._validate_resource_id("exchange_space_id", exchange_space_id, RT.ExchangeSpace,
                                                        optional=True)
        # TODO: currently we are using the exchange_name's id as the canonical name and exchange_space_id is unused?
        exchange_name = self.rr.read(canonical_name)
        if not exchange_name:
            raise NotFound("ExchangeName with id %s does not exist" % canonical_name)

        exchange_name_id = exchange_name._id        # yes, this should be same, but let's make it look cleaner

        # Get associated XS first
        exchange_space_list, assocs = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangeName, exchange_name_id)
        if len(exchange_space_list) != 1:
            log.warn("ExchangeName %s has no associated Exchange Space" % exchange_name_id)

        exchange_space = exchange_space_list[0] if exchange_space_list else None

        # Remove association between itself and XS
        for assoc in assocs:
            self.rr.delete_association(assoc._id)

        # Remove XN
        self.rr.delete(exchange_name_id)

        # Call container API
        if exchange_space:
            xs = self.container.ex_manager.create_xs(exchange_space.name, declare=False)
            xn = self.container.ex_manager._create_xn(exchange_name.xn_type, exchange_name.name, xs, declare=False)
            self.container.ex_manager.delete_xn(xn)

    # -------------------------------------------------------------------------
    # ExchangePoint management

    def create_exchange_point(self, exchange_point=None, exchange_space_id=''):
        """Create an exchange point resource within the exchange space provided by the id.
        """
        self._validate_resource_obj("exchange_point", exchange_point, RT.ExchangePoint, checks="noid,name")
        exchange_space_obj = self._validate_resource_id("exchange_space_id", exchange_space_id, RT.ExchangeSpace)

        xs_xps, _ = self.rr.find_objects(subject=exchange_space_id, predicate=PRED.hasExchangePoint, id_only=False)
        exchange_point_id = None
        for xs_xp in xs_xps:
            if xs_xp.name == exchange_point.name and xs_xp.topology_type == exchange_point.topology_type:
                exchange_point_id = xs_xp._id

        if not exchange_point_id:
            exchange_point_id, _ver = self.rr.create(exchange_point)
            self.rr.create_association(exchange_space_id, PRED.hasExchangePoint, exchange_point_id)

        # call container API
        xs = self.container.ex_manager.create_xs(exchange_space_obj.name)
        self.container.ex_manager.create_xp(exchange_point.name, xs, xptype=exchange_point.topology_type)

        return exchange_point_id

    def update_exchange_point(self, exchange_point=None):
        """Update an existing exchange point resource.
        """
        self._validate_resource_obj("exchange_point", exchange_point, RT.ExchangePoint, checks="id")

        self.rr.update(exchange_point)

    def read_exchange_point(self, exchange_point_id=''):
        """Return an existing exchange point resource.
        """
        exchange_point_obj = self._validate_resource_id("exchange_point_id", exchange_point_id, RT.ExchangePoint)

        return exchange_point_obj

    def delete_exchange_point(self, exchange_point_id=''):
        """Delete an existing exchange point resource.
        """
        exchange_point_obj = self._validate_resource_id("exchange_point_id", exchange_point_id, RT.ExchangePoint)

        # get associated XS first
        exchange_space_list, assoc_list = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangePoint, exchange_point_id)
        if len(exchange_space_list) != 1:
            log.warn("ExchangePoint %s has no associated ExchangeSpace" % exchange_point_id)

        exchange_space = exchange_space_list[0] if exchange_space_list else None

        # delete association to XS
        for assoc in assoc_list:
            self.rr.delete_association(assoc._id)

        # delete from RR
        self.rr.delete(exchange_point_id)

        # call container API
        if exchange_space:
            xs = self.container.ex_manager.create_xs(exchange_space.name, declare=False)
            xp = self.container.ex_manager.create_xp(exchange_point_obj.name, xs, xptype=exchange_point_obj.topology_type, declare=False)
            self.container.ex_manager.delete_xp(xp)

    # -------------------------------------------------------------------------
    # ExchangeBroker management

    def create_exchange_broker(self, exchange_broker=None):
        """Creates an exchange broker resource
        """
        self._validate_resource_obj("exchange_broker", exchange_broker, RT.ExchangeBroker, checks="noid,name")

        xbs, _ = self.rr.find_resources(RT.ExchangeBroker, name=exchange_broker.name)
        if xbs:
            return xbs[0]._id

        exchange_broker_id, _ = self.rr.create(exchange_broker)
        return exchange_broker_id

    def update_exchange_broker(self, exchange_broker=None):
        """Updates an existing exchange broker resource.
        """
        self._validate_resource_obj("exchange_broker", exchange_broker, RT.ExchangeBroker, checks="id")

        self.rr.update(exchange_broker)

    def read_exchange_broker(self, exchange_broker_id=''):
        """Returns an existing exchange broker resource.
        """
        exchange_broker_obj = self._validate_resource_id("exchange_broker_id", exchange_broker_id, RT.ExchangeBroker)

        return exchange_broker_obj

    def delete_exchange_broker(self, exchange_broker_id=''):
        """Deletes an existing exchange broker resource.
        """
        exchange_broker_obj = self._validate_resource_id("exchange_broker_id", exchange_broker_id, RT.ExchangeBroker)

        self.rr.delete(exchange_broker_id)

    def add_exchange_space_to_exchange_broker(self, exchange_space_id='', exchange_broker_id=''):
        """Adds an exchange space to an exchange broker.
        """
        exchange_space_obj = self._validate_resource_id("exchange_space_id", exchange_space_id, RT.ExchangeSpace)
        exchange_broker_obj = self._validate_resource_id("exchange_broker_id", exchange_broker_id, RT.ExchangeBroker)

        assocs = self.rr.find_associations(exchange_space_id, PRED.hasExchangeBroker, exchange_broker_id, id_only=True)
        if assocs:
            raise BadRequest("ExchangeSpace already present on ExchangeBroker")

        self.rr.create_association(exchange_space_id, PRED.hasExchangeBroker, exchange_broker_id)

    def remove_exchange_space_from_exchange_broker(self, exchange_space_id='', exchange_broker_id=''):
        """Removes an exchange space from an exchange broker.
        """
        exchange_space_obj = self._validate_resource_id("exchange_space_id", exchange_space_id, RT.ExchangeSpace)
        exchange_broker_obj = self._validate_resource_id("exchange_broker_id", exchange_broker_id, RT.ExchangeBroker)

        assocs = self.rr.find_associations(exchange_space_id, PRED.hasExchangeBroker, exchange_broker_id, id_only=True)
        if not assocs:
            raise BadRequest("ExchangeSpace not present on ExchangeBroker")
        for assoc in assocs:
            self.rr.delete_association(assoc)

    # -------------------------------------------------------------------------
    # Misc

    def call_management(self, url='', method=''):
        """Makes a call to the RabbitMQ Management HTTP API
        """
        return self.container.ex_manager._make_management_call(url, method=method)
