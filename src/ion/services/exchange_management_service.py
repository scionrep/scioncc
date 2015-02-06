#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject, log, RT, PRED, Conflict, Inconsistent, NotFound, BadRequest
#from pyon.ion.exchange import ExchangeSpace, ExchangeName, ExchangePoint

from interface.services.core.iexchange_management_service import BaseExchangeManagementService


class ExchangeManagementService(BaseExchangeManagementService):
    """
    The Exchange Management Service is the service that manages the Exchange and its associated
    resources, such as ExchangeSpaces, Names, Points and Brokers.
    """
    EX_TYPE_MAP = {'XN_SERVICE': 'service',
                   'XN_PROCESS': 'process',
                   'XN_QUEUE': 'queue'}

    def create_exchange_space(self, exchange_space=None, org_id=''):
        """Creates an Exchange Space distributed resource from the parameter exchange_space object.
        """
        log.debug("create_exchange_space(%s, org_id=%s)", exchange_space, org_id)
        org_obj = self._validate_resource_id("org_id", org_id, RT.Org)
        self._validate_resource_obj("exchange_space", exchange_space, RT.ExchangeSpace)

        xs_objs, _ = self.container.resource_registry.find_objects(subject=org_id, predicate=PRED.hasExchangeSpace, id_only=False)
        for xs in xs_objs:
            if xs.name == exchange_space.name:
                return xs._id

        exchange_space_id, _ = self.container.resource_registry.create(exchange_space)
        self.container.resource_registry.create_association(org_id, PRED.hasExchangeSpace, exchange_space_id)

#        if exchange_space.name == "ioncore":
#            # Bottom turtle initialization - what's different here?

        self.container.ex_manager.create_xs(exchange_space.name)
        
        return exchange_space_id

    def update_exchange_space(self, exchange_space=None):
        """Updates an existing Exchange Space resource with data passed in as a parameter.
        """
        self._validate_resource_obj("exchange_space", exchange_space, RT.ExchangeSpace)

        self.container.resource_registry.update(exchange_space)

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
        self.container.resource_registry.delete(exchange_space_id)

        # call container API to delete
        xs = self.container.ex_manager.create_xs(exchange_space_obj.name, declare=False)
        self.container.ex_manager.delete_xs(xs)

    def find_exchange_spaces(self, filters=None):
        """Returns a list of Exchange Space resources for the given Resource Filter.
        """
        raise NotImplementedError()

    def declare_exchange_name(self, exchange_name=None, exchange_space_id=''):
        """Create an Exchange Name resource resource
        """
        self._validate_resource_obj("exchange_name", exchange_name, RT.ExchangeName)
        exchange_space_obj = self._validate_resource_id("exchange_space_id", exchange_space_id, RT.ExchangeSpace)

        # get xntype and translate
        # @TODO should we just consolidate these to be the same?
        if exchange_name.xn_type not in self.EX_TYPE_MAP:
            raise BadRequest("Unknown exchange name type: %s" % exchange_name.xn_type)

        xns, _ = self.container.resource_registry.find_objects(exchange_space_id, PRED.hasExchangeName, id_only=False)
        exchange_name_id = None
        for xn in xns:
            if xn.name == exchange_name.name and xn.xn_type == exchange_name.xn_type:
                exchange_name_id = xn._id


        xntype = self.EX_TYPE_MAP[exchange_name.xn_type]

        exchange_space = self.read_exchange_space(exchange_space_id)
        if not exchange_name_id:
            exchange_name_id, _ = self.container.resource_registry.create(exchange_name)

            aid = self.container.resource_registry.create_association(exchange_space_id, PRED.hasExchangeName, exchange_name_id)

        # call container API
        xs = self.container.ex_manager.create_xs(exchange_space.name, use_ems=False)
        self.container.ex_manager._create_xn(xntype, exchange_name.name, xs, use_ems=False)

        return exchange_name_id  #QUestion - is this the correct canonical name?

    def undeclare_exchange_name(self, canonical_name='', exchange_space_id=''):
        """Remove an exhange name resource

        @param canonical_name    str
        @param exchange_space_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        # @TODO: currently we are using the exchange_name's id as the canonical name
        # and exchange_space_id is unused?
        exchange_name = self.container.resource_registry.read(canonical_name)
        if not exchange_name:
            raise NotFound("Exchange Name with id %s does not exist" % canonical_name)

        exchange_name_id = exchange_name._id        # yes, this should be same, but let's make it look cleaner

        # get associated XS first
        exchange_space_list, assoc_list = self.container.resource_registry.find_subjects(RT.ExchangeSpace, PRED.hasExchangeName, exchange_name_id)
        if not len(exchange_space_list) == 1:
            raise NotFound("Associated Exchange Space to Exchange Name %s does not exist" % exchange_name_id)

        exchange_space = exchange_space_list[0]

        # remove association between itself and XS
        _, assocs = self.container.resource_registry.find_subjects(RT.ExchangeSpace, PRED.hasExchangeName, exchange_name_id, id_only=True)
        for assoc in assocs:
            self.container.resource_registry.delete_association(assoc._id)

        # remove XN
        self.container.resource_registry.delete(exchange_name_id)

        # call container API
        xntype = self.EX_TYPE_MAP[exchange_name.xn_type]
        xs = self.container.ex_manager.create_xs(exchange_space.name, use_ems=False, declare=False)
        xn = self.container.ex_manager._create_xn(xntype, exchange_name.name, xs, use_ems=False, declare=False)
        self.container.ex_manager.delete_xn(xn, use_ems=False)

    def find_exchange_names(self, filters=None):
        """Returns a list of exchange name resources for the given resource filter.
        """
        raise NotImplementedError()

    def create_exchange_point(self, exchange_point=None, exchange_space_id=''):
        """Create an exchange point resource within the exchange space provided by the id.
        """

        xs_xps, assocs = self.container.resource_registry.find_objects(subject=exchange_space_id, predicate=PRED.hasExchangePoint, id_only=False)
        exchange_point_id = None
        for xs_xp in xs_xps:
            if xs_xp.name == exchange_point.name and xs_xp.topology_type == exchange_point.topology_type:
                exchange_point_id = xs_xp._id


        exchange_space          = self.read_exchange_space(exchange_space_id)
        if not exchange_point_id:
            exchange_point_id, _ver = self.container.resource_registry.create(exchange_point)

            self.container.resource_registry.create_association(exchange_space_id, PRED.hasExchangePoint, exchange_point_id)

        # call container API
        xs = self.container.ex_manager.create_xs(exchange_space.name, use_ems=False)
        self.container.ex_manager.create_xp(exchange_point.name, xs, xptype=exchange_point.topology_type, use_ems=False)

        return exchange_point_id


    def update_exchange_point(self, exchange_point=None):
        """Update an existing exchange point resource.

        @param exchange_point    ExchangePoint
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        self.container.resource_registry.update(exchange_point)


    def read_exchange_point(self, exchange_point_id=''):
        """Return an existing exchange point resource.

        @param exchange_point_id    str
        @retval exchange_point    ExchangePoint
        @throws NotFound    object with specified id does not exist
        """
        exchange_point = self.container.resource_registry.read(exchange_point_id)
        if not exchange_point:
            raise NotFound("Exchange Point %s does not exist" % exchange_point_id)
        return exchange_point

    def delete_exchange_point(self, exchange_point_id=''):
        """Delete an existing exchange point resource.

        @param exchange_point_id    str
        @throws NotFound    object with specified id does not exist
        """
        exchange_point = self.container.resource_registry.read(exchange_point_id)
        if not exchange_point:
            raise NotFound("Exchange Point %s does not exist" % exchange_point_id)

        # get associated XS first
        exchange_space_list, assoc_list = self.container.resource_registry.find_subjects(RT.ExchangeSpace, PRED.hasExchangePoint, exchange_point_id)
        if not len(exchange_space_list) == 1:
            raise NotFound("Associated Exchange Space to Exchange Point %s does not exist" % exchange_point_id)

        exchange_space = exchange_space_list[0]

        # delete association to XS
        for assoc in assoc_list:
            self.container.resource_registry.delete_association(assoc._id)

        # delete from RR
        self.container.resource_registry.delete(exchange_point_id)

        # call container API
        xs = self.container.ex_manager.create_xs(exchange_space.name, use_ems=False, declare=False)
        xp = self.container.ex_manager.create_xp(exchange_point.name, xs, xptype=exchange_point.topology_type, use_ems=False, declare=False)
        self.container.ex_manager.delete_xp(xp, use_ems=False)

    def find_exchange_points(self, filters=None):
        """Returns a list of exchange point resources for the provided resource filter.

        @param filters    ResourceFilter
        @retval exchange_point_list    []
        """
        raise NotImplementedError()


    def create_exchange_broker(self, exchange_broker=None):
        """Creates an exchange broker resource

        @param exchange_broker    ExchangeBroker
        @retval exchange_broker_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        xbs, _ = self.container.resource_registry.find_resources(RT.ExchangeBroker)
        for xb in xbs:
            if xb.name == exchange_broker.name:
                return xb._id

        exchange_broker_id, _ver = self.container.resource_registry.create(exchange_broker)
        return exchange_broker_id

    def update_exchange_broker(self, exchange_broker=None):
        """Updates an existing exchange broker resource.

        @param exchange_broker    ExchangeBroker
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        self.container.resource_registry.update(exchange_broker)

    def read_exchange_broker(self, exchange_broker_id=''):
        """Returns an existing exchange broker resource.

        @param exchange_broker_id    str
        @retval exchange_broker    ExchangeBroker
        @throws NotFound    object with specified id does not exist
        """
        exchange_broker = self.container.resource_registry.read(exchange_broker_id)
        if not exchange_broker:
            raise NotFound("Exchange Broker %s does not exist" % exchange_broker_id)
        return exchange_broker

    def delete_exchange_broker(self, exchange_broker_id=''):
        """Deletes an existing exchange broker resource.

        @param exchange_broker_id    str
        @throws NotFound    object with specified id does not exist
        """
        exchange_broker = self.container.resource_registry.read(exchange_broker_id)
        if not exchange_broker:
            raise NotFound("Exchange Broker %s does not exist" % exchange_broker_id)
        self.container.resource_registry.delete(exchange_broker_id)

    def find_exchange_broker(self, filters=None):
        """Returns a list of exchange broker resources for the provided resource filter.

        @param filters    ResourceFilter
        @retval exchange_broker_list    []
        """
        raise NotImplementedError()

    def call_management(self, url='', method=''):
        """Makes a call to the RabbitMQ Management HTTP API

        @param url    str
        @param method    str
        @retval content    dict
        @throws Timeout    the call to the management API tiemed out
        @throws ServiceUnavailable    a connection error occured to the management API
        @throws ServerError    the management API responded with an HTTP error, or any other issue
        """
        return self.container.ex_manager._make_management_call(url, method=method, use_ems=False)
