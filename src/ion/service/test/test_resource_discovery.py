#!/usr/bin/env python

"""Integration and Unit tests for resource management service """

__author__ = 'Luke Campbell <LCampbell@ASAScience.com>, Michael Meisinger'

from unittest import SkipTest
from nose.plugins.attrib import attr
from mock import Mock, patch, sentinel

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import PRED, CFG, RT, OT, LCS, BadRequest, NotFound, IonObject, DotDict, ResourceQuery, EventQuery, log

from ion.service.resource_management_service import ResourceManagementService
from ion.util.geo_utils import GeoUtils
from ion.util.testing_utils import create_dummy_resources, create_dummy_events

from interface.services.core.iresource_management_service import ResourceManagementServiceClient

from interface.objects import GeospatialBounds, TemporalBounds, View, CustomAttribute, GeospatialLocation, TestInstrument, TestDataset


@attr('INT', group='core')
class ResourceQueryTest(IonIntegrationTestCase):
    """Tests search in a somewhat integration environment. Only a container and a
    ResourceManagementService instance but no service deployment and process"""

    def setUp(self):
        self._start_container()

        self.discovery = ResourceManagementService()
        self.discovery.container = self.container
        self.discovery.on_init()

        self.rr = self.container.resource_registry

    def _geopt(self, x1, y1):
        return GeospatialLocation(latitude=float(x1), longitude=float(y1))

    def _geobb(self, x1, y1, x2=None, y2=None, z1=0.0, z2=None):
        if x2 is None: x2 = x1
        if y2 is None: y2 = y1
        if z2 is None: z2 = z1
        return GeospatialBounds(geospatial_latitude_limit_north=float(y2),
                                geospatial_latitude_limit_south=float(y1),
                                geospatial_longitude_limit_west=float(x1),
                                geospatial_longitude_limit_east=float(x2),
                                geospatial_vertical_min=float(z1),
                                geospatial_vertical_max=float(z2))

    def _temprng(self, t1="", t2=None):
        if t2 is None: t2 = t1
        return TemporalBounds(start_datetime=str(t1), end_datetime=str(t2))


    def _geodp(self, x1, y1, x2=None, y2=None, z1=0.0, z2=None, t1="", t2=None):
        if x2 is None: x2 = x1
        if y2 is None: y2 = y1
        if z2 is None: z2 = z1
        if t2 is None: t2 = t1
        bounds = self._geobb(x1, y1, x2, y2, z1, z2)
        attrs = dict(location=GeoUtils.calc_geospatial_point_center(bounds, return_location=True),
                     geospatial_bounds=bounds,
                     temporal_bounds=self._temprng(t1, t2))
        return attrs


    def test_basic_searching(self):
        t0 = 1363046400
        hour = 60*24
        day = 60*60*24
        resources = [
            ("ID1", TestInstrument(name='sonobuoy1', firmware_version='A1')),
            ("ID2", TestInstrument(name='sonobuoy2', firmware_version='A2')),
            ("ID3", TestInstrument(name='sonobuoy3', firmware_version='A3')),

            ("DP1", TestDataset(name='testData1', **self._geodp(5, 5, 15, 15, 0, 100, t0, t0+day))),
            ("DP2", TestDataset(name='testData2', **self._geodp(25, 5, 35, 15, 0, 100, t0+hour+day, t0+2*day))),
            ("DP3", TestDataset(name='testData3', **self._geodp(30, 10, 40, 20, 50, 200, t0+100*day, t0+110*day))),
            ("DP4", TestDataset(name='testData4', **self._geodp(30, 5, 32, 10, 5, 20, t0+100*day, t0+110*day))),
        ]
        # create a large range of resources to test skip(offset)
        for i in range(20):
            resources.append(("INS%03d" % i, TestInstrument(name='range%03d' % i)))
        res_by_alias = {}
        for (alias, resource) in resources:
            rid,_ = self.rr.create(resource)
            res_by_alias[alias] = rid
        #self._breakpoint(locals(), globals())

        # ----------------------------------------------------
        # Resource attribute search

        # Resource attribute equals
        # Variant 1: Test via query DSL expression
        query_str = "{'and': [], 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A2'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 1)
        self.assertIsInstance(result[0], TestInstrument)
        self.assertTrue(result[0].name == 'sonobuoy2')
        self.assertTrue(result[0].firmware_version == 'A2')

        # Variant 2: Test the query expression
        query_str = """{'QUERYEXP': 'qexp_v1.0',
            'query_args': {'datastore': 'resources', 'id_only': False, 'limit': 0, 'profile': 'RESOURCES', 'skip': 0},
            'where': ['xop:attilike', ('firmware_version', 'A2')],
            'order_by': {}}"""
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 1)
        self.assertIsInstance(result[0], TestInstrument)
        self.assertTrue(result[0].name == 'sonobuoy2')
        self.assertTrue(result[0].firmware_version == 'A2')


        result = self.discovery.query(query_obj, id_only=False, search_args=dict(attribute_filter=["firmware_version"]))
        self.assertTrue(all(isinstance(eo, dict) for eo in result))
        self.assertTrue(all("firmware_version" in eo for eo in result))
        self.assertTrue(all(len(eo) <= 4 for eo in result))

        # Resource attribute match
        query_str = "{'and': [], 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A*'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 3)

        # Resource attribute match with limit
        query_str = "{'and': [], 'limit': 2, 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A*'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 2)

        # Resource attribute match with limit and skip (offset)

        # -- limit 1 without skip (using Discovery Intermediate Format)
        query_str = "{'and': [], 'limit': 1, 'or': [], 'query': {'field': 'name', 'index': 'resources_index', 'value': 'range*'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        # -- limit 1 with skip (using Discovery Intermediate Format)
        query_str = "{'and': [], 'limit': 1, 'skip': 10, 'or': [], 'query': {'field': 'name', 'index': 'resources_index', 'value': 'range*'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=False)
        # check same length and not equal (one uses SKIP 100, other doesn't)
        self.assertEquals(len(result), len(result1))
        self.assertNotEquals(result, result1)

        # Resource attribute match only count (results should return single value, a count of available results)
        search_args_str = "{'count': True}"
        search_args = eval(search_args_str)
        query_str = "{'and': [], 'limit': 2, 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A*'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False, search_args=search_args)
        self.assertEquals(len(result), 1)

        # ----------------------------------------------------
        # Geospatial search

        # Geospatial search - query bbox fully overlaps

        # Note that in Discovery intermediate format top_left=x1,y2 and bottom_right=x2,y1 contrary to naming
        query_str = "{'and': [], 'or': [], 'query': {'top_left': [0.0, 20.0], 'bottom_right': [20.0, 0.0], 'field': 'index_location', 'index': 'resources_index'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result1), 1)
        #for dp in ["DP1", "DP2", "DP3", "DP4"]:
        #    self.assertIn(res_by_alias[dp], result)

        # Geospatial bbox operators - overlaps (this is the default and should be the same as above)
        query_str = "{'and': [], 'or': [], 'query': {'top_left': [0.0, 20.0], 'bottom_right': [20.0, 0.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'overlaps'}}"
        query_obj = eval(query_str)
        result2 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result1), len(result2))
        self.assertEquals(result1, result2)

        # Geospatial bbox operators - contains (the resource contains the query)
        query_str = "{'and': [], 'or': [], 'query': {'top_left': [0.0, 20.0], 'bottom_right': [20.0, 0.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result3 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 0)

        query_str = "{'and': [], 'or': [], 'query': {'top_left': [8.0, 11.0], 'bottom_right': [12.0, 9.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result3 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 1)

        # Geospatial bbox operators - within (the resource with the query)
        query_str = "{'and': [], 'or': [], 'query': {'top_left': [0.0, 20.0], 'bottom_right': [20.0, 0.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result3 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 1)

        query_str = "{'and': [], 'or': [], 'query': {'top_left': [15.0, 5.0], 'bottom_right': [5.0, 15.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result3 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 1)

        query_str = "{'and': [], 'or': [], 'query': {'top_left': [14.0, 5.0], 'bottom_right': [5.0, 15.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result3 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 0)

        # Geospatial - WKT (a box 4,4 to 4,14 to 14,14 to 14,4, to 4,4 overlaps DP1 but is not contained by it or does not have it within)
        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POLYGON((4 4,4 14,14 14,14 4,4 4))', 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'overlaps'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 1)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POLYGON((4 4,4 14,14 14,14 4,4 4))', 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 0)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POLYGON((4 4,4 14,14 14,14 4,4 4))', 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 0)

        # -- with buffer (eg. point with radius CIRCLE)
        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POINT(10.0 10.0)', 'buffer': 1.0, 'field': 'geospatial_point_center', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 1)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POINT(10.0 10.0)', 'buffer': 1.0, 'field': 'geospatial_point_center', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 0)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POINT(10.0 10.0)', 'buffer': '15000m', 'field': 'geospatial_point_center', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 1)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POINT(10.0 10.0)', 'buffer': '15000m', 'field': 'geospatial_point_center', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 0)


        # ----------------------------------------------------
        # Vertical search

        query_str = "{'and': [], 'or': [], 'query': {'field': 'geospatial_bounds', 'index': 'resources_index', 'vertical_bounds': {'from': 0.0, 'to': 500.0}, 'cmpop': 'overlaps'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=True)
        self.assertGreaterEqual(len(result), 4)
        for dp in ["DP1", "DP2", "DP3", "DP4"]:
            self.assertIn(res_by_alias[dp], result)

        query_str = "{'and': [], 'or': [], 'query': {'field': 'geospatial_bounds', 'index': 'resources_index', 'vertical_bounds': {'from': 1.0, 'to': 2.0}, 'cmpop': 'overlaps'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=True)
        self.assertEquals(len(result1), 2)

        query_str = "{'and': [], 'or': [], 'query': {'field': 'geospatial_bounds', 'index': 'resources_index', 'vertical_bounds': {'from': 110.0, 'to': 120.0}, 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=True)
        self.assertEquals(len(result1), 1)
        self.assertEquals(res_by_alias["DP3"], result1[0])

        query_str = "{'and': [], 'or': [], 'query': {'field': 'geospatial_bounds', 'index': 'resources_index', 'vertical_bounds': {'from': 5.0, 'to': 30.0}, 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=True)
        self.assertEquals(len(result1), 1)
        self.assertEquals(res_by_alias["DP4"], result1[0])

        # ----------------------------------------------------
        # Temporal search

        # search_string = "search 'nominal_datetime' timebounds from '%s' to '%s' from 'resources_index'" %('2013-03-12','2013-03-19')
        # result = self.discovery.parse(search_string, id_only=True)
        # self.assertEquals(len(result), 2)
        # for dp in ["DP1", "DP2"]:
        #     self.assertIn(res_by_alias[dp], result)
        #
        # search_string = "search 'nominal_datetime' timebounds from '%s' to '%s' from 'resources_index'" %('2013-03-12','2013-11-19')
        # result = self.discovery.parse(search_string, id_only=True)
        # self.assertEquals(len(result), 4)
        # for dp in ["DP1", "DP2", "DP3", "DP4"]:
        #     self.assertIn(res_by_alias[dp], result)
        #
        # search_string = "search 'nominal_datetime' timebounds from '%s' to '%s' from 'resources_index'" %('2013-03-12','2013-03-13')
        # result = self.discovery.parse(search_string, id_only=True)
        # self.assertEquals(len(result), 1)
        # for dp in ["DP1"]:
        #     self.assertIn(res_by_alias[dp], result)

    def test_event_search(self):
        from interface.objects import ResourceOperatorEvent, ResourceCommandEvent
        t0 = 136304640000

        events = [
            ("RME1", ResourceCommandEvent(origin="O1", origin_type="OT1", sub_type="ST1", ts_created=str(t0))),
            ("RME2", ResourceCommandEvent(origin="O2", origin_type="OT1", sub_type="ST2", ts_created=str(t0+1))),
            ("RME3", ResourceCommandEvent(origin="O2", origin_type="OT2", sub_type="ST3", ts_created=str(t0+2))),

            ("RLE1", ResourceOperatorEvent(origin="O1", origin_type="OT3", sub_type="ST4", ts_created=str(t0+3))),
            ("RLE2", ResourceOperatorEvent(origin="O3", origin_type="OT3", sub_type="ST5", ts_created=str(t0+4))),
            ("RLE3", ResourceOperatorEvent(origin="O3", origin_type="OT2", sub_type="ST6", ts_created=str(t0+5))),

        ]
        ev_by_alias = {}
        for (alias, event) in events:
            evid = self.container.event_repository.put_event(event)
            ev_by_alias[alias] = evid

        # ----------------------------------------------------

        #raise self.SkipTest("Translate to other query syntax")

        query_str = "{'and': [], 'or': [], 'query': {'field': 'origin', 'index': 'events_index', 'value': 'O1'}}"
        result = self.discovery.query(eval(query_str), id_only=False)
        self.assertEquals(len(result), 2)

        query_str = "{'and': [], 'or': [], 'query': {'field': 'origin_type', 'index': 'events_index', 'value': 'OT2'}}"
        result = self.discovery.query(eval(query_str), id_only=False)
        self.assertEquals(len(result), 2)

        query_str = "{'and': [], 'or': [], 'query': {'field': 'sub_type', 'index': 'events_index', 'value': 'ST6'}}"
        result = self.discovery.query(eval(query_str), id_only=False)
        self.assertEquals(len(result), 1)

        # search_string = "search 'ts_created' values from 136304640000 to 136304640000 from 'events_index'"
        # result = self.discovery.parse(search_string, id_only=False)
        # self.assertEquals(len(result), 1)
        #
        # search_string = "search 'type_' is 'ResourceCommandEvent' from 'events_index' order by 'ts_created'"
        # result = self.discovery.parse(search_string, id_only=False)
        # self.assertEquals(len(result), 3)


    def test_query_view(self):
        res_objs = [
            (IonObject(RT.ActorIdentity, name="Act1"), ),
            (IonObject(RT.ActorIdentity, name="Act2"), ),

            (IonObject(RT.TestInstrument, name="ID1", lcstate=LCS.DEPLOYED, firmware_version='A1'), "Act1"),
            (IonObject(RT.TestInstrument, name="ID2", lcstate=LCS.INTEGRATED, firmware_version='A2'), "Act2"),

            (IonObject(RT.TestPlatform, name="PD1"), ),
            (IonObject(RT.TestPlatform, name="PD2"), ),

            (IonObject(RT.TestSite, name="Site1", lcstate=LCS.DEPLOYED), ),
        ]
        assocs = [
            ("PD1", PRED.hasTestDevice, "ID1"),
            ("PD2", PRED.hasTestDevice, "ID2"),

        ]
        res_by_name = create_dummy_resources(res_objs, assocs)

        # ----------------------------------------------------
        # Resource attribute search

        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.TestInstrument))
        view_obj = View(name="All TestInstrument resources", view_definition=rq.get_query())
        view_id = self.discovery.create_view(view_obj)

        # TEST: View by ID
        result = self.discovery.query_view(view_id, id_only=False)
        self.assertEquals(len(result), 2)
        self.assertTrue(all(True for ro in result if ro.type_ == RT.TestInstrument))

        # TEST: View by Name
        result = self.discovery.query_view(view_name="All TestInstrument resources", id_only=False)
        self.assertEquals(len(result), 2)
        self.assertTrue(all(True for ro in result if ro.type_ == RT.TestInstrument))

        # TEST: View plus ext_query
        rq = ResourceQuery()
        rq.set_filter(rq.filter_name("ID1"))
        result = self.discovery.query_view(view_id, id_only=False, ext_query=rq.get_query())
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID1")

        # TEST: View with params (anonymous)
        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.TestInstrument),
                      rq.filter_attribute("firmware_version", "$(firmware_version)"))
        view_obj = View(name="TestInstrument resources with a specific firmware - parameterized",
                        view_definition=rq.get_query())
        view_id = self.discovery.create_view(view_obj)

        view_params = {"firmware_version": "A2"}
        result = self.discovery.query_view(view_id, id_only=False, search_args=view_params)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID2")

        # TEST: View with params (anonymous) - no values provided
        result = self.discovery.query_view(view_id, id_only=False)
        self.assertEquals(len(result), 0)

        # View with params (with definitions and defaults)
        view_param_def = [CustomAttribute(name="firmware_version",
                                          type="str",
                                          default="A1")]
        view_obj = View(name="TestInstrument resources with a specific firmware - parameterized with defaults",
                        view_definition=rq.get_query(),
                        view_parameters=view_param_def)
        view_id = self.discovery.create_view(view_obj)

        # TEST: Definition defaults
        result = self.discovery.query_view(view_id, id_only=False)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID1")

        # TEST: Parameterized values
        result = self.discovery.query_view(view_id, id_only=False, search_args=view_params)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID2")

        # TEST: Parameterized association query for resource owner
        rq = ResourceQuery()
        rq.set_filter(rq.filter_associated_from_object("$(owner)"))
        view_obj = View(name="Resources owned by actor - parameterized", view_definition=rq.get_query())
        view_id = self.discovery.create_view(view_obj)
        view_params = {"owner": res_by_name["Act2"]}
        result = self.discovery.query_view(view_id, id_only=False, search_args=view_params)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID2")

        # TEST: Parameterized association query for resource owner with parameter value
        view_params = {"owner": res_by_name["Act2"], "query_info": True}
        result = self.discovery.query_view(view_id, id_only=False, search_args=view_params)
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0].name, "ID2")
        self.assertIn("_query_info", result[1])
        self.assertIn("statement_sql", result[1])

        # TEST: Builtin views
        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.TestSite))
        result = self.discovery.query_view(view_name="resources_index", id_only=False, ext_query=rq.get_query())
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "Site1")


        # --- Events setup

        from interface.objects import ResourceOperatorEvent, ResourceCommandEvent
        t0 = 136304640000
        events = [
            ("RME1", ResourceCommandEvent(origin="O1", origin_type="OT1", sub_type="ST1", ts_created=str(t0))),
            ("RME2", ResourceCommandEvent(origin="O2", origin_type="OT1", sub_type="ST2", ts_created=str(t0+1))),
            ("RME3", ResourceCommandEvent(origin="O2", origin_type="OT2", sub_type="ST3", ts_created=str(t0+2))),

            ("RLE1", ResourceOperatorEvent(origin="O1", origin_type="OT3", sub_type="ST4", ts_created=str(t0+3))),
            ("RLE2", ResourceOperatorEvent(origin="O3", origin_type="OT3", sub_type="ST5", ts_created=str(t0+4))),
            ("RLE3", ResourceOperatorEvent(origin="O3", origin_type="OT2", sub_type="ST6", ts_created=str(t0+5))),

        ]
        ev_by_alias = create_dummy_events(events)

        # TEST: Event query with views
        eq = EventQuery()
        eq.set_filter(eq.filter_type(OT.ResourceCommandEvent))
        view_obj = View(name="All ResourceCommandEvent events", view_definition=eq.get_query())
        view_id = self.discovery.create_view(view_obj)
        result = self.discovery.query_view(view_id, id_only=False)
        self.assertEquals(len(result), 3)
        self.assertTrue(all(True for eo in result if eo.type_ == OT.ResourceCommandEvent))

        # TEST: Event query with views - stripped format
        result = self.discovery.query_view(view_id, id_only=False, search_args=dict(attribute_filter=["origin"]))
        self.assertEquals(len(result), 3)
        self.assertTrue(all(True for eo in result if isinstance(eo, dict)))
        self.assertTrue(all(True for eo in result if "origin" in eo))
        self.assertTrue(all(True for eo in result if len(eo) <= 4))

        # TEST: Builtin views
        eq = EventQuery()
        eq.set_filter(eq.filter_type(OT.ResourceCommandEvent))
        result = self.discovery.query_view(view_name="events_index", id_only=False, ext_query=eq.get_query())
        self.assertEquals(len(result), 3)

    def test_complex_queries(self):
        res_objs = [
            dict(res=IonObject(RT.ActorIdentity, name="Act1")),
            dict(res=IonObject(RT.ActorIdentity, name="Act2")),

            dict(res=IonObject(RT.Org, name="Org1"), act="Act1"),
            dict(res=IonObject(RT.TestSite, name="Obs1"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.TestSite, name="PS1"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.TestSite, name="PSC1"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.TestSite, name="IS1"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.TestDeviceModel, name="PM1", manufacturer="CGSN"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.TestDeviceModel, name="PMC1", manufacturer="Bluefin"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.TestDeviceModel, name="PM2", manufacturer="Webb"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.TestDeviceModel, name="IM1", manufacturer="SeaBird"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.TestDeviceModel, name="IM2", manufacturer="Teledyne"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.TestPlatform, name="PD1"), act="Act1", org="Org1", lcstate=LCS.DEPLOYED),
            dict(res=IonObject(RT.TestPlatform, name="PDC1"), act="Act1", org="Org1", lcstate=LCS.INTEGRATED),
            dict(res=IonObject(RT.TestInstrument, name="ID1", firmware_version='A1'), act="Act1", org="Org1", lcstate=LCS.DEPLOYED),
            dict(res=IonObject(RT.TestInstrument, name="ID2", firmware_version='A2'), act="Act1", org="Org1", lcstate=LCS.INTEGRATED),

            dict(res=IonObject(RT.Org, name="Org2"), act="Act2"),
            dict(res=IonObject(RT.TestSite, name="Obs2"), act="Act2", org="Org2"),
            dict(res=IonObject(RT.TestSite, name="PS2"), act="Act2", org="Org2"),
            dict(res=IonObject(RT.TestPlatform, name="PD2"), act="Act2", org="Org2"),
            dict(res=IonObject(RT.TestInstrument, name="ID3", lcstate=LCS.DEPLOYED, firmware_version='A3'), act="Act2", org="Org2"),
            dict(res=IonObject(RT.Stream, name="Stream1")),
        ]
        assocs = [
            ("Obs1", PRED.hasTestSite, "PS1"),
            ("PS1", PRED.hasTestSite, "PSC1"),
            ("PSC1", PRED.hasTestSite, "IS1"),
            ("PS1", PRED.hasTestDevice, "PD1"),
            ("PSC1", PRED.hasTestDevice, "PDC1"),
            ("IS1", PRED.hasTestDevice, "ID1"),
            ("PD1", PRED.hasTestDevice, "PDC1"),
            ("PDC1", PRED.hasTestDevice, "ID1"),

            ("PS1", PRED.hasTestModel, "PM1"),
            ("PSC1", PRED.hasTestModel, "PMC1"),
            ("IS1", PRED.hasTestModel, "IM1"),
            ("PD1", PRED.hasTestModel, "PM1"),
            ("PDC1", PRED.hasTestModel, "PMC1"),
            ("ID1", PRED.hasTestModel, "IM1"),
            ("PD2", PRED.hasTestModel, "PM2"),
            ("ID2", PRED.hasTestModel, "IM2"),

        ]
        res_by_name = create_dummy_resources(res_objs, assocs)

        log.info("TEST: Query for all resources owned by actor Act1")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_associated_from_object(res_by_name["Act1"], None, "hasOwner"))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 14)

        log.info("TEST: Query for all Site descendants of TestSite Obs1")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_object_descendants(res_by_name["Obs1"], [RT.TestSite, RT.TestSite], PRED.hasTestSite))
        result = self.discovery.query(rq.get_query(), id_only=False)
        # import pprint
        # pprint.pprint(rq.get_query())
        self.assertEquals(len(result), 3)

        log.info("TEST: Query for all resources belonging to Org Org1")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_associated_from_subject(res_by_name["Org1"], None, "hasResource"))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 13)

        log.info("TEST: Query for all resources belonging to Org Org1 AND of type TestInstrument")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_associated_from_subject(res_by_name["Org1"], None, "hasResource"),
                      rq.filter_type(RT.TestInstrument))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 2)

        log.info("TEST: Query for instruments whose platform parent has a name of PDC1")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.TestInstrument),
                      rq.filter_associated_from_subject(subject_type=RT.TestPlatform, predicate=PRED.hasTestDevice, target_filter=rq.filter_name("PDC1")))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID1")

        log.info("TEST: Query for instruments in Org1 whose platform parent has a specific attribute set")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.TestInstrument),
                      rq.filter_associated_from_subject(res_by_name["Org1"], None, "hasResource"),
                      rq.filter_associated_from_subject(subject_type=RT.TestPlatform, predicate=PRED.hasTestDevice, target_filter=rq.filter_name("PDC1")))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID1")

        log.info("TEST: Query for instruments in Org1 that are lcstate INTEGRATED")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.TestInstrument),
                      rq.filter_lcstate(LCS.INTEGRATED),
                      rq.filter_associated_from_subject(res_by_name["Org1"], None, "hasResource"))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID2")

        log.info("TEST: Query for instruments in Org1 that are lcstate INTEGRATED OR platforms in Org1 that are lcstate DEPLOYED")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_or(rq.filter_and(rq.filter_type(RT.TestInstrument),
                                                 rq.filter_lcstate(LCS.INTEGRATED)),
                                   rq.filter_and(rq.filter_type(RT.TestPlatform),
                                                 rq.filter_lcstate(LCS.DEPLOYED))),
                      rq.filter_associated_from_subject(res_by_name["Org1"], None, "hasResource"))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 2)
        #self.assertEquals(result[0].name, "ID2")

