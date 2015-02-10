#!/usr/bin/env python

"""Common datastore definitions"""

__author__ = 'Michael Meisinger'

from putil.logging import log

from pyon.core.exception import BadRequest
from pyon.util.containers import get_safe, named_any, DotDict


class DataStore(object):
    """
    Common definitions and base class for data stores
    """
    # Constants for common datastore names
    DS_RESOURCES = "resources"
    DS_OBJECTS = "objects"
    DS_EVENTS = "events"
    DS_DIRECTORY = DS_RESOURCES
    DS_STATE = "state"

    # Enumeration of index profiles for datastores
    DS_PROFILE_LIST = ['OBJECTS', 'RESOURCES', 'DIRECTORY', 'STATE', 'EVENTS', 'BASIC']
    DS_PROFILE = DotDict(zip(DS_PROFILE_LIST, DS_PROFILE_LIST))
    DS_PROFILE.lock()

    # Maps common datastore logical names to index profiles
    DS_PROFILE_MAPPING = {
        DS_RESOURCES: DS_PROFILE.RESOURCES,
        DS_OBJECTS: DS_PROFILE.OBJECTS,
        DS_EVENTS: DS_PROFILE.EVENTS,
        DS_STATE: DS_PROFILE.STATE,
        }

    def __init__(self, datastore_name=None, profile=None, config=None, container=None, scope=None, **kwargs):
        pass


class DatastoreFactory(object):
    """Helps to create instances of datastores"""

    DS_BASE = "base"    # A standalone variant                                    of
    DS_FULL = "full"    # A datastore that requires pyon initialization

    @classmethod
    def get_datastore(cls, datastore_name=None, variant=DS_BASE, config=None, container=None, profile=None, scope=None):
        #log.info("get_datastore(%s, variant=%s, profile=%s, scope=%s, config=%s)", datastore_name, variant, profile, scope, "")

        # Step 1: Get datastore server config
        if not config and container:
            config = container.CFG
        if config:
            if "container" in config:
                server_cfg = cls.get_server_config(config)
            else:
                server_cfg = config
                config = None

        if not server_cfg:
            raise BadRequest("No config available to determine datastore")

        # Step 2: Find type specific implementation class
        if config:
            server_types = get_safe(config, "container.datastore.server_types", None)
            if not server_types:
                # Some tests fudge the CFG - make it more lenient
                #raise BadRequest("Server types not configured!")
                variant_store = cls.get_datastore_class(server_cfg, variant=variant)

            else:
                server_type = server_cfg.get("type", "postgresql")
                type_cfg = server_types.get(server_type, None)
                if not type_cfg:
                    raise BadRequest("Server type '%s' not configured!" % server_type)

                variant_store = type_cfg.get(variant, cls.DS_BASE)
        else:
            # Fallback in case a server config was given (NOT NICE)
            variant_store = cls.get_datastore_class(server_cfg, variant=variant)


        # Step 3: Instantiate type specific implementation
        store_class = named_any(variant_store)
        profile = profile or DataStore.DS_PROFILE_MAPPING.get(datastore_name, DataStore.DS_PROFILE.BASIC)
        log.debug("get_datastore(%s, profile=%s, scope=%s, variant=%s) -> %s", datastore_name, profile, scope, variant, store_class.__name__)
        store = store_class(datastore_name=datastore_name, config=server_cfg, profile=profile, scope=scope)

        return store

    @classmethod
    def get_datastore_class(cls, server_cfg, variant=None):
        server_type = server_cfg.get('type', 'postgresql')
        if server_type == 'postgresql':
            store_cls = "pyon.datastore.postgresql.base_store.PostgresDataStore"
        else:
            raise BadRequest("Unknown datastore server type: %s" % server_type)
        return store_cls

    @classmethod
    def get_server_config(cls, config=None):
        default_server = get_safe(config, "container.datastore.default_server", "postgresql")

        server_cfg = get_safe(config, "server.%s" % default_server, None)
        if not server_cfg:
            # Support tests that mock out the CFG
            pg_cfg = get_safe(config, "server.postgresql", None)
            if pg_cfg:
                server_cfg = pg_cfg
            else:
                raise BadRequest("No datastore config available!")

        return server_cfg


# -------------------------------------------------------------------------
# Geospatial, temporal object utils

def get_obj_geospatial_point(doc, calculate=True):
    """Extracts a geospatial point (lat, lon, elev) from given object dict, by looking for an attribute with
    GeospatialIndex or GeospatialPoint or GeospatialLocation type or computing the center from a bounds
    """
    geo_center = None
    # TODO: Be more flexible about finding attributes with the right types
    if "location" in doc:
        geo_center = doc["location"]
    if "geospatial_point_center" in doc:
        geo_center = doc["geospatial_point_center"]
    if not geo_center and calculate:
        # Try to calculate center point from bounds
        present, geo_bounds = get_obj_geospatial_bounds(doc, calculate=False, return_geo_bounds=True)
        if present:
            try:
                from ion.util.geo_utils import GeoUtils
                geo_bounds_obj = DotDict(**geo_bounds)
                geo_center = GeoUtils.calc_geospatial_point_center(geo_bounds_obj)
            except Exception:
                log.exception("Could not calculate geospatial center point")
    if geo_center and isinstance(geo_center, dict):
        if "lat" in geo_center and "lon" in geo_center:
            lat, lon = geo_center.get("lat", 0), geo_center.get("lon", 0)
            if lat or lon:
                return True, (lat, lon, 0)
        elif "latitude" in geo_center and "longitude" in geo_center:
            lat, lon = geo_center.get("latitude", 0), geo_center.get("longitude", 0)
            elev = geo_center.get("elevation", 0)
            if lat or lon or elev:
                return True, (lat, lon, elev)
        elif "geospatial_latitude" in geo_center and "geospatial_longitude" in geo_center:
            lat, lon = geo_center.get("geospatial_latitude", 0), geo_center.get("geospatial_longitude", 0)
            elev = geo_center.get("geospatial_vertical_location", 0)
            if lat or lon:
                return True, (lat, lon, elev)
    return False, (0, 0, 0)


def get_obj_geospatial_bounds(doc, calculate=True, return_geo_bounds=False):
    """Extracts geospatial bounds (list of x,y coordinates) from given object dict, by looking for an
    attribute with GeospatialBounds type, or by computing from a geospatial point
    """
    geo_bounds = None
    if "geospatial_bounds" in doc:
        geo_bounds = doc["geospatial_bounds"]
    if "bounding_box" in doc:
        geo_bounds = doc["bounding_box"]
    if geo_bounds and isinstance(geo_bounds, dict):
        if "geospatial_longitude_limit_west" in geo_bounds and "geospatial_latitude_limit_south" in geo_bounds and \
            "geospatial_longitude_limit_east" in geo_bounds and "geospatial_latitude_limit_north" in geo_bounds:
            if return_geo_bounds:
                return True, geo_bounds
            try:
                x1 = float(geo_bounds["geospatial_longitude_limit_west"])
                y1 = float(geo_bounds["geospatial_latitude_limit_south"])
                x2 = float(geo_bounds["geospatial_longitude_limit_east"])
                y2 = float(geo_bounds["geospatial_latitude_limit_north"])
                if not any((x1, x2, y1, y2)):
                    return False, None
                return True, ((x1, y1), (x2, y1), (x2, y2), (x1, y2), (x1, y1))
            except ValueError as ve:
                log.warn("GeospatialBounds values not parseable %s: %s", geo_bounds, ve)
    if calculate:
        # Set bounds from center point
        present, (lat, lon, elev) = get_obj_geospatial_point(doc, False)
        if present:
            return True, ((lat, lon), )  # Polygon with 1 point

    return False, None

def get_obj_vertical_bounds(doc, calculate=True):
    """Extracts vertical bounds (min, max) from given object dict, by looking for an
    attribute with GeospatialBounds type, or by computing from a geospatial point
    """
    geo_bounds = None
    if "geospatial_bounds" in doc:
        geo_bounds = doc["geospatial_bounds"]
    if "vertical_range" in doc:
        geo_bounds = doc["vertical_range"]
    if geo_bounds and isinstance(geo_bounds, dict):
        if "geospatial_vertical_min" in geo_bounds and "geospatial_vertical_max" in geo_bounds:
            try:
                z1 = float(geo_bounds["geospatial_vertical_min"])
                z2 = float(geo_bounds["geospatial_vertical_max"])
                if not any((z1, z2)):
                    return False, (0, 0)
                return True, (z1, z2)
            except ValueError as ve:
                log.warn("GeospatialBounds vertical values not parseable %s: %s", geo_bounds, ve)
    if calculate:
        # Set bounds from center point
        present, (lat, lon, elev) = get_obj_geospatial_point(doc, False)
        if present:
            return True, (elev, elev)  # Point range

    return False, (0, 0)


def get_obj_temporal_bounds(doc):
    """Extracts a temporal bounds from given object dict, by looking for an attribute with TemporalBounds type"""
    temp_range = None
    if "temporal_range" in doc:
        temp_range = doc["temporal_range"]
    if temp_range and isinstance(temp_range, dict):
        if "start_datetime" in temp_range and "end_datetime" in temp_range:
            try:
                t1 = float(temp_range["start_datetime"])
                t2 = float(temp_range["end_datetime"])
                if not any((t1, t2)):
                    return False, (0, 0)
                return True, (t1, t2)
            except ValueError as ve:
                log.warn("TemporalBounds values not parseable %s: %s", temp_range, ve)
    return False, (0, 0)
