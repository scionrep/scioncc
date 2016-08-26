""" Ingestion of data packets into datasets from streams. """

__author__ = 'Michael Meisinger'

from pyon.public import log, StandaloneProcess, BadRequest, CFG, StreamSubscriber, named_any, get_safe

from interface.objects import StreamRoute, DataPacket

CONFIG_KEY = "process.ingestion_process"


class IngestionProcess(StandaloneProcess):

    def on_init(self):
        self.ingestion_profile = self.CFG.get_safe(CONFIG_KEY + ".ingestion_profile", "default")

        log.info("Ingestion starting using profile '%s'", self.ingestion_profile)
        self.exchange_name = "ingestion_process"

        self.ingestion_config = self.CFG.get_safe(CONFIG_KEY + ".profile_" + self.ingestion_profile) or {}
        if not self.ingestion_config:
            raise BadRequest("No config found for profile '%s'" % self.ingestion_profile)

        plugin_cls = get_safe(self.ingestion_config, "plugin")
        self.plugin = named_any(plugin_cls)(self)
        log.info("Started ingestion plugin '%s'", plugin_cls)

        self.persistence_formats = {}
        self.persistence_objects = {}
        self.default_persistence_format = get_safe(self.ingestion_config, "persist.persistence_format")
        self._require_persistence_layer(self.default_persistence_format)

        self.stream_sub = StreamSubscriber(process=self, exchange_name=self.exchange_name,
                                           callback=self.process_package)
        streams = get_safe(self.ingestion_config, "stream_subscriptions") or []
        for stream in streams:
            if isinstance(stream, list):
                stream = StreamRoute(exchange_point=stream[0], routing_key=stream[1])

            log.info("Ingestion subscribed to stream '%s'", stream)
            self.stream_sub.add_stream_subscription(stream)

        self.plugin.on_init()

        self.stream_sub.start()

    def on_quit(self):
        self.stream_sub.stop()

    def _require_persistence_layer(self, format_name):
        if format_name in self.persistence_formats:
            return self.persistence_formats[format_name]
        if format_name == "hdf5":
            from ion.data.persist.hdf5_dataset import DatasetHDF5Persistence
            persistence_factory = DatasetHDF5Persistence.get_persistence
            self.persistence_formats[format_name] = persistence_factory
        else:
            raise BadRequest("Unknown persistence format: %s" % format_name)
        return self.persistence_formats[format_name]

    def process_package(self, packet, route, stream):
        if not isinstance(packet, DataPacket):
            log.warn("Ingestion received a non DataPacket message")

        #print "INGEST", packet, route, stream
        try:
            ds_info = self.plugin.get_dataset_info(packet)

            self._persist_packet(packet, ds_info)
        except Exception as ex:
            log.exception("Error during ingestion")

    def _persist_packet(self, packet, ds_info):
        layer_format = ds_info["schema"]["attributes"].get("persistence", {}).get("format", self.default_persistence_format)
        ds_persistence = "%s_%s" % (layer_format, ds_info["dataset_id"])
        if ds_persistence in self.persistence_objects:
            persistence = self.persistence_objects[ds_persistence]
        else:
            persistence_factory = self._require_persistence_layer(layer_format)
            persistence = persistence_factory(ds_info["dataset_id"], ds_info["schema"], layer_format)
            self.persistence_objects[ds_persistence] = persistence

        persistence.require_dataset()
        persistence.extend_dataset(packet)


class IngestionPlugin(object):
    """
    Base class for application specific ingestion plugins. Their purpose is to resolve dataset identifiers
    to dataset definitions, e.g. by retrieving this information from a database.

    The same plugin instance is used all the time, so that this instance can cache lookup information.
    """

    def __init__(self, ingestion_process):
        self.ingestion_process = ingestion_process
        self.rr = self.ingestion_process.container.resource_registry

    def on_init(self):
        pass

    def get_dataset_info(self, packet):
        """ Dataset info and schema retrieval """
        raise NotImplementedError("Must provide implementation")
