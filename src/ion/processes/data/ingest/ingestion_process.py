""" Ingestion of data packets into datasets from streams. """

__author__ = 'Michael Meisinger'

from pyon.public import log, StandaloneProcess, BadRequest, CFG, StreamSubscriber, named_any

from interface.objects import StreamRoute, DataPacket

CONFIG_KEY = "process.ingestion_process"


class IngestionProcess(StandaloneProcess):

    def on_init(self):
        log.info("Ingestion starting")
        self.exchange_name = "ingestion_process"

        plugin_cls = CFG.get_safe(CONFIG_KEY + ".plugin")
        self.plugin = named_any(plugin_cls)(self)
        log.info("Started ingestion plugin '%s'", plugin_cls)

        self.persistence_format = CFG.get_safe(CONFIG_KEY + ".persist.persistence_format")
        if self.persistence_format == "hdf5":
            from ion.data.persist.hdf5_dataset import DatasetHDF5Persistence
            self.persistence_factory = DatasetHDF5Persistence.get_persistence
        else:
            raise BadRequest("Unknown persistence format: %s" % self.persistence_format)

        self.stream_sub = StreamSubscriber(process=self, exchange_name=self.exchange_name,
                                           callback=self.process_package)
        streams = CFG.get_safe(CONFIG_KEY + ".stream_subscriptions") or []
        for stream in streams:
            if isinstance(stream, list):
                stream = StreamRoute(exchange_point=stream[0], routing_key=stream[1])

            log.info("Ingestion subscribed to stream '%s'", stream)
            self.stream_sub.add_stream_subscription(stream)

        self.plugin.on_init()

        self.stream_sub.start()

    def on_quit(self):
        self.stream_sub.stop()

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
        persistence = self.persistence_factory(ds_info["dataset_id"], ds_info["schema"])

        persistence.require_dataset()
        persistence.extend_dataset(packet)


class IngestionPlugin(object):
    def __init__(self, ingestion_process):
        self.ingestion_process = ingestion_process
        self.rr = self.ingestion_process.container.resource_registry

    def on_init(self):
        pass

    def get_dataset_info(self, packet):
        """ Dataset info and schema retrieval """
        raise NotImplementedError("Must provide implementation")
