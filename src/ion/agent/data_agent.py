""" Agent acquiring data from web or file data sources and streaming packets. """

__author__ = 'Michael Meisinger'

from gevent.event import Event

from pyon.public import BadRequest, EventPublisher, log, NotFound, OT, RT, get_safe
from pyon.util.async import spawn

from ion.agent.streaming_agent import StreamingAgent, AgentPlugin
from ion.data.packet.packet_builder import DataPacketBuilder

from interface.objects import DataPacket

class DataAgent(StreamingAgent):
    agent_plugin = None
    sampling_gl = None
    sampling_gl_quit = None
    sampling_interval = 5

    def on_connect(self, connect_args=None):
        pass

    def on_start_streaming(self, streaming_args=None):
        self.sampling_gl_quit = Event()
        self.sampling_interval = self.agent_config.get("sampling_interval", 5)
        self.sampling_gl = spawn(self._sample_data_loop, self.sampling_interval)
        if self.agent_plugin and hasattr(self.agent_plugin, 'on_start_streaming'):
            self.agent_plugin.on_start_streaming(streaming_args)

    def on_stop_streaming(self):
        if self.agent_plugin and hasattr(self.agent_plugin, 'on_stop_streaming'):
            self.agent_plugin.on_stop_streaming()
        self.sampling_gl_quit.set()
        self.sampling_gl.join(timeout=3)
        self.sampling_gl.kill()
        self.sampling_gl = None
        self.sampling_gl_quit = None

    def _sample_data_loop(self, sample_interval):
        while not self.sampling_gl_quit.wait(timeout=sample_interval):
            try:
                if self.agent_plugin:
                    sample = self.agent_plugin.acquire_samples()
                    if sample:
                        #log.info("Sample %s", sample)
                        packet = DataPacketBuilder.build_packet_from_samples(sample,
                                    resource_id=self.resource_id, stream_name=self.stream_name)
                        self.stream_pub.publish(packet)
            except Exception as ex:
                log.exception("Error in sampling greenlet")


class DataAgentPlugin(AgentPlugin):

    def acquire_samples(self, max_samples=0):
        return None
