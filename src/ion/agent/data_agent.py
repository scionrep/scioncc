
from gevent.event import Event

from pyon.public import BadRequest, EventPublisher, log, NotFound, OT, RT, get_safe
from pyon.util.async import spawn


from ion.agent.streaming_agent import StreamingAgent, AgentPlugin


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

    def on_stop_streaming(self):
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
                    log.info("Sample %s", sample)
            except Exception as ex:
                log.exception("Error in sampling greenlet")


class DataAgentPlugin(AgentPlugin):

    def acquire_samples(self, max_samples=0):
        return None
