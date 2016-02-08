""" Support for controlling agents """


from pyon.public import log, RT, BadRequest, Container
from ion.core.process.proc_util import ProcessStateGate


class AgentControl(object):

    def __init__(self):
        self.process_id = None

    def launch_agent(self, resource_id, agent_type, agent_config):
        if agent_type == "data_agent":
            agent_mod, agent_cls = "ion.agent.data.data_agent", "DataAgent"
        elif agent_type == "streaming_agent":
            agent_mod, agent_cls = "ion.agent.data.streaming_agent", "StreamingAgent"
        elif agent_type == "instrument_agent":
            agent_mod, agent_cls = "ion.agent.data.streaming_agent", "StreamingAgent"
        else:
            raise BadRequest("Unknown agent type: %s" % agent_type)

        agent_config = agent_config.copy() if agent_config else {}
        agent_config["resource_id"] = resource_id
        config = dict(agent_config=agent_config)

        self.process_id = Container.instance.spawn_process(agent_type, agent_mod, agent_cls, config)
        return self.process_id

    def terminate_agent(self, config):
        Container.instance.terminate_process(self.process_id)
