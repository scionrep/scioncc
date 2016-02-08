
import numpy as np

from pyon.public import log, get_ion_ts

from interface.objects import DataPacket


class DataPacketBuilder(object):
    def __init__(self):
        pass

    @classmethod
    def build_packet_from_samples(cls, samples):
        data = None
        new_packet = DataPacket(ts_created=get_ion_ts(), data=data)
        return new_packet