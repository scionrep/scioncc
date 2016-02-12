""" Constructs data packet messages. """

__author__ = 'Michael Meisinger'

try:
    import numpy as np
except ImportError:
    np = None

from pyon.public import log, get_ion_ts

from interface.objects import DataPacket


class DataPacketBuilder(object):
    def __init__(self):
        pass

    #{'data': [[1454993060525L, 12.1]], 'cols': ['time', 'cpu_percent']}
    @classmethod
    def build_packet_from_samples(cls, samples, **kwargs):
        num_samples = len(samples["data"])
        dtype_parts = []
        for coldef in samples["cols"]:
            if coldef == "time":
                dtype_parts.append((coldef, "u8"))
            else:
                dtype_parts.append((coldef, "f8"))
        dt = np.dtype(dtype_parts)
        data_array = np.zeros(num_samples, dtype=dt)
        for row_num, data_row in enumerate(samples["data"]):
            row_tuple = tuple(np.fromstring(dv, dtype="i8") if isinstance(dv, basestring) else dv for dv in data_row)
            data_array[row_num] = np.array(row_tuple, dtype=dt)
        data = samples.copy()
        data["data"] = data_array
        new_packet = DataPacket(ts_created=get_ion_ts(), data=data)
        for attr in new_packet.__dict__.keys():
            if attr in ('data', 'ts_created'):
                continue
            if attr in kwargs:
                setattr(new_packet, attr, kwargs[attr])
        return new_packet
