#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from nose.plugins.attrib import attr
import gevent
import yaml
import os
import random

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import BadRequest, NotFound, IonObject, RT, PRED, OT, CFG, StreamSubscriber, log
from pyon.ion.identifier import create_simple_unique_id

from ion.data.packet.packet_builder import DataPacketBuilder
from ion.data.persist.hdf5_dataset import DS_BASE_PATH, DS_FILE_PREFIX, DatasetHDF5Persistence, DS_TIMEIDX_PATH, DS_TIMEINGEST_PATH
from ion.data.schema.schema import DataSchemaParser
from ion.util.hdf_utils import HDFLockingFile
from ion.util.ntp_time import NTP4Time

from interface.objects import DataPacket


@attr('INT', group='data')
class TestHDF5Persist(IonIntegrationTestCase):
    """Test for HDF5 persistence
    """

    def setUp(self):
        from ion.data.persist.hdf5_dataset import h5py
        if h5py is None:
            self.skipTest("No h5py available to test")

        self._start_container()
        #self.container.start_rel_from_url('res/deploy/basic.yml')

        self.rr = self.container.resource_registry
        self.system_actor_id = None

    def tearDown(self):
        pass

    def _get_data_packet(self, index, num_rows=1):
        """ Return a data packet with number of samples.
        The index indicates the offset from the starting timestamp, 10 sec per sample."""
        base_ts = 1000000000
        index_ts = base_ts + 10 * index

        # Core samples as provided by agent.acquire_samples
        sample_list = []
        for i in xrange(num_rows):
            ts = index_ts + i * 10
            sample = [NTP4Time(ts).to_ntp64(),
                      float(index + i),
                      random.random()*100]

            sample_list.append(sample)

        sample_desc = dict(cols=["time", "var1", "random1"],
                           data=sample_list)

        packet = DataPacketBuilder.build_packet_from_samples(sample_desc,
                                                             resource_id="ds_id", stream_name="basic_streams")

        return packet

    # Test row interval algorithm
    # Test packed sample format
    # Test other time formats
    # Test out of order timestamps
    # Test with large files (index extend etc)

    def test_hdf5_persist(self):
        # Test HDF5 writing, time indexing, array extension etc
        ds_schema_str = """
        type: scion_data_schema_1
        description: Schema for test datasets
        attributes:
          basic_shape: 1d_timeseries
          time_variable: time
          persistence:
            format: hdf5
            layout: vars_individual
            row_increment: 1000
            time_index_step: 1000
        variables:
          - name: time
            base_type: ntp_time
            storage_dtype: i8
            unit: ""
            description: NTPv4 timestamp
          - name: var1
            base_type: float
            storage_dtype: f8
            unit: ""
            description: Sample value
          - name: random1
            base_type: float
            storage_dtype: f8
            unit: ""
            description: Random values
        """
        ds_schema = yaml.load(ds_schema_str)
        ds_id = create_simple_unique_id()
        ds_filename = self.container.file_system.get("%s/%s%s.hdf5" % (DS_BASE_PATH, DS_FILE_PREFIX, ds_id))

        self.hdf5_persist = DatasetHDF5Persistence.get_persistence(ds_id, ds_schema, "hdf5")
        self.hdf5_persist.require_dataset()

        self.assertTrue(os.path.exists(ds_filename))
        self.addCleanup(os.remove, ds_filename)

        # Add 100 values in packets of 10
        for i in xrange(10):
            packet = self._get_data_packet(i*10, 10)
            self.hdf5_persist.extend_dataset(packet)

        data_res = self.hdf5_persist.get_data()
        self.assertEqual(len(data_res), 3)
        self.assertEqual(len(data_res["time"]), 100)
        self.assertEqual(len(data_res["var1"]), 100)
        self.assertEqual(len(data_res["random1"]), 100)
        self.assertEqual(data_res["var1"][1], 1.0)

        with HDFLockingFile(ds_filename, "r") as hdff:
            ds_time = hdff["vars/time"]
            cur_idx = ds_time.attrs["cur_row"]
            self.assertEqual(cur_idx, 100)
            self.assertEqual(len(ds_time), 1000)

            ds_tidx = hdff[DS_TIMEIDX_PATH]
            cur_tidx = ds_tidx.attrs["cur_row"]
            self.assertEqual(cur_tidx, 1)
            self.assertEqual(len(ds_tidx), 1000)

        # Add 1000 values in packets of 10
        for i in xrange(100):
            packet = self._get_data_packet(100 + i*10, 10)
            self.hdf5_persist.extend_dataset(packet)

        data_res = self.hdf5_persist.get_data()
        self.assertEqual(len(data_res["time"]), 1100)

        with HDFLockingFile(ds_filename, "r") as hdff:
            ds_time = hdff["vars/time"]
            cur_idx = ds_time.attrs["cur_row"]
            self.assertEqual(cur_idx, 1100)
            self.assertEqual(len(ds_time), 2000)

            ds_tidx = hdff[DS_TIMEIDX_PATH]
            cur_tidx = ds_tidx.attrs["cur_row"]
            self.assertEqual(cur_tidx, 2)
            self.assertEqual(len(ds_tidx), 1000)


            self.assertEqual(ds_time[0], ds_tidx[0][0])
            self.assertEqual(ds_time[1000], ds_tidx[1][0])

        info_res = self.hdf5_persist.get_data_info()

        self.assertEqual(info_res["ds_rows"], 1100)
        self.assertEqual(info_res["ts_first"], 1000000000.0)
        self.assertEqual(info_res["ts_last"], 1000010990.0)

    def test_hdf5_persist_prune(self):
        # Test auto-pruning
        ds_schema_str = """
type: scion_data_schema_1
description: Schema for test datasets
attributes:
  basic_shape: 1d_timeseries
  time_variable: time
  persistence:
    format: hdf5
    layout: vars_individual
    row_increment: 1000
    time_index_step: 1000
  pruning:
    trigger_mode: on_ingest
    prune_mode: max_age_rel
    prune_action: rewrite
    trigger_age: 1000.0
    retain_age: 500.0
variables:
  - name: time
    base_type: ntp_time
    storage_dtype: i8
    unit: ""
    description: NTPv4 timestamp
  - name: var1
    base_type: float
    storage_dtype: f8
    unit: ""
    description: Sample value
  - name: random1
    base_type: float
    storage_dtype: f8
    unit: ""
    description: Random values
"""
        ds_schema = yaml.load(ds_schema_str)
        ds_id = create_simple_unique_id()
        ds_filename = self.container.file_system.get("%s/%s%s.hdf5" % (DS_BASE_PATH, DS_FILE_PREFIX, ds_id))

        self.hdf5_persist = DatasetHDF5Persistence.get_persistence(ds_id, ds_schema, "hdf5")
        self.hdf5_persist.require_dataset()

        self.assertTrue(os.path.exists(ds_filename))
        self.addCleanup(os.remove, ds_filename)

        # Add 100 values in packets of 10 (right up to the prune trigger)
        for i in xrange(10):
            packet = self._get_data_packet(i * 10, 10)
            self.hdf5_persist.extend_dataset(packet)

        data_res = self.hdf5_persist.get_data()
        self.assertEqual(len(data_res["time"]), 100)
        self.assertEqual(len(data_res["var1"]), 100)
        self.assertEqual(len(data_res["random1"]), 100)
        self.assertEqual(data_res["var1"][1], 1.0)

        log.info("*** STEP 2: First prune")

        # Add 2 values (stepping across the prune trigger - inclusive boundary)
        packet = self._get_data_packet(100, 2)
        self.hdf5_persist.extend_dataset(packet)

        data_res = self.hdf5_persist.get_data()
        self.assertEqual(len(data_res["time"]), 51)
        self.assertEqual(len(data_res["var1"]), 51)
        self.assertEqual(len(data_res["random1"]), 51)
        self.assertEqual(data_res["var1"][0], 51.0)
        self.assertEqual(data_res["var1"][50], 101.0)

        log.info("*** STEP 3: Additional data")

        # Add 100 values in packets of 10 (right up to the prune trigger)
        packet = self._get_data_packet(102, 8)
        self.hdf5_persist.extend_dataset(packet)
        for i in xrange(4):
            packet = self._get_data_packet(110 + i * 10, 10)
            self.hdf5_persist.extend_dataset(packet)

        packet = self._get_data_packet(150, 2)
        self.hdf5_persist.extend_dataset(packet)

        data_res = self.hdf5_persist.get_data()
        self.assertEqual(len(data_res["time"]), 101)
        self.assertEqual(data_res["var1"][0], 51.0)
        self.assertEqual(data_res["var1"][100], 151.0)

        log.info("*** STEP 4: Second prune")

        packet = self._get_data_packet(152, 1)
        self.hdf5_persist.extend_dataset(packet)

        data_res = self.hdf5_persist.get_data()
        self.assertEqual(len(data_res["time"]), 51)
        self.assertEqual(data_res["var1"][0], 102.0)
        self.assertEqual(data_res["var1"][50], 152.0)

        log.info("*** STEP 5: Third prune")

        packet = self._get_data_packet(153, 100)
        self.hdf5_persist.extend_dataset(packet)

        data_res = self.hdf5_persist.get_data()
        self.assertEqual(len(data_res["time"]), 51)
        self.assertEqual(data_res["var1"][0], 202.0)
        self.assertEqual(data_res["var1"][50], 252.0)

    def test_hdf5_persist_decimate(self):
        # Test HDF5 writing, time indexing, array extension etc
        ds_schema_str = """
        type: scion_data_schema_1
        description: Schema for test datasets
        attributes:
          basic_shape: 1d_timeseries
          time_variable: time
          persistence:
            format: hdf5
            layout: vars_individual
            row_increment: 1000
            time_index_step: 1000
        variables:
          - name: time
            base_type: ntp_time
            storage_dtype: i8
            unit: ""
            description: NTPv4 timestamp
          - name: var1
            base_type: float
            storage_dtype: f8
            unit: ""
            description: Sample value
          - name: random1
            base_type: float
            storage_dtype: f8
            unit: ""
            description: Random values
        """
        ds_schema = yaml.load(ds_schema_str)
        ds_id = create_simple_unique_id()
        ds_filename = self.container.file_system.get("%s/%s%s.hdf5" % (DS_BASE_PATH, DS_FILE_PREFIX, ds_id))

        self.hdf5_persist = DatasetHDF5Persistence.get_persistence(ds_id, ds_schema, "hdf5")
        self.hdf5_persist.require_dataset()

        self.assertTrue(os.path.exists(ds_filename))
        self.addCleanup(os.remove, ds_filename)

        # Add 100000 values in packets of 100
        for i in xrange(100):
            packet = self._get_data_packet(i * 100, 100)
            self.hdf5_persist.extend_dataset(packet)

        data_res = self.hdf5_persist.get_data()
        self.assertEqual(len(data_res), 3)
        self.assertEqual(len(data_res["time"]), 10000)

        data_res = self.hdf5_persist.get_data(dict(max_rows=999, decimate=True, decimate_method="minmax"))
        self.assertEqual(len(data_res), 3)
        self.assertLessEqual(len(data_res["time"]), 1000)

