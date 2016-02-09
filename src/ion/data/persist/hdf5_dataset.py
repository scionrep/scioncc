""" Persistence of datasets using HDF5. """

__author__ = 'Michael Meisinger'

import os
import numpy as np

from pyon.public import log, StandaloneProcess, BadRequest, CFG, StreamSubscriber, named_any, Container

try:
    import h5py
except ImportError:
    log.warn("Missing h5py library.")
    h5py = None


DS_LAYOUT_COMBINED = "vars_combined"
DS_LAYOUT_INDIVIDUAL = "vars_individual"

DS_FILE_PREFIX = "ds_"
DS_BASE_PATH = "SCIDATA/datasets"
DEFAULT_ROW_INCREMENT = 1000
DEFAULT_TIME_VARIABLE = "time"
DS_VARIABLES = "data"


class DatasetHDF5Persistence(object):

    @classmethod
    def get_persistence(cls, dataset_id, ds_schema, format_name):
        return DatasetHDF5Persistence(dataset_id, ds_schema, format_name)

    def __init__(self, dataset_id, ds_schema, format_name):
        if not h5py:
            raise BadRequest("Must have h5py")
        self.dataset_id = dataset_id
        self.dataset_schema = ds_schema
        self.format_name = format_name
        self.container = Container.instance
        self._parse_schema()

        log.info("Create new persistence layer %s for dataset_id=%s", self.format_name, self.dataset_id)

    def _parse_schema(self):
        # Dataset global attributes
        self.persistence_attrs = self.dataset_schema["attributes"].get("persistence", None) or {}
        self.ds_layout = self.persistence_attrs.get("layout", DS_LAYOUT_INDIVIDUAL)
        if self.ds_layout not in (DS_LAYOUT_COMBINED, DS_LAYOUT_INDIVIDUAL):
            log.warn("Illegal dataset persistence layout %s - using %s", self.ds_layout, DS_LAYOUT_INDIVIDUAL)
            self.ds_layout = DS_LAYOUT_INDIVIDUAL
        self.ds_increment = int(self.persistence_attrs.get("row_increment", DEFAULT_ROW_INCREMENT))

        self.time_var = self.persistence_attrs.get("time_variable", DEFAULT_TIME_VARIABLE)
        # Mapping of variable name to column position
        self.var_index = {}
        for position, var_info in enumerate(self.dataset_schema["variables"]):
            var_name = var_info["name"]
            self.var_index[var_name] = position

    def _get_ds_filename(self):
        local_fn = "%s%s.hdf5" % (DS_FILE_PREFIX, self.dataset_id)
        ds_filename = self.container.file_system.get("%s/%s" % (DS_BASE_PATH, local_fn))
        return ds_filename

    def require_dataset(self):
        ds_filename = self._get_ds_filename()
        if os.path.exists(ds_filename):
            return ds_filename, False

        log.info("Creating new HDF5 dataset for id=%s", self.dataset_id)
        os.makedirs(os.path.split(ds_filename)[0])

        data_file = h5py.File(ds_filename, "w")
        data_file.attrs["dataset_id"] = self.dataset_id
        data_file.attrs["layout"] = self.ds_layout

        data_file.create_group("vars")
        initial_shape = (self.ds_increment, )

        if self.ds_layout == DS_LAYOUT_INDIVIDUAL:
            for position, var_info in enumerate(self.dataset_schema["variables"]):
                var_name = var_info["name"]
                base_type = var_info.get("base_type", "float")
                dtype = var_info.get("storage_dtype", "f8")
                dset = data_file.create_dataset("vars/%s" % var_name, initial_shape,
                                                dtype=dtype, maxshape=(None, ))
                dset.attrs["base_type"] = str(base_type)
                dset.attrs["position"] = position
                dset.attrs["description"] = str(var_info.get("description", "") or "")
                dset.attrs["unit"] = str(var_info.get("unit", "") or "")
                dset.attrs["last_row"] = 0

        elif self.ds_layout == DS_LAYOUT_COMBINED:
            dtype_parts = []
            for var_info in self.dataset_schema["variables"]:
                var_name = var_info["name"]
                base_type = var_info.get("base_type", "float")
                dtype = var_info.get("storage_dtype", "f8")
                dtype_parts.append((var_name, dtype))

            dset = data_file.create_dataset("vars/%s" % DS_VARIABLES, initial_shape,
                                            dtype=np.dtype(dtype_parts), maxshape=(None, ))
            dset.attrs["dtype_repr"] = repr(dset.dtype)[6:-1]
            dset.attrs["last_row"] = 0

        data_file.close()
        return ds_filename, True

    def _resize_dataset(self, var_ds, num_rows):
        cur_size = len(var_ds)
        new_size = cur_size + (int(num_rows / self.ds_increment) + 1) * self.ds_increment
        log.debug("Resizing dataset %s from %s to %s", var_ds, cur_size, new_size)
        var_ds.resize(new_size, axis=0)

    def extend_dataset(self, packet):
        num_rows = len(packet.data["data"])
        ds_filename = self._get_ds_filename()
        data_file = h5py.File(ds_filename, "r+")

        if self.ds_layout == DS_LAYOUT_INDIVIDUAL:
            for var_idx, var_name in enumerate(packet.data["cols"]):
                ds_path = "vars/%s" % var_name
                if ds_path not in data_file:
                    log.warn("Variable '%s' not in dataset - ignored", var_name)
                    continue
                var_ds = data_file[ds_path]
                cur_size = len(var_ds)
                cur_idx = var_ds.attrs["last_row"]
                if cur_idx + num_rows > cur_size:
                    self._resize_dataset(var_ds, num_rows)
                data_slice = packet.data["data"][:][var_name]
                var_ds[cur_idx:cur_idx+num_rows] = data_slice
                var_ds.attrs["last_row"] += num_rows

        elif self.ds_layout == DS_LAYOUT_COMBINED:
            ds_path = "vars/%s" % DS_VARIABLES
            if ds_path not in data_file:
                raise BadRequest("Cannot find combined dataset")
            var_ds = data_file[ds_path]
            cur_size = len(var_ds)
            cur_idx = var_ds.attrs["last_row"]
            if cur_idx + num_rows > cur_size:
                self._resize_dataset(var_ds, num_rows)
            ds_var_names = [var_info["name"] for var_info in self.dataset_schema["variables"]]
            pvi = {col_name: col_idx for col_idx, col_name in enumerate(packet.data["cols"]) if col_name in ds_var_names}
            for row_idx in xrange(num_rows):
                row_data = packet.data["data"][row_idx]
                row_vals = tuple(row_data[vn] if vn in pvi else None for vn in ds_var_names)
                var_ds[cur_idx+row_idx] = row_vals
            var_ds.attrs["last_row"] += num_rows

        HDF5Tools.dump_hdf5(data_file, with_data=True)

        data_file.close()


class HDF5Tools(object):
    @classmethod
    def dump_hdf5(cls, data_file, leave_open=False, with_data=False):
        should_close = False
        if isinstance(data_file, basestring) and os.path.exists(data_file):
            filename = data_file
            data_file = h5py.File(data_file, "r")
            should_close = True
            print "HDF5", filename, data_file

        else:
            print "HDF5", data_file

        def dump_item(entry_name):
            parts = entry_name.split("/")
            entry = data_file[entry_name]
            ilevel = len(parts)
            print "%s%s %s" % ("  "*ilevel, parts[-1], entry)
            if entry.attrs:
                print "%s  [%s]" % ("  "*ilevel, ", ".join("%s=%s" % (k, v) for (k, v) in entry.attrs.iteritems()))

            if with_data and hasattr(entry, "value"):
                print "%s  %s" % ("  "*ilevel, entry.value)

        data_file.visit(dump_item)

        if should_close and not leave_open:
            data_file.close()

        return data_file

