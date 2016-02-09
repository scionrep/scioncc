""" Persistence of datasets using HDF5. """

__author__ = 'Michael Meisinger'

import os

from pyon.public import log, StandaloneProcess, BadRequest, CFG, StreamSubscriber, named_any, Container

try:
    import h5py
except ImportError:
    log.warn("Missing h5py library.")
    h5py = None


class DatasetHDF5Persistence(object):

    @classmethod
    def get_persistence(cls, dataset_id, ds_schema):
        return DatasetHDF5Persistence(dataset_id, ds_schema)

    def __init__(self, dataset_id, ds_schema):
        if not h5py:
            raise BadRequest("Must have h5py")
        self.dataset_id = dataset_id
        self.dataset_schema = ds_schema
        self.container = Container.instance

    def _get_ds_filename(self):
        local_fn = "ds_%s.hdf5" % self.dataset_id
        ds_filename = self.container.file_system.get("SCIDATA/datasets/%s" % local_fn)
        #log.debug("Dataset filename is '%s'", ds_filename)
        return ds_filename

    def require_dataset(self):
        ds_filename = self._get_ds_filename()
        if os.path.exists(ds_filename):
            return ds_filename, False

        log.info("Creating new HDF5 dataset for id=%s", self.dataset_id)
        os.makedirs(os.path.split(ds_filename)[0])

        data_file = h5py.File(ds_filename, "w")
        data_file.create_group("vars")

        row_increment = int(self.dataset_schema["attributes"].get("persistence", {}).get("row_increment", 1000))
        initial_shape = (row_increment, )

        for var_info in self.dataset_schema["variables"]:
            var_name = var_info["name"]
            base_type = var_info.get("base_type", "float")
            dtype = var_info.get("storage_dtype", "f8")
            dset = data_file.create_dataset("vars/%s" % var_name, initial_shape,
                                            dtype=dtype, maxshape=(None, ))
            dset.attrs["base_type"] = str(base_type)
            dset.attrs["description"] = str(var_info.get("description", "") or "")
            dset.attrs["unit"] = str(var_info.get("unit", "") or "")
            dset.attrs["last_row"] = 0

        data_file.close()
        return ds_filename, True

    def _resize_dataset(self, var_ds, row_increment, num_rows):
        cur_len = len(var_ds)
        new_size = cur_len + (int(num_rows / row_increment) + 1) * row_increment
        log.debug("Resizing dataset %s from %s to %s", var_ds, cur_len, new_size)
        var_ds.resize(new_size, axis=0)

    def extend_dataset(self, packet):
        row_increment = int(self.dataset_schema["attributes"].get("persistence", {}).get("row_increment", 1000))
        num_rows = len(packet.data["data"])
        ds_filename = self._get_ds_filename()
        data_file = h5py.File(ds_filename, "r+")
        for var_idx, var_name in enumerate(packet.data["cols"]):
            ds_path = "vars/%s" % var_name
            if ds_path not in data_file:
                log.warn("Variable '%s' not in dataset - ignored", var_name)
                continue
            var_ds = data_file[ds_path]
            cur_len = len(var_ds)
            if int(var_ds.attrs["last_row"]) + num_rows > cur_len:
                self._resize_dataset(var_ds, row_increment, num_rows)
            data_slice = packet.data["data"][:][var_name]
            var_ds[cur_len:cur_len+num_rows] = data_slice
            var_ds.attrs["last_row"] += num_rows

        #HDF5Tools.dump_hdf5(data_file, with_data=True)

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
            if with_data and hasattr(entry, "value"):
                print "%s %s" % ("  "*ilevel, entry.value)

        data_file.visit(dump_item)

        if should_close and not leave_open:
            data_file.close()

        return data_file

