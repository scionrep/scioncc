""" Persistence of datasets using HDF5. """

__author__ = 'Michael Meisinger'

import os

from pyon.public import log, BadRequest, CFG, Container
from pyon.util.ion_time import IonTime
from ion.util.hdf_utils import HDFLockingFile

try:
    import numpy as np
except ImportError:
    np = None
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

DS_TIMEIDX_PATH = "index/time_idx"
DS_TIMEINGEST_PATH = "index/time_ingest"
INTERNAL_ROW_INCREMENT = 1000
DEFAULT_TIME_INDEX_STEP = 1000


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

        log.debug("Create new persistence layer %s for dataset_id=%s", self.format_name, self.dataset_id)

    def _parse_schema(self):
        # Dataset global attributes
        self.persistence_attrs = self.dataset_schema["attributes"].get("persistence", None) or {}
        self.ds_layout = self.persistence_attrs.get("layout", DS_LAYOUT_INDIVIDUAL)
        if self.ds_layout not in (DS_LAYOUT_COMBINED, DS_LAYOUT_INDIVIDUAL):
            log.warn("Illegal dataset persistence layout %s - using %s", self.ds_layout, DS_LAYOUT_INDIVIDUAL)
            self.ds_layout = DS_LAYOUT_INDIVIDUAL
        self.ds_increment = int(self.persistence_attrs.get("row_increment", DEFAULT_ROW_INCREMENT))
        self.var_defs = self.dataset_schema["variables"]
        self.var_defs_map = {vi["name"]: vi for vi in self.var_defs}

        self.time_idx_step = int(self.persistence_attrs.get("time_index_step", DEFAULT_TIME_INDEX_STEP))
        self.time_var = self.persistence_attrs.get("time_variable", DEFAULT_TIME_VARIABLE)
        # Mapping of variable name to column position
        self.var_index = {}
        for position, var_info in enumerate(self.var_defs):
            var_name = var_info["name"]
            self.var_index[var_name] = position
        if self.time_var not in self.var_defs_map:
            raise BadRequest("No time variable present")

    def _get_ds_filename(self):
        local_fn = "%s%s.hdf5" % (DS_FILE_PREFIX, self.dataset_id)
        ds_filename = self.container.file_system.get("%s/%s" % (DS_BASE_PATH, local_fn))
        return ds_filename

    def require_dataset(self):
        ds_filename = self._get_ds_filename()
        if os.path.exists(ds_filename):
            return ds_filename, False

        log.info("Creating new HDF5 file for dataset_id=%s, file='%s'", self.dataset_id, ds_filename)
        dir_path = os.path.split(ds_filename)[0]
        try:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
        except OSError as exc:
            import errno
            if exc.errno == errno.EEXIST and os.path.isdir(dir_path):
                pass
            else:
                raise

        data_file = HDFLockingFile(ds_filename, "w", retry_count=10, retry_wait=0.5)
        try:
            data_file.attrs["dataset_id"] = self.dataset_id
            data_file.attrs["layout"] = self.ds_layout
            data_file.attrs["format"] = "scion_hdf5_v1"

            data_file.create_group("vars")
            initial_shape = (self.ds_increment, )

            if self.ds_layout == DS_LAYOUT_INDIVIDUAL:
                for position, var_info in enumerate(self.var_defs):
                    var_name = var_info["name"]
                    base_type = var_info.get("base_type", "float")
                    dtype = var_info.get("storage_dtype", "f8")
                    dset = data_file.create_dataset("vars/%s" % var_name, initial_shape,
                                                    dtype=dtype, maxshape=(None, ))
                    dset.attrs["base_type"] = str(base_type)
                    dset.attrs["position"] = position
                    dset.attrs["description"] = str(var_info.get("description", "") or "")
                    dset.attrs["unit"] = str(var_info.get("unit", "") or "")
                    if var_name == self.time_var:
                        dset.attrs["cur_row"] = 0

            elif self.ds_layout == DS_LAYOUT_COMBINED:
                dtype_parts = []
                for var_info in self.var_defs:
                    var_name = var_info["name"]
                    base_type = var_info.get("base_type", "float")
                    dtype = var_info.get("storage_dtype", "f8")
                    dtype_parts.append((var_name, dtype))

                dset = data_file.create_dataset("vars/%s" % DS_VARIABLES, initial_shape,
                                                dtype=np.dtype(dtype_parts), maxshape=(None, ))
                dset.attrs["dtype_repr"] = repr(dset.dtype)[6:-1]
                dset.attrs["cur_row"] = 0

            # Internal time index
            data_file.create_group("index")
            dtype_tidx = [("time", "u8")]
            ds_tidx = data_file.create_dataset(DS_TIMEIDX_PATH, (INTERNAL_ROW_INCREMENT, ),
                                               dtype=dtype_tidx, maxshape=(None, ))
            ds_tidx.attrs["cur_row"] = 0
            ds_tidx.attrs["description"] = "Index of every %s-th time value" % self.time_idx_step
            ds_tidx.attrs["step"] = self.time_idx_step

            # Internal ingest time
            dtype_tingest = [("time", "u8"), ("row", "u4"), ("count", "u4")]
            ds_tingest = data_file.create_dataset(DS_TIMEINGEST_PATH, (INTERNAL_ROW_INCREMENT, ),
                                                  dtype=dtype_tingest, maxshape=(None, ))
            ds_tingest.attrs["cur_row"] = 0
            ds_tingest.attrs["description"] = "Maintains ingest times"

        finally:
            data_file.close()

        return ds_filename, True

    def _resize_dataset(self, var_ds, num_rows, row_increment=None):
        row_increment = row_increment or self.ds_increment
        cur_size = len(var_ds)
        new_size = cur_size + (int(num_rows / row_increment) + 1) * row_increment
        log.debug("Resizing dataset %s from %s to %s", var_ds, cur_size, new_size)
        var_ds.resize(new_size, axis=0)

    def extend_dataset(self, packet):
        ingest_ts = IonTime().to_ntp64()
        num_rows, cur_idx, time_idx_rows = len(packet.data["data"]), 0, []
        ds_filename = self._get_ds_filename()
        data_file = HDFLockingFile(ds_filename, "r+", retry_count=10, retry_wait=0.5)
        try:
            if self.ds_layout == DS_LAYOUT_INDIVIDUAL:
                # Fill time var and get index values
                if self.time_var not in packet.data["cols"]:
                    raise BadRequest("Packet has no time")
                var_ds = data_file["vars/%s" % self.time_var]
                cur_size, cur_idx = len(var_ds), var_ds.attrs["cur_row"]
                if cur_idx + num_rows > cur_size:
                    self._resize_dataset(var_ds, num_rows)
                data_slice = packet.data["data"][:][self.time_var]
                var_ds[cur_idx:cur_idx+num_rows] = data_slice
                var_ds.attrs["cur_row"] += num_rows

                # Fill other variables with values from packet or NaN
                for var_name in self.var_defs_map.keys():
                    var_ds = data_file["vars/%s" % var_name]
                    if cur_idx + num_rows > cur_size:
                        self._resize_dataset(var_ds, num_rows)
                    if var_name in packet.data["cols"]:
                        data_slice = packet.data["data"][:][var_name]
                        var_ds[cur_idx:cur_idx+num_rows] = data_slice
                    else:
                        # Leave the initial fill value (zeros)
                        pass
                        #var_ds[cur_idx:cur_idx+num_rows] = [None]*num_rows

                extra_vars = set(packet.data["cols"]) - set(self.var_defs_map.keys())
                if extra_vars:
                    log.warn("Data packet had extra vars not in dataset: %s", extra_vars)

            elif self.ds_layout == DS_LAYOUT_COMBINED:
                var_ds = data_file["vars/%s" % DS_VARIABLES]
                cur_size, cur_idx = len(var_ds), var_ds.attrs["cur_row"]
                if cur_idx + num_rows > cur_size:
                    self._resize_dataset(var_ds, num_rows)
                ds_var_names = [var_info["name"] for var_info in self.var_defs]
                pvi = {col_name: col_idx for col_idx, col_name in enumerate(packet.data["cols"]) if col_name in ds_var_names}
                for row_idx in xrange(num_rows):
                    row_data = packet.data["data"][row_idx]
                    row_vals = tuple(row_data[vn] if vn in pvi else None for vn in ds_var_names)
                    var_ds[cur_idx+row_idx] = row_vals
                var_ds.attrs["cur_row"] += num_rows

            # Update time_ingest (ts, begin row, count)
            ds_tingest = data_file[DS_TIMEINGEST_PATH]
            if ds_tingest.attrs["cur_row"] + 1 > len(ds_tingest):
                self._resize_dataset(ds_tingest, 1, INTERNAL_ROW_INCREMENT)
            ds_tingest[ds_tingest.attrs["cur_row"]] = (np.fromstring(ingest_ts, dtype="u8"), cur_idx, num_rows)
            ds_tingest.attrs["cur_row"] += 1

            # Update time_idx (every nth row's time)
            new_idx_row = (cur_idx + num_rows + self.time_idx_step - 1) / self.time_idx_step
            old_idx_row = (cur_idx + self.time_idx_step - 1) / self.time_idx_step
            num_tidx_rows = new_idx_row - old_idx_row
            time_ds = data_file["vars/%s" % (self.time_var if self.ds_layout == DS_LAYOUT_INDIVIDUAL else DS_VARIABLES)]
            time_idx_rows = [time_ds[idx_row*self.time_idx_step] for idx_row in xrange(old_idx_row, new_idx_row)]
            if time_idx_rows:
                ds_tidx = data_file[DS_TIMEIDX_PATH]
                tidx_cur_row = ds_tidx.attrs["cur_row"]
                if tidx_cur_row + num_tidx_rows > len(ds_tidx):
                    self._resize_dataset(ds_tidx, num_tidx_rows, INTERNAL_ROW_INCREMENT)
                ds_tidx[tidx_cur_row:tidx_cur_row+num_tidx_rows] = time_idx_rows
                ds_tidx.attrs["cur_row"] += num_tidx_rows

            #HDF5Tools.dump_hdf5(data_file, with_data=True)
        finally:
            data_file.close()

    # -------------------------------------------------------------------------

    def get_data(self, data_filter=None):
        data_filter = data_filter or {}
        ds_filename = self._get_ds_filename()
        if not os.path.exists(ds_filename):
            return {}
        data_file = HDFLockingFile(ds_filename, "r", retry_count=10, retry_wait=0.2)
        try:
            res_data = {}
            read_vars = data_filter.get("variables", []) or [var_info["name"] for var_info in self.var_defs]
            time_format = data_filter.get("time_format", "unix_millis")
            max_rows = data_filter.get("max_rows", 999999999)
            time_slice = None
            if self.ds_layout == DS_LAYOUT_INDIVIDUAL:
                time_ds = data_file["vars/%s" % self.time_var]
                cur_idx = time_ds.attrs["cur_row"]
                for var_name in read_vars:
                    ds_path = "vars/%s" % var_name
                    if ds_path not in data_file:
                        log.warn("Variable '%s' not in dataset - ignored", var_name)
                        continue
                    var_ds = data_file[ds_path]
                    data_array = var_ds[max(cur_idx-max_rows, 0):cur_idx]
                    if var_name == self.time_var and self.var_defs_map[var_name].get("base_type", "") == "ntp_time":
                        if time_format == "unix_millis":
                            data_array = [int(1000*IonTime.from_ntp64(dv.tostring()).to_unix()) for dv in data_array]
                        else:
                            data_array = data_array.tolist()
                    else:
                        data_array = data_array.tolist()
                    if var_name == self.time_var:
                        time_slice = data_array

                    res_data[var_name] = data_array

                if data_filter.get("transpose_time", False) is True:
                    time_series = res_data.pop(self.time_var)
                    for var_name, var_series in res_data.iteritems():
                        res_data[var_name] = [(tv, dv) for (tv, dv) in zip(time_series, var_series)]

                # Downsample: http://stackoverflow.com/questions/20322079/downsample-a-1d-numpy-array

            elif self.ds_layout == DS_LAYOUT_COMBINED:
                raise NotImplementedError()

            start_time = data_filter.get("start_time", None)
            start_time_include = data_filter.get("start_time_include", True) is True
            if time_slice and res_data and start_time:
                start_time = int(start_time)
                time_idx = len(time_slice)
                for idx, tv in enumerate(time_slice):
                    if tv == start_time and start_time_include:
                        time_idx = idx
                        break
                    elif tv > start_time:
                        time_idx = idx
                        break
                for var_name, var_series in res_data.iteritems():
                    res_data[var_name] = var_series[time_idx:]

            return res_data

        finally:
            data_file.close()


class HDF5Tools(object):
    @classmethod
    def dump_hdf5(cls, data_file, leave_open=False, with_data=False):
        should_close = False
        if isinstance(data_file, basestring) and os.path.exists(data_file):
            filename = data_file
            data_file = HDFLockingFile(data_file, "r", retry_count=10, retry_wait=0.5)
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

