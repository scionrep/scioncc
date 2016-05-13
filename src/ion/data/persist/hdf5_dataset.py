""" Persistence of datasets using HDF5. """

__author__ = 'Michael Meisinger'

import os

from pyon.public import log, BadRequest, CFG, Container
from ion.util.hdf_utils import HDFLockingFile
from ion.util.ntp_time import NTP4Time

try:
    import numpy as np
except ImportError:
    np = None
try:
    import h5py
except ImportError:
    log.warn("Missing h5py library.")
    h5py = None


DS_LAYOUT_COMBINED = "vars_combined"        # All vars in 1 table, structured type
DS_LAYOUT_INDIVIDUAL = "vars_individual"    # Each var in separate table, time co-indexed

DS_FILE_PREFIX = "ds_"
DS_BASE_PATH = "SCIDATA/datasets"
DEFAULT_ROW_INCREMENT = 1000
DEFAULT_TIME_VARIABLE = "time"
DS_VARIABLES = "data"

DS_TIMEIDX_PATH = "index/time_idx"
DS_TIMEINGEST_PATH = "index/time_ingest"
INTERNAL_ROW_INCREMENT = 1000
TIMEINDEX_ROW_INCREMENT = 1000
DEFAULT_TIME_INDEX_STEP = 1000
DEFAULT_MAX_ROWS = 1000000


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
        self.expand_info = self._get_expand_info()

    def _get_ds_filename(self):
        local_fn = "%s%s.hdf5" % (DS_FILE_PREFIX, self.dataset_id)
        ds_filename = self.container.file_system.get("%s/%s" % (DS_BASE_PATH, local_fn))
        return ds_filename

    def _get_expand_info(self):
        """ Returns packed value expansion info from analyzing dataset schema """
        need_expand, num_steps, step_increment, expand_cols = False, 0, 0, {}
        for var_def in self.var_defs:
            packing_cfg = var_def.get("packing", {})
            packing_type = packing_cfg.get("type", "")
            if not packing_type:
                continue
            if packing_type == "fixed_sampling_rate":
                dtype = np.dtype(var_def["storage_dtype"])
                if len(dtype.shape) > 1:
                    raise BadRequest("Unsupported higher order dtype shape")
                elif len(dtype.shape) == 1 and dtype.shape[0] > 1:
                    if need_expand:
                        # Check compatibility
                        if dtype.shape[0] != num_steps:
                            raise BadRequest("Cannot expand multiple variables with different pack size")
                        row_period = packing_cfg["samples_period"]
                        if step_increment != row_period / num_steps:
                            raise BadRequest("Cannot expand multiple variables with different step increment")
                    else:
                        need_expand = True
                        num_steps = dtype.shape[0]
                        row_period = packing_cfg["samples_period"]
                        step_increment = row_period / num_steps
                    expand_cols[var_def["name"]] = dict(basedt=np.dtype(dtype.base))
            else:
                raise BadRequest("Unsupported packing type")
        expand_info = dict(need_expand=need_expand,         # Bool
                           num_steps=num_steps,             # Number of values (steps) per packed row
                           step_increment=step_increment,   # Seconds fraction per time step
                           expand_cols=expand_cols)         # Colnames to be expanded
        return expand_info

    def require_dataset(self):
        """
        Ensures a dataset HDF5 file exists and creates it if necessary usign the dataset
        schema definition.
        """
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
            dtype_tidx = [("time", "i8")]
            ds_tidx = data_file.create_dataset(DS_TIMEIDX_PATH, (INTERNAL_ROW_INCREMENT, ),
                                               dtype=dtype_tidx, maxshape=(None, ))
            ds_tidx.attrs["cur_row"] = 0
            ds_tidx.attrs["description"] = "Index of every %s-th time value" % self.time_idx_step
            ds_tidx.attrs["step"] = self.time_idx_step

            # Internal ingest time
            dtype_tingest = [("time", "i8"), ("row", "u4"), ("count", "u4")]
            ds_tingest = data_file.create_dataset(DS_TIMEINGEST_PATH, (INTERNAL_ROW_INCREMENT, ),
                                                  dtype=dtype_tingest, maxshape=(None, ))
            ds_tingest.attrs["cur_row"] = 0
            ds_tingest.attrs["description"] = "Maintains ingest times"

        finally:
            data_file.close()

        return ds_filename, True

    def _resize_dataset(self, var_ds, num_rows, row_increment=None):
        """ Performs a resize operation on a dataset table """
        row_increment = row_increment or self.ds_increment
        cur_size = len(var_ds)
        new_size = cur_size + (int(num_rows / row_increment) + 1) * row_increment
        log.debug("Resizing dataset %s from %s to %s", var_ds, cur_size, new_size)
        var_ds.resize(new_size, axis=0)

    def extend_dataset(self, packet):
        """
        Adds values from a data packet to the dataset and updates indexes and metadata
        """
        ingest_ts = NTP4Time.utcnow()
        num_rows, cur_idx, time_idx_rows = len(packet.data["data"]), 0, []
        ds_filename = self._get_ds_filename()
        data_file = HDFLockingFile(ds_filename, "r+", retry_count=10, retry_wait=0.5)
        try:
            if self.ds_layout == DS_LAYOUT_INDIVIDUAL:
                # Get index values from time var
                if self.time_var not in packet.data["cols"]:
                    raise BadRequest("Packet has no time")
                var_ds = data_file["vars/%s" % self.time_var]
                cur_size, cur_idx = len(var_ds), var_ds.attrs["cur_row"]
                var_ds.attrs["cur_row"] += num_rows

                # Fill variables with values from packet or NaN
                for var_name in self.var_defs_map.keys():
                    var_ds = data_file["vars/%s" % var_name]
                    if cur_idx + num_rows > cur_size:
                        self._resize_dataset(var_ds, num_rows)
                    if var_name in packet.data["cols"]:
                        data_slice = packet.data["data"][:][var_name]
                        var_ds[cur_idx:cur_idx+num_rows] = data_slice
                    else:
                        # Leave the initial fill value (zeros)
                        #var_ds[cur_idx:cur_idx+num_rows] = [None]*num_rows
                        pass

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
            ds_tingest[ds_tingest.attrs["cur_row"]] = (ingest_ts.to_np_value(), cur_idx, num_rows)
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
                    self._resize_dataset(ds_tidx, num_tidx_rows, TIMEINDEX_ROW_INCREMENT)
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
            max_rows = data_filter.get("max_rows", DEFAULT_MAX_ROWS)
            start_time = data_filter.get("start_time", None)
            end_time = data_filter.get("end_time", None)
            start_time_include = data_filter.get("start_time_include", True) is True
            should_decimate = data_filter.get("decimate", False) is True
            time_slice = None

            start_row, end_row = self._get_row_interval(data_file, start_time, end_time, start_time_include)
            log.info("ROW INTERVAL %s %s", start_row, end_row)
            if self.expand_info.get("need_expand", False):
                max_rows = max_rows / self.expand_info["num_steps"]  # Compensate expansion

            if self.ds_layout == DS_LAYOUT_INDIVIDUAL:
                ds_time = data_file["vars/%s" % self.time_var]
                cur_idx = ds_time.attrs["cur_row"]
                for var_name in read_vars:
                    ds_path = "vars/%s" % var_name
                    if ds_path not in data_file:
                        log.warn("Variable '%s' not in dataset - ignored", var_name)
                        continue
                    ds_var = data_file[ds_path]
                    data_array = ds_var[max(start_row, end_row-max_rows, 0):end_row]
                    if var_name == self.time_var and self.var_defs_map[var_name].get("base_type", "") == "ntp_time":
                        if time_format == "unix_millis":
                            data_array = [int(1000*NTP4Time.from_ntp64(dv.tostring()).to_unix()) for dv in data_array]
                        else:
                            data_array = data_array.tolist()
                    else:
                        data_array = data_array.tolist()
                    if var_name == self.time_var:
                        time_slice = data_array

                    res_data[var_name] = data_array

                # At this point we have dict with variable to data array mapping with target (unix) timestamps
                self._expand_packed_rows(res_data, data_filter)

                if data_filter.get("transpose_time", False) is True:
                    time_series = res_data.pop(self.time_var)
                    for var_name, var_series in res_data.iteritems():
                        res_data[var_name] = [(tv, dv) for (tv, dv) in zip(time_series, var_series)]

                # Downsample: http://stackoverflow.com/questions/20322079/downsample-a-1d-numpy-array

            elif self.ds_layout == DS_LAYOUT_COMBINED:
                raise NotImplementedError()

            return res_data

        finally:
            data_file.close()

    def _get_row_interval(self, data_file, start_time, end_time, start_time_include=True):
        """ Lookup delimiting row numbers using time index, matching start and end time.
        Note: This applies before pack expansion, so step values are not considered right now.
        """
        ds_tidx = data_file[DS_TIMEIDX_PATH]
        ds_time = data_file["vars/%s" % self.time_var]
        cur_idx = ds_time.attrs["cur_row"]
        log.info("Get time %s %s (%s)", start_time, end_time, cur_idx)
        start_row, end_row = 0, cur_idx
        if not start_time and not end_time:
            return start_row, end_row
        time_type = self.var_defs_map[self.time_var].get("base_type", "")

        # Could use binary search - for now just iterate
        start_time_val = float(start_time)/1000 if start_time else 0
        end_time_val = float(end_time)/1000 if end_time else 0
        if start_time and end_time and start_time >= end_time:
            end_time = end_time_val = 0

        def gte_time(data_val, cmp_val, allow_equal=True):
            # Support NTP4 timestamp and Unit millis (i8)
            if time_type == "ntp_time":
                if allow_equal:
                    return NTP4Time.from_ntp64(data_val.tostring()).to_unix() >= cmp_val
                else:
                    return NTP4Time.from_ntp64(data_val.tostring()).to_unix() > cmp_val
            else:
                if allow_equal:
                    return data_val >= cmp_val
                else:
                    return data_val > cmp_val

        tidx_slice, step, incr, start_win, end_win, done = None, 0, TIMEINDEX_ROW_INCREMENT, 0, 0, False
        while step * incr < len(ds_tidx) and not done:
            tidx_slice = ds_tidx[step*incr:min((step+1)*incr, len(ds_tidx))]
            if start_time and not start_win and gte_time(tidx_slice[-1], start_time_val, True):
                for i, ts_val in enumerate(tidx_slice):
                    if gte_time(ts_val, start_time_val, start_time_include):
                        start_win = max(0, step * incr + i - 1)
                        if not end_time:
                            done = True
                        break
            if end_time and gte_time(tidx_slice[-1], end_time_val, True):
                for i, ts_val in enumerate(tidx_slice):
                    if gte_time(ts_val, end_time_val):
                        end_win = max(0, step * incr + i - 1)
                        done = True
                        break
            step += 1

        # Lookup real rows
        if start_time:
            ts_slice = ds_time[start_win * self.time_idx_step: min((start_win + 1) * self.time_idx_step + 1, cur_idx)]
            for i, ts_val in enumerate(ts_slice):
                if gte_time(ts_val, start_time_val, start_time_include):
                    start_row = start_win * self.time_idx_step + i
                    break
        if end_time:
            ts_slice = ds_time[end_win * self.time_idx_step: min((end_win + 1) * self.time_idx_step + 1, cur_idx)]
            for i, ts_val in enumerate(ts_slice):
                if gte_time(ts_val, end_time_val, False):
                    end_row = start_win * self.time_idx_step + i
                    break

        return start_row, end_row

    def _expand_packed_rows(self, res_data, data_filter):
        """ Expand packed data representations """
        if not self.expand_info.get("need_expand", False):
            return
        num_steps, step_increment, expand_cols = self.expand_info["num_steps"], self.expand_info["step_increment"], self.expand_info["expand_cols"]
        max_rows = data_filter.get("max_rows", DEFAULT_MAX_ROWS)
        log.info("Row expansion num_steps=%s step_incr=%s", num_steps, step_increment)

        for var_name, data_array in res_data.iteritems():
            if var_name in expand_cols:
                new_array = np.zeros(len(data_array) * num_steps, dtype=expand_cols[var_name]["basedt"])
                for i, val in enumerate(data_array):
                    new_array[i*num_steps:(i+1)*num_steps] = val
            elif var_name == self.time_var:
                dtype = self.var_defs_map[var_name]["storage_dtype"]
                new_array = np.zeros(len(data_array) * num_steps, dtype=dtype)
                # This assumes unix_millis time format
                for i, val in enumerate(data_array):
                    new_array[i*num_steps:(i+1)*num_steps] = np.array([val + j*step_increment*1000 for j in xrange(num_steps)], dtype=dtype)
            else:
                dtype = self.var_defs_map[var_name]["storage_dtype"]
                new_array = np.zeros(len(data_array) * num_steps, dtype=dtype)
                for i, val in enumerate(data_array):
                    new_array[i*num_steps:(i+1)*num_steps] = np.array([val]*num_steps, dtype=dtype)
            if max_rows:
                new_array = new_array[-max_rows:]
            res_data[var_name] = new_array.tolist()


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

