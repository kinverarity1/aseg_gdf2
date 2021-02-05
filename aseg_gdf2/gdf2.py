from collections import OrderedDict
import glob
import logging
import os
import re
import pprint

import pandas as pd
from pandas.io.json import json_normalize
from dask import dataframe as dd

logger = logging.getLogger(__name__)


def read(filename, **kwargs):
    """Load GDF2 data package.

    Arguments:
        dfn_filename (str)
        method (str): how to read the data file. Either assume it is
            delimited by whitespace (``'whitespace'``) or use the
            field widths specified in the .dfn file (``'fixed-widths'``)
        engine (str): library to use in reading the .dat file - either
            `'pandas'`, if you are reading small files and/or have a lot
            of RAM, or `'dask'` if you are reading huge files which will
            not fit in RAM. Both are equivalent in terms of functionality
            but you should use `'pandas'` if you can.

    Returns: :class:`aseg_gdf2.GDF2` object.

    Attributes:
        engine (either PandasEngine or DaskEngine): the object which
            is used to read data. You can change it by setting it to
            either `'pandas'` or `'dask'`.

    """
    filename = str(filename)
    for fn in glob.glob(filename + "*"):
        if fn.lower().endswith("dfn"):
            return GDF2(fn, **kwargs)
        elif fn.lower()[-3:] in ("dat", "ddf", "des", "met"):
            for ext in ("DFN", "dfn"):
                fn2 = fn[:-3] + ext
                if os.path.isfile(fn2):
                    return GDF2(fn2, **kwargs)
        else:
            for ext in ("DFN", "dfn"):
                fn2 = fn[:-3] + ext
                if os.path.isfile(fn2):
                    return GDF2(fn2, **kwargs)
    raise OSError("No data package found at {}".format(filename))


class RecordTypesDict(dict):
    def df(self):
        """Convert record_types dict to panda DataFrame.

        Args:
            self (dict): produced by GDF2._parse_dfn

        Returns: DataFrame

        """
        dfs = []
        for k, v in self.items():
            df = json_normalize(v, ["fields"])
            df["record_type"] = k
            dfs.append(df)
        df = pd.concat(dfs)
        cols = ["record_type"] + [k for k in df.columns if not k == "record_type"]
        return df[cols]


class GDF2(object):
    """Load GDF2 data package.

    Arguments:
        dfn_filename (str)
        method (str): how to read the data file. Either assume it is
            delimited by whitespace (``'whitespace'``) or use the
            field widths specified in the .dfn file (``'fixed-widths'``)
        engine (str): library to use in reading the .dat file - either
            `'pandas'`, if you are reading small files and/or have a lot
            of RAM, or `'dask'` if you are reading huge files which will
            not fit in RAM. Both are equivalent in terms of functionality
            but you should use `'pandas'` if you can.

    Attributes:
        engine (either PandasEngine or DaskEngine): the object which
            is used to read data. You can change it by setting it to
            either `'pandas'` or `'dask'`.

    """

    def __init__(self, dfn_filename, method="whitespace", engine="pandas", **kwargs):
        self._nrecords = None
        self._engine = engine
        self._parse_dfn(dfn_filename)
        self.dat_filename = self._find_dat_file()
        self.method = method
        self._engines = {
            "pandas": PandasEngine(parent=self),
            "dask": DaskEngine(parent=self),
        }

    def __repr__(self):
        r = super().__repr__()
        # _nrecords is a stub to delay nrecords resolution till requested.
        nrecords = self._nrecords
        if nrecords is None:
            nrecords = "?"
        r = r[:-1] + " nrecords={}".format(nrecords) + r[-1]
        return r

    @property
    def engine(self):
        return self._engines[self._engine]

    @engine.setter
    def engine(self, value):
        if value == "pandas" or value == PandasEngine:
            self._engine = "pandas"
        elif value == "dask" or value == DaskEngine:
            self._engine = "dask"

    def df(self, *args, **kwargs):
        """Return the data table as a pandas.DataFrame.

        The actual function called is either ``PandasEngine.df`` or
        ``DaskEngine.df``.

        """
        return self.engine.df(*args, **kwargs)

    def iterrows(self, *args, **kwargs):
        """Iterate over rows in the data table.

        The actual function called is either `PandasEngine.iterrows`
        or `DaskEngine.iterrows`.

        """
        return self.engine.iterrows(*args, **kwargs)

    def _parse_dfn(self, dfn_filename, join_null_data_rts=True, **kwargs):
        self.record_types = RecordTypesDict()
        with open(dfn_filename, "r") as f:
            self.dfn_filename = dfn_filename
            self.dfn_contents = f.read()

        for i, line in enumerate(self.dfn_contents.splitlines()):
            if not line.startswith("DEFN"):
                logger.warning("line {} does not begin with DEFN: {}".format(i, line))
                continue

            if not ";" in line:
                logger.debug("line {}: No field definitions: {}".format(i, line))
                m = re.search(r"RT=(\w*)", line)
                if m:
                    rt = m.group(1)
                    logger.info("line {}: added record type RT={}".format(i, rt))
                    self.record_types[m.group(1)] = {"fields": [], "format": None}
            else:
                m = re.search(r"RT=(\w*)", line)
                if m:
                    rt = m.group(1)
                    if join_null_data_rts:
                        if rt == "DATA":
                            rt = ""
                    if not rt in self.record_types:
                        logger.info("line {}: added record type RT={}".format(i, rt))
                        self.record_types[rt] = {"fields": [], "format": None}

                fields = line.split(";")[1:]
                logger.debug(
                    "line {}: looping through {} field definitions for RT={}".format(
                        i, len(fields), rt
                    )
                )
                for field in fields:
                    f = {
                        "name": "",
                        "format": "",
                        "unit": "",
                        "null": None,
                        "long_name": "",
                        "comment": "",
                    }
                    field = field.strip()
                    if field == "END DEFN":
                        logger.debug(
                            "line {}: end of field definition for record type RT={}".format(
                                i, rt
                            )
                        )
                        continue
                    f["name"], remaining = field.strip().split(":", 1)
                    if ":" in remaining:
                        f["format"], remaining = remaining.strip().split(":", 1)
                        for chunk in remaining.split(","):
                            chunk = chunk.strip()

                            m = re.search("UNITS? *= *(.*)", chunk)
                            if m:
                                f["unit"] = m.group(1)
                                continue

                            m = re.search("NAME *= *(.*)", chunk)
                            if m:
                                f["long_name"] = m.group(1)
                                continue

                            m = re.search("NULL *= *(.*)", chunk)
                            if m:
                                f["null"] = m.group(1)
                                continue

                            f["comment"] = chunk
                    else:
                        f["format"] = remaining
                    m = re.match("([0-9]*)([a-zA-Z]{1})([0-9]+).*", f["format"])
                    if m:
                        n = m.group(1)
                        try:
                            n = int(n)
                        except ValueError:
                            n = 1
                        f["cols"] = n
                        f["width"] = int(m.group(3))
                    else:
                        logger.critical(
                            "No field width found in format code {}".format(f["format"])
                        )

                    dtype = str
                    if "F" in f["format"]:
                        dtype = float
                    elif "I" in f["format"]:
                        dtype = int
                    f["inferred_dtype"] = dtype

                    logger.info(
                        "line {}: adding field {} to record type RT={}".format(
                            i, str(f), rt
                        )
                    )
                    self.record_types[rt]["fields"].append(f)
                    if f["name"] == "RT":
                        self.record_types[rt]["format"] = f["format"]
        dups = self._find_duplicate_field_names()
        if dups:
            logger.warning(f"DFN file has duplicate fields: {dups}")

    def _find_dat_file(self):
        for ext in ("dat", "DAT"):
            filename = self.dfn_filename[:-3] + ext
            if os.path.isfile(filename):
                return filename
        logger.error("No data file located.")
        return ""

    # def _parse_dat(self):
    @property
    def _read_dat(self):

        na_values = {}
        colnames, colnamesdict = self.column_names("", retdict=True)
        for colname in colnames:
            field_name = colnamesdict[colname]
            null = self.get_field_definition(field_name)["null"]
            if not null is None:
                na_values[colname] = null
        logger.debug("_parse_dat: na_values = {}".format(na_values))

        value = {
            "": {
                PandasEngine: {
                    "func": None,
                    "args": [self.dat_filename],
                    "kwargs": {
                        "names": self.column_names(""),
                        "index_col": False,
                        "header": None,
                        "keep_default_na": True,
                        "na_values": na_values,
                        "dtype": dict(zip(self.column_names(), self.column_dtypes())),
                    },
                },
                DaskEngine: {
                    "func": None,
                    "args": [self.dat_filename],
                    "kwargs": {
                        "names": self.column_names(""),
                        "header": None,
                        "keep_default_na": True,
                        "na_values": na_values,
                        "dtype": dict(zip(self.column_names(), self.column_dtypes())),
                    },
                },
            }
        }
        if self.method == "fixed-widths":
            for engine in (PandasEngine, DaskEngine):
                value[""][engine]["func"] = engine.read_fwf
                value[""][engine]["func_name"] = "read_fwf"
                value[""][engine]["kwargs"].update(
                    {"widths": [c["width"] for c in self.get_column_definitions("")]}
                )
        elif self.method == "whitespace":
            for engine in (PandasEngine, DaskEngine):
                value[""][engine]["func"] = engine.read_table
                value[""][engine]["func_name"] = "read_table"
                value[""][engine]["kwargs"].update({"delimiter": r"\s+"})
        return value

    @property
    def nrecords(self):
        if not self._nrecords:
            # Count the number of rows
            def blocks(files, size=65536):
                while True:
                    b = files.read(size)
                    if not b:
                        break
                    yield b

            with open(self.dat_filename, "r", errors="ignore") as f:
                newlines = sum(bl.count("\n") for bl in blocks(f))

            with open(self.dat_filename, "rb") as f:
                f.seek(-1, os.SEEK_END)
                last_byte = f.read(1)
                if last_byte in (b"\r", b"\n"):
                    self._nrecords = newlines
                else:
                    # If the last line contains non-EOL chars, it should be counted
                    # as a record... ?
                    self._nrecords = newlines + 1
        return self._nrecords

    @nrecords.setter
    def nrecords(self, value):
        raise NotImplementedError("nrecords is read-only")

    def _find_duplicate_field_names(self):
        """Find duplicate field names.

        Returns: a dictionary of {field_name: occurrence_count} for those
            which occur more than once.

        """
        counts = {}
        for c in self.column_names():
            if not c in counts:
                counts[c] = 1
            else:
                counts[c] += 1
        return {k: v for k, v in counts.items() if v > 1}

    def fix_duplicate_field_names(self, suffix="__{n}"):
        """Fix field names so that any duplicates are made unique.

        Args:
            suffix (str): Python string with formatting field {n} to represent
                the cumulative count of the duplicated field name. So e.g.
                the default "__{n}" would turn the field name "LINENUM" into
                "LINENUM__1" for the first occurrence, "LINENUM__2" for the 
                second, and so on. Other options could be ":{n}" or "({n})"...
        
        Returns: nothing, operates in-place on the GDF2 object.

        """
        dups = self._find_duplicate_field_names()
        for dup_name in dups.keys():
            dup_count = 1
            for i, field in enumerate(self.record_types['']['fields']):
                if field['name'] == dup_name:
                    new_name = f"{dup_name}" + suffix.format(n=dup_count)
                    self.record_types['']['fields'][i]['name'] = new_name
                    dup_count += 1

    def field_names(self, record_type=""):
        """Return field names from the .dfn file.

        This may not correspond to columns in the data table / pd.DataFrame,
        because a field may be defined as having multiple values (== "columns")
        for each record (== "row"). See column_names() for an alternative.

        """
        return [f["name"] for f in self.record_types[record_type]["fields"]]

    def column_names(self, record_type="", retdict=False):
        """Provide a name for each column of the data table / pd.DataFrame
        object.

        This accounts for the fact that 2D fields are allowed e.g. a
        single field "Con" in the .dfn file may account for 30 columns in the
        .dat file with a format code of say 30F10.5.

        This method will expand the single field name "Con" into 30 field
        names, "Con[0]", "Con[1]", and so on.

        """
        namesdict = {}
        names = []
        for field in self.record_types[record_type]["fields"]:
            name = field["name"]
            if field["cols"] == 1:
                namesdict[name] = name
                names.append(name)
            else:
                namesdict[name] = []
                for i in range(field["cols"]):
                    colname = "{}[{:d}]".format(name, i)
                    namesdict[colname] = name
                    namesdict[name].append(colname)
                    names.append(colname)
        if retdict:
            return names, namesdict
        else:
            return names

    def column_dtypes(self, record_type="", retdict=False):
        """Provide a dtype for each column of the data table / pd.DataFrame
        object.

        This accounts for the fact that 2D fields are allowed e.g. a
        single field "Con" in the .dfn file may account for 30 columns in the
        .dat file with a format code of say 30F10.5.


        """
        dtypesdict = {}
        dtypes = []
        for field in self.record_types[record_type]["fields"]:
            dtype = field["inferred_dtype"]
            dtypesdict[dtype] = dtype
            dtypes.append(dtype)
        if retdict:
            return dtypes, dtypesdict
        else:
            return dtypes


    def get_column_definitions(self, record_type=""):
        """Return the field definition for all the columns i.e. the same
        as get_field_definitions(), but with 2D fields expanded.

        Args:
            record_type (str): record type - NULL by default

        Returns:
            a sequence of dictionaries with "field_name", "column_name",
            etc.

        """
        columns = []
        for field in self.record_types[record_type]["fields"]:
            for column_name in self.get_field_columns(field["name"], record_type):
                m = re.match("([0-9]*)([a-zA-Z]{1})([0-9]+)(.*)", field["format"])
                if m:
                    column_format = m.group(2) + m.group(3) + m.group(4)
                else:
                    column_format = field["format"]
                column = {
                    "name": column_name,
                    "unit": field["unit"],
                    "null": field["null"],
                    "width": field["width"],
                    "column_format": column_format,
                    "field_name": field["name"],
                    "field_comment": field["comment"],
                    "field_format": field["format"],
                    "field_long_name": field["long_name"],
                    "field_cols": field["cols"],
                }
                columns.append(column)
        return columns

    def get_field_definition(self, field_name, record_type=""):
        """Find field_name in record_types definition and
        return the dictionary."""
        for field in self.record_types[record_type]["fields"]:
            if field["name"] == field_name:
                return field

    def get_field_columns(self, field_name, record_type=""):
        """Expand a field name (if necessary) into the constituent column names.

        Args:
            field_name (str): field name from definition file.
            record_type (str): record type - NULL by default.

        Returns: a tuple of column names which are guaranteed to exist in
            the data table methods.

        """
        field = self.get_field_definition(field_name, record_type=record_type)
        if field is None and re.search(r"\[\d*\]$", field_name):
            return (field_name,)
        if field["cols"] == 1:
            return (field_name,)
        else:
            columns = []
            for i in range(field["cols"]):
                columns.append("{}[{:d}]".format(field_name, i))
            return tuple(columns)

    def get_fields_data(self, field_names, record_type=""):
        """Return a tuple of ndarrays with the data for requested fields.

        Args:
            field_names (list-like): list of field names from
                (must exist in `gdf.field_names()`)
            record_type (str):

        Returns: a tuple of ndarrays

        """
        columns = []
        field_to_columns_mapping = {}
        for field_name in field_names:
            # Check to see if it is 1D or 2D
            field = self.get_field_definition(field_name, record_type=record_type)
            if field["cols"] == 1:
                columns.append(field_name)
                field_to_columns_mapping[field_name] = [field_name]
            elif field["cols"] > 1:
                field_columns = self.get_field_columns(
                    field_name, record_type=record_type
                )
                columns += list(field_columns)
                field_to_columns_mapping[field_name] = list(field_columns)
        df = self.df(record_type=record_type, usecols=columns)
        field_arrays = []
        for field_name in field_names:
            array = df[field_to_columns_mapping[field_name]].values
            if array.shape[1] == 1:
                array = array.ravel()
            field_arrays.append(array)
        return tuple(field_arrays)

    def get_field_data(self, field_name, record_type=""):
        """Return the data for a field.

        This is simply a wrapper around ``GDF2.get_fields_data``.

        """
        return self.get_fields_data([field_name], record_type=record_type)[0]


class Engine(object):
    def __init__(self, parent):
        """Create reading engine.

        Args:
            parent (aseg_gdf2.gdf2.GDF2 object)

        """
        self.parent = parent

    def expand_field_names(self, record_type="", **kwargs):
        names, namesdict = self.parent.column_names(record_type, retdict=True)
        rt = self.parent._read_dat[record_type][self.__class__]
        kws = dict(**rt["kwargs"])
        kws.update(kwargs)
        logger.debug("df(kws=\n{})".format(pprint.pformat(kws)))
        if "usecols" in kws:
            logger.debug("initial usecols = {}".format(kws["usecols"]))
            for key in kws["usecols"]:
                if key in namesdict and isinstance(namesdict[key], list):
                    logger.debug("Expanding {} for kws['usecols']".format(key))
                    kws["usecols"] = [k for k in kws["usecols"] if not k == key]
                    for col_key in namesdict[key]:
                        kws["usecols"].append(col_key)
            logger.debug("final usecols = {}".format(kws["usecols"]))
        logger.debug("final na_values = {}".format(kws["na_values"]))
        return rt, kws

    def df(self, **kwargs):
        rt, kws = self.expand_field_names(**kwargs)
        return rt["func"](*rt["args"], **kws)

    def iterrows(self, *args, chunksize=5000, **kwargs):
        """Iterate over rows of the data table. Each row is a dict."""
        for chunk in self.df(*args, chunksize=chunksize, **kwargs):
            for row in chunk.itertuples():
                yield dict(row._asdict())


class PandasEngine(Engine):
    read_fwf = pd.read_fwf
    read_table = pd.read_table


class DaskEngine(Engine):
    read_fwf = dd.read_fwf
    read_table = dd.read_table
