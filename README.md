# aseg_gdf2

[![License](http://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/kinverarity1/aseg_gdf2/blob/master/LICENSE)

Python code to help read ASEG GDF2 data packages.

GDF2 files are a plain text format for storing geophysical data. The file format is [defined](https://www.aseg.org.au/technical/aseg-technical-standards) by the Australian Society of Exploration Geophysicists (ASEG). This module provides Python functions for reading the definition file (.dfn) and extracting data from the data table file (.dat). It's designed to work on machines with low-ish memory, and to do so it has a dependency on [pandas](https://pandas.pydata.org/) and [dask](https://docs.dask.org/en/latest/dataframe.html).

It is still in very early stages of development. Help would be very welcome!

## Examples

Take a look at the [example notebooks](notebooks)!

### Quick start example

```python
>>> import aseg_gdf2
>>> gdf = aseg_gdf2.read(r'tests/example_datasets/3bcfc711/GA1286_Waveforms')
>>> gdf.nrecords
23039
>>> gdf.field_names()
['FLTNUM', 'Rx_Voltage', 'Flight', 'Time', 'Tx_Current']
>>> for row in gdf.iterrows():
...     print(row)
...     break
{'Index': 0, 'FLTNUM': 1.0, 'Rx_Voltage': -0.0, 'Flight': 1, 'Time': 0.0052, 'Tx_Current': 0.00176}
{'Index': 1, 'FLTNUM': 1.0, 'Rx_Voltage': -0.0, 'Flight': 1, 'Time': 0.0104, 'Tx_Current': 0.00176}
{'Index': 2, 'FLTNUM': 1.0, 'Rx_Voltage': -0.0, 'Flight': 1, 'Time': 0.0156, 'Tx_Current': 0.00176}
{'Index': 3, 'FLTNUM': 1.0, 'Rx_Voltage': -0.0, 'Flight': 1, 'Time': 0.0208, 'Tx_Current': 0.00176}
{'Index': 4, 'FLTNUM': 1.0, 'Rx_Voltage': -0.0, 'Flight': 1, 'Time': 0.026, 'Tx_Current': 0.00176}
{'Index': 5, 'FLTNUM': 1.0, 'Rx_Voltage': -0.0, 'Flight': 1, 'Time': 0.0312, 'Tx_Current': 0.00176}
```

You can also get the data table as a pandas.DataFrame:

```python
>>> print(gdf.df())
```

```
   FLTNUM  Rx_Voltage  Flight    Time  Tx_Current
0     1.0        -0.0       1  0.0052     0.00176
1     1.0        -0.0       1  0.0104     0.00176
2     1.0        -0.0       1  0.0156     0.00176
3     1.0        -0.0       1  0.0208     0.00176
4     1.0        -0.0       1  0.0260     0.00176
```

Get the data just for one field/column:

```python
>>> gdf.get_field_data('Time')
array([  5.20000000e-03,   1.04000000e-02,   1.56000000e-02, ...,
         5.99844000e+01,   5.99896000e+01,   5.99948000e+01])
```

What about fields which are 2D arrays? Some GDF2 data files have fields with more than one value per row/record. e.g. in this one the last four fields each take up 30 columns:

```python
>>> gdf = aseg_gdf2.read(r'tests/example_datasets/9a13704a/Mugrave_WB_MGA52.dfn')
>>> print(gdf.record_types.df()[["name", "unit", "format", "cols"]])
```

```
          name  unit   format  cols
0           RT             A4     1
1     COMMENTS            A76     1
0   GA_Project            I10     1
1       Job_No            I10     1
2     Fiducial          F15.2     1
3     DATETIME  days   F18.10     1
4         LINE            I10     1
5      Easting     m    F12.2     1
6        NORTH     m    F15.2     1
7      DTM_AHD          F10.2     1
8        RESI1          F10.3     1
9       HEIGHT     m    F10.2     1
10      INVHEI     m    F10.2     1
11         DOI     m    F10.2     1
12        Elev     m  30F12.2    30
13         Con  mS/m  30F15.5    30
14     Con_doi  mS/m  30F15.5    30
15        RUnc        30F12.3    30
```

You can see the field names in the normal manner:

```python
>>> gdf.field_names()
['GA_Project',
 'Job_No',
 'Fiducial',
 'DATETIME',
 'LINE',
 'Easting',
 'NORTH',
 'DTM_AHD',
 'RESI1',
 'HEIGHT',
 'INVHEI',
 'DOI',
 'Elev',
 'Con',
 'Con_doi',
 'RUnc']
```

 Or you can see the column names:

```python
>>> gdf.column_names()
['GA_Project', 'Job_No', 'Fiducial', 'DATETIME', 'LINE', 'Easting', 'NORTH', 'DTM_AHD', 'RESI1',
 'HEIGHT', 'INVHEI', 'DOI', 'Elev[0]', 'Elev[1]', 'Elev[2]', 'Elev[3]', 'Elev[4]', 'Elev[5]',
 'Elev[6]', 'Elev[7]', 'Elev[8]', 'Elev[9]', 'Elev[10]', 'Elev[11]', 'Elev[12]', 'Elev[13]',
 'Elev[14]', 'Elev[15]', 'Elev[16]', 'Elev[17]', 'Elev[18]', 'Elev[19]', 'Elev[20]', 'Elev[21]',
 'Elev[22]', 'Elev[23]', 'Elev[24]', 'Elev[25]', 'Elev[26]', 'Elev[27]', 'Elev[28]', 'Elev[29]',
 'Con[0]', 'Con[1]', 'Con[2]', 'Con[3]', 'Con[4]', 'Con[5]', 'Con[6]', 'Con[7]', 'Con[8]', 'Con[9]',
 'Con[10]', 'Con[11]', 'Con[12]', 'Con[13]', 'Con[14]', 'Con[15]', 'Con[16]', 'Con[17]', 'Con[18]',
 'Con[19]', 'Con[20]', 'Con[21]', 'Con[22]', 'Con[23]', 'Con[24]', 'Con[25]', 'Con[26]', 'Con[27]',
 'Con[28]', 'Con[29]', 'Con_doi[0]', 'Con_doi[1]', 'Con_doi[2]', 'Con_doi[3]', 'Con_doi[4]',
 'Con_doi[5]', 'Con_doi[6]', 'Con_doi[7]', 'Con_doi[8]', 'Con_doi[9]', 'Con_doi[10]', 'Con_doi[11]',
 'Con_doi[12]', 'Con_doi[13]', 'Con_doi[14]', 'Con_doi[15]', 'Con_doi[16]', 'Con_doi[17]',
 'Con_doi[18]', 'Con_doi[19]', 'Con_doi[20]', 'Con_doi[21]', 'Con_doi[22]', 'Con_doi[23]',
 'Con_doi[24]', 'Con_doi[25]', 'Con_doi[26]', 'Con_doi[27]', 'Con_doi[28]', 'Con_doi[29]', 'RUnc[0]',
 'RUnc[1]', 'RUnc[2]', 'RUnc[3]', 'RUnc[4]', 'RUnc[5]', 'RUnc[6]', 'RUnc[7]', 'RUnc[8]', 'RUnc[9]',
 'RUnc[10]', 'RUnc[11]', 'RUnc[12]', 'RUnc[13]', 'RUnc[14]', 'RUnc[15]', 'RUnc[16]', 'RUnc[17]',
 'RUnc[18]', 'RUnc[19]', 'RUnc[20]', 'RUnc[21]', 'RUnc[22]', 'RUnc[23]', 'RUnc[24]', 'RUnc[25]',
 'RUnc[26]', 'RUnc[27]', 'RUnc[28]', 'RUnc[29]']
```

We can get the data in exactly the same way as a normal "column" field.

```python
>>> gdf.get_field_data("Elev")
array([[ 354.1,  352.1,  349.8, ..., -105.8, -171.2, -245.7],
       [ 353.8,  351.8,  349.5, ..., -106.1, -171.5, -246. ],
       [ 353.7,  351.7,  349.4, ..., -106.2, -171.6, -246.1],
       ...,
       [ 510.5,  508.5,  506.2, ...,   50.6,  -14.8,  -89.3],
       [ 510.5,  508.5,  506.2, ...,   50.6,  -14.8,  -89.3],
       [ 510.6,  508.6,  506.3, ...,   50.7,  -14.7,  -89.2]])
```

We can also get a combination of ordinary column fields and 2D fields:

```python
>>> gdf.get_fields_data(["Easting", "NORTH", "Elev"])
(array([948001.6, 948001.9, 948001.5, 948000.6, 947999.1, 947997.2,
        947995.1, 947993.4, 947992.5, 947992.5, 947993.3, 947994.7,
        947996. , 947997.1, 947997.8, 947997.9, 800001.6, 800002.4,
        800003. , 800003.5, 800003.5, 800003.3, 800002.9, 800002.8,
        800002.8, 800003.1, 800003.7, 800004.1, 800004.3, 800004.5,
        800004.4, 800004.2, 800004.1, 800004.1, 800003.9, 800003.7,
        800003.3, 800002.6]),
 array([7035223.1, 7035196.8, 7035169.5, 7035141.6, 7035113.6, 7035085.9,
        7035058.5, 7035031.3, 7035004.2, 7034976.6, 7034948.3, 7034919.2,
        7034889.4, 7034859. , 7034828.4, 7034797.9, 7029884.1, 7029855.3,
        7029826.9, 7029798.6, 7029770.1, 7029741.5, 7029712.8, 7029684.3,
        7029656.1, 7029628.1, 7029600.1, 7029572. , 7029543.8, 7029515.5,
        7029487.4, 7029459.7, 7029432.1, 7029404.5, 7029376.8, 7029348.7,
        7029320.2, 7029291.4]),
 array([[ 354.1,  352.1,  349.8, ..., -105.8, -171.2, -245.7],
        [ 353.8,  351.8,  349.5, ..., -106.1, -171.5, -246. ],
        [ 353.7,  351.7,  349.4, ..., -106.2, -171.6, -246.1],
        ...,
        [ 510.5,  508.5,  506.2, ...,   50.6,  -14.8,  -89.3],
        [ 510.5,  508.5,  506.2, ...,   50.6,  -14.8,  -89.3],
        [ 510.6,  508.6,  506.3, ...,   50.7,  -14.7,  -89.2]]))
```

Under the hood this works using pandas' [``usecols`` keyword argument](https://pandas.pydata.org/pandas-docs/version/0.22/generated/pandas.read_fwf.html).

## Installation

```python
pip install -U aseg_gdf2
```

## List of changes

### Version 0.3
- Fix #19 (`GDF2(..., method='fixed-widths')` was broken)

### Version 0.2
- Add transparent support for using either pandas or dask to read the data table file
- Simplify field data API: `gdf.get_field_data()` and `gdf.get_fields_data()`

### Version 0.1.2
- Fix #16 - expanded column names not working in gdf2.df_chunked()

### Version 0.1
- Initial development

## License

MIT.
