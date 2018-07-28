# aseg_gdf2

Python code to help read ASEG GDF2 packages. See the [ASEG technical standards](https://www.aseg.org.au/technical/aseg-technical-standards) page for more information about the file format.

Still very much a work in progress.

## Usage

```python
In [1]: import aseg_gdf2

In [2]: gdf = aseg_gdf2.read(r'tests/example_datasets/3bcfc711/GA1286_Waveforms')

In [3]: gdf.field_names()
Out[3]: ['FLTNUM', 'Rx_Voltage', 'Flight', 'Time', 'Tx_Current']

In [4]]: for row in gdf.iterrows():
   ...:     print(row)
   ...:
OrderedDict([('Index', 0), ('FLTNUM', 1.0), ('Rx_Voltage', -0.0), ('Flight', 1), ('Time', 0.0052), ('Tx_Current', 0.00176)])
OrderedDict([('Index', 1), ('FLTNUM', 1.0), ('Rx_Voltage', -0.0), ('Flight', 1), ('Time', 0.0104), ('Tx_Current', 0.00176)])
OrderedDict([('Index', 2), ('FLTNUM', 1.0), ('Rx_Voltage', -0.0), ('Flight', 1), ('Time', 0.0156), ('Tx_Current', 0.00176)])
...
```

For .dat files that will fit in memory, you can read them into a pandas.DataFrame:

```python
In [5]: gdf.df()
Out[5]:
       FLTNUM  Rx_Voltage  Flight     Time  Tx_Current
0         1.0        -0.0       1   0.0052     0.00176
1         1.0        -0.0       1   0.0104     0.00176
2         1.0        -0.0       1   0.0156     0.00176
3         1.0        -0.0       1   0.0208     0.00176
4         1.0        -0.0       1   0.0260     0.00176
5         1.0        -0.0       1   0.0312     0.00176
...       ...         ...     ...      ...         ...
23034     2.0         0.0       2  59.9687    -0.00170
23035     2.0        -0.0       2  59.9740    -0.00170
23036     2.0        -0.0       2  59.9792    -0.00170
23037     2.0        -0.0       2  59.9844    -0.00170
23038     2.0        -0.0       2  59.9896    -0.00170
23039     2.0        -0.0       2  59.9948    -0.00170

[23040 rows x 5 columns]
```

For .dat files that are too big for memory, you can use the ``chunksize=`` keyword argument to specify the number of rows. Normally you could get away with a few hundred thousand, but for the example we'll use something less:

```python
In [6]: for chunk in gdf.df_chunked(chunksize=10000):
    ...:     print('{} length = {}'.format(type(chunk), len(chunk)))
    ...:
<class 'pandas.core.frame.DataFrame'> length = 10000
<class 'pandas.core.frame.DataFrame'> length = 10000
<class 'pandas.core.frame.DataFrame'> length = 3040
```

The metadata from the .dfn file is there too:

```python
In [7]: gdf.record_types
Out[7]:
{'': {'fields': [{'cols': 1,
    'comment': '',
    'format': 'F10.1',
    'long_name': 'FlightNumber',
    'name': 'FLTNUM',
    'null': None,
    'unit': '',
    'width': 10},
   {'cols': 1,
    'comment': '',
    'format': 'F10.5',
    'long_name': 'Rx_Voltage',
    'name': 'Rx_Voltage',
    'null': '-99.99999',
    'unit': 'Volt',
    'width': 10},
   {'cols': 1,
    'comment': '',
    'format': 'I6',
    'long_name': 'Flight',
    'name': 'Flight',
    'null': '-9999',
    'unit': '',
    'width': 6},
   {'cols': 1,
    'comment': '',
    'format': 'F10.4',
    'long_name': 'Time',
    'name': 'Time',
    'null': '-999.9999',
    'unit': 'msec',
    'width': 10},
   {'cols': 1,
    'comment': '',
    'format': 'F13.5',
    'long_name': 'Tx_Current',
    'name': 'Tx_Current',
    'null': '-99999.99999',
    'unit': 'Amp',
    'width': 13}],
  'format': None}}
  ```

Get the data just for one field/column:

```python
In [8]: gdf.get_field('Time')
Out[8]:
array([  5.20000000e-03,   1.04000000e-02,   1.56000000e-02, ...,
         5.99844000e+01,   5.99896000e+01,   5.99948000e+01])
```

What about fields which are 2D arrays? Some GDF2 data files have fields with more than one value per row/record. e.g. in this one the last four fields each take up 30 columns:

```python
In [9]: gdf = aseg_gdf2.read(r'tests/example_datasets/9a13704a/Mugrave_WB_MGA52.dfn')

In [10]: print(gdf.dfn_contents)
```
```
DEFN   ST=RECD,RT=COMM;RT:A4;COMMENTS:A76
DEFN 1 ST=RECD,RT=;GA_Project:I10:Geoscience Australia airborne survey project number
DEFN 2 ST=RECD,RT=;Job_No:I10:SkyTEM Australia Job Number
DEFN 3 ST=RECD,RT=;Fiducial:F15.2:Fiducial
DEFN 4 ST=RECD,RT=;DATETIME:F18.10:UNIT=days,Decimal days since midnight December 31st 1899
DEFN 5 ST=RECD,RT=;LINE:I10:Line number
DEFN 6 ST=RECD,RT=;Easting:F12.2:NULL=-9999999.99,UNIT=m,Easting (GDA94 MGA Zone 52)
DEFN 7 ST=RECD,RT=;NORTH:F15.2:NULL=-9999999999.99,UNIT=m,Northing (GDA 94 MGA Zone 52)
DEFN 8 ST=RECD,RT=;DTM_AHD:F10.2:NULL=-99999.99,Digital terrain model (AUSGeoid09 datum)
DEFN 9  ST=RECD,RT=;RESI1:F10.3:NULL=-9999.999,Residual of data
DEFN 10 ST=RECD,RT=;HEIGHT:F10.2:NULL=-99999.99,UNIT=m,Laser altimeter measured height of Tx loop centre above ground
DEFN 11 ST=RECD,RT=;INVHEI:F10.2:NULL=-99999.99,UNIT=m,Calulated inversion height
DEFN 12 ST=RECD,RT=;DOI:F10.2:NULL=-99999.99,UNIT=m,Calculated depth of investigation
DEFN 13 ST=RECD,RT=;Elev:30F12.2:NULL=-9999999.99,UNIT=m,Elevation to the top of each layer
DEFN 14 ST=RECD,RT=;Con:30F15.5:NULL=-9999999.99999,UNIT=mS/m,Inverted Conductivity for each layer
DEFN 15 ST=RECD,RT=;Con_doi:30F15.5:NULL=-9999999.99999,UNIT=mS/m, Inverted conductivity for each layer, masked to the depth of investigation
DEFN 16 ST=RECD,RT=;RUnc:30F12.3:NULL=-999999.999,Relative uncertainty of conductivity layer;END DEFN
```

You can see the field names in the normal manner:

```python
In [11]: gdf.field_names()
Out[11]:
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

 Or you can see an "expanded" version of the fields, which is used for the column headings of the data table:

 ```python
 In [12]: gdf.column_names()
Out[12]:
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

In [13]: gdf.df().head()
Out[13]:
   GA_Project  Job_No   Fiducial      DATETIME    LINE   Easting      NORTH  \
0        1288   10013  3621109.0  42655.910984  112601  948001.6  7035223.1
1        1288   10013  3621110.0  42655.910995  112601  948001.9  7035196.8
2        1288   10013  3621111.0  42655.911007  112601  948001.5  7035169.5
3        1288   10013  3621112.0  42655.911019  112601  948000.6  7035141.6
4        1288   10013  3621113.0  42655.911030  112601  947999.1  7035113.6

   DTM_AHD  RESI1  HEIGHT    ...     RUnc[20]  RUnc[21]  RUnc[22]  RUnc[23]  \
0    354.1  1.091   40.98    ...         1.39      1.76      2.35      3.26
1    353.8  1.101   41.08    ...         1.43      1.84      2.47      3.41
2    353.7  0.813   41.03    ...         1.45      1.88      2.53      3.48
3    353.9  0.567   40.79    ...         1.45      1.87      2.53      3.49
4    354.2  0.522   40.37    ...         1.45      1.88      2.54      3.52

   RUnc[24]  RUnc[25]  RUnc[26]  RUnc[27]  RUnc[28]  RUnc[29]
0      4.45      5.74      6.94      8.00      8.99      98.0
1      4.62      5.90      7.09      8.15      9.15      98.0
2      4.70      5.97      7.16      8.22      9.21      98.0
3      4.71      5.98      7.16      8.21      9.20      98.0
4      4.74      6.01      7.18      8.23      9.22      98.0

[5 rows x 132 columns]
```

You can retrieve one of the original field arrays using ``get_field()``:

```python
In [14]: gdf.get_field('Elev')
Out[14]:
array([[ 354.1,  352.1,  349.8, ..., -105.8, -171.2, -245.7],
       [ 353.8,  351.8,  349.5, ..., -106.1, -171.5, -246. ],
       [ 353.7,  351.7,  349.4, ..., -106.2, -171.6, -246.1],
       ...,
       [ 510.5,  508.5,  506.2, ...,   50.6,  -14.8,  -89.3],
       [ 510.5,  508.5,  506.2, ...,   50.6,  -14.8,  -89.3],
       [ 510.6,  508.6,  506.3, ...,   50.7,  -14.7,  -89.2]])
```

