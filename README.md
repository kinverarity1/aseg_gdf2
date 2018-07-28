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

