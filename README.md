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
 'Elev[0]',
 'Elev[1]',
 'Elev[2]',
 'Elev[3]',
 'Elev[4]',
 'Elev[5]',
 'Elev[6]',
 'Elev[7]',
 'Elev[8]',
 'Elev[9]',
 'Elev[10]',
 'Elev[11]',
 'Elev[12]',
 'Elev[13]',
 'Elev[14]',
 'Elev[15]',
 'Elev[16]',
 'Elev[17]',
 'Elev[18]',
 'Elev[19]',
 'Elev[20]',
 'Elev[21]',
 'Elev[22]',
 'Elev[23]',
 'Elev[24]',
 'Elev[25]',
 'Elev[26]',
 'Elev[27]',
 'Elev[28]',
 'Elev[29]',
 'Con[0]',
 'Con[1]',
 'Con[2]',
 'Con[3]',
 'Con[4]',
 'Con[5]',
 'Con[6]',
 'Con[7]',
 'Con[8]',
 'Con[9]',
 'Con[10]',
 'Con[11]',
 'Con[12]',
 'Con[13]',
 'Con[14]',
 'Con[15]',
 'Con[16]',
 'Con[17]',
 'Con[18]',
 'Con[19]',
 'Con[20]',
 'Con[21]',
 'Con[22]',
 'Con[23]',
 'Con[24]',
 'Con[25]',
 'Con[26]',
 'Con[27]',
 'Con[28]',
 'Con[29]',
 'Con_doi[0]',
 'Con_doi[1]',
 'Con_doi[2]',
 'Con_doi[3]',
 'Con_doi[4]',
 'Con_doi[5]',
 'Con_doi[6]',
 'Con_doi[7]',
 'Con_doi[8]',
 'Con_doi[9]',
 'Con_doi[10]',
 'Con_doi[11]',
 'Con_doi[12]',
 'Con_doi[13]',
 'Con_doi[14]',
 'Con_doi[15]',
 'Con_doi[16]',
 'Con_doi[17]',
 'Con_doi[18]',
 'Con_doi[19]',
 'Con_doi[20]',
 'Con_doi[21]',
 'Con_doi[22]',
 'Con_doi[23]',
 'Con_doi[24]',
 'Con_doi[25]',
 'Con_doi[26]',
 'Con_doi[27]',
 'Con_doi[28]',
 'Con_doi[29]',
 'RUnc[0]',
 'RUnc[1]',
 'RUnc[2]',
 'RUnc[3]',
 'RUnc[4]',
 'RUnc[5]',
 'RUnc[6]',
 'RUnc[7]',
 'RUnc[8]',
 'RUnc[9]',
 'RUnc[10]',
 'RUnc[11]',
 'RUnc[12]',
 'RUnc[13]',
 'RUnc[14]',
 'RUnc[15]',
 'RUnc[16]',
 'RUnc[17]',
 'RUnc[18]',
 'RUnc[19]',
 'RUnc[20]',
 'RUnc[21]',
 'RUnc[22]',
 'RUnc[23]',
 'RUnc[24]',
 'RUnc[25]',
 'RUnc[26]',
 'RUnc[27]',
 'RUnc[28]',
 'RUnc[29]']

In [13]: gdf.df()
Out[13]:
    GA_Project  Job_No   Fiducial      DATETIME    LINE   Easting      NORTH  \
0         1288   10013  3621109.0  42655.910984  112601  948001.6  7035223.1
1         1288   10013  3621110.0  42655.910995  112601  948001.9  7035196.8
2         1288   10013  3621111.0  42655.911007  112601  948001.5  7035169.5
3         1288   10013  3621112.0  42655.911019  112601  948000.6  7035141.6
4         1288   10013  3621113.0  42655.911030  112601  947999.1  7035113.6
5         1288   10013  3621114.0  42655.911042  112601  947997.2  7035085.9
6         1288   10013  3621115.0  42655.911053  112601  947995.1  7035058.5
7         1288   10013  3621116.0  42655.911065  112601  947993.4  7035031.3
8         1288   10013  3621117.0  42655.911076  112601  947992.5  7035004.2
9         1288   10013  3621118.0  42655.911088  112601  947992.5  7034976.6
10        1288   10013  3621119.0  42655.911100  112601  947993.3  7034948.3
11        1288   10013  3621120.0  42655.911111  112601  947994.7  7034919.2
12        1288   10013  3621121.0  42655.911123  112601  947996.0  7034889.4
13        1288   10013  3621122.0  42655.911134  112601  947997.1  7034859.0
14        1288   10013  3621123.0  42655.911146  112601  947997.8  7034828.4
15        1288   10013  3621124.0  42655.911157  112601  947997.9  7034797.9
16        1288   10013  1404700.0  42630.258102  912002  800001.6  7029884.1
17        1288   10013  1404701.0  42630.258113  912002  800002.4  7029855.3
18        1288   10013  1404702.0  42630.258125  912002  800003.0  7029826.9
19        1288   10013  1404703.0  42630.258137  912002  800003.5  7029798.6
20        1288   10013  1404704.0  42630.258148  912002  800003.5  7029770.1
21        1288   10013  1404705.0  42630.258160  912002  800003.3  7029741.5
22        1288   10013  1404706.0  42630.258171  912002  800002.9  7029712.8
23        1288   10013  1404707.0  42630.258183  912002  800002.8  7029684.3
24        1288   10013  1404708.0  42630.258194  912002  800002.8  7029656.1
25        1288   10013  1404709.0  42630.258206  912002  800003.1  7029628.1
26        1288   10013  1404710.0  42630.258218  912002  800003.7  7029600.1
27        1288   10013  1404711.0  42630.258229  912002  800004.1  7029572.0
28        1288   10013  1404712.0  42630.258241  912002  800004.3  7029543.8
29        1288   10013  1404713.0  42630.258252  912002  800004.5  7029515.5
30        1288   10013  1404714.0  42630.258264  912002  800004.4  7029487.4
31        1288   10013  1404715.0  42630.258275  912002  800004.2  7029459.7
32        1288   10013  1404716.0  42630.258287  912002  800004.1  7029432.1
33        1288   10013  1404717.0  42630.258299  912002  800004.1  7029404.5
34        1288   10013  1404718.0  42630.258310  912002  800003.9  7029376.8
35        1288   10013  1404719.0  42630.258322  912002  800003.7  7029348.7
36        1288   10013  1404720.0  42630.258333  912002  800003.3  7029320.2
37        1288   10013  1404721.0  42630.258345  912002  800002.6  7029291.4

    DTM_AHD  RESI1  HEIGHT    ...     RUnc[20]  RUnc[21]  RUnc[22]  RUnc[23]  \
0     354.1  1.091   40.98    ...        1.390     1.760     2.350     3.260
1     353.8  1.101   41.08    ...        1.430     1.840     2.470     3.410
2     353.7  0.813   41.03    ...        1.450     1.880     2.530     3.480
3     353.9  0.567   40.79    ...        1.450     1.870     2.530     3.490
4     354.2  0.522   40.37    ...        1.450     1.880     2.540     3.520
5     354.5  0.534   39.87    ...        1.490     1.930     2.620     3.620
6     354.6  0.712   39.39    ...        1.540     2.010     2.720     3.740
7     354.7  0.922   38.98    ...        1.580     2.070     2.810     3.840
8     354.6  0.908   38.62    ...        1.630     2.140     2.890     3.930
9     354.5  0.898   38.28    ...        1.700     2.240     3.030     4.090
10    354.3  1.020   37.97    ...        1.740     2.320     3.140     4.230
11    354.1  1.068   37.74    ...        1.690     2.260     3.080     4.180
12    353.9  1.082   37.62    ...        1.630     2.170     2.970     4.070
13    353.8  1.039   37.58    ...        1.660     2.210     3.030     4.150
14    353.9  0.971   37.55    ...        1.740     2.310     3.140     4.270
15    354.0  0.907   37.49    ...        1.800     2.370     3.190     4.320
16    512.8  0.963   36.38    ...        0.329     0.346     0.367     0.391
17    512.6  0.814   36.49    ...        0.333     0.351     0.371     0.395
18    512.4  0.927   36.50    ...        0.339     0.357     0.377     0.401
19    512.3  1.055   36.34    ...        0.344     0.363     0.383     0.406
20    512.3  1.083   36.03    ...        0.350     0.368     0.389     0.411
21    512.5  1.174   35.65    ...        0.356     0.375     0.395     0.417
22    512.7  1.502   35.32    ...        0.361     0.380     0.401     0.422
23    512.9  2.178   35.14    ...        0.365     0.385     0.406     0.427
24    512.9  2.969   35.17    ...        0.369     0.389     0.410     0.430
25    512.8  2.648   35.35    ...        0.372     0.392     0.414     0.434
26    512.6  1.818   35.62    ...        0.376     0.396     0.417     0.437
27    512.4  1.401   35.96    ...        0.379     0.399     0.421     0.441
28    512.0  1.316   36.42    ...        0.383     0.403     0.425     0.445
29    511.7  1.435   36.98    ...        0.386     0.407     0.429     0.449
30    511.4  1.571   37.63    ...        0.390     0.411     0.432     0.453
31    511.2  1.631   38.45    ...        0.393     0.414     0.436     0.458
32    511.0  1.591   39.55    ...        0.397     0.419     0.441     0.463
33    510.6  1.342   40.87    ...        0.404     0.425     0.448     0.470
34    510.5  1.101   42.07    ...        0.412     0.434     0.456     0.479
35    510.5  1.098   42.87    ...        0.424     0.446     0.468     0.491
36    510.5  1.330   43.30    ...        0.445     0.467     0.488     0.510
37    510.6  1.352   43.56    ...        0.485     0.505     0.525     0.546

    RUnc[24]  RUnc[25]  RUnc[26]  RUnc[27]  RUnc[28]  RUnc[29]
0      4.450     5.740     6.940     8.000     8.990    98.000
1      4.620     5.900     7.090     8.150     9.150    98.000
2      4.700     5.970     7.160     8.220     9.210    98.000
3      4.710     5.980     7.160     8.210     9.200    98.000
4      4.740     6.010     7.180     8.230     9.220    98.000
5      4.840     6.110     7.280     8.320     9.320    98.000
6      4.980     6.250     7.410     8.460     9.460    98.000
7      5.080     6.350     7.520     8.570     9.580    98.000
8      5.190     6.460     7.630     8.680     9.700    98.000
9      5.350     6.620     7.790     8.850     9.890    98.000
10     5.500     6.770     7.930     9.000    98.000    98.000
11     5.460     6.740     7.900     8.960     9.990    98.000
12     5.360     6.640     7.810     8.860     9.890    98.000
13     5.450     6.730     7.900     8.950     9.980    98.000
14     5.580     6.870     8.040     9.100    98.000    98.000
15     5.630     6.930     8.110     9.170    98.000    98.000
16     0.414     0.424     0.428     0.446     0.473     0.521
17     0.418     0.423     0.426     0.446     0.474     0.523
18     0.421     0.421     0.422     0.445     0.476     0.525
19     0.424     0.419     0.418     0.444     0.477     0.528
20     0.427     0.418     0.414     0.443     0.479     0.530
21     0.431     0.417     0.411     0.443     0.482     0.533
22     0.435     0.417     0.409     0.445     0.485     0.536
23     0.438     0.416     0.409     0.448     0.488     0.539
24     0.441     0.417     0.412     0.452     0.491     0.542
25     0.445     0.421     0.419     0.457     0.495     0.546
26     0.448     0.431     0.426     0.462     0.499     0.550
27     0.453     0.441     0.431     0.466     0.504     0.555
28     0.458     0.451     0.439     0.471     0.510     0.562
29     0.463     0.461     0.447     0.475     0.515     0.569
30     0.469     0.470     0.455     0.481     0.522     0.576
31     0.475     0.479     0.462     0.486     0.528     0.585
32     0.482     0.488     0.470     0.492     0.536     0.595
33     0.491     0.499     0.481     0.499     0.545     0.609
34     0.501     0.511     0.493     0.507     0.556     0.625
35     0.514     0.527     0.508     0.518     0.571     0.647
36     0.533     0.549     0.531     0.537     0.592     0.676
37     0.569     0.586     0.567     0.572     0.629     0.720

[38 rows x 132 columns]
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

