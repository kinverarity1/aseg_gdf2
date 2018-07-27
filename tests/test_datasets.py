import os, sys; sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

dset = lambda *args: os.path.join(os.path.dirname(__file__), 'example_datasets', *args)

import pytest

import aseg_gdf2

def test_iterrows_1():
    gdf = aseg_gdf2.read(dset('3bcfc711', 'GA1286_Waveforms'))
    for row in gdf.iterrows():
        assert row['Time'] == 0.0052
        break

def test_iterrows_2():
    gdf = aseg_gdf2.read(dset('3bcfc711', 'GA1286_Waveforms'))
    for row in gdf.iterrows():
        pass
    assert row['Time'] == 59.9948
    
def test_chunksize():
    gdf = aseg_gdf2.read(dset('3bcfc711', 'GA1286_Waveforms'))
    lengths = []
    for chunk in gdf.chunks(chunksize=10000):
        lengths.append(len(chunk))
    assert lengths == [10000, 10000, 3040]
    