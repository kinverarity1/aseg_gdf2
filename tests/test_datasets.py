import os, sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pathlib import Path

repo = Path(__file__).parent.parent

import pytest
import numpy as np

import aseg_gdf2


def test_iterrows_1():
    gdf = aseg_gdf2.read(
        str(repo / "tests" / "example_datasets" / "3bcfc711" / "GA1286_Waveforms")
    )
    for row in gdf.iterrows():
        assert row["Time"] == 0.0052
        break


def test_iterrows_2():
    gdf = aseg_gdf2.read(
        str(repo / "tests" / "example_datasets" / "3bcfc711" / "GA1286_Waveforms")
    )
    for row in gdf.iterrows():
        pass
    assert row["Time"] == 59.9948


def test_length_df():
    gdf = aseg_gdf2.read(
        str(repo / "tests" / "example_datasets" / "3bcfc711" / "GA1286_Waveforms")
    )
    assert len(gdf.df()) == 23040


def test_nrecords():
    gdf = aseg_gdf2.read(
        str(repo / "tests" / "example_datasets" / "3bcfc711" / "GA1286_Waveforms")
    )
    assert gdf.nrecords == 23040


def test_2d_field_data():
    gdf = aseg_gdf2.read(
        str(repo / "tests" / "example_datasets" / "9a13704a" / "Mugrave_WB_MGA52")
    )
    assert gdf.get_field_data("Con_doi")[4, -6] == [174.27675]


def test_2d_field_null():
    gdf = aseg_gdf2.read(
        str(repo / "tests" / "example_datasets" / "9a13704a" / "Mugrave_WB_MGA52")
    )
    assert np.isnan(gdf.get_field_data("Con_doi")[5, -6])


def test_df_2d_usecols():
    gdf = aseg_gdf2.read(
        str(repo / "tests" / "example_datasets" / "9a13704a" / "Mugrave_WB_MGA52")
    )
    df = gdf.df(usecols=["LINE", "RUnc"])
    assert (
        df.columns.values
        == [
            "LINE",
            "RUnc[0]",
            "RUnc[1]",
            "RUnc[2]",
            "RUnc[3]",
            "RUnc[4]",
            "RUnc[5]",
            "RUnc[6]",
            "RUnc[7]",
            "RUnc[8]",
            "RUnc[9]",
            "RUnc[10]",
            "RUnc[11]",
            "RUnc[12]",
            "RUnc[13]",
            "RUnc[14]",
            "RUnc[15]",
            "RUnc[16]",
            "RUnc[17]",
            "RUnc[18]",
            "RUnc[19]",
            "RUnc[20]",
            "RUnc[21]",
            "RUnc[22]",
            "RUnc[23]",
            "RUnc[24]",
            "RUnc[25]",
            "RUnc[26]",
            "RUnc[27]",
            "RUnc[28]",
            "RUnc[29]",
        ]
    ).all()
