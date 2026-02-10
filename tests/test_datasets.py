import os, sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pathlib import Path

repo = Path(__file__).parent.parent

import pytest
import numpy as np

import aseg_gdf2


@pytest.mark.parametrize("engine", ["pandas", "dask"])
@pytest.mark.parametrize("method", ["whitespace", "fixed-widths"])
class TestDatasets:
    def test_iterrows_1(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "example_datasets" / "3bcfc711" / "GA1286_Waveforms"),
            method=method,
            engine=engine
        )
        for row in gdf.iterrows():
            assert row["Time"] == 0.0052
            break

    def test_iterrows_2(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "example_datasets" / "3bcfc711" / "GA1286_Waveforms"),
            method=method,
            engine=engine
        )
        for row in gdf.iterrows():
            pass
        assert row["Time"] == 59.9948

    def test_length_df(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "example_datasets" / "3bcfc711" / "GA1286_Waveforms"),
            method=method,
            engine=engine
        )
        assert len(gdf.df()) == 23040

    def test_nrecords(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "example_datasets" / "3bcfc711" / "GA1286_Waveforms"),
            method=method,
            engine=engine
        )
        assert gdf.nrecords == 23040

    def test_2d_field_data(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "example_datasets" / "9a13704a" / "Mugrave_WB_MGA52"),
            method=method,
            engine=engine
        )
        assert gdf.get_field_data("Con_doi")[4, -6] == [174.27675]

    def test_2d_field_null(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "example_datasets" / "9a13704a" / "Mugrave_WB_MGA52"),
            method=method,
            engine=engine
        )
        assert np.isnan(gdf.get_field_data("Con_doi")[5, -6])

    def test_df_2d_usecols(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "example_datasets" / "9a13704a" / "Mugrave_WB_MGA52"),
            method=method,
            engine=engine
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

    def test_definition_with_spaces(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "example_datasets" / "8e598964" / "AusAEM_02_NT&WA_AEM_Tranche1_GA_vsum_inversion"),
            method=method,
            engine=engine,
            clean_column_names=True
        )

        columns = gdf.get_column_definitions()

        assert columns[0]["name"] == "uniqueid"
        assert columns[0]["unit"] == ""
        assert columns[0]["null"] is None
        assert columns[0]["width"] == 12
        assert columns[0]["column_format"] == "I12"
        assert columns[0]["field_name"] == "uniqueid"
        assert columns[0]["field_comment"] == "Inversion sequence number"
        assert columns[0]["field_format"] == "I12"
        assert columns[0]["field_long_name"] == ""
        assert columns[0]["field_cols"] == 1

        assert columns[22]["name"] == "conductivity_0"
        assert columns[22]["unit"] == "S/m"
        assert columns[22]["null"] is None
        assert columns[22]["width"] == 15
        assert columns[22]["column_format"] == "E15.6"
        assert columns[22]["field_name"] == "conductivity"
        assert columns[22]["field_comment"] == "Layer conductivity"
        assert columns[22]["field_format"] == "30E15.6"
        assert columns[22]["field_long_name"] == ""
        assert columns[22]["field_cols"] == 30

    class TestCleanColumnNames:
        def test_column_names(self, engine, method):
            gdf = aseg_gdf2.read(
                str(repo / "tests" / "example_datasets" / "8e598964" / "AusAEM_02_NT&WA_AEM_Tranche1_GA_vsum_inversion"),
                method=method,
                engine=engine,
                clean_column_names=True
            )

            df = gdf.df(usecols=["flight", "conductivity"])

            assert (
                    df.columns.values
                    == ["flight",
                        "conductivity_0", "conductivity_1", "conductivity_2",
                        "conductivity_3", "conductivity_4", "conductivity_5",
                        "conductivity_6", "conductivity_7", "conductivity_8",
                        "conductivity_9", "conductivity_10", "conductivity_11",
                        "conductivity_12", "conductivity_13", "conductivity_14",
                        "conductivity_15", "conductivity_16", "conductivity_17",
                        "conductivity_18", "conductivity_19", "conductivity_20",
                        "conductivity_21", "conductivity_22", "conductivity_23",
                        "conductivity_24", "conductivity_25", "conductivity_26",
                        "conductivity_27", "conductivity_28", "conductivity_29"]
            ).all()

        def test_field_columns(self, engine, method):
            gdf = aseg_gdf2.read(
                str(repo / "tests" / "example_datasets" / "8e598964" / "AusAEM_02_NT&WA_AEM_Tranche1_GA_vsum_inversion"),
                method=method,
                engine=engine,
                clean_column_names=True
            )

            field_columns = gdf.get_field_columns("conductivity")

            assert (
                    field_columns
                    == ("conductivity_0", "conductivity_1", "conductivity_2",
                        "conductivity_3", "conductivity_4", "conductivity_5",
                        "conductivity_6", "conductivity_7", "conductivity_8",
                        "conductivity_9", "conductivity_10", "conductivity_11",
                        "conductivity_12", "conductivity_13", "conductivity_14",
                        "conductivity_15", "conductivity_16", "conductivity_17",
                        "conductivity_18", "conductivity_19", "conductivity_20",
                        "conductivity_21", "conductivity_22", "conductivity_23",
                        "conductivity_24", "conductivity_25", "conductivity_26",
                        "conductivity_27", "conductivity_28", "conductivity_29")
            )

        def test_iterator(self, engine, method):
            gdf = aseg_gdf2.read(
                str(repo / "tests" / "example_datasets" / "8e598964" / "AusAEM_02_NT&WA_AEM_Tranche1_GA_vsum_inversion"),
                method=method,
                engine=engine,
                clean_column_names=True
            )

            row = next(gdf.iterrows(usecols=["flight", "conductivity"]))
            assert row == {
                "Index": 0,
                "conductivity_0": "2.058674e-02",
                "conductivity_1": "2.245640e-02",
                "conductivity_10": "6.110007e-02",
                "conductivity_11": "4.741013e-02",
                "conductivity_12": "3.573940e-02",
                "conductivity_13": "2.653530e-02",
                "conductivity_14": "1.958333e-02",
                "conductivity_15": "1.446299e-02",
                "conductivity_16": "1.074203e-02",
                "conductivity_17": "8.055714e-03",
                "conductivity_18": "6.119678e-03",
                "conductivity_19": "4.722892e-03",
                "conductivity_2": "2.672326e-02",
                "conductivity_20": "3.711956e-03",
                "conductivity_21": "2.977024e-03",
                "conductivity_22": "2.440536e-03",
                "conductivity_23": "2.047553e-03",
                "conductivity_24": "1.759314e-03",
                "conductivity_25": "1.548537e-03",
                "conductivity_26": "1.395983e-03",
                "conductivity_27": "1.288301e-03",
                "conductivity_28": "1.216609e-03",
                "conductivity_29": "1.175960e-03",
                "conductivity_3": "3.434309e-02",
                "conductivity_4": "4.631136e-02",
                "conductivity_5": "6.222465e-02",
                "conductivity_6": "7.800699e-02",
                "conductivity_7": "8.694177e-02",
                "conductivity_8": "8.522267e-02",
                "conductivity_9": "7.498534e-02",
                "flight": 59
            }
