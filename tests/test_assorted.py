import os, sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from pathlib import Path

repo = Path(__file__).parent.parent

import pytest

import aseg_gdf2


@pytest.mark.parametrize("engine", ["pandas", "dask"])
@pytest.mark.parametrize("method", ["whitespace", "fixed-widths"])
class TestAssorted:
    def test_read_ext_dfn(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "aseg_examples" / "Example_AeroMag_MuppetTown_2009.dfn"),
            engine=engine,
            method=method
        )

    def test_read_ext_des(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "aseg_examples" / "Example_AeroMag_MuppetTown_2009.des"),
            engine=engine,
            method=method
        )

    def test_read_ext_dat(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "aseg_examples" / "Example_AeroMag_MuppetTown_2009.dat"),
            engine=engine,
            method=method
        )

    def test_read_no_ext(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "aseg_examples" / "Example_AeroMag_MuppetTown_2009"),
            engine=engine,
            method=method
        )

    def test_read_incorrect(self, engine, method):
        with pytest.raises(OSError):
            gdf = aseg_gdf2.read(
                str(
                    repo
                    / "tests"
                    / "aseg_examples"
                    / "Example_AeroMag_MuppetTown_2009.data"
                ),
                engine=engine,
            method=method
            )

    def test_find_dat_file(self, engine, method):
        gdf = aseg_gdf2.read(
            str(repo / "tests" / "aseg_examples" / "Example_AeroMag_MuppetTown_2009"),
            engine=engine,
            method=method
        )
        assert gdf.dat_filename.lower().endswith("example_aeromag_muppettown_2009.dat")
