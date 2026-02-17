"""
Tests for the GDF2 class in aseg_gdf2
"""
import os
import sys

from pathlib import Path

import pytest

import aseg_gdf2

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
repo = Path(__file__).parent.parent

data_src_1 = os.path.join(
    repo, "tests", "example_datasets", "3bcfc711", "GA1286_Waveforms"
)


@pytest.mark.parametrize("engine", ["pandas", "dask"])
@pytest.mark.parametrize("method", ["whitespace", "fixed-widths"])
class TestGDF2Class:
    def test_repr_1(self, engine, method):
        """
        GDF2's repr will display the number of records: nrecords
        """
        gdf = aseg_gdf2.read(data_src_1, engine=engine, method=method)

        # Gdf.nrecords is not called yet, so it is set to "?"
        assert gdf.__repr__().endswith("nrecords=?>")
        # Call gdf.nrecords
        nrecords = gdf.nrecords
        assert nrecords == 23040
        # After nrecords is called nrecords will be repored in __repr__
        assert gdf.__repr__().endswith("nrecords=23040>")

    def test_get_column_definitions_1(self, engine, method):
        """
        GDF2.get_column_definitions() will return a list of dictionaries
        """
        gdf = aseg_gdf2.read(data_src_1, engine=engine, method=method)
        res = gdf.get_column_definitions()
        assert len(res) == 5
        for item in res:
            assert "name" in item
