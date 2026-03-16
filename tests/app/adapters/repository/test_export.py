from app.adapters.repository.export import factory, TestExportRepository
import pandas as pd
import polars as pl
from unittest.mock import patch

from tests.conftest import DECK_TEST_DIR


def test_export_csv(test_settings):
    repo = factory("CSV", DECK_TEST_DIR)
    with patch("pandas.DataFrame.to_csv"):
        repo.synthetize_df(pd.DataFrame(), "CMO_SBM")


def test_export_parquet(test_settings):
    repo = factory("PARQUET", DECK_TEST_DIR)
    with patch("pyarrow.parquet.write_table"):
        repo.synthetize_df(pd.DataFrame(), "CMO_SBM")


def test_parquet_synthetize_pl_writes_file(tmp_path):
    repo = factory("PARQUET", str(tmp_path))
    df = pl.DataFrame({"valor": [1.0, 2.0, 3.0]})
    result = repo.synthetize_pl(df, "test_output")
    assert result is True
    assert (tmp_path / "test_output.parquet").is_file()


def test_test_export_repository_synthetize_df_returns_true():
    repo = TestExportRepository(str(DECK_TEST_DIR))
    result = repo.synthetize_df(pd.DataFrame(), "any_file")
    assert result is True
