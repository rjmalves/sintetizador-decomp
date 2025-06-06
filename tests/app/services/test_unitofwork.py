from app.services.unitofwork import factory
import pandas as pd
from unittest.mock import patch
from tests.conftest import DECK_TEST_DIR


def test_fs_uow(test_settings):
    uow = factory("FS", DECK_TEST_DIR)
    with uow:
        dadger = uow.files.get_dadger()
        assert dadger is not None
        with patch("pyarrow.parquet.write_table"):
            uow.export.synthetize_df(pd.DataFrame(), "CMO_SBM")
