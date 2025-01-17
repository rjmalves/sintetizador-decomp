from datetime import datetime, timedelta
from os.path import join
from unittest.mock import MagicMock, patch

import pandas as pd
from idecomp.decomp import Dadger
from idecomp.decomp.dec_fcf_cortes import DecFcfCortes

from app.internal.constants import (
    POLICY_SYNTHESIS_METADATA_OUTPUT,
)
from app.model.policy.policysynthesis import PolicySynthesis
from app.services.synthesis.policy import PolicySynthetizer
from app.services.unitofwork import factory
from tests.conftest import DECK_TEST_DIR

uow = factory("FS", DECK_TEST_DIR)
synthetizer = PolicySynthetizer()


def __synthetize_with_mock(synthesis_str) -> tuple[pd.DataFrame, pd.DataFrame]:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        PolicySynthetizer.synthetize([synthesis_str], uow)

    m.assert_called()

    df = __get_synthesis_mock(synthesis_str, m)
    df_meta = __get_synthesis_mock(POLICY_SYNTHESIS_METADATA_OUTPUT, m)
    assert df is not None
    assert df_meta is not None
    return df, df_meta


def __get_synthesis_mock(key: str, mock: MagicMock) -> pd.DataFrame | None:
    for c in mock.mock_calls:
        if c.args[1] == key:
            return c.args[0]
    return None


def __validate_metadata(key: str, df_metadata: pd.DataFrame):
    s = PolicySynthesis.factory(key)
    assert s is not None
    assert str(s) in df_metadata["chave"].tolist()
    assert s.variable.short_name in df_metadata["nome_curto"].tolist()
    assert s.variable.long_name in df_metadata["nome_longo"].tolist()


def test_synthesis_est(test_settings):
    synthesis_str = "CORTES"
    df, df_meta = __synthetize_with_mock(synthesis_str)
    # dadger = Dadger.read(join(DECK_TEST_DIR, "dadger.rv0")).dt
    # start_date = datetime(day=dadger.dia, month=dadger.mes, year=dadger.ano)
    # assert df.at[0, STAGE_COL] == 1
    # assert df.at[0, START_DATE_COL] == start_date
    # assert df.at[0, END_DATE_COL] == start_date + timedelta(days=7)
    __validate_metadata(synthesis_str, df_meta)
