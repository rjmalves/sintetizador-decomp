from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from app.internal.constants import (
    COEF_TYPE_COL,
    COEF_VALUE_COL,
    CUT_INDEX_COL,
    ENTITY_INDEX_COL,
    ITERATION_COL,
    POLICY_SYNTHESIS_METADATA_OUTPUT,
    SCENARIO_COL,
    STAGE_COL,
    STATE_VALUE_COL,
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


def test_synthesis_cortes_coeficientes(test_settings: None):
    synthesis_str = "CORTES_COEFICIENTES"
    df, df_meta = __synthetize_with_mock(synthesis_str)

    # TODO improve comparison: function that compares df directly with dec_fcf_cortes
    assert df.at[0, STAGE_COL] == 1
    assert df.at[0, CUT_INDEX_COL] == 34
    assert df.at[0, ITERATION_COL] == 1
    assert np.isnan(df.at[0, SCENARIO_COL])
    assert df.at[0, COEF_TYPE_COL] == 1
    assert df.at[0, ENTITY_INDEX_COL] == 0
    assert df.at[0, COEF_VALUE_COL] == 133525140.6137163
    assert df.at[0, STATE_VALUE_COL] == 123349094.0040892

    __validate_metadata(synthesis_str, df_meta)


def test_synthesis_cortes_variaveis(test_settings: None):
    synthesis_str = "CORTES_VARIAVEIS"
    df, df_meta = __synthetize_with_mock(synthesis_str)

    assert df.at[0, COEF_TYPE_COL] == 1
    assert df.at[0, "nome_curto_coeficiente"] == "RHS"
    assert df.at[0, "nome_longo_coeficiente"] == "Right Hand Side"
    assert df.at[0, "unidade_coeficiente"] == "10^3 R$"
    assert df.at[0, "unidade_estado"] == "10^3 R$"

    __validate_metadata(synthesis_str, df_meta)
