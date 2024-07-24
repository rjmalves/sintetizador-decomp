from unittest.mock import patch, MagicMock
from app.services.unitofwork import factory
from app.services.synthesis.system import SystemSynthetizer
from app.model.system.systemsynthesis import SystemSynthesis
import numpy as np
from tests.conftest import DECK_TEST_DIR
import pandas as pd
from app.internal.constants import (
    SYSTEM_SYNTHESIS_METADATA_OUTPUT,
    STAGE_COL,
    START_DATE_COL,
    END_DATE_COL,
    BLOCK_COL,
    VALUE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    THERMAL_CODE_COL,
    THERMAL_NAME_COL,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    EER_CODE_COL,
    EER_NAME_COL,
)
from os.path import join
from idecomp.decomp import DecEcoDiscr
from datetime import datetime

uow = factory("FS", DECK_TEST_DIR)
synthetizer = SystemSynthetizer()


def __sintetiza_com_mock(synthesis_str) -> tuple[pd.DataFrame, pd.DataFrame]:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        SystemSynthetizer.synthetize([synthesis_str], uow)

    m.assert_called()

    df = __obtem_dados_sintese_mock(synthesis_str, m)
    df_meta = __obtem_dados_sintese_mock(SYSTEM_SYNTHESIS_METADATA_OUTPUT, m)
    assert df is not None
    assert df_meta is not None
    return df, df_meta


def __obtem_dados_sintese_mock(
    chave: str, mock: MagicMock
) -> pd.DataFrame | None:
    for c in mock.mock_calls:
        if c.args[1] == chave:
            return c.args[0]
    return None


def __valida_metadata(chave: str, df_metadata: pd.DataFrame):
    s = SystemSynthesis.factory(chave)
    assert s is not None
    assert str(s) in df_metadata["chave"].tolist()
    assert s.variable.short_name in df_metadata["nome_curto"].tolist()
    assert s.variable.long_name in df_metadata["nome_longo"].tolist()


def test_sintese_est(test_settings):
    synthesis_str = "EST"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - pegar dados direto da leitura do deck de decomp
    assert df.at[0, STAGE_COL] == 1
    assert df.at[0, START_DATE_COL] == datetime(day=27, month=4, year=2024)
    assert df.at[0, END_DATE_COL] == datetime(day=4, month=5, year=2024)
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_pat(test_settings):
    synthesis_str = "PAT"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - pegar dados direto da leitura do deck de decomp
    assert df.at[0, STAGE_COL] == 1
    assert df.at[0, START_DATE_COL] == datetime(day=27, month=4, year=2024)
    assert df.at[0, BLOCK_COL] == 1
    assert df.at[0, VALUE_COL] == 28
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_sbm(test_settings):
    synthesis_str = "SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - pegar dados direto da leitura do deck de decomp
    assert df.at[0, SUBMARKET_CODE_COL] == 1
    assert df.at[0, SUBMARKET_NAME_COL] == "SE"
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_ute(test_settings):
    synthesis_str = "UTE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - pegar dados direto da leitura do deck de decomp
    assert df.at[0, THERMAL_CODE_COL] == 1
    assert df.at[0, THERMAL_NAME_COL] == "ANGRA 1"
    assert df.at[0, SUBMARKET_CODE_COL] == 1
    assert df.at[0, SUBMARKET_NAME_COL] == "SE"
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_uhe(test_settings):
    synthesis_str = "UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    # TODO - pegar dados direto da leitura do deck de decomp
    assert df.at[0, HYDRO_CODE_COL] == 108
    assert df.at[0, HYDRO_NAME_COL] == "TRAICAO"
    assert df.at[0, EER_CODE_COL] == 1
    assert df.at[0, EER_NAME_COL] == "SUDESTE"
    assert df.at[0, SUBMARKET_CODE_COL] == 1
    assert df.at[0, SUBMARKET_NAME_COL] == "SE"
    __valida_metadata(synthesis_str, df_meta)
