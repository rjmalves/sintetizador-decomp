from unittest.mock import patch, MagicMock
from app.services.unitofwork import factory
from app.services.synthesis.execution import ExecutionSynthetizer
import numpy as np
from tests.conftest import DECK_TEST_DIR
import pandas as pd
from app.internal.constants import (
    OPERATION_SYNTHESIS_METADATA_OUTPUT,
)

uow = factory("FS", DECK_TEST_DIR)
synthetizer = ExecutionSynthetizer()


def __sintetiza_com_mock(synthesis_str) -> tuple[pd.DataFrame, pd.DataFrame]:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ExecutionSynthetizer.synthetize([synthesis_str], uow)
        # ExecutionSynthetizer.clear_cache()
    m.assert_called()
    df = __obtem_dados_sintese_mock(synthesis_str, m)
    # df_meta = __obtem_dados_sintese_mock(OPERATION_SYNTHESIS_METADATA_OUTPUT, m)
    assert df is not None
    # assert df_meta is not None
    # return df, df_meta
    return df


def __obtem_dados_sintese_mock(
    chave: str, mock: MagicMock
) -> pd.DataFrame | None:
    for c in mock.mock_calls:
        if c.args[1] == chave:
            return c.args[0]
    return None


def test_sintese_programa(test_settings):
    synthesis_str = "PROGRAMA"
    df = __sintetiza_com_mock(synthesis_str)
    # TODO df_meta
    assert df.at[0, "programa"] == "DECOMP"


# def test_sintese_programa(test_settings):
#     m = MagicMock(lambda df, filename: df)
#     with patch(
#         "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
#         new=m,
#     ):
#         synthetizer.synthetize(["PROGRAMA"], uow)
#     m.assert_called_once()
#     df = m.mock_calls[0].args[0]
#     assert df.at[0, "programa"] == "DECOMP"


# def test_sintese_convergencia(test_settings):
#     m = MagicMock(lambda df, filename: df)
#     with patch(
#         "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
#         new=m,
#     ):
#         synthetizer.synthetize(["CONVERGENCIA"], uow)
#     m.assert_called_once()
#     df = m.mock_calls[0].args[0]
#     assert df.at[0, "iter"] == 1
#     assert df.at[0, "zinf"] == 219143667.6
#     assert df.at[0, "zsup"] == 219143975.6
#     assert df.at[0, "gap"] == 0.0001405
#     assert df.at[0, "tempo"] == 114
#     assert df.at[0, "deficit"] == 0.0
#     assert df.at[0, "numero_inviabilidades"] == 0.0
#     assert df.at[0, "viol_MWmed"] == 0.0
#     assert df.at[0, "viol_m3s"] == 0.0
#     assert df.at[0, "viol_hm3"] == 0.0
#     assert np.isnan(df.at[0, "dZinf"])
#     assert df.at[0, "execucao"] == 0.0


# def test_sintese_tempo(test_settings):
#     m = MagicMock(lambda df, filename: df)
#     with patch(
#         "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
#         new=m,
#     ):
#         synthetizer.synthetize(["TEMPO"], uow)
#     m.assert_called_once()
#     df = m.mock_calls[0].args[0]
#     assert df.at[0, "etapa"] == "Leitura de Dados"
#     assert df.at[0, "tempo"] == 5.0
#     assert df.at[0, "execucao"] == 0.0


# def test_sintese_custos(test_settings):
#     m = MagicMock(lambda df, filename: df)
#     with patch(
#         "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
#         new=m,
#     ):
#         synthetizer.synthetize(["CUSTOS"], uow)
#     m.assert_called_once()
#     df = m.mock_calls[0].args[0]
#     assert df.at[1, "parcela"] == "geracao_termica"
#     assert df.at[1, "valor_esperado"] == 498476.99
#     assert df.at[0, "desvio_padrao"] == 0.0


# def test_sintese_inviabilidades(test_settings):
#     m = MagicMock(lambda df, filename: df)
#     with patch(
#         "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
#         new=m,
#     ):
#         synthetizer.synthetize(["INVIABILIDADES"], uow)
#     m.assert_called_once()
#     df = m.mock_calls[0].args[0]

#     assert df.at[0, "tipo"] == "TI"
#     assert df.at[0, "iteracao"] == 2
#     assert df.at[0, "cenario"] == 161
#     assert df.at[0, "estagio"] == 6
#     assert df.at[0, "codigo"] == 71.0
#     assert df.at[0, "violacao"] == 4.35584346
#     assert df.at[0, "unidade"] == "m3/s"
#     assert np.isnan(df.at[0, "patamar"])
#     assert np.isnan(df.at[0, "limite"])
#     assert np.isnan(df.at[0, "submercado"])
#     assert df.at[0, "execucao"] == 0
