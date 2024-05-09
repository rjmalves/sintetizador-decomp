from unittest.mock import patch, MagicMock
from app.services.unitofwork import factory
from app.services.synthesis.execution import ExecutionSynthetizer
import numpy as np
from tests.conftest import DECK_TEST_DIR

uow = factory("FS", DECK_TEST_DIR)
synthetizer = ExecutionSynthetizer()


def test_sintese_programa(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["PROGRAMA"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "programa"] == "DECOMP"


def test_sintese_convergencia(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["CONVERGENCIA"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "iter"] == 1
    assert df.at[0, "zinf"] == 219143667.6
    assert df.at[0, "zsup"] == 219143975.6
    assert df.at[0, "gap"] == 0.0001405
    assert df.at[0, "tempo"] == 114
    assert df.at[0, "deficit"] == 0.0
    assert df.at[0, "numero_inviabilidades"] == 0.0
    assert df.at[0, "viol_MWmed"] == 0.0
    assert df.at[0, "viol_m3s"] == 0.0
    assert df.at[0, "viol_hm3"] == 0.0
    assert np.isnan(df.at[0, "dZinf"])
    assert df.at[0, "execucao"] == 0.0


def test_sintese_tempo(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["TEMPO"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "etapa"] == "Leitura de Dados"
    assert df.at[0, "tempo"] == 5.0
    assert df.at[0, "execucao"] == 0.0


def test_sintese_custos(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["CUSTOS"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[1, "parcela"] == "GERACAO TERMICA"
    assert df.at[1, "mean"] == 498476.99
    assert df.at[0, "std"] == 0.0


def test_sintese_inviabilidades(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["INVIABILIDADES"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]

    assert df.at[0, "tipo"] == "TI"
    assert df.at[0, "iteracao"] == 2
    assert df.at[0, "cenario"] == 161
    assert df.at[0, "estagio"] == 6
    assert df.at[0, "codigo"] == 71.0
    assert df.at[0, "violacao"] == 4.35584346
    assert df.at[0, "unidade"] == "m3/s"
    assert np.isnan(df.at[0, "patamar"])
    assert np.isnan(df.at[0, "limite"])
    assert np.isnan(df.at[0, "submercado"])
    assert df.at[0, "execucao"] == 0
