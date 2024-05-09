from unittest.mock import patch, MagicMock
from app.services.unitofwork import factory
from app.services.synthesis.operation import OperationSynthetizer

from tests.conftest import DECK_TEST_DIR

uow = factory("FS", DECK_TEST_DIR)
synthetizer = OperationSynthetizer()


def test_sintese_sin_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["EARPF_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert round(df.at[0, "valor"], 2) == 81.59


def test_sintese_sbm_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["EARPF_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 78.16


def test_sintese_uhe_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["QAFL_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 136.0


def test_sintese_sbp_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["INT_SBP_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 563.88


def test_sintese_sin_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["GHID_SIN_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]

    assert df.at[0, "valor"] == 62994.8


def test_sintese_sbm_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["GTER_SBM_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 2660


def test_sintese_ree_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["GHID_REE_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 8650.46


def test_sintese_uhe_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["GHID_UHE_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0


def test_sintese_sbp_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["INT_SBP_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.0


def test_sintese_ever_total(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["EVER_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 74449.6


def test_sintese_qtur_qver_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["QTUR_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 0.52


def test_sintese_qdef_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["QDEF_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 128.27


def test_sintese_earmi_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["EARMI_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 171002.91


def test_sintese_earmi_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["EARMI_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 249301.67


def test_sintese_earpi_sin(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["EARPI_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert round(df.at[0, "valor"], 2) == 85.49


def test_sintese_varmi_uhe(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["VARMI_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 668.3


def test_sintese_varmi_sbm(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["VARMI_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 47525.25


def test_sintese_earmi_ree(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["EARMI_REE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "valor"] == 40150.0


def test_sintese_gter_ute_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["GTER_UTE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]

    assert df.at[0, "valor"] == 76.0


def test_sintese_gter_ute_pat(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["GTER_UTE_PAT"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]

    assert df.at[0, "valor"] == 76.0


def test_sintese_ever_ree_est(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["EVER_REE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]

    assert df.at[0, "valor"] == 1604.6


def test_sintese_custos(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["COP_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]

    assert df.at[0, "valor"] == 95411.65
