from os.path import join
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from idecomp.decomp import Decomptim, Relato

from app.internal.constants import (
    EXECUTION_SYNTHESIS_METADATA_OUTPUT,
)
from app.model.execution.executionsynthesis import ExecutionSynthesis
from app.services.synthesis.execution import ExecutionSynthetizer
from app.services.unitofwork import factory
from tests.conftest import DECK_TEST_DIR

uow = factory("FS", DECK_TEST_DIR)
synthetizer = ExecutionSynthetizer()


def __sintetiza_com_mock(synthesis_str) -> tuple[pd.DataFrame, pd.DataFrame]:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ExecutionSynthetizer.synthetize([synthesis_str], uow)

    m.assert_called()

    df = __obtem_dados_sintese_mock(synthesis_str, m)
    df_meta = __obtem_dados_sintese_mock(EXECUTION_SYNTHESIS_METADATA_OUTPUT, m)
    print(df)
    print(df_meta)
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
    s = ExecutionSynthesis.factory(chave)
    assert s is not None
    assert str(s) in df_metadata["chave"].tolist()
    assert s.variable.short_name in df_metadata["nome_curto"].tolist()
    assert s.variable.long_name in df_metadata["nome_longo"].tolist()


def test_sintese_programa(test_settings):
    synthesis_str = "PROGRAMA"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    assert df.at[0, "programa"] == "DECOMP"
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_versao(test_settings):
    synthesis_str = "VERSAO"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    assert df.at[0, "versao"] == "31.21"
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_titulo(test_settings):
    synthesis_str = "TITULO"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    assert (
        df.at[0, "titulo"]
        == "PMO - MAIO/24 - JUNHO/24 - REV 0 - FCF COM CVAR - 12 REE - VALOR ESPERADO"
    )
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_convergencia(test_settings):
    synthesis_str = "CONVERGENCIA"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_relato = Relato.read(join(DECK_TEST_DIR, "relato.rv0")).convergencia
    iteracao = int(df["iteracao"].max())
    dados_sintese = df.loc[df["iteracao"] == iteracao].to_numpy()
    dados_relato = df_relato.loc[df_relato["iteracao"] == iteracao][
        [
            "iteracao",
            "zinf",
            "zsup",
            "gap_percentual",
            "tempo",
            "deficit_demanda_MWmed",
            "numero_inviabilidades",
            "inviabilidades_MWmed",
            "inviabilidades_m3s",
            "inviabilidades_hm3",
        ]
    ]

    dados_relato["delta_zinf"] = (
        dados_relato["zinf"].iloc[0]
        - df_relato.loc[df_relato["iteracao"] == (iteracao - 1)]["zinf"].iloc[0]
    ) / dados_relato["zinf"].iloc[0]

    dados_relato["tempo"] = (
        dados_relato["tempo"].iloc[0]
        - df_relato.loc[df_relato["iteracao"] == (iteracao - 1)]["tempo"].iloc[
            0
        ]
    )

    dados_relato["execucao"] = 0
    dados_relato = dados_relato.to_numpy()

    assert np.allclose(dados_sintese, dados_relato, rtol=1e-2)
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_tempo(test_settings):
    synthesis_str = "TEMPO"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_tempo = Decomptim.read(join(DECK_TEST_DIR, "decomp.tim")).tempos_etapas
    dados_sintese = df["tempo"].to_numpy()
    dados_decomptim = df_tempo["Tempo"].dt.total_seconds()[0:-1].to_numpy()

    assert np.allclose(dados_sintese, dados_decomptim, rtol=1e-2)
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_custos(test_settings):
    synthesis_str = "CUSTOS"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    df_relato = Relato.read(
        join(DECK_TEST_DIR, "relato.rv0")
    ).relatorio_operacao_custos
    dados_sintese = df["valor_esperado"].to_numpy()
    dados_relato = [float(df_relato[c].sum()) for c in df["parcela"].tolist()]

    assert np.allclose(dados_sintese, dados_relato, rtol=1e-2)
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_inviabilidades(test_settings):
    synthesis_str = "INVIABILIDADES"
    df, df_meta = __sintetiza_com_mock(synthesis_str)

    assert df.at[0, "tipo"] == "RE"
    assert df.at[0, "iteracao"] == 2
    assert df.at[0, "cenario"] == 1
    assert df.at[0, "estagio"] == 4
    assert df.at[0, "codigo"] == 181
    assert df.at[0, "violacao"] == 3.52589265
    assert df.at[0, "unidade"] == "MWmed"
    assert df.at[0, "patamar"] == 1
    assert df.at[0, "limite"] == "L. INF"
    assert df.at[0, "submercado"] is None
    assert df.at[0, "execucao"] == 0
    __valida_metadata(synthesis_str, df_meta)
