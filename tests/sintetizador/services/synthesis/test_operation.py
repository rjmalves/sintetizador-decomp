from unittest.mock import patch, MagicMock
from app.services.unitofwork import factory
from app.services.synthesis.operation import OperationSynthetizer
from app.model.operation.operationsynthesis import OperationSynthesis, UNITS
from app.services.deck.bounds import OperationVariableBounds
import pandas as pd
import numpy as np
from os.path import join
from app.internal.constants import (
    OPERATION_SYNTHESIS_METADATA_OUTPUT,
)
from tests.conftest import DECK_TEST_DIR
from idecomp.decomp import (
    DecOperSist,
    DecOperRee,
    DecOperUsih,
    DecOperUsit,
    DecOperInterc,
    Relato,
)


uow = factory("FS", DECK_TEST_DIR)


def __compara_sintese_dec_oper(
    df_sintese: pd.DataFrame,
    df_dec_oper: pd.DataFrame,
    col_dec_oper: str,
    *args,
    **kwargs,
):
    estagio = kwargs.get("estagio", 1)
    cenario = kwargs.get("cenario", 1)
    filtros_sintese = (df_sintese["estagio"] == estagio) & (
        df_sintese["cenario"] == cenario
    )
    filtros_dec_oper = (df_dec_oper["estagio"] == estagio) & (
        df_dec_oper["cenario"] == cenario
    )
    # Processa argumentos adicionais
    for col, val in kwargs.items():
        if col not in ["estagio", "cenario"]:
            if col in df_sintese.columns:
                filtros_sintese = filtros_sintese & (df_sintese[col].isin(val))
            if col in df_dec_oper.columns:
                filtros_dec_oper = filtros_dec_oper & (
                    df_dec_oper[col].isin(val)
                )

    dados_sintese = df_sintese.loc[filtros_sintese, "valor"].to_numpy()
    dados_dec_oper = df_dec_oper.loc[filtros_dec_oper, col_dec_oper].to_numpy()

    assert len(dados_sintese) > 0
    assert len(dados_dec_oper) > 0

    try:
        assert np.allclose(dados_sintese, dados_dec_oper, rtol=1e-2)
    except AssertionError:
        print("Síntese:")
        print(df_sintese.loc[filtros_sintese])
        print("Dec_oper:")
        print(df_dec_oper.loc[filtros_dec_oper])
        raise


def __valida_limites(
    df: pd.DataFrame, tol: float = 0.2, lower=True, upper=True
):
    num_amostras = df.shape[0]
    if upper:
        try:
            assert (
                df["valor"] <= (df["limite_superior"] + tol)
            ).sum() == num_amostras
        except AssertionError:
            print(df.loc[df["valor"] > (df["limite_superior"] + tol)])
            raise
    if lower:
        try:
            assert (
                df["valor"] >= (df["limite_inferior"] - tol)
            ).sum() == num_amostras
        except AssertionError:
            print(df.loc[df["valor"] < (df["limite_inferior"] - tol)])
            raise


def __valida_metadata(chave: str, df_metadata: pd.DataFrame, calculated: bool):
    s = OperationSynthesis.factory(chave)
    assert s is not None
    assert str(s) in df_metadata["chave"].tolist()
    assert s.variable.short_name in df_metadata["nome_curto_variavel"].tolist()
    assert s.variable.long_name in df_metadata["nome_longo_variavel"].tolist()
    assert (
        s.spatial_resolution.value
        in df_metadata["nome_curto_agregacao"].tolist()
    )
    assert (
        s.spatial_resolution.long_name
        in df_metadata["nome_longo_agregacao"].tolist()
    )
    unit_str = UNITS[s].value if s in UNITS else ""
    assert unit_str in df_metadata["unidade"].tolist()
    assert calculated in df_metadata["calculado"].tolist()
    assert (
        OperationVariableBounds.is_bounded(s)
        in df_metadata["limitado"].tolist()
    )


def __sintetiza_com_mock(synthesis_str) -> tuple[pd.DataFrame, pd.DataFrame]:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize([synthesis_str], uow)
        OperationSynthetizer.clear_cache()
    m.assert_called()
    df = __obtem_dados_sintese_mock(synthesis_str, m)
    df_meta = __obtem_dados_sintese_mock(OPERATION_SYNTHESIS_METADATA_OUTPUT, m)
    assert df is not None
    assert df_meta is not None
    return df, df_meta


def __sintetiza_com_mock_wildcard(synthesis_str) -> pd.DataFrame:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize([synthesis_str], uow)
        OperationSynthetizer.clear_cache()
    m.assert_called()
    df_meta = __obtem_dados_sintese_mock(OPERATION_SYNTHESIS_METADATA_OUTPUT, m)
    assert df_meta is not None
    return df_meta


def __obtem_dados_sintese_mock(
    chave: str, mock: MagicMock
) -> pd.DataFrame | None:
    for c in mock.mock_calls:
        if c.args[1] == chave:
            return c.args[0]
    return None


def test_sintese_cmo_sbm(test_settings):
    synthesis_str = "CMO_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = DecOperSist.read(
        join(DECK_TEST_DIR, "dec_oper_sist.csv")
    ).tabela
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "cmo",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        patamar=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_cter_sin(test_settings):
    synthesis_str = "CTER_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = Relato.read(
        join(DECK_TEST_DIR, "relato.rv0")
    ).relatorio_operacao_custos
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "geracao_termica",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_cop_sin(test_settings):
    synthesis_str = "COP_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = Relato.read(
        join(DECK_TEST_DIR, "relato.rv0")
    ).relatorio_operacao_custos
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "custo_presente",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_cfu_sin(test_settings):
    synthesis_str = "CFU_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = Relato.read(
        join(DECK_TEST_DIR, "relato.rv0")
    ).relatorio_operacao_custos
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "custo_futuro",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earmi_ree(test_settings):
    synthesis_str = "EARMI_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = DecOperRee.read(
        join(DECK_TEST_DIR, "dec_oper_ree.csv")
    ).tabela
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "earm_inicial_MWmes",
        estagio=1,
        cenario=1,
        codigo_ree=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earmi_sbm(test_settings):
    synthesis_str = "EARMI_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = (
        DecOperRee.read(join(DECK_TEST_DIR, "dec_oper_ree.csv"))
        .tabela.groupby(["estagio", "cenario", "codigo_submercado"])
        .sum()
        .reset_index()
    )
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "earm_inicial_MWmes",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earmi_sin(test_settings):
    synthesis_str = "EARMI_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = (
        DecOperRee.read(join(DECK_TEST_DIR, "dec_oper_ree.csv"))
        .tabela.groupby(["estagio", "cenario"])
        .sum()
        .reset_index()
    )
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "earm_inicial_MWmes",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earpi_ree(test_settings):
    synthesis_str = "EARPI_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = DecOperRee.read(
        join(DECK_TEST_DIR, "dec_oper_ree.csv")
    ).tabela
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "earm_inicial_percentual",
        estagio=1,
        cenario=1,
        codigo_ree=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earpi_sbm(test_settings):
    synthesis_str = "EARPI_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = (
        DecOperRee.read(join(DECK_TEST_DIR, "dec_oper_ree.csv"))
        .tabela.groupby(["estagio", "cenario", "codigo_submercado"])
        .sum()
        .reset_index()
    )
    df_dec_oper["earm_inicial_percentual"] = 100 * (
        df_dec_oper["earm_inicial_MWmes"] / df_dec_oper["earm_maximo_MWmes"]
    )
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "earm_inicial_percentual",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earpi_sin(test_settings):
    synthesis_str = "EARPI_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = (
        DecOperRee.read(join(DECK_TEST_DIR, "dec_oper_ree.csv"))
        .tabela.groupby(["estagio", "cenario"])
        .sum()
        .reset_index()
    )
    df_dec_oper["earm_inicial_percentual"] = 100 * (
        df_dec_oper["earm_inicial_MWmes"] / df_dec_oper["earm_maximo_MWmes"]
    )
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "earm_inicial_percentual",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earmf_ree(test_settings):
    synthesis_str = "EARMF_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = DecOperRee.read(
        join(DECK_TEST_DIR, "dec_oper_ree.csv")
    ).tabela
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "earm_final_MWmes",
        estagio=1,
        cenario=1,
        codigo_ree=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earmf_sbm(test_settings):
    synthesis_str = "EARMF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = (
        DecOperRee.read(join(DECK_TEST_DIR, "dec_oper_ree.csv"))
        .tabela.groupby(["estagio", "cenario", "codigo_submercado"])
        .sum()
        .reset_index()
    )
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "earm_final_MWmes",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earmf_sin(test_settings):
    synthesis_str = "EARMF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = (
        DecOperRee.read(join(DECK_TEST_DIR, "dec_oper_ree.csv"))
        .tabela.groupby(["estagio", "cenario"])
        .sum()
        .reset_index()
    )
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "earm_final_MWmes",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earpf_ree(test_settings):
    synthesis_str = "EARPF_REE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = DecOperRee.read(
        join(DECK_TEST_DIR, "dec_oper_ree.csv")
    ).tabela
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "earm_final_percentual",
        estagio=1,
        cenario=1,
        codigo_ree=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earpf_sbm(test_settings):
    synthesis_str = "EARPF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = DecOperSist.read(
        join(DECK_TEST_DIR, "dec_oper_sist.csv")
    ).tabela
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "earm_final_percentual",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earpf_sin(test_settings):
    synthesis_str = "EARPF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = (
        DecOperRee.read(join(DECK_TEST_DIR, "dec_oper_ree.csv"))
        .tabela.groupby(["estagio", "cenario"])
        .sum()
        .reset_index()
    )
    df_dec_oper["earm_final_percentual"] = 100 * (
        df_dec_oper["earm_final_MWmes"] / df_dec_oper["earm_maximo_MWmes"]
    )
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "earm_final_percentual",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_gter_sbm(test_settings):
    synthesis_str = "GTER_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = DecOperSist.read(
        join(DECK_TEST_DIR, "dec_oper_sist.csv")
    ).tabela
    df_dec_oper["geracao_termica_MW"] += df_dec_oper[
        "geracao_termica_antecipada_MW"
    ]
    __compara_sintese_dec_oper(
        df,
        df_dec_oper,
        "geracao_termica_MW",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        patamar=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_gter_sin(test_settings):
    synthesis_str = "GTER_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = (
        DecOperSist.read(join(DECK_TEST_DIR, "dec_oper_sist.csv"))
        .tabela.groupby(["estagio", "cenario", "patamar"])
        .sum()
        .reset_index()
    )
    df_dec_oper["geracao_termica_MW"] += df_dec_oper[
        "geracao_termica_antecipada_MW"
    ]
    __compara_sintese_dec_oper(
        df, df_dec_oper, "geracao_termica_MW", estagio=1, cenario=1, patamar=[1]
    )
    __valida_metadata(synthesis_str, df_meta, False)
