from typing import TYPE_CHECKING, Callable

import pandas as pd

from app.internal.constants import (
    BLOCK_COL,
    EER_CODE_COL,
    SUBMARKET_CODE_COL,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def stub_EVER(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """
    Realiza o cálculo da energia vertida a partir dos valores
    das energias vertidas turbinável e não-turbinável.
    """
    turbineable_synthesis = OperationSynthesis(
        Variable.ENERGIA_VERTIDA_TURBINAVEL,
        synthesis.spatial_resolution,
    )
    non_turbineable_synthesis = OperationSynthesis(
        Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
        synthesis.spatial_resolution,
    )
    turbineable_df = cls._get_from_cache(turbineable_synthesis)
    spilled_df = cls._get_from_cache(non_turbineable_synthesis)

    spilled_df.loc[:, VALUE_COL] = (
        turbineable_df[VALUE_COL].to_numpy() + spilled_df[VALUE_COL].to_numpy()
    )
    return spilled_df


def stub_grouping_hydro(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """
    Realiza o cálculo de uma variável agrupando a partir
    da extração feita por UHE.
    """
    hydro_synthesis = OperationSynthesis(
        synthesis.variable,
        SpatialResolution.USINA_HIDROELETRICA,
    )

    hydro_df = cls._get_from_cache(hydro_synthesis)

    grouping_col_map = {
        SpatialResolution.RESERVATORIO_EQUIVALENTE: EER_CODE_COL,
        SpatialResolution.SUBMERCADO: SUBMARKET_CODE_COL,
        SpatialResolution.SISTEMA_INTERLIGADO: None,
    }

    return cls._group_hydro_df(
        hydro_df,
        grouping_column=grouping_col_map[synthesis.spatial_resolution],
    )


def stub_grouping_submarket(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """
    Realiza o cálculo de uma variável agrupando a partir da extração
    feita por SBM.
    """
    submarket_synthesis = OperationSynthesis(
        synthesis.variable,
        SpatialResolution.SUBMERCADO,
    )

    hydro_df = cls._get_from_cache(submarket_synthesis)

    grouping_col_map = {
        SpatialResolution.SISTEMA_INTERLIGADO: None,
    }

    return cls._group_hydro_df(
        hydro_df,
        grouping_column=grouping_col_map[synthesis.spatial_resolution],
    )


def stub_GHID_REE(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """
    Realiza o cálculo da geração hidráulica a partir da extração
    feita por UHE.
    """
    hydro_synthesis = OperationSynthesis(
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.USINA_HIDROELETRICA,
    )

    hydro_df = cls._get_from_cache(hydro_synthesis)

    grouping_col_map = {
        SpatialResolution.RESERVATORIO_EQUIVALENTE: EER_CODE_COL,
        SpatialResolution.SUBMERCADO: SUBMARKET_CODE_COL,
        SpatialResolution.SISTEMA_INTERLIGADO: None,
    }

    return cls._group_hydro_df(
        hydro_df,
        grouping_column=grouping_col_map[synthesis.spatial_resolution],
    )


def stub_valid_values_dec_oper_sist(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
    col: str,
    blocks: list[int] | None = None,
) -> pd.DataFrame:
    df = cls._resolve_dec_oper_sist(uow, col)
    filters = ~df[VALUE_COL].isna()
    if blocks:
        filters &= df[BLOCK_COL].isin(blocks)
    df = df.loc[filters].reset_index(drop=True)
    return df


def stub_stored_volume_dec_oper_usih(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
    col: str,
) -> pd.DataFrame:
    df = cls._resolve_dec_oper_usih(uow, col)
    df = df.loc[(df[BLOCK_COL] == 0) & (~df[VALUE_COL].isna())].reset_index(
        drop=True
    )
    return df


def stub_thermal_submarkets_dec_oper_sist(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
    col: str,
) -> pd.DataFrame:
    df = cls._resolve_dec_oper_sist(uow, col)
    thermals = Deck.thermals(uow)
    submarkets = thermals[SUBMARKET_CODE_COL].unique().tolist()
    df = df.loc[df[SUBMARKET_CODE_COL].isin(submarkets)].reset_index(drop=True)
    return df


def stub_percent_SIN(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """
    Realiza o cálculo de uma variável em percentual para o SIN a
    partir da extração feita por SBM.
    """
    earpi = Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL
    earmi = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL
    earpf = Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL
    earmf = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL

    variable_map = {
        earpi: earmi,
        earpf: earmf,
    }
    absolute_submarket_synthesis = OperationSynthesis(
        variable_map[synthesis.variable],
        SpatialResolution.SUBMERCADO,
    )
    percent_submarket_synthesis = OperationSynthesis(
        synthesis.variable,
        SpatialResolution.SUBMERCADO,
    )

    # Groups absolute values
    absolute_df = cls._get_from_cache(absolute_submarket_synthesis)
    result_df = cls._group_submarket_df(
        absolute_df,
        grouping_column=None,
    )
    # Calculates capacity
    percent_df = cls._get_from_cache(percent_submarket_synthesis)
    percent_df[VALUE_COL] = (
        100
        * absolute_df[VALUE_COL].to_numpy()
        / percent_df[VALUE_COL].to_numpy()
    )
    capacity_df = cls._group_submarket_df(
        percent_df,
        grouping_column=None,
    )
    # Calculates percentage
    result_df[VALUE_COL] = 100 * (result_df[VALUE_COL] / capacity_df[VALUE_COL])
    return result_df


def stub_mappings(
    cls: "type[OperationSynthetizer]", s: OperationSynthesis
) -> Callable[..., pd.DataFrame] | None:
    if s.variable == Variable.ENERGIA_VERTIDA:
        return lambda synthesis, uow: stub_EVER(cls, synthesis, uow)

    if (
        s.variable
        in [
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        ]
        and s.spatial_resolution != SpatialResolution.USINA_HIDROELETRICA
    ):
        return lambda synthesis, uow: stub_grouping_hydro(cls, synthesis, uow)

    if (
        s.variable
        in [
            Variable.MERCADO,
            Variable.MERCADO_LIQUIDO,
            Variable.DEFICIT,
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            Variable.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
            Variable.GERACAO_HIDRAULICA,
            Variable.GERACAO_TERMICA,
            Variable.GERACAO_USINAS_NAO_SIMULADAS,
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
        ]
        and s.spatial_resolution == SpatialResolution.SISTEMA_INTERLIGADO
    ):
        return lambda synthesis, uow: stub_grouping_submarket(
            cls, synthesis, uow
        )

    if (
        s.variable == Variable.GERACAO_HIDRAULICA
        and s.spatial_resolution == SpatialResolution.RESERVATORIO_EQUIVALENTE
    ):
        return lambda synthesis, uow: stub_GHID_REE(cls, synthesis, uow)

    if (
        s.variable
        in [
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
        ]
        and s.spatial_resolution == SpatialResolution.SISTEMA_INTERLIGADO
    ):
        return lambda synthesis, uow: stub_percent_SIN(cls, synthesis, uow)

    return None
