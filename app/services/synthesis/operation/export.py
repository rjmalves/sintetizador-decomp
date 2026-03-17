from typing import TYPE_CHECKING

import pandas as pd
import polars as pl

from app.internal.constants import (
    OPERATION_SYNTHESIS_METADATA_OUTPUT,
    OPERATION_SYNTHESIS_STATS_ROOT,
    STRING_DF_TYPE,
    VARIABLE_COL,
)
from app.model.operation.operationsynthesis import (
    SYNTHESIS_DEPENDENCIES,
    UNITS,
    OperationSynthesis,
)
from app.services.deck.bounds import OperationVariableBounds
from app.services.deck.deck import Deck
from app.services.synthesis.operation.cache import store_in_cache_if_needed
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.operations import calc_statistics
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def export_metadata(
    cls: "type[OperationSynthetizer]",
    success_synthesis: list[OperationSynthesis],
    uow: AbstractUnitOfWork,
) -> None:
    metadata_df = pd.DataFrame(
        columns=[
            "chave",
            "nome_curto_variavel",
            "nome_longo_variavel",
            "nome_curto_agregacao",
            "nome_longo_agregacao",
            "unidade",
            "calculado",
            "limitado",
        ]
    )
    for s in success_synthesis:
        metadata_df.loc[metadata_df.shape[0]] = [
            str(s),
            s.variable.short_name,
            s.variable.long_name,
            s.spatial_resolution.value,
            s.spatial_resolution.long_name,
            UNITS[s].value if s in UNITS else "",
            s in SYNTHESIS_DEPENDENCIES,
            OperationVariableBounds.is_bounded(s),
        ]
    with uow:
        uow.export.synthetize_df(
            metadata_df, OPERATION_SYNTHESIS_METADATA_OUTPUT
        )


def add_synthesis_stats(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    df: pd.DataFrame,
) -> None:
    df[VARIABLE_COL] = s.variable.value

    if s.spatial_resolution not in cls.SYNTHESIS_STATS:
        cls.SYNTHESIS_STATS[s.spatial_resolution] = [df]
    else:
        cls.SYNTHESIS_STATS[s.spatial_resolution].append(df)


def export_scenario_synthesis(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    df: pd.DataFrame,
    uow: AbstractUnitOfWork,
) -> None:
    filename = str(s)
    with time_and_log(
        message_root="Tempo para preparacao para exportacao",
        logger=cls.logger,
    ):
        df = df.sort_values(
            s.spatial_resolution.sorting_synthesis_df_columns
        ).reset_index(drop=True)
        probs_df = Deck.expanded_probabilities(uow)
        df_pl = pl.from_pandas(df)
        probs_pl = pl.from_pandas(probs_df)
        stats_pl = calc_statistics(df_pl, probs_pl)
        stats_df = stats_pl.to_pandas()
        add_synthesis_stats(cls, s, stats_df)
        store_in_cache_if_needed(cls, s, df)
    with time_and_log(
        message_root="Tempo para exportacao dos dados", logger=cls.logger
    ):
        with uow:
            df = df[s.spatial_resolution.all_synthesis_df_columns]
            uow.export.synthetize_df(df, filename)


def export_stats(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
) -> None:
    for res, dfs in cls.SYNTHESIS_STATS.items():
        with uow:
            df = pd.concat(dfs, ignore_index=True)
            df = df[[VARIABLE_COL] + res.all_synthesis_df_columns]
            df = df.astype({VARIABLE_COL: STRING_DF_TYPE})
            df = df.sort_values(
                [VARIABLE_COL] + res.sorting_synthesis_df_columns
            ).reset_index(drop=True)
            uow.export.synthetize_df(
                df, f"{OPERATION_SYNTHESIS_STATS_ROOT}_{res.value}"
            )
