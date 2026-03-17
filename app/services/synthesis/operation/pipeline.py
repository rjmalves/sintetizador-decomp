from logging import WARNING
from typing import TYPE_CHECKING, Any, Callable

import pandas as pd

from app.internal.constants import IDENTIFICATION_COLUMNS, VALUE_COL
from app.model.operation.operationsynthesis import OperationSynthesis
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def post_resolve_file(
    cls: "type[OperationSynthetizer]",
    df: pd.DataFrame,
    col: str,
) -> pd.DataFrame:
    if col not in df.columns:
        cls._log(f"Coluna {col} não encontrada no arquivo", WARNING)
        df[col] = 0.0
    df = df.rename(
        columns={
            col: VALUE_COL,
        }
    )
    cols = [c for c in df.columns if c in IDENTIFICATION_COLUMNS]
    df = df[cols + [VALUE_COL]]
    return df


def get_unique_column_values_in_order(
    df: pd.DataFrame, cols: list[str]
) -> dict[str, list[Any]]:
    return {col: df[col].unique().tolist() for col in cols}


def set_ordered_entities(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    entities: dict[str, list[Any]],
) -> None:
    cls.ORDERED_SYNTHESIS_ENTITIES[s] = entities


def get_ordered_entities(
    cls: "type[OperationSynthetizer]", s: OperationSynthesis
) -> dict[str, list[Any]]:
    return cls.ORDERED_SYNTHESIS_ENTITIES[s]


def post_resolve(
    cls: "type[OperationSynthetizer]",
    df: pd.DataFrame,
    s: OperationSynthesis,
    uow: AbstractUnitOfWork,
    early_hooks: list[Callable[..., pd.DataFrame]] | None = None,
    late_hooks: list[Callable[..., pd.DataFrame]] | None = None,
) -> pd.DataFrame:
    if early_hooks is None:
        early_hooks = []
    if late_hooks is None:
        late_hooks = []

    with time_and_log(
        message_root="Tempo para compactacao dos dados", logger=cls.logger
    ):
        spatial_resolution = s.spatial_resolution

        for hook in early_hooks:
            df = hook(s, df, uow)

        df = df.sort_values(
            spatial_resolution.sorting_synthesis_df_columns
        ).reset_index(drop=True)

        entity_columns_order = get_unique_column_values_in_order(
            df,
            spatial_resolution.sorting_synthesis_df_columns,
        )
        other_columns_order = get_unique_column_values_in_order(
            df,
            spatial_resolution.non_entity_sorting_synthesis_df_columns,
        )
        set_ordered_entities(
            cls, s, {**entity_columns_order, **other_columns_order}
        )

        for hook in late_hooks:
            df = hook(s, df, uow)
    return df
