from datetime import datetime
from typing import Callable, Dict, List

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import polars as pl
from idecomp.decomp import Dadger

from app.internal.constants import (
    BLOCK_COL,
    LOWER_BOUND_COL,
    OPERATION_SYNTHESIS_COMMON_COLUMNS,
    PANDAS_GROUPING_ENGINE,
    PROBABILITY_COL,
    QUANTILES_FOR_STATISTICS,
    SCENARIO_COL,
    STAGE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)


def fast_group_df(
    df: pd.DataFrame,
    grouping_columns: list,
    extract_columns: list,
    operation: str,
    reset_index: bool = True,
) -> pd.DataFrame:
    """
    Agrupa um DataFrame aplicando uma operação, tentando utilizar a engine mais
    adequada para o agrupamento.
    """
    grouped_df = df.groupby(grouping_columns, sort=False)[extract_columns]

    operation_map: Dict[str, Callable[..., pd.DataFrame]] = {
        "mean": grouped_df.mean,
        "std": grouped_df.std,
        "sum": grouped_df.sum,
    }

    try:
        grouped_df = operation_map[operation](engine=PANDAS_GROUPING_ENGINE)
    except ZeroDivisionError:
        grouped_df = operation_map[operation](engine="cython")

    if reset_index:
        grouped_df = grouped_df.reset_index()
    return grouped_df


def quantile_scenario_labels(q: float) -> str:
    if q == 0:
        return "min"
    if q == 1:
        return "max"
    if q == 0.5:
        return "median"
    return f"p{int(100 * q)}"


def _calc_quantiles_pl(
    df: pl.DataFrame, quantiles: List[float]
) -> pl.DataFrame:
    value_columns = [SCENARIO_COL, VALUE_COL]
    grouping_columns = [c for c in df.columns if c not in value_columns]
    if df.is_empty():
        return df.clear()
    results: List[pl.DataFrame] = []
    for q in quantiles:
        label = quantile_scenario_labels(q)
        q_df = df.group_by(grouping_columns).agg(
            pl.col(VALUE_COL).quantile(q, interpolation="linear")
        )
        q_df = q_df.with_columns(pl.lit(label).alias(SCENARIO_COL))
        results.append(q_df)
    return pl.concat(results)


def _calc_mean_std_pl(df: pl.DataFrame, probs: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df.clear()

    value_columns = [SCENARIO_COL, VALUE_COL]
    grouping_columns = [c for c in df.columns if c not in value_columns]

    # Rename probs VALUE_COL to PROBABILITY_COL before joining to avoid collision
    probs_renamed = probs.select(
        [STAGE_COL, SCENARIO_COL, pl.col(VALUE_COL).alias(PROBABILITY_COL)]
    )
    df_with_prob = df.join(
        probs_renamed, on=[STAGE_COL, SCENARIO_COL], how="left"
    )

    # Weighted mean: sum(value * probability) / sum(probability)
    df_mean = df_with_prob.group_by(grouping_columns).agg(
        (
            (pl.col(VALUE_COL) * pl.col(PROBABILITY_COL)).sum()
            / pl.col(PROBABILITY_COL).sum()
        ).alias(VALUE_COL)
    )
    df_mean = df_mean.with_columns(pl.lit("mean").alias(SCENARIO_COL))

    # Weighted std: sqrt(sum(prob * (value - w_mean)^2) / ((n-1)/n * sum(prob)))
    # Computed in two passes: first get weighted mean per group, then compute deviation.
    df_wmean_per_group = df_with_prob.group_by(grouping_columns).agg(
        (
            (pl.col(VALUE_COL) * pl.col(PROBABILITY_COL)).sum()
            / pl.col(PROBABILITY_COL).sum()
        ).alias("_wmean"),
        pl.col(VALUE_COL).count().alias("_n"),
    )
    df_for_std = df_with_prob.join(
        df_wmean_per_group, on=grouping_columns, how="left"
    )
    df_std = df_for_std.group_by(grouping_columns).agg(
        (
            pl.when(
                ((pl.col("_n").first() - 1) / pl.col("_n").first())
                * pl.col(PROBABILITY_COL).sum()
                > 0
            )
            .then(
                (
                    (
                        pl.col(PROBABILITY_COL)
                        * (pl.col(VALUE_COL) - pl.col("_wmean")).pow(2)
                    ).sum()
                    / (
                        ((pl.col("_n").first() - 1) / pl.col("_n").first())
                        * pl.col(PROBABILITY_COL).sum()
                    )
                ).sqrt()
            )
            .otherwise(pl.lit(float("nan")))
            .alias(VALUE_COL)
        )
    )
    df_std = df_std.with_columns(pl.lit("std").alias(SCENARIO_COL))

    return pl.concat([df_mean, df_std])


def calc_statistics(df: pl.DataFrame, probs: pl.DataFrame) -> pl.DataFrame:
    df_q = _calc_quantiles_pl(df, QUANTILES_FOR_STATISTICS)
    df_m = _calc_mean_std_pl(df, probs)
    return pl.concat([df_q, df_m])


__MONTH_STR_INT_MAP = {
    "JAN": 1,
    "FEV": 2,
    "MAR": 3,
    "ABR": 4,
    "MAI": 5,
    "JUN": 6,
    "JUL": 7,
    "AGO": 8,
    "SET": 9,
    "OUT": 10,
    "NOV": 11,
    "DEZ": 12,
}


def cast_ac_fields_to_stage(
    ac: Dadger.AC,
    stage_start_dates: list[datetime],
    stage_end_dates: list[datetime],
) -> int:
    if not ac.mes:
        return 1

    week_index = ac.semana
    month_index = __MONTH_STR_INT_MAP[ac.mes]

    if month_index == stage_end_dates[0].month:
        return week_index if week_index else 1
    else:
        stage_date = [s for s in stage_start_dates if s.month == month_index][0]
        return stage_start_dates.index(stage_date) + 1
