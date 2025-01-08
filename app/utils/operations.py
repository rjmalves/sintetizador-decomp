from datetime import datetime
from typing import Callable, Dict, List

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
from idecomp.decomp import Dadger

from app.internal.constants import (
    BLOCK_COL,
    LOWER_BOUND_COL,
    OPERATION_SYNTHESIS_COMMON_COLUMNS,
    PANDAS_GROUPING_ENGINE,
    PROBABILITY_COL,
    QUANTILES_FOR_STATISTICS,
    SCENARIO_COL,
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
    """
    Obtem um rótulo para um cenário baseado no quantil.
    """
    if q == 0:
        label = "min"
    elif q == 1:
        label = "max"
    elif q == 0.5:
        label = "median"
    else:
        label = f"p{int(100 * q)}"
    return label


def _calc_quantiles(df: pd.DataFrame, quantiles: List[float]) -> pd.DataFrame:
    """
    Realiza o pós-processamento para calcular uma lista de quantis
    de uma variável operativa dentre todos os estágios e patamares,
    agrupando de acordo com as demais colunas.
    """
    value_columns = [SCENARIO_COL, VALUE_COL]
    grouping_columns = [c for c in df.columns if c not in value_columns]
    quantile_df = (
        df.groupby(grouping_columns, sort=False)[[SCENARIO_COL, VALUE_COL]]
        .quantile(quantiles)
        .reset_index()
    )

    level_column = [c for c in quantile_df.columns if "level_" in c]
    if len(level_column) != 1:
        raise RuntimeError()

    quantile_df = quantile_df.drop(columns=[SCENARIO_COL]).rename(
        columns={level_column[0]: SCENARIO_COL}
    )
    num_entities = quantile_df.shape[0] // len(quantiles)
    quantile_labels = np.tile(
        np.array([quantile_scenario_labels(q) for q in quantiles]),
        num_entities,
    )
    quantile_df[SCENARIO_COL] = quantile_labels
    return quantile_df


def _calc_mean_std(df: pd.DataFrame, probs: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza o pós-processamento para calcular o valor médio e o desvio
    padrão de uma variável operativa dentre todos os estágios e patamares,
    agrupando de acordo com as demais colunas.
    """

    def _add_probability(
        df: pd.DataFrame, grouping_columns: List[str], probs: pd.DataFrame
    ) -> pd.DataFrame:
        # Assume ordenação por cada uma das "entity_columns",
        # estagio, cenario e patamar
        # Aplica-se 'repeat' por patamar e 'tile' por cada entity_column e estagio
        entity_columns = [
            c
            for c in grouping_columns
            if c
            not in OPERATION_SYNTHESIS_COMMON_COLUMNS
            + [LOWER_BOUND_COL, UPPER_BOUND_COL]
        ]
        num_entities = (
            df.drop_duplicates(subset=entity_columns).shape[0]
            if len(entity_columns) > 0
            else 1
        )
        num_blocks = len(df[BLOCK_COL].unique().tolist())
        probs_values = probs[VALUE_COL].to_numpy()
        probs_column = np.repeat(probs_values, num_blocks)
        probs_column = np.tile(probs_column, num_entities)
        df[PROBABILITY_COL] = probs_column
        return df

    def _weighted_mean(df: pd.DataFrame) -> np.ndarray:
        weights = df[PROBABILITY_COL]
        vals = df[VALUE_COL]
        return np.average(vals, weights=weights)

    def _weighted_std(df: pd.DataFrame) -> np.ndarray:
        weights = df[PROBABILITY_COL].to_numpy()
        vals = df[VALUE_COL].to_numpy()
        n_vals = len(vals)
        weighted_avg = np.average(vals, weights=weights)
        n = np.sum(weights * (vals - weighted_avg) ** 2)
        d = ((n_vals - 1) / n_vals) * np.sum(weights)
        return np.sqrt(n / d)

    value_columns = [SCENARIO_COL, VALUE_COL, PROBABILITY_COL]
    grouping_columns = [c for c in df.columns if c not in value_columns]

    df = _add_probability(df, grouping_columns, probs)
    df_mean = (
        df.groupby(grouping_columns, sort=False)
        .apply(_weighted_mean, include_groups=False)
        .reset_index()
    )
    df_mean[SCENARIO_COL] = "mean"
    df_std = (
        df.groupby(grouping_columns, sort=False)
        .apply(_weighted_std, include_groups=False)
        .reset_index()
    )
    df_std[SCENARIO_COL] = "std"

    df_mean_std = pd.concat([df_mean, df_std], ignore_index=True)
    df_mean_std = df_mean_std.rename(columns={0: VALUE_COL})
    return df_mean_std


def calc_statistics(df: pd.DataFrame, probs: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza o pós-processamento de um DataFrame com dados da
    síntese da operação de uma determinada variável, calculando
    estatísticas como quantis e média para cada variável, em cada
    estágio e patamar.
    """
    df_q = _calc_quantiles(df, QUANTILES_FOR_STATISTICS)
    df_m = _calc_mean_std(df, probs)
    df_stats = pd.concat([df_q, df_m], ignore_index=True)
    return df_stats


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
        return week_index
    else:
        stage_date = [s for s in stage_start_dates if s.month == month_index][0]
        return stage_start_dates.index(stage_date) + 1
