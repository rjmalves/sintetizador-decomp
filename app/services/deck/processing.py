from __future__ import annotations

from typing import TYPE_CHECKING, cast

import numpy as np
import pandas as pd

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    END_DATE_COL,
    SCENARIO_COL,
    STAGE_COL,
    START_DATE_COL,
)

if TYPE_CHECKING:
    from app.services.unitofwork import AbstractUnitOfWork


def add_dates_to_df(
    df: pd.DataFrame, uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    def _internal(
        line: pd.Series, stages_durations: pd.DataFrame
    ) -> np.ndarray:
        return cast(
            np.ndarray,
            stages_durations.loc[
                stages_durations[STAGE_COL] == line[STAGE_COL],
                [START_DATE_COL, END_DATE_COL],
            ]
            .to_numpy()
            .flatten(),
        )

    from app.services.deck.deck import Deck

    stages_durations = Deck.stages_durations(uow)
    df[[START_DATE_COL, END_DATE_COL]] = df.apply(
        lambda line: _internal(line, stages_durations),
        axis=1,
        result_type="expand",
    )
    return df


def add_dates_to_df_merge(
    df: pd.DataFrame, uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    from app.services.deck.deck import Deck

    stages_durations = Deck.stages_durations(uow)
    stages_with_dates = stages_durations[
        [STAGE_COL, START_DATE_COL, END_DATE_COL]
    ].copy()
    return pd.merge(df, stages_with_dates, on=STAGE_COL, how="left")


def add_stages_durations_to_df(
    df: pd.DataFrame, uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    def _internal(line: pd.Series, stages_durations: pd.DataFrame) -> float:
        return cast(
            float, stages_durations.at[line[STAGE_COL], BLOCK_DURATION_COL]
        )

    from app.services.deck.deck import Deck

    stages_durations = Deck.stages_durations(uow).set_index(STAGE_COL)
    df[BLOCK_COL] = 0
    df[BLOCK_DURATION_COL] = df.apply(
        lambda line: _internal(line, stages_durations),
        axis=1,
    )
    return df


def add_block_durations_to_df(
    df: pd.DataFrame, uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    def _internal(line: pd.Series, blocks_durations: pd.DataFrame) -> float:
        if pd.isna(line[BLOCK_COL]):
            return np.nan
        else:
            return cast(
                float,
                blocks_durations.loc[
                    (blocks_durations[STAGE_COL] == line[STAGE_COL])
                    & (blocks_durations[BLOCK_COL] == line[BLOCK_COL]),
                    BLOCK_DURATION_COL,
                ].iloc[0],
            )

    from app.services.deck.deck import Deck

    blocks_durations = Deck.blocks_durations(uow)
    df[BLOCK_DURATION_COL] = df.apply(
        lambda line: _internal(line, blocks_durations),
        axis=1,
    )
    return df


def fill_average_block_in_df(
    df: pd.DataFrame, uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    """Fill patamar 0 (average) duration by summing block durations.

    Assumes rows are ordered by patamar in the pattern [1, 2, 3, NaN, 1, 2, 3, NaN, ...].
    """
    from app.services.deck.deck import Deck

    num_blocks = len(Deck.blocks(uow))
    num_lines = df.shape[0]
    num_blocks_with_average = num_blocks + 1
    df[BLOCK_COL] = df[BLOCK_COL].fillna(0).astype(int)
    aux_df = df.copy()
    aux_df["aux"] = np.repeat(
        np.arange(num_lines // num_blocks_with_average),
        num_blocks_with_average,
    )
    aux_df = aux_df.loc[~aux_df[BLOCK_DURATION_COL].isna()]
    df.loc[num_blocks::num_blocks_with_average, BLOCK_DURATION_COL] = (
        aux_df.groupby("aux")[BLOCK_DURATION_COL].sum().to_numpy()
    )
    return df


def expand_scenarios_in_df_single_stochastic_stage(
    df: pd.DataFrame, stage: int, num_scenarios: int
) -> pd.DataFrame:
    deterministic_stages_df = df.loc[df[STAGE_COL] != stage].copy()
    stochastic_stage_df = df.loc[df[STAGE_COL] == stage].copy()
    expanded_df = pd.concat(
        [deterministic_stages_df] * num_scenarios, ignore_index=True
    )
    expanded_df[SCENARIO_COL] = np.repeat(
        np.arange(1, num_scenarios + 1), deterministic_stages_df.shape[0]
    )
    return pd.concat([expanded_df, stochastic_stage_df], ignore_index=True)


def expand_scenarios_in_df(df: pd.DataFrame) -> pd.DataFrame:
    unique_scenarios_df = df[[STAGE_COL, SCENARIO_COL]].drop_duplicates()
    num_scenarios_df = unique_scenarios_df.groupby(
        STAGE_COL, as_index=False
    ).count()
    deterministic_stages = num_scenarios_df.loc[
        num_scenarios_df[SCENARIO_COL] == 1, STAGE_COL
    ].tolist()
    stochastic_stages = num_scenarios_df.loc[
        num_scenarios_df[SCENARIO_COL] > 1, STAGE_COL
    ].tolist()
    if len(deterministic_stages) > 0 and len(stochastic_stages) == 1:
        stage = stochastic_stages[0]
        num_scenarios = num_scenarios_df.loc[
            num_scenarios_df[STAGE_COL] == stage,
            SCENARIO_COL,
        ].values[0]
        return expand_scenarios_in_df_single_stochastic_stage(
            df, stage, num_scenarios
        )
    if len(stochastic_stages) == 0 or len(deterministic_stages) == 0:
        return df
    raise RuntimeError("Formato dos cenários não reconhecido")
