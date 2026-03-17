"""
Temporal helpers for DECOMP synthesis.

Functions computing stage/block durations, dates, and counts.
All functions accept (cache, uow) and use the cache dict owned by Deck.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, List, cast

import pandas as pd

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    END_DATE_COL,
    STAGE_COL,
    START_DATE_COL,
)

if TYPE_CHECKING:
    from app.services.unitofwork import AbstractUnitOfWork


def study_starting_date(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> datetime:
    name = "study_starting_date"
    if name not in cache:
        from idecomp.decomp.modelos.dadger import DT

        from app.services.deck.deck import Deck

        dt_reg = Deck._validate_data(
            Deck.dadger(uow).dt,
            DT,
            "registro DT",
        )
        year = Deck._validate_data(dt_reg.ano, int, "ano de início do estudo")
        month = Deck._validate_data(dt_reg.mes, int, "mês de início do estudo")
        day = Deck._validate_data(dt_reg.dia, int, "dia de início do estudo")
        cache[name] = datetime(year, month, day)
    return cast(datetime, cache[name])


def dec_eco_discr(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "dec_eco_discr"
    df = cache.get(name)
    if df is None:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck._get_dec_eco_discr(uow).tabela,
            pd.DataFrame,
            name,
        )
        df = df.rename(columns={"duracao": BLOCK_DURATION_COL})
        cache[name] = df
    return df.copy()


def blocks(cache: Dict[str, Any], uow: "AbstractUnitOfWork") -> List[int]:
    name = "blocks"
    if name not in cache:
        df = dec_eco_discr(cache, uow)
        cache[name] = df[BLOCK_COL].dropna().unique().tolist()
    return cast(List[int], cache[name])


def stages_durations(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "stages_durations"
    df = cache.get(name)
    if df is None:
        df = dec_eco_discr(cache, uow)
        df = df.loc[df[BLOCK_COL].isna()]
        df["duracao_acumulada"] = df[BLOCK_DURATION_COL].cumsum()
        start = study_starting_date(cache, uow)
        df[START_DATE_COL] = df.apply(
            lambda linha: (
                start
                + timedelta(
                    hours=df.loc[
                        df[STAGE_COL] < linha[STAGE_COL], BLOCK_DURATION_COL
                    ]
                    .to_numpy()
                    .sum()
                )
            ),
            axis=1,
        )
        df[END_DATE_COL] = df.apply(
            lambda linha: (
                linha[START_DATE_COL]
                + timedelta(hours=linha[BLOCK_DURATION_COL])
            ),
            axis=1,
        )
        df = df[
            [
                STAGE_COL,
                START_DATE_COL,
                END_DATE_COL,
                BLOCK_DURATION_COL,
                "numero_aberturas",
            ]
        ].copy()
        cache[name] = df
    return df.reset_index(drop=True)


def blocks_durations(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    def _eval_pat0(df_pat: pd.DataFrame) -> pd.DataFrame:
        df_pat_0 = df_pat.groupby(
            [START_DATE_COL, STAGE_COL], as_index=False
        ).sum(numeric_only=True)
        df_pat_0[BLOCK_COL] = 0
        df_pat = pd.concat([df_pat, df_pat_0], ignore_index=True)
        df_pat.sort_values([START_DATE_COL, BLOCK_COL], inplace=True)
        return df_pat

    name = "blocks_durations"
    df = cache.get(name)
    if df is None:
        from app.services.deck import processing

        df = dec_eco_discr(cache, uow)
        df = df.loc[~df[BLOCK_COL].isna()]
        df = df[
            [
                STAGE_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
            ]
        ].copy()
        df = processing.add_dates_to_df(df, uow)
        df = _eval_pat0(df).reset_index(drop=True)
        cache[name] = df
    return df


def stages_start_date(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> List[datetime]:
    name = "stages_start_date"
    dates = cache.get(name)
    if dates is None:
        dates = stages_durations(cache, uow)[START_DATE_COL].tolist()
        cache[name] = dates
    return cast(List[datetime], dates)


def stages_end_date(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> List[datetime]:
    name = "stages_end_date"
    dates = cache.get(name)
    if dates is None:
        dates = stages_durations(cache, uow)[END_DATE_COL].tolist()
        cache[name] = dates
    return cast(List[datetime], dates)


def num_stages(cache: Dict[str, Any], uow: "AbstractUnitOfWork") -> int:
    name = "num_stages"
    n = cache.get(name)
    if n is None:
        n = len(stages_start_date(cache, uow))
        cache[name] = n
    return cast(int, cache[name])


def version(cache: Dict[str, Any], uow: "AbstractUnitOfWork") -> str:
    name = "version"
    v = cache.get(name)
    if v is None:
        from app.services.deck.deck import Deck

        v = Deck._validate_data(
            Deck._get_dec_oper_sist(uow).versao,
            str,
            name,
        )
        cache[name] = v
    return cast(str, v)


def title(cache: Dict[str, Any], uow: "AbstractUnitOfWork") -> str:
    name = "title"
    t = cache.get(name)
    if t is None:
        from idecomp.decomp.modelos.dadger import TE

        from app.services.deck.deck import Deck

        dadger = Deck.dadger(uow)
        te = Deck._validate_data(dadger.te, TE, "registro TE do dadger")
        t = Deck._validate_data(te.titulo, str, "título do estudo")
        cache[name] = t
    return cast(str, t)
