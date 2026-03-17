from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from app.internal.constants import (
    EER_CODE_COL,
    EER_NAME_COL,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    IV_SUBMARKET_CODE,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    THERMAL_CODE_COL,
    THERMAL_NAME_COL,
)
from app.services.unitofwork import AbstractUnitOfWork


def hydro_eer_submarket_map(
    cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    name = "hydro_eer_submarket_map"
    mapping = cache.get(name)
    if mapping is None:
        from app.services.deck.deck import Deck

        mapping = Deck._validate_data(
            Deck.relato(uow).uhes_rees_submercados,
            pd.DataFrame,
            "mapa UHE - REE - SBM",
        )
        mapping = mapping.drop("nome_submercado_newave", axis=1)
        mapping = mapping.rename(
            columns={
                "codigo_usina": HYDRO_CODE_COL,
                "nome_usina": HYDRO_NAME_COL,
                "codigo_ree": EER_CODE_COL,
                "nome_ree": EER_NAME_COL,
                "codigo_submercado": SUBMARKET_CODE_COL,
                "nome_submercado": SUBMARKET_NAME_COL,
            }
        )
        mapping = mapping.sort_values(by=[HYDRO_CODE_COL]).reset_index(
            drop=True
        )
        mapping = mapping.set_index(HYDRO_CODE_COL, drop=False)
        cache[name] = mapping
    return mapping


def eers(cache: Dict[str, Any], uow: AbstractUnitOfWork) -> pd.DataFrame:
    name = "eers"
    eers_df = cache.get(name)
    if eers_df is None:
        mapping = hydro_eer_submarket_map(cache, uow)
        eers_df = (
            (mapping[[EER_CODE_COL, EER_NAME_COL]].drop_duplicates())
            .sort_values(by=[EER_CODE_COL])
            .reset_index(drop=True)
        )
        cache[name] = eers_df
    return eers_df


def submarkets(cache: Dict[str, Any], uow: AbstractUnitOfWork) -> pd.DataFrame:
    name = "submarkets"
    submarkets_df = cache.get(name)
    if submarkets_df is None:
        from app.services.deck.deck import Deck

        dadger = Deck.dadger(uow)
        sbm_df: pd.DataFrame = dadger.sb(df=True)
        sbm_df = sbm_df.rename(columns={"nome_submercado": SUBMARKET_NAME_COL})
        sbm_df.loc[sbm_df.shape[0], :] = [IV_SUBMARKET_CODE, "IV"]
        submarkets_df = sbm_df.astype({SUBMARKET_CODE_COL: int})
        cache[name] = submarkets_df
    return submarkets_df


def thermals(cache: Dict[str, Any], uow: AbstractUnitOfWork) -> pd.DataFrame:
    name = "thermals"
    thermals_df = cache.get(name)
    if thermals_df is None:
        from app.services.deck.deck import Deck

        dadger_ct = Deck.dadger(uow).ct()
        registers = dadger_ct if isinstance(dadger_ct, list) else [dadger_ct]
        sbm_df = submarkets(cache, uow).set_index(SUBMARKET_CODE_COL)
        data: Dict[str, list[Any]] = {
            THERMAL_CODE_COL: [],
            THERMAL_NAME_COL: [],
            SUBMARKET_CODE_COL: [],
        }
        for ct in registers:
            if ct.codigo_usina not in data[THERMAL_CODE_COL]:
                data[THERMAL_CODE_COL].append(ct.codigo_usina)
                data[THERMAL_NAME_COL].append(ct.nome_usina)
                data[SUBMARKET_CODE_COL].append(ct.codigo_submercado)
        thermals_df = pd.DataFrame(data=data)
        thermals_df[SUBMARKET_NAME_COL] = thermals_df[SUBMARKET_CODE_COL].apply(
            lambda c: sbm_df.at[c, SUBMARKET_NAME_COL]
        )
        thermals_df = thermals_df.sort_values(
            by=[THERMAL_CODE_COL]
        ).reset_index(drop=True)
        cache[name] = thermals_df
    return thermals_df
