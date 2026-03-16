from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List

import numpy as np
import pandas as pd

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    EER_CODE_COL,
    EER_NAME_COL,
    END_DATE_COL,
    HYDRO_CODE_COL,
    SCENARIO_COL,
    STAGE_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    VALUE_COL,
)
from app.services.deck import processing

if TYPE_CHECKING:
    from app.services.unitofwork import AbstractUnitOfWork


def _merge_relato_relato2_df_data(
    relato_df: pd.DataFrame,
    relato2_df: pd.DataFrame,
    col: str,
    uow: "AbstractUnitOfWork",
) -> pd.DataFrame:
    from app.services.deck.deck import Deck

    relato_df = relato_df.copy()
    relato2_df = relato2_df.copy()
    # Merge stage data
    relato_stages = relato_df[STAGE_COL].unique().tolist()
    relato2_stages = relato2_df[STAGE_COL].unique().tolist()
    stages = list(set(relato_stages + relato2_stages))
    # Merge scenario data
    relato_scenarios = relato_df[SCENARIO_COL].unique().tolist()
    relato2_scenarios = relato2_df[SCENARIO_COL].unique().tolist()
    scenarios = list(set(relato_scenarios + relato2_scenarios))
    # Create empty table
    all_start_dates = Deck.stages_start_date(uow)
    all_end_dates = Deck.stages_end_date(uow)
    start_dates = [all_start_dates[i - 1] for i in stages]
    end_dates = [all_end_dates[i - 1] for i in stages]
    empty_table = np.zeros((len(start_dates), len(scenarios)))
    df = pd.DataFrame(empty_table, columns=scenarios)
    # Fill table
    df[STAGE_COL] = stages
    df[START_DATE_COL] = start_dates
    df[END_DATE_COL] = end_dates
    for e in relato_stages:
        df.loc[
            df[STAGE_COL] == e,
            scenarios,
        ] = float(relato_df.loc[relato_df[STAGE_COL] == e, col].iloc[0])
    for e in relato2_stages:
        df.loc[
            df[STAGE_COL] == e,
            scenarios,
        ] = relato2_df.loc[relato2_df[STAGE_COL] == e, col].to_numpy()
    df = df[[STAGE_COL, START_DATE_COL, END_DATE_COL] + scenarios]
    df = pd.melt(
        df,
        id_vars=[STAGE_COL, START_DATE_COL, END_DATE_COL],
        var_name=SCENARIO_COL,
        value_name=VALUE_COL,
    )
    df[BLOCK_COL] = 0
    df[SCENARIO_COL] = df[SCENARIO_COL].astype(int)
    df = processing.add_stages_durations_to_df(df, uow)
    return df


def _merge_relato_relato2_energy_balance_df_data(
    relato_df: pd.DataFrame,
    relato2_df: pd.DataFrame,
    col: str,
    uow: "AbstractUnitOfWork",
) -> pd.DataFrame:
    from app.services.deck.deck import Deck

    # Fix block names
    relato_df.loc[relato_df["patamar"] == "Medio", "patamar"] = "0"
    relato_df["patamar"] = relato_df["patamar"].astype(int)
    relato_df = relato_df.sort_values(["estagio", "cenario", "patamar"])
    relato2_df.loc[relato2_df["patamar"] == "Medio", "patamar"] = "0"
    relato2_df["patamar"] = relato2_df["patamar"].astype(int)
    relato2_df = relato2_df.sort_values(["estagio", "cenario", "patamar"])
    # Merge stage data
    relato_stages = relato_df[STAGE_COL].unique().tolist()
    relato2_stages = relato2_df[STAGE_COL].unique().tolist()
    stages = list(set(relato_stages + relato2_stages))
    # Merge scenario data
    relato_scenarios = relato_df[SCENARIO_COL].unique().tolist()
    relato2_scenarios = relato2_df[SCENARIO_COL].unique().tolist()
    scenarios = list(set(relato_scenarios + relato2_scenarios))
    # Create empty table
    submarkets = relato_df["nome_submercado"].unique().tolist()
    start_dates = [Deck.stages_start_date(uow)[i - 1] for i in stages]
    num_blocks = len(Deck.blocks(uow)) + 1
    num_submarkets = len(submarkets)
    end_dates = [Deck.stages_end_date(uow)[i - 1] for i in stages]
    empty_table = np.zeros(
        (
            len(start_dates) * num_blocks * num_submarkets,
            len(scenarios),
        )
    )
    df = pd.DataFrame(empty_table, columns=scenarios)
    # Fill table
    df[STAGE_COL] = np.repeat(stages, num_blocks * num_submarkets)
    df[BLOCK_COL] = np.tile(
        np.repeat(list(range(num_blocks)), num_submarkets),
        df.shape[0] // (num_blocks * num_submarkets),
    )
    df[SUBMARKET_NAME_COL] = np.tile(submarkets, df.shape[0] // num_submarkets)
    df[START_DATE_COL] = np.repeat(start_dates, num_blocks * num_submarkets)
    df[END_DATE_COL] = np.repeat(end_dates, num_blocks * num_submarkets)
    for e in relato_stages:
        df.loc[
            (df[STAGE_COL] == e),
            scenarios,
        ] = (
            relato_df.loc[relato_df[STAGE_COL] == e, col]
            .to_numpy()[:, np.newaxis]
            .repeat(len(scenarios), axis=1)
        )
    for e in relato2_stages:
        df.loc[
            df[STAGE_COL] == e,
            scenarios,
        ] = (
            relato2_df.loc[relato2_df[STAGE_COL] == e, col]
            .to_numpy()
            .reshape(-1, len(scenarios))
        )
    df = df[
        [
            STAGE_COL,
            SUBMARKET_NAME_COL,
            BLOCK_COL,
            START_DATE_COL,
            END_DATE_COL,
        ]
        + scenarios
    ]
    df = pd.melt(
        df,
        id_vars=[
            STAGE_COL,
            SUBMARKET_NAME_COL,
            START_DATE_COL,
            END_DATE_COL,
            BLOCK_COL,
        ],
        var_name=SCENARIO_COL,
        value_name=VALUE_COL,
    )
    df[SCENARIO_COL] = df[SCENARIO_COL].astype(int)
    df = processing.add_block_durations_to_df(df, uow)
    submarket_df = Deck.submarkets(uow)
    df[SUBMARKET_CODE_COL] = df.apply(
        lambda line: submarket_df.loc[
            submarket_df[SUBMARKET_NAME_COL] == line[SUBMARKET_NAME_COL],
            SUBMARKET_CODE_COL,
        ].iloc[0],
        axis=1,
    )
    df = df.drop(columns=[SUBMARKET_NAME_COL])
    energy_balance_cols = [
        SUBMARKET_CODE_COL,
        STAGE_COL,
        START_DATE_COL,
        END_DATE_COL,
        SCENARIO_COL,
        BLOCK_COL,
        BLOCK_DURATION_COL,
        VALUE_COL,
    ]
    df = df[energy_balance_cols]
    return df.copy()


def operation_report_data(col: str, uow: "AbstractUnitOfWork") -> pd.DataFrame:
    from app.services.deck.deck import Deck

    relato_df = Deck._validate_data(
        Deck.relato(uow).relatorio_operacao_custos,
        pd.DataFrame,
        "relatório da operação do relato",
    )
    relato_df = relato_df.copy()
    relato2_df = Deck.relato2(uow).relatorio_operacao_custos
    if relato2_df is None:
        relato2_df = pd.DataFrame(columns=relato_df.columns)
        relato2_df = relato2_df.astype(relato_df.dtypes)
    relato2_df = relato2_df.copy()

    return _merge_relato_relato2_df_data(relato_df, relato2_df, col, uow)


def energy_balance_report_data(
    col: str, uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    from app.services.deck.deck import Deck

    relato_df = Deck._validate_data(
        Deck.relato(uow).balanco_energetico,
        pd.DataFrame,
        "balanço energético do relato",
    )
    relato_df = relato_df.copy()
    relato2_df = Deck.relato2(uow).balanco_energetico
    if relato2_df is None:
        relato2_df = pd.DataFrame(columns=relato_df.columns)
        relato2_df = relato2_df.astype(relato_df.dtypes)
    relato2_df = relato2_df.copy()

    # Fix hydro gen variable
    relato_df["geracao_hidraulica"] += relato_df["geracao_itaipu_60hz"]
    relato2_df["geracao_hidraulica"] += relato2_df["geracao_itaipu_60hz"]

    return _merge_relato_relato2_energy_balance_df_data(
        relato_df, relato2_df, col, uow
    )


def _afluent_energy_for_coupling(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "afluent_energy_for_coupling"
    df = cache.get(name)
    if df is None:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck.relato(uow).ena_acoplamento_ree,
            pd.DataFrame,
            "ENA para acoplamento do relato",
        )
        df = df.rename(
            columns={
                "nome_submercado": SUBMARKET_NAME_COL,
                "nome_ree": EER_NAME_COL,
            }
        )
        df = df.melt(
            id_vars=[
                SCENARIO_COL,
                EER_CODE_COL,
                EER_NAME_COL,
                SUBMARKET_NAME_COL,
            ],
            var_name=STAGE_COL,
            value_name=VALUE_COL,
        )
        df[STAGE_COL] = df[STAGE_COL].apply(lambda s: int(s.lstrip("estagio_")))
        df[BLOCK_COL] = 0
        df = processing.add_block_durations_to_df(df, uow)
        start_dates = Deck.stages_start_date(uow)
        end_dates = Deck.stages_end_date(uow)
        df[START_DATE_COL] = df[STAGE_COL].apply(lambda x: start_dates[x - 1])
        df[END_DATE_COL] = df[STAGE_COL].apply(lambda x: end_dates[x - 1])

        eer_sbm_df = Deck.hydro_eer_submarket_map(uow)
        eer_sbm_df = eer_sbm_df.drop_duplicates(
            [
                SUBMARKET_CODE_COL,
                SUBMARKET_NAME_COL,
            ]
        )
        sbm_name_map = {
            line[SUBMARKET_NAME_COL]: line[SUBMARKET_CODE_COL]
            for _, line in eer_sbm_df.iterrows()
        }
        df[SUBMARKET_CODE_COL] = df[SUBMARKET_NAME_COL].apply(
            lambda x: sbm_name_map[x]
        )
        df = df.drop(columns=[SUBMARKET_NAME_COL, EER_NAME_COL])
        df = df.sort_values(
            [
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
            ]
        )
        df = df[
            [
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
                STAGE_COL,
                START_DATE_COL,
                END_DATE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                VALUE_COL,
            ]
        ]
        cache[name] = df
    return df.copy()


def eer_afluent_energy(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    return _afluent_energy_for_coupling(cache, uow)


def sbm_afluent_energy(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    df = _afluent_energy_for_coupling(cache, uow)
    return (
        df.groupby(
            [
                SUBMARKET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                START_DATE_COL,
                END_DATE_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
            ]
        )
        .sum()
        .reset_index()
    )


def _add_eer_sbm_to_expanded_df(
    df: pd.DataFrame, uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    # Assume a ordenação por usina, estagio, cenario, patamar
    from app.services.deck.deck import Deck

    hydro_order = df[HYDRO_CODE_COL].unique().tolist()
    num_repeats = df.shape[0] // (len(hydro_order))
    map_df = Deck.hydro_eer_submarket_map(uow)
    submarket_codes = map_df.loc[hydro_order, SUBMARKET_CODE_COL].to_numpy()
    eer_codes = map_df.loc[hydro_order, EER_CODE_COL].to_numpy()
    df[SUBMARKET_CODE_COL] = np.repeat(submarket_codes, num_repeats)
    df[EER_CODE_COL] = np.repeat(eer_codes, num_repeats)
    return df


def hydro_operation_report_data(
    col: str, uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    from app.services.deck.deck import Deck

    relato_df = Deck._validate_data(
        Deck.relato(uow).relatorio_operacao_uhe,
        pd.DataFrame,
        "relatório da operação das UHE do relato",
    )
    relato2_df = Deck.relato2(uow).relatorio_operacao_uhe
    if relato2_df is None:
        relato2_df = pd.DataFrame(columns=relato_df.columns)
        relato2_df = relato2_df.astype(relato_df.dtypes)

    relato_df = relato_df.loc[~pd.isna(relato_df["FPCGC"])]
    relato2_df = relato2_df.loc[~pd.isna(relato2_df["FPCGC"])]
    hydros = relato_df[HYDRO_CODE_COL].unique().tolist()
    dfs: List[pd.DataFrame] = []
    for hydro in hydros:
        df = _merge_relato_relato2_df_data(
            relato_df.loc[relato_df[HYDRO_CODE_COL] == hydro],
            relato2_df.loc[relato2_df[HYDRO_CODE_COL] == hydro],
            col,
            uow,
        )
        df[HYDRO_CODE_COL] = hydro
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    df = _add_eer_sbm_to_expanded_df(df, uow)
    return df


def hydro_generation_report_data(uow: "AbstractUnitOfWork") -> pd.DataFrame:
    from app.services.deck.deck import Deck

    relato_df = Deck._validate_data(
        Deck.relato(uow).relatorio_operacao_uhe,
        pd.DataFrame,
        "relatório da operação das UHE do relato",
    )
    relato2_df = Deck.relato2(uow).relatorio_operacao_custos
    if relato2_df is None:
        relato2_df = pd.DataFrame(columns=relato_df.columns)
        relato2_df = relato2_df.astype(relato_df.dtypes)

    relato_df = relato_df.loc[~pd.isna(relato_df["FPCGC"])]
    relato2_df = relato2_df.loc[~pd.isna(relato2_df["FPCGC"])]
    hydros = relato_df[HYDRO_CODE_COL].unique().tolist()
    dfs: List[pd.DataFrame] = []
    col_block_mapping = {
        "geracao_media": 0,
        "geracao_patamar_1": 1,
        "geracao_patamar_2": 2,
        "geracao_patamar_3": 3,
    }
    for hydro in hydros:
        hydro_dfs = []
        for col, block in col_block_mapping.items():
            df = _merge_relato_relato2_df_data(
                relato_df.loc[relato_df[HYDRO_CODE_COL] == hydro],
                relato2_df.loc[relato2_df[HYDRO_CODE_COL] == hydro],
                col,
                uow,
            )
            df[BLOCK_COL] = block
            hydro_dfs.append(df)
        df = pd.concat(hydro_dfs, ignore_index=True)
        df[HYDRO_CODE_COL] = hydro
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    df = _add_eer_sbm_to_expanded_df(df, uow)
    return df
