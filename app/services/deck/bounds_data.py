"""
Bound data helpers for DECOMP synthesis.

Functions computing stored-energy, stored-volume, hydro flow, thermal
generation, and exchange capacity bounds from DECOMP output files.
All functions accept (cache, uow) and use the cache dict owned by Deck.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict

import numpy as np
import pandas as pd
from cfinterface.components.register import Register

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    EER_CODE_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    HYDRO_CODE_COL,
    LOWER_BOUND_COL,
    SCENARIO_COL,
    STAGE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    THERMAL_CODE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.utils.operations import fast_group_df

if TYPE_CHECKING:
    from app.services.unitofwork import AbstractUnitOfWork


# ---------------------------------------------------------------------------
# Stored energy bounds
# ---------------------------------------------------------------------------


def stored_energy_upper_bounds_eer(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "stored_energy_upper_bounds_eer"
    if name not in cache:
        from app.services.deck.deck import Deck

        df = Deck.dec_oper_ree(uow)
        df = df.loc[
            df[SCENARIO_COL] == 1,
            [STAGE_COL, EER_CODE_COL, "earm_maximo_MWmes"],
        ].reset_index(drop=True)
        df = df.sort_values([STAGE_COL, EER_CODE_COL])
        df = df.rename(columns={"earm_maximo_MWmes": VALUE_COL})
        map_df = Deck.hydro_eer_submarket_map(uow)
        df[SUBMARKET_CODE_COL] = df[EER_CODE_COL].apply(
            lambda x: map_df.loc[
                map_df[EER_CODE_COL] == x, SUBMARKET_CODE_COL
            ].iloc[0]
        )
        cache[name] = df
    return cache[name].copy()


def stored_energy_lower_bounds_eer(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    def _eval_eer_lower_bound_at_stage(eer_code: int, stage: int) -> float:
        from idecomp.decomp.modelos.dadger import HE

        from app.services.deck.deck import Deck

        dadger = Deck.dadger(uow)
        cm_registers = dadger.cm(codigo_ree=eer_code)
        if isinstance(cm_registers, Register):
            cm_registers = [cm_registers]
        elif cm_registers is None:
            cm_registers = []
        he_codes = [cm.codigo_restricao for cm in cm_registers]
        bound = 0.0
        upper_bound_df = stored_energy_upper_bounds_eer(cache, uow)
        for he_code in he_codes:
            he = dadger.he(codigo_restricao=he_code, estagio=stage)
            if isinstance(he, HE):
                if he.tipo_limite == 1:
                    bound += he.limite if he.limite else 0.0
                else:
                    lim = he.limite if he.limite else 0.0
                    bound += (lim / 100.0) * upper_bound_df.loc[
                        (upper_bound_df[EER_CODE_COL] == eer_code)
                        & (upper_bound_df[STAGE_COL] == stage),
                        VALUE_COL,
                    ].iloc[0]
        return bound

    name = "stored_energy_lower_bounds_eer"
    if name not in cache:
        from app.services.deck.deck import Deck

        stages_list: list[int] = []
        eer_codes: list[int] = []
        bounds_list: list[float] = []
        eers = Deck.eers(uow)[EER_CODE_COL].tolist()
        n_stages = len(Deck.stages_start_date(uow))
        for stage in range(1, n_stages + 1):
            for eer in eers:
                stages_list.append(stage)
                eer_codes.append(eer)
                bounds_list.append(_eval_eer_lower_bound_at_stage(eer, stage))
        df = pd.DataFrame(
            {
                STAGE_COL: stages_list,
                EER_CODE_COL: eer_codes,
                VALUE_COL: bounds_list,
            }
        )
        df = df.sort_values([STAGE_COL, EER_CODE_COL])
        map_df = Deck.hydro_eer_submarket_map(uow)
        df[SUBMARKET_CODE_COL] = df[EER_CODE_COL].apply(
            lambda x: map_df.loc[
                map_df[EER_CODE_COL] == x, SUBMARKET_CODE_COL
            ].iloc[0]
        )
        cache[name] = df
    return cache[name].copy()


def stored_energy_upper_bounds_sbm(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "stored_energy_upper_bounds_sbm"
    if name not in cache:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck.relato(uow).energia_armazenada_maxima_submercado,
            pd.DataFrame,
            "energia armazenada máxima por submercado",
        )
        df = df.rename(columns={"nome_submercado": SUBMARKET_NAME_COL})
        map_df = Deck.hydro_eer_submarket_map(uow)
        df[SUBMARKET_CODE_COL] = df[SUBMARKET_NAME_COL].apply(
            lambda x: map_df.loc[
                map_df[SUBMARKET_NAME_COL] == x, SUBMARKET_CODE_COL
            ].iloc[0]
        )
        cache[name] = df
    return cache[name].copy()


# ---------------------------------------------------------------------------
# Stored volume bounds
# ---------------------------------------------------------------------------


def stored_volume_upper_bounds(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "stored_volume_upper_bounds"
    if name not in cache:
        from app.services.deck.deck import Deck

        df = Deck.dec_oper_usih(uow)
        df = df.loc[
            (df[SCENARIO_COL] == 1) & (df[BLOCK_COL] == 0),
            [STAGE_COL, HYDRO_CODE_COL, "volume_util_maximo_hm3"],
        ].reset_index(drop=True)
        df = df.sort_values([STAGE_COL, HYDRO_CODE_COL])
        df = df.rename(columns={"volume_util_maximo_hm3": VALUE_COL})
        map_df = Deck.hydro_eer_submarket_map(uow)
        df[EER_CODE_COL] = df[HYDRO_CODE_COL].apply(
            lambda x: map_df.loc[
                map_df[HYDRO_CODE_COL] == x, EER_CODE_COL
            ].iloc[0]
        )
        df[SUBMARKET_CODE_COL] = df[HYDRO_CODE_COL].apply(
            lambda x: map_df.loc[
                map_df[HYDRO_CODE_COL] == x, SUBMARKET_CODE_COL
            ].iloc[0]
        )
        df = df.drop(index=df.loc[df[VALUE_COL].isna()].index).reset_index(
            drop=True
        )
        cache[name] = df
    return cache[name].copy()


def stored_volume_lower_bounds(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "stored_volume_lower_bounds"
    if name not in cache:
        from app.services.deck.deck import Deck

        df = Deck.dec_oper_usih(uow)
        df = df.loc[
            (df[SCENARIO_COL] == 1) & (df[BLOCK_COL] == 0),
            [STAGE_COL, HYDRO_CODE_COL, "volume_minimo_hm3"],
        ].reset_index(drop=True)
        df = df.sort_values([STAGE_COL, HYDRO_CODE_COL])
        df = df.rename(columns={"volume_minimo_hm3": VALUE_COL})
        map_df = Deck.hydro_eer_submarket_map(uow)
        df[EER_CODE_COL] = df[HYDRO_CODE_COL].apply(
            lambda x: map_df.loc[
                map_df[HYDRO_CODE_COL] == x, EER_CODE_COL
            ].iloc[0]
        )
        df[SUBMARKET_CODE_COL] = df[HYDRO_CODE_COL].apply(
            lambda x: map_df.loc[
                map_df[HYDRO_CODE_COL] == x, SUBMARKET_CODE_COL
            ].iloc[0]
        )
        df = df.drop(index=df.loc[df[VALUE_COL].isna()].index).reset_index(
            drop=True
        )
        cache[name] = df
    return cache[name].copy()


# ---------------------------------------------------------------------------
# Hydro flow bounds (internal helpers)
# ---------------------------------------------------------------------------


def _initialize_df_hydro_bounds(uow: "AbstractUnitOfWork") -> pd.DataFrame:
    from app.services.deck.deck import Deck

    df_blocks = Deck.blocks_durations(uow)
    df_blocks = df_blocks.loc[df_blocks[BLOCK_COL] != 0]
    hydros = Deck.hydro_eer_submarket_map(uow)[HYDRO_CODE_COL].unique()
    stages = df_blocks[STAGE_COL].unique()
    blocks = df_blocks[BLOCK_COL].unique()
    num_hydros = len(hydros)
    num_stages = len(stages)
    num_blocks = len(blocks)

    df = pd.DataFrame(
        {
            HYDRO_CODE_COL: np.tile(
                np.repeat(hydros.tolist(), num_blocks), num_stages
            ),
            STAGE_COL: np.repeat(stages.tolist(), num_blocks * num_hydros),
            BLOCK_COL: np.tile(blocks.tolist(), num_hydros * num_stages),
        }
    )
    return df.copy()


def _hydro_operative_constraints_id(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "hydro_operative_constraints_id"
    obj = cache.get(name)
    if obj is None:
        from app.services.deck.deck import Deck

        cache[name] = Deck._validate_data(
            Deck.dadger(uow).hq(df=True),
            pd.DataFrame,
            "registros HQ do dadger",
        )
    return cache[name]


def _hydro_operative_constraints_bounds(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "hydro_operative_constraints_bounds"
    obj = cache.get(name)
    if obj is None:
        from app.services.deck.deck import Deck

        cache[name] = Deck._validate_data(
            Deck.dadger(uow).lq(df=True),
            pd.DataFrame,
            "registros LQ do dadger",
        )
    return cache[name]


def _hydro_operative_constraints_coefficients(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "hydro_operative_constraints_coefficients"
    obj = cache.get(name)
    if obj is None:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck.dadger(uow).cq(df=True),
            pd.DataFrame,
            "registros CQ do dadger",
        )
        df_count = df.groupby(by=["codigo_restricao"], as_index=False).count()[
            ["codigo_restricao", "tipo"]
        ]
        constraints_remove = df_count.loc[df_count["tipo"] > 1][
            "codigo_restricao"
        ].unique()
        df = df.loc[~df["codigo_restricao"].isin(constraints_remove)]
        cache[name] = df
    return cache[name]


def get_hydro_flow_operative_constraints(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork", constraint_type: str
) -> pd.DataFrame:
    from app.services.deck.deck import Deck

    df_hq = _hydro_operative_constraints_id(cache, uow)
    df_lq = _hydro_operative_constraints_bounds(cache, uow)
    df_cq = _hydro_operative_constraints_coefficients(cache, uow)

    df_type = df_cq.loc[df_cq["tipo"] == constraint_type].copy()
    df_type = pd.merge(
        df_type,
        df_hq[["codigo_restricao", "estagio_inicial", "estagio_final"]],
        how="left",
        on=["codigo_restricao"],
    )
    constraints_ids = df_type["codigo_restricao"].tolist()
    df_constraints_bounds = df_lq.loc[
        df_lq["codigo_restricao"].isin(constraints_ids)
    ]
    constraint_data = []
    for _idx, row in df_type.iterrows():
        cid = row["codigo_restricao"]
        hydro_code = row["codigo_usina"]
        multiplier = row["coeficiente"]
        initial_stage = row["estagio_inicial"]
        final_stage = row["estagio_final"]
        consulted_stage = initial_stage
        for stage in np.arange(initial_stage, final_stage + 1, 1):
            for block in Deck.blocks(uow):
                find_constraint = df_constraints_bounds.loc[
                    (df_constraints_bounds["codigo_restricao"] == cid)
                ]
                find_constraint_stage = find_constraint.loc[
                    find_constraint["estagio"] == stage
                ]
                if not find_constraint_stage.empty:
                    consulted_stage = stage
                lower_bound = float(
                    find_constraint.loc[
                        (find_constraint["estagio"] == consulted_stage),
                        f"limite_inferior_{str(int(block))}",
                    ].iloc[0]
                )
                upper_bound = float(
                    find_constraint.loc[
                        (find_constraint["estagio"] == consulted_stage),
                        f"limite_superior_{str(int(block))}",
                    ].iloc[0]
                )
                data = {
                    HYDRO_CODE_COL: hydro_code,
                    STAGE_COL: stage,
                    BLOCK_COL: int(block),
                    LOWER_BOUND_COL: lower_bound / multiplier,
                    UPPER_BOUND_COL: upper_bound / multiplier,
                }
                constraint_data.append(data)

    return pd.DataFrame(constraint_data)


def _overwrite_hydro_bounds_with_operative_constraints(
    df: pd.DataFrame,
    df_constraints: pd.DataFrame,
) -> pd.DataFrame:
    df = pd.merge(
        df,
        df_constraints,
        how="left",
        on=[HYDRO_CODE_COL, STAGE_COL, BLOCK_COL],
    )
    df[LOWER_BOUND_COL] = df[
        [LOWER_BOUND_COL + "_x", LOWER_BOUND_COL + "_y"]
    ].max(axis=1)
    df[UPPER_BOUND_COL] = df[
        [UPPER_BOUND_COL + "_x", UPPER_BOUND_COL + "_y"]
    ].min(axis=1)
    df.drop(
        columns=[
            LOWER_BOUND_COL + "_x",
            LOWER_BOUND_COL + "_y",
            UPPER_BOUND_COL + "_x",
            UPPER_BOUND_COL + "_y",
        ],
        inplace=True,
    )
    return df


def _eval_block_0_bounds(
    uow: "AbstractUnitOfWork", df: pd.DataFrame
) -> pd.DataFrame:
    from app.services.deck.deck import Deck

    df_pat = df.copy()
    df_blocks = Deck.blocks_durations(uow)
    df = pd.merge(
        df,
        df_blocks[[STAGE_COL, BLOCK_COL, BLOCK_DURATION_COL]],
        how="left",
        on=[STAGE_COL, BLOCK_COL],
    )
    df[LOWER_BOUND_COL] = df[LOWER_BOUND_COL] * df[BLOCK_DURATION_COL]
    df[UPPER_BOUND_COL] = df[UPPER_BOUND_COL] * df[BLOCK_DURATION_COL]
    df_pat0 = fast_group_df(
        df,
        [STAGE_COL, HYDRO_CODE_COL],
        [
            BLOCK_DURATION_COL,
            LOWER_BOUND_COL,
            UPPER_BOUND_COL,
        ],
        "sum",
    )
    df_pat0[LOWER_BOUND_COL] = (
        df_pat0[LOWER_BOUND_COL] / df_pat0[BLOCK_DURATION_COL]
    )
    df_pat0[UPPER_BOUND_COL] = (
        df_pat0[UPPER_BOUND_COL] / df_pat0[BLOCK_DURATION_COL]
    )
    df_pat0[BLOCK_COL] = 0
    df = pd.concat([df_pat, df_pat0], ignore_index=True, join="inner")
    df.sort_values([HYDRO_CODE_COL, STAGE_COL, BLOCK_COL], inplace=True)
    return df.reset_index(drop=True)


def hydro_spilled_flow_bounds(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "hydro_spilled_flow_bounds"
    obj = cache.get(name)
    if obj is None:
        df = _initialize_df_hydro_bounds(uow)
        df[LOWER_BOUND_COL] = 0.00
        df[UPPER_BOUND_COL] = float("inf")
        df_constraints = get_hydro_flow_operative_constraints(
            cache, uow, "QVER"
        )
        df = _overwrite_hydro_bounds_with_operative_constraints(
            df, df_constraints
        )
        df = _eval_block_0_bounds(uow, df)
        cache[name] = df
    return cache[name]


def hydro_outflow_bounds(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "hydro_outflow_bounds"
    obj = cache.get(name)
    if obj is None:
        df = _initialize_df_hydro_bounds(uow)
        df[LOWER_BOUND_COL] = 0.00
        df[UPPER_BOUND_COL] = float("inf")
        df_constraints = get_hydro_flow_operative_constraints(
            cache, uow, "QDEF"
        )
        df = _overwrite_hydro_bounds_with_operative_constraints(
            df, df_constraints
        )
        df = _eval_block_0_bounds(uow, df)
        cache[name] = df
    return cache[name]


def hydro_turbined_flow_bounds(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    def _get_turbined_flow_bounds_from_avl(
        uow: "AbstractUnitOfWork", df: pd.DataFrame
    ) -> pd.DataFrame:
        from app.services.deck.deck import Deck

        df_qmax = Deck.avl_turb_max(uow)
        df_qmax = df_qmax.rename(
            {
                "estagio": STAGE_COL,
                "codigo_usina": HYDRO_CODE_COL,
            }
        )
        df = pd.merge(
            df,
            df_qmax[
                [
                    STAGE_COL,
                    HYDRO_CODE_COL,
                    "vazao_turbinada_maxima_pl_m3s",
                ]
            ],
            how="left",
            on=[HYDRO_CODE_COL, STAGE_COL],
        )
        df["vazao_turbinada_maxima_pl_m3s"] = df[
            "vazao_turbinada_maxima_pl_m3s"
        ].fillna(float("inf"))
        df[UPPER_BOUND_COL] = df["vazao_turbinada_maxima_pl_m3s"]
        df.drop(columns=["vazao_turbinada_maxima_pl_m3s"], inplace=True)
        return df

    name = "hydro_turbined_bounds"
    obj = cache.get(name)
    if obj is None:
        df = _initialize_df_hydro_bounds(uow)
        df[LOWER_BOUND_COL] = 0.00
        df[UPPER_BOUND_COL] = float("inf")
        df = _get_turbined_flow_bounds_from_avl(uow, df)
        df_constraints = get_hydro_flow_operative_constraints(
            cache, uow, "QTUR"
        )
        df = _overwrite_hydro_bounds_with_operative_constraints(
            df, df_constraints
        )
        df = _eval_block_0_bounds(uow, df)
        cache[name] = df
    return cache[name]


# ---------------------------------------------------------------------------
# Thermal generation bounds
# ---------------------------------------------------------------------------


def thermal_generation_bounds(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "thermal_generation_bounds"
    obj = cache.get(name)
    if obj is None:
        from app.services.deck.deck import Deck

        df = Deck.dec_oper_usit(uow)
        df.rename(
            {
                "geracao_minima_MW": LOWER_BOUND_COL,
                "geracao_maxima_MW": UPPER_BOUND_COL,
            },
            axis=1,
            inplace=True,
        )
        df = df.drop_duplicates(
            subset=[
                STAGE_COL,
                BLOCK_COL,
                THERMAL_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            keep="first",
        )
        cache[name] = df[
            [
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                THERMAL_CODE_COL,
                SUBMARKET_CODE_COL,
                LOWER_BOUND_COL,
                UPPER_BOUND_COL,
            ]
        ]
    return cache[name]


# ---------------------------------------------------------------------------
# Exchange capacity bounds
# ---------------------------------------------------------------------------


def exchange_bounds(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "exchange_bounds"
    obj = cache.get(name)
    if obj is None:
        from app.services.deck.deck import Deck

        df = Deck.dec_oper_interc(uow)
        df.rename(
            {"capacidade_MW": UPPER_BOUND_COL},
            axis=1,
            inplace=True,
        )
        df[LOWER_BOUND_COL] = 0
        cache[name] = df[
            [
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                EXCHANGE_SOURCE_CODE_COL,
                EXCHANGE_TARGET_CODE_COL,
                LOWER_BOUND_COL,
                UPPER_BOUND_COL,
            ]
        ]
    return cache[name]
