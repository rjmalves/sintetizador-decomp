"""
Infrastructure helpers for DECOMP synthesis.

Cached accessors for primary file objects (dadger, relato, relato2) and
execution metadata: costs, convergence, infeasibilities, runtimes, probabilities.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List

import numpy as np
import pandas as pd

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    ITERATION_COL,
    RUNTIME_COL,
    SCENARIO_COL,
    STAGE_COL,
    SUBMARKET_NAME_COL,
    UNIT_COL,
    VALUE_COL,
)

if TYPE_CHECKING:
    from idecomp.decomp import Dadger, Relato  # type: ignore[attr-defined]

    from app.services.unitofwork import AbstractUnitOfWork


def dadger(cache: Dict[str, Any], uow: "AbstractUnitOfWork") -> "Dadger":
    obj = cache.get("dadger")
    if obj is None:
        from idecomp.decomp import Dadger  # type: ignore[attr-defined]

        from app.services.deck import accessors
        from app.services.deck.deck import Deck

        obj = Deck._validate_data(
            accessors.get_dadger(uow),
            Dadger,
            "dadger",
        )
        cache["dadger"] = obj
    return obj


def relato(cache: Dict[str, Any], uow: "AbstractUnitOfWork") -> "Relato":
    obj = cache.get("relato")
    if obj is None:
        from idecomp.decomp import Relato  # type: ignore[attr-defined]

        from app.services.deck import accessors
        from app.services.deck.deck import Deck

        obj = Deck._validate_data(
            accessors.get_relato(uow),
            Relato,
            "relato",
        )
        cache["relato"] = obj
    return obj


def relato2(cache: Dict[str, Any], uow: "AbstractUnitOfWork") -> "Relato":
    obj = cache.get("relato2")
    if obj is None:
        from idecomp.decomp import Relato  # type: ignore[attr-defined]

        from app.services.deck import accessors
        from app.services.deck.deck import Deck

        obj = Deck._validate_data(
            accessors.get_relato2(uow),
            Relato,
            "relato2",
        )
        cache["relato2"] = obj
    return obj


def costs(cache: Dict[str, Any], uow: "AbstractUnitOfWork") -> pd.DataFrame:
    name = "costs"
    obj = cache.get(name)
    if obj is None:
        from app.services.deck.deck import Deck

        relato_obj = Deck.relato(uow)
        df = Deck._validate_data(
            relato_obj.relatorio_operacao_custos,
            pd.DataFrame,
            "relatório da operação do relato",
        )
        stages = df["estagio"].unique()
        dfs: List[pd.DataFrame] = []
        costs_columns = [
            "geracao_termica",
            "violacao_desvio",
            "violacao_turbinamento_reservatorio",
            "violacao_turbinamento_fio",
            "penalidade_vertimento_reservatorio",
            "penalidade_vertimento_fio",
            "penalidade_intercambio",
        ]
        for s in stages:
            means = (
                df.loc[df["estagio"] == s, costs_columns].to_numpy().flatten()
            )
            df_stage = pd.DataFrame(
                data={
                    "parcela": costs_columns,
                    "valor_esperado": means,
                    "desvio_padrao": [0.0] * len(means),
                }
            )
            df_stage[STAGE_COL] = s
            dfs.append(df_stage)
        df_complete = pd.concat(dfs, ignore_index=True)
        df_complete = df_complete.astype(
            {
                "valor_esperado": np.float64,
                "desvio_padrao": np.float64,
            }
        )
        df_complete = df_complete.groupby("parcela").sum()
        df_complete = df_complete.reset_index()
        obj = Deck._validate_data(
            df_complete, pd.DataFrame, "custos de operação"
        )
        cache[name] = obj
    return obj.copy()


def convergence(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "convergence"
    obj = cache.get(name)
    if obj is None:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck.relato(uow).convergencia,
            pd.DataFrame,
            "relatório de convergência do relato",
        )
        df_processed = df.rename(
            columns={
                "iteracao": ITERATION_COL,
                "zinf": "zinf",
                "zsup": "zsup",
                "gap_percentual": "gap",
                "tempo": RUNTIME_COL,
                "numero_inviabilidades ": "inviabilidades",
                "deficit_demanda_MWmed": "deficit",
                "inviabilidades_MWmed": "violacao_MWmed",
                "inviabilidades_m3s": "violacao_m3s",
                "inviabilidades_hm3": "violacao_hm3",
            }
        )
        df_processed.drop(
            columns=["deficit_nivel_seguranca_MWmes"], inplace=True
        )
        df_processed.loc[1:, RUNTIME_COL] = (
            df_processed[RUNTIME_COL].to_numpy()[1:]
            - df_processed[RUNTIME_COL].to_numpy()[:-1]
        )
        df_processed["delta_zinf"] = df_processed["zinf"]
        df_processed.loc[1:, "delta_zinf"] = (
            df_processed["zinf"].to_numpy()[1:]
            - df_processed["zinf"].to_numpy()[:-1]
        )
        df_processed.loc[1:, "delta_zinf"] /= df_processed["zinf"].to_numpy()[
            :-1
        ]
        df_processed.at[0, "delta_zinf"] = np.nan
        obj = Deck._validate_data(df_processed, pd.DataFrame, "convergência")
        cache[name] = obj
    return obj


def infeasibilities_iterations(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "infeasibilities_iterations"
    obj = cache.get(name)
    if obj is None:
        from app.services.deck.deck import Deck

        obj = Deck._validate_data(
            Deck._get_inviabunic(uow).inviabilidades_iteracoes,
            pd.DataFrame,
            "inviabilidades das iterações",
        )
        cache[name] = obj
    return obj


def infeasibilities_final_simulation(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "infeasibilities_final_simulation"
    obj = cache.get(name)
    if obj is None:
        from app.services.deck.deck import Deck

        obj = Deck._validate_data(
            Deck._get_inviabunic(uow).inviabilidades_simulacao_final,
            pd.DataFrame,
            "inviabilidades da simulação final",
        )
        cache[name] = obj
    return obj


def _posprocess_infeasibilities_units(
    infeasibility: Any, uow: "AbstractUnitOfWork"
) -> Any:
    from app.model.execution.infeasibility import InfeasibilityType

    if infeasibility.type == InfeasibilityType.DEFICIT.value:
        from app.services.deck.deck import Deck

        df_blocks = Deck.blocks_durations(uow)
        durations = df_blocks.loc[
            (df_blocks[STAGE_COL] == infeasibility.stage)
            & (df_blocks[BLOCK_COL] > 0),
            BLOCK_DURATION_COL,
        ].to_numpy()
        block_index = Deck._validate_data(
            infeasibility.block, int, "índice do patamar"
        )
        fracao = durations[block_index - 1] / np.sum(durations)

        max_stored_energy = Deck.stored_energy_upper_bounds_sbm(uow)
        max_stored_energy_submarket = float(
            max_stored_energy.loc[
                max_stored_energy[SUBMARKET_NAME_COL]
                == infeasibility.submarket,
                "energia_armazenada_maxima",
            ].iloc[0]
        )
        violation_perc = 100 * (
            infeasibility.violation * fracao / max_stored_energy_submarket
        )
        infeasibility.violation = violation_perc
        infeasibility.unit = "%EARmax"
    return infeasibility


def infeasibilities(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "infeasibilities"
    obj = cache.get(name)
    if obj is None:
        from app.model.execution.infeasibility import Infeasibility
        from app.services.deck.deck import Deck

        df_iter = infeasibilities_iterations(cache, uow)
        df_fs = infeasibilities_final_simulation(cache, uow)
        df_fs[ITERATION_COL] = -1
        df_infeas = pd.concat([df_iter, df_fs], ignore_index=True)
        infeasibilities_aux: list[Any] = []
        for _, linha in df_infeas.iterrows():
            infeas = Infeasibility.factory(linha, Deck._get_hidr(uow))
            infeas = _posprocess_infeasibilities_units(infeas, uow)
            infeasibilities_aux.append(infeas)

        types: list[str] = []
        iterations: list[int] = []
        stages: list[int] = []
        scenarios: list[int] = []
        constraint_codes: list[Any] = []
        violations: list[float] = []
        units: list[str] = []
        blocks: list[Any] = []
        bounds: list[Any] = []
        submarkets: list[Any] = []

        for i in infeasibilities_aux:
            types.append(i.type)
            iterations.append(i.iteration)
            stages.append(i.stage)
            scenarios.append(i.scenario)
            constraint_codes.append(i.constraint_code)
            violations.append(i.violation)
            units.append(i.unit)
            blocks.append(i.block)
            bounds.append(i.bound)
            submarkets.append(i.submarket)

        df = pd.DataFrame(
            data={
                "tipo": types,
                ITERATION_COL: iterations,
                SCENARIO_COL: scenarios,
                STAGE_COL: stages,
                "codigo": constraint_codes,
                "violacao": violations,
                UNIT_COL: units,
                BLOCK_COL: blocks,
                "limite": bounds,
                SUBMARKET_NAME_COL: submarkets,
            }
        )
        obj = Deck._validate_data(df, pd.DataFrame, "inviabilidades")
        cache[name] = obj
    return obj


def runtimes(cache: Dict[str, Any], uow: "AbstractUnitOfWork") -> pd.DataFrame:
    name = "runtimes"
    obj = cache.get(name)
    if obj is None:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck._get_decomptim(uow).tempos_etapas,
            pd.DataFrame,
            "tempos das etapas",
        )
        df = df.rename(columns={"Etapa": "etapa", "Tempo": RUNTIME_COL})
        obj = Deck._validate_data(
            df, pd.DataFrame, "tempos de execução por etapa"
        )
        cache[name] = obj
    return obj


def probabilities(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "probabilities"
    obj = cache.get(name)
    if obj is None:
        from app.services.deck.deck import Deck

        obj = Deck._validate_data(
            Deck._get_vazoes(uow).probabilidades,
            pd.DataFrame,
            "probabilidades",
        )
        obj = obj.rename(columns={"probabilidade": VALUE_COL})
        cache[name] = obj
    return obj


def expanded_probabilities(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "expanded_probabilities"
    obj = cache.get(name)
    if obj is None:
        from app.services.deck import processing

        df = probabilities(cache, uow)
        df = processing.expand_scenarios_in_df(df)
        factors_df = (
            df.groupby(STAGE_COL, as_index=False).sum().set_index(STAGE_COL)
        )
        df[VALUE_COL] = df.apply(
            lambda line: (
                line[VALUE_COL] / factors_df.at[line[STAGE_COL], VALUE_COL]
            ),
            axis=1,
        )
        cache[name] = df
        obj = df
    return obj
