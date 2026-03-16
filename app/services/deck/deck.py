import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Type, TypeVar

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
from cfinterface.components.register import Register
from idecomp.decomp import Dadger, Decomptim, Hidr, InviabUnic, Relato, Vazoes
from idecomp.decomp.avl_turb_max import AvlTurbMax
from idecomp.decomp.dec_eco_discr import DecEcoDiscr
from idecomp.decomp.dec_fcf_cortes import DecFcfCortes
from idecomp.decomp.dec_oper_gnl import DecOperGnl
from idecomp.decomp.dec_oper_interc import DecOperInterc
from idecomp.decomp.dec_oper_ree import DecOperRee
from idecomp.decomp.dec_oper_sist import DecOperSist
from idecomp.decomp.dec_oper_usih import DecOperUsih
from idecomp.decomp.dec_oper_usit import DecOperUsit
from idecomp.decomp.modelos.dadger import DT, HE, TE

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    EER_CODE_COL,
    END_DATE_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    HYDRO_CODE_COL,
    ITERATION_COL,
    IV_SUBMARKET_CODE,
    LOWER_BOUND_COL,
    RUNTIME_COL,
    SCENARIO_COL,
    STAGE_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    THERMAL_CODE_COL,
    UNIT_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.model.execution.infeasibility import Infeasibility, InfeasibilityType
from app.services.deck import (
    accessors,
    entities,
    operations,
    processing,
    reports,
)
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.operations import cast_ac_fields_to_stage, fast_group_df


class Deck:
    T = TypeVar("T")
    logger: Optional[logging.Logger] = None

    DECK_DATA_CACHING: Dict[str, Any] = {}

    @classmethod
    def _get_dadger(cls, uow: AbstractUnitOfWork) -> Dadger:
        return accessors.get_dadger(uow)

    @classmethod
    def _get_relato(cls, uow: AbstractUnitOfWork) -> Relato:
        return accessors.get_relato(uow)

    @classmethod
    def _get_relato2(cls, uow: AbstractUnitOfWork) -> Relato:
        return accessors.get_relato2(uow)

    @classmethod
    def _get_inviabunic(cls, uow: AbstractUnitOfWork) -> InviabUnic:
        return accessors.get_inviabunic(uow)

    @classmethod
    def _get_decomptim(cls, uow: AbstractUnitOfWork) -> Decomptim:
        return accessors.get_decomptim(uow)

    @classmethod
    def _get_vazoes(cls, uow: AbstractUnitOfWork) -> Vazoes:
        return accessors.get_vazoes(uow)

    @classmethod
    def _get_hidr(cls, uow: AbstractUnitOfWork) -> Hidr:
        return accessors.get_hidr(uow)

    @classmethod
    def _get_dec_eco_discr(cls, uow: AbstractUnitOfWork) -> DecEcoDiscr:
        return accessors.get_dec_eco_discr(uow)

    @classmethod
    def _get_dec_oper_sist(cls, uow: AbstractUnitOfWork) -> DecOperSist:
        return accessors.get_dec_oper_sist(uow)

    @classmethod
    def _get_dec_oper_ree(cls, uow: AbstractUnitOfWork) -> DecOperRee:
        return accessors.get_dec_oper_ree(uow)

    @classmethod
    def _get_dec_oper_usih(cls, uow: AbstractUnitOfWork) -> DecOperUsih:
        return accessors.get_dec_oper_usih(uow)

    @classmethod
    def _get_dec_oper_usit(cls, uow: AbstractUnitOfWork) -> DecOperUsit:
        return accessors.get_dec_oper_usit(uow)

    @classmethod
    def _get_dec_oper_gnl(cls, uow: AbstractUnitOfWork) -> DecOperGnl:
        return accessors.get_dec_oper_gnl(uow)

    @classmethod
    def _get_dec_oper_interc(cls, uow: AbstractUnitOfWork) -> DecOperInterc:
        return accessors.get_dec_oper_interc(uow)

    @classmethod
    def _get_avl_turb_max(cls, uow: AbstractUnitOfWork) -> AvlTurbMax:
        return accessors.get_avl_turb_max(uow)

    @classmethod
    def _get_dec_fcf_cortes(
        cls, stage: int, uow: AbstractUnitOfWork
    ) -> Optional[DecFcfCortes]:
        return accessors.get_dec_fcf_cortes(stage, uow)

    @classmethod
    def _validate_data(cls, data, type: Type[T], msg: str = "dados") -> T:
        if not isinstance(data, type):
            if cls.logger is not None:
                cls.logger.error(f"Erro na leitura de {msg}")
            raise RuntimeError()
        return data

    @classmethod
    def dadger(cls, uow: AbstractUnitOfWork) -> Dadger:
        dadger = cls.DECK_DATA_CACHING.get("dadger")
        if dadger is None:
            dadger = cls._validate_data(
                cls._get_dadger(uow),
                Dadger,
                "dadger",
            )
            cls.DECK_DATA_CACHING["dadger"] = dadger
        return dadger

    @classmethod
    def study_starting_date(cls, uow: AbstractUnitOfWork) -> datetime:
        name = "study_starting_date"
        if name not in cls.DECK_DATA_CACHING:
            dt_reg = cls._validate_data(
                cls.dadger(uow).dt,
                DT,
                "registro DT",
            )
            year = cls._validate_data(
                dt_reg.ano,
                int,
                "ano de início do estudo",
            )
            month = cls._validate_data(
                dt_reg.mes,
                int,
                "mês de início do estudo",
            )
            day = cls._validate_data(
                dt_reg.dia,
                int,
                "dia de início do estudo",
            )
            cls.DECK_DATA_CACHING[name] = datetime(year, month, day)
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def relato(cls, uow: AbstractUnitOfWork) -> Relato:
        relato = cls.DECK_DATA_CACHING.get("relato")
        if relato is None:
            relato = cls._validate_data(
                cls._get_relato(uow),
                Relato,
                "relato",
            )
            cls.DECK_DATA_CACHING["relato"] = relato
        return relato

    @classmethod
    def stored_energy_upper_bounds_eer(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        name = "stored_energy_upper_bounds_eer"
        if name not in cls.DECK_DATA_CACHING:
            df = cls.dec_oper_ree(uow)
            df = df.loc[
                df[SCENARIO_COL] == 1,
                [STAGE_COL, EER_CODE_COL, "earm_maximo_MWmes"],
            ].reset_index(drop=True)
            df = df.sort_values([STAGE_COL, EER_CODE_COL])
            df = df.rename(columns={"earm_maximo_MWmes": VALUE_COL})
            map_df = cls.hydro_eer_submarket_map(uow)
            df[SUBMARKET_CODE_COL] = df[EER_CODE_COL].apply(
                lambda x: map_df.loc[
                    map_df[EER_CODE_COL] == x, SUBMARKET_CODE_COL
                ].iloc[0]
            )
            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name].copy()

    @classmethod
    def stored_energy_lower_bounds_eer(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        def __eval_eer_lower_bound_at_stage(eer_code: int, stage: int) -> float:
            dadger = cls.dadger(uow)
            cm_registers = dadger.cm(codigo_ree=eer_code)
            if isinstance(cm_registers, Register):
                cm_registers = [cm_registers]
            elif cm_registers is None:
                cm_registers = []
            he_codes = [cm.codigo_restricao for cm in cm_registers]
            bound = 0.0
            upper_bound_df = cls.stored_energy_upper_bounds_eer(uow)
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
        if name not in cls.DECK_DATA_CACHING:
            stages: list[int] = []
            eer_codes: list[int] = []
            bounds: list[float] = []
            eers = cls.eers(uow)[EER_CODE_COL].tolist()
            num_stages = len(cls.stages_start_date(uow))
            for stage in range(1, num_stages + 1):
                for eer in eers:
                    stages.append(stage)
                    eer_codes.append(eer)
                    bounds.append(__eval_eer_lower_bound_at_stage(eer, stage))
            df = pd.DataFrame(
                {
                    STAGE_COL: stages,
                    EER_CODE_COL: eer_codes,
                    VALUE_COL: bounds,
                }
            )
            df = df.sort_values([STAGE_COL, EER_CODE_COL])
            map_df = cls.hydro_eer_submarket_map(uow)
            df[SUBMARKET_CODE_COL] = df[EER_CODE_COL].apply(
                lambda x: map_df.loc[
                    map_df[EER_CODE_COL] == x, SUBMARKET_CODE_COL
                ].iloc[0]
            )
            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name].copy()

    @classmethod
    def stored_energy_upper_bounds_sbm(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        name = "stored_energy_upper_bounds_sbm"
        if name not in cls.DECK_DATA_CACHING:
            df = cls._validate_data(
                cls.relato(uow).energia_armazenada_maxima_submercado,
                pd.DataFrame,
                "energia armazenada máxima por submercado",
            )
            df = df.rename(columns={"nome_submercado": SUBMARKET_NAME_COL})
            map_df = cls.hydro_eer_submarket_map(uow)
            df[SUBMARKET_CODE_COL] = df[SUBMARKET_NAME_COL].apply(
                lambda x: map_df.loc[
                    map_df[SUBMARKET_NAME_COL] == x, SUBMARKET_CODE_COL
                ].iloc[0]
            )
            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name].copy()

    @classmethod
    def stored_volume_upper_bounds(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        name = "stored_volume_upper_bounds"
        if name not in cls.DECK_DATA_CACHING:
            df = cls.dec_oper_usih(uow)
            df = df.loc[
                (df[SCENARIO_COL] == 1) & (df[BLOCK_COL] == 0),
                [STAGE_COL, HYDRO_CODE_COL, "volume_util_maximo_hm3"],
            ].reset_index(drop=True)
            df = df.sort_values([STAGE_COL, HYDRO_CODE_COL])
            df = df.rename(columns={"volume_util_maximo_hm3": VALUE_COL})
            map_df = cls.hydro_eer_submarket_map(uow)
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
            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name].copy()

    @classmethod
    def stored_volume_lower_bounds(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        name = "stored_volume_lower_bounds"
        if name not in cls.DECK_DATA_CACHING:
            df = cls.dec_oper_usih(uow)
            df = df.loc[
                (df[SCENARIO_COL] == 1) & (df[BLOCK_COL] == 0),
                [STAGE_COL, HYDRO_CODE_COL, "volume_minimo_hm3"],
            ].reset_index(drop=True)
            df = df.sort_values([STAGE_COL, HYDRO_CODE_COL])
            df = df.rename(columns={"volume_minimo_hm3": VALUE_COL})
            map_df = cls.hydro_eer_submarket_map(uow)
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
            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name].copy()

    @classmethod
    def relato2(cls, uow: AbstractUnitOfWork) -> Relato:
        relato = cls.DECK_DATA_CACHING.get("relato2")
        if relato is None:
            relato = cls._validate_data(
                cls._get_relato2(uow),
                Relato,
                "relato2",
            )
            cls.DECK_DATA_CACHING["relato2"] = relato
        return relato

    @classmethod
    def costs(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        costs = cls.DECK_DATA_CACHING.get("costs")
        if costs is None:
            # TODO - Os custos de segundo mês não estão sendo considerados (relato2)
            relato = cls.relato(uow)
            df = cls._validate_data(
                relato.relatorio_operacao_custos,
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
                    df.loc[df["estagio"] == s, costs_columns]
                    .to_numpy()
                    .flatten()
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

            costs = cls._validate_data(
                df_complete,
                pd.DataFrame,
                "custos de operação",
            )
            cls.DECK_DATA_CACHING["costs"] = costs
        return costs.copy()

    @classmethod
    def convergence(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        convergence = cls.DECK_DATA_CACHING.get("convergence")
        if convergence is None:
            df = cls._validate_data(
                cls.relato(uow).convergencia,
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
            df_processed.loc[1:, "delta_zinf"] /= df_processed[
                "zinf"
            ].to_numpy()[:-1]
            df_processed.at[0, "delta_zinf"] = np.nan

            convergence = cls._validate_data(
                df_processed,
                pd.DataFrame,
                "convergência",
            )
            cls.DECK_DATA_CACHING["convergence"] = convergence
        return convergence

    @classmethod
    def infeasibilities_iterations(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        infeasibilities_iterations = cls.DECK_DATA_CACHING.get(
            "infeasibilities_iterations"
        )
        if infeasibilities_iterations is None:
            infeasibilities_iterations = cls._validate_data(
                cls._get_inviabunic(uow).inviabilidades_iteracoes,
                pd.DataFrame,
                "inviabilidades das iterações",
            )
            cls.DECK_DATA_CACHING["infeasibilities_iterations"] = (
                infeasibilities_iterations
            )
        return infeasibilities_iterations

    @classmethod
    def infeasibilities_final_simulation(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        infeasibilities_final_simulation = cls.DECK_DATA_CACHING.get(
            "infeasibilities_final_simulation"
        )
        if infeasibilities_final_simulation is None:
            infeasibilities_final_simulation = cls._validate_data(
                cls._get_inviabunic(uow).inviabilidades_simulacao_final,
                pd.DataFrame,
                "inviabilidades da simulação final",
            )
            cls.DECK_DATA_CACHING["infeasibilities_final_simulation"] = (
                infeasibilities_final_simulation
            )
        return infeasibilities_final_simulation

    @classmethod
    def _posprocess_infeasibilities_units(
        cls, infeasibility: Infeasibility, uow: AbstractUnitOfWork
    ) -> "Infeasibility":
        if infeasibility.type == InfeasibilityType.DEFICIT.value:
            df_blocks = cls.blocks_durations(uow)
            durations = df_blocks.loc[
                (df_blocks[STAGE_COL] == infeasibility.stage)
                & (df_blocks[BLOCK_COL] > 0),
                BLOCK_DURATION_COL,
            ].to_numpy()
            block_index = cls._validate_data(
                infeasibility.block, int, "índice do patamar"
            )
            fracao = durations[block_index - 1] / np.sum(durations)
            violation_perc = infeasibility.violation * fracao

            max_stored_energy = cls.stored_energy_upper_bounds_sbm(uow)
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

    @classmethod
    def infeasibilities(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        infeasibilities = cls.DECK_DATA_CACHING.get("infeasibilities")
        if infeasibilities is None:
            df_iter = cls.infeasibilities_iterations(uow)
            df_fs = cls.infeasibilities_final_simulation(uow)
            df_fs[ITERATION_COL] = -1
            df_infeas = pd.concat([df_iter, df_fs], ignore_index=True)
            infeasibilities_aux: list[Infeasibility] = []
            for _, linha in df_infeas.iterrows():
                infeasibility = Infeasibility.factory(linha, cls._get_hidr(uow))
                infeasibility_posprocess = (
                    cls._posprocess_infeasibilities_units(infeasibility, uow)
                )
                infeasibilities_aux.append(infeasibility_posprocess)

            types: list[str] = []
            iterations: list[int] = []
            stages: list[int] = []
            scenarios: list[int] = []
            constraint_codes: list[int | None] = []
            violations: list[float] = []
            units: list[str] = []
            blocks: list[int | None] = []
            bounds: list[str | None] = []
            submarkets: list[str | None] = []

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

            infeasibilities = cls._validate_data(
                df,
                pd.DataFrame,
                "inviabilidades",
            )
            cls.DECK_DATA_CACHING["infeasibilities"] = infeasibilities

        return infeasibilities

    @classmethod
    def runtimes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        runtimes = cls.DECK_DATA_CACHING.get("runtimes")
        if runtimes is None:
            df = cls._validate_data(
                cls._get_decomptim(uow).tempos_etapas,
                pd.DataFrame,
                "tempos das etapas",
            )
            df = df.rename(columns={"Etapa": "etapa", "Tempo": RUNTIME_COL})
            runtimes = cls._validate_data(
                df,
                pd.DataFrame,
                "tempos de execução por etapa",
            )
            cls.DECK_DATA_CACHING["runtimes"] = runtimes
        return runtimes

    @classmethod
    def probabilities(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        probabilities = cls.DECK_DATA_CACHING.get("probabilities")
        if probabilities is None:
            probabilities = cls._validate_data(
                cls._get_vazoes(uow).probabilidades,
                pd.DataFrame,
                "probabilidades",
            )
            probabilities = probabilities.rename(
                columns={"probabilidade": VALUE_COL}
            )
            cls.DECK_DATA_CACHING["probabilities"] = probabilities
        return probabilities

    @classmethod
    def expanded_probabilities(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("expanded_probabilities")
        if df is None:
            df = cls.probabilities(uow)
            df = cls._expand_scenarios_in_df(df)
            factors_df = (
                df.groupby(STAGE_COL, as_index=False).sum().set_index(STAGE_COL)
            )
            df[VALUE_COL] = df.apply(
                lambda line: (
                    line[VALUE_COL] / factors_df.at[line[STAGE_COL], VALUE_COL]
                ),
                axis=1,
            )
            cls.DECK_DATA_CACHING["expanded_probabilities"] = df
        return df

    @staticmethod
    def _stub_nodes_scenarios_v31_0_2(df: pd.DataFrame) -> pd.DataFrame:
        return operations._stub_nodes_scenarios_v31_0_2(df)

    @classmethod
    def dec_eco_discr(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_eco_discr"
        df = cls.DECK_DATA_CACHING.get(name)
        if df is None:
            df = cls._validate_data(
                cls._get_dec_eco_discr(uow).tabela,
                pd.DataFrame,
                name,
            )
            df = df.rename(columns={"duracao": BLOCK_DURATION_COL})
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()

    @classmethod
    def blocks_durations(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        def __eval_pat0(df_pat: pd.DataFrame) -> pd.DataFrame:
            df_pat_0 = df_pat.groupby(
                [START_DATE_COL, STAGE_COL], as_index=False
            ).sum(numeric_only=True)
            df_pat_0[BLOCK_COL] = 0
            df_pat = pd.concat([df_pat, df_pat_0], ignore_index=True)
            df_pat.sort_values([START_DATE_COL, BLOCK_COL], inplace=True)
            return df_pat

        name = "blocks_durations"
        df = cls.DECK_DATA_CACHING.get(name)
        if df is None:
            df = cls.dec_eco_discr(uow)
            df = df.loc[~df[BLOCK_COL].isna()]
            df = df[
                [
                    STAGE_COL,
                    BLOCK_COL,
                    BLOCK_DURATION_COL,
                ]
            ].copy()
            df = cls._add_dates_to_df(df, uow)
            df = __eval_pat0(df).reset_index(drop=True)
            cls.DECK_DATA_CACHING[name] = df
        return df

    @classmethod
    def stages_durations(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "stages_durations"
        df = cls.DECK_DATA_CACHING.get(name)
        if df is None:
            df = cls.dec_eco_discr(uow)
            df = df.loc[df[BLOCK_COL].isna()]
            df["duracao_acumulada"] = df[BLOCK_DURATION_COL].cumsum()
            df[START_DATE_COL] = df.apply(
                lambda linha: (
                    cls.study_starting_date(uow)
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
            cls.DECK_DATA_CACHING[name] = df
        return df.reset_index(drop=True)

    @classmethod
    def stages_start_date(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        name = "stages_start_date"
        dates = cls.DECK_DATA_CACHING.get(name)
        if dates is None:
            dates = cls.stages_durations(uow)[START_DATE_COL].tolist()
            cls.DECK_DATA_CACHING[name] = dates
        return dates

    @classmethod
    def stages_end_date(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        name = "stages_end_date"
        dates = cls.DECK_DATA_CACHING.get(name)
        if dates is None:
            dates = cls.stages_durations(uow)[END_DATE_COL].tolist()
            cls.DECK_DATA_CACHING[name] = dates
        return dates

    @classmethod
    def num_stages(cls, uow: AbstractUnitOfWork) -> int:
        name = "num_stages"
        stages = cls.DECK_DATA_CACHING.get(name)
        if stages is None:
            stages = len(cls.stages_start_date(uow))
            cls.DECK_DATA_CACHING[name] = stages
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def blocks(cls, uow: AbstractUnitOfWork) -> List[int]:
        name = "blocks"
        if name not in cls.DECK_DATA_CACHING:
            df = cls.dec_eco_discr(uow)
            cls.DECK_DATA_CACHING[name] = (
                df[BLOCK_COL].dropna().unique().tolist()
            )
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def hydro_eer_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.hydro_eer_submarket_map(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def eers(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.eers(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def submarkets(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.submarkets(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def thermals(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.thermals(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def _add_dates_to_df(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return processing.add_dates_to_df(df, uow)

    @classmethod
    def _add_dates_to_df_merge(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return processing.add_dates_to_df_merge(df, uow)

    @classmethod
    def _add_stages_durations_to_df(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return processing.add_stages_durations_to_df(df, uow)

    @classmethod
    def _add_block_durations_to_df(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return processing.add_block_durations_to_df(df, uow)

    @classmethod
    def _fill_average_block_in_df(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return processing.fill_average_block_in_df(df, uow)

    @classmethod
    def _expand_scenarios_in_df_single_stochastic_stage(
        cls, df: pd.DataFrame, stage: int, num_scenarios: int
    ) -> pd.DataFrame:
        return processing.expand_scenarios_in_df_single_stochastic_stage(
            df, stage, num_scenarios
        )

    @classmethod
    def _expand_scenarios_in_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        return processing.expand_scenarios_in_df(df)

    @classmethod
    def version(cls, uow: AbstractUnitOfWork) -> str:
        name = "version"
        version = cls.DECK_DATA_CACHING.get(name)
        if version is None:
            version = cls._validate_data(
                cls._get_dec_oper_sist(uow).versao,
                str,
                name,
            )
            cls.DECK_DATA_CACHING[name] = version
        return version

    @classmethod
    def title(cls, uow: AbstractUnitOfWork) -> str:
        name = "title"
        title = cls.DECK_DATA_CACHING.get(name)
        if title is None:
            dadger = cls.dadger(uow)
            te = cls._validate_data(
                dadger.te,
                TE,
                "registro TE do dadger",
            )
            title = cls._validate_data(
                te.titulo,
                str,
                "título do estudo",
            )

            cls.DECK_DATA_CACHING[name] = title
        return title

    @classmethod
    def dec_oper_sist(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_sist(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def dec_oper_ree(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_ree(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def _add_eer_sbm_to_df(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return operations._add_eer_sbm_to_df(df, uow)

    @classmethod
    def dec_oper_usih(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_usih(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def dec_oper_usit(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_usit(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def dec_oper_gnl(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_gnl(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def _add_iv_submarket_code(cls, df: pd.DataFrame) -> pd.DataFrame:
        return operations._add_iv_submarket_code(df)

    @classmethod
    def _eval_net_exchange(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return operations._eval_net_exchange(df, uow)

    @classmethod
    def dec_oper_interc(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_interc(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def dec_oper_interc_net(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_interc_net(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def avl_turb_max(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.avl_turb_max(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def _dec_fcf_cortes_per_stage(
        cls, stage: int, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return operations._dec_fcf_cortes_per_stage(
            cls.DECK_DATA_CACHING, stage, uow
        )

    @classmethod
    def dec_fcf_cortes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_fcf_cortes(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def cortes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.cortes(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def variaveis_cortes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.variaveis_cortes(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def _merge_relato_relato2_df_data(
        cls,
        relato_df: pd.DataFrame,
        relato2_df: pd.DataFrame,
        col: str,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        return reports._merge_relato_relato2_df_data(
            relato_df, relato2_df, col, uow
        )

    @classmethod
    def _merge_relato_relato2_energy_balance_df_data(
        cls,
        relato_df: pd.DataFrame,
        relato2_df: pd.DataFrame,
        col: str,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        return reports._merge_relato_relato2_energy_balance_df_data(
            relato_df, relato2_df, col, uow
        )

    @classmethod
    def operation_report_data(
        cls, col: str, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return reports.operation_report_data(col, uow)

    @classmethod
    def energy_balance_report_data(
        cls, col: str, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return reports.energy_balance_report_data(col, uow)

    @classmethod
    def _afluent_energy_for_coupling(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return reports._afluent_energy_for_coupling(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def eer_afluent_energy(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return reports.eer_afluent_energy(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def sbm_afluent_energy(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return reports.sbm_afluent_energy(cls.DECK_DATA_CACHING, uow)

    @classmethod
    def _add_eer_sbm_to_expanded_df(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return reports._add_eer_sbm_to_expanded_df(df, uow)

    @classmethod
    def hydro_operation_report_data(
        cls, col: str, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return reports.hydro_operation_report_data(col, uow)

    @classmethod
    def hydro_generation_report_data(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return reports.hydro_generation_report_data(uow)

    @classmethod
    def __initialize_df_hydro_bounds(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        df_blocks = cls.blocks_durations(uow)
        # Não adiciona patamar 0 (média)
        df_blocks = df_blocks.loc[df_blocks[BLOCK_COL] != 0]
        hydros = cls.hydro_eer_submarket_map(uow)[HYDRO_CODE_COL].unique()
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

    @classmethod
    def __hydro_operative_constraints_id(
        cls,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        name = "hydro_operative_constraints_id"
        hydro_operative_constraints_id = cls.DECK_DATA_CACHING.get(name)
        if hydro_operative_constraints_id is None:
            cls.DECK_DATA_CACHING[name] = cls._validate_data(
                cls.dadger(uow).hq(df=True),
                pd.DataFrame,
                "registros HQ do dadger",
            )
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def __hydro_operative_constraints_bounds(
        cls,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        name = "hydro_operative_constraints_bounds"
        hydro_operative_constraints_bounds = cls.DECK_DATA_CACHING.get(name)
        if hydro_operative_constraints_bounds is None:
            cls.DECK_DATA_CACHING[name] = cls._validate_data(
                cls.dadger(uow).lq(df=True),
                pd.DataFrame,
                "registros LQ do dadger",
            )
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def __hydro_operative_constraints_coefficients(
        cls,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        name = "hydro_operative_constraints_coefficients"
        hydro_operative_constraints_coefficients = cls.DECK_DATA_CACHING.get(
            name
        )
        if hydro_operative_constraints_coefficients is None:
            df = cls._validate_data(
                cls.dadger(uow).cq(df=True),
                pd.DataFrame,
                "registros CQ do dadger",
            )
            # Elimina restricoes HQ com mais de um componente
            df_count = df.groupby(
                by=["codigo_restricao"], as_index=False
            ).count()[["codigo_restricao", "tipo"]]
            constraints_remove = df_count.loc[df_count["tipo"] > 1][
                "codigo_restricao"
            ].unique()
            df = df.loc[~df["codigo_restricao"].isin(constraints_remove)]
            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def _get_hydro_flow_operative_constraints(
        cls, uow: AbstractUnitOfWork, type: str
    ) -> pd.DataFrame:
        df_hq = cls.__hydro_operative_constraints_id(uow)
        df_lq = cls.__hydro_operative_constraints_bounds(uow)
        df_cq = cls.__hydro_operative_constraints_coefficients(uow)

        df_type = df_cq.loc[df_cq["tipo"] == type].copy()
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
        for idx, row in df_type.iterrows():
            id = row["codigo_restricao"]
            hydro_code = row["codigo_usina"]
            multiplier = row["coeficiente"]
            initial_stage = row["estagio_inicial"]
            final_stage = row["estagio_final"]
            consulted_stage = initial_stage
            for stage in np.arange(initial_stage, final_stage + 1, 1):
                for block in cls.blocks(uow):
                    find_constraint = df_constraints_bounds.loc[
                        (df_constraints_bounds["codigo_restricao"] == id)
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

    @classmethod
    def __overwrite_hydro_bounds_with_operative_constraints(
        cls,
        df: pd.DataFrame,
        df_constraints: pd.DataFrame,
    ):
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

    @classmethod
    def __eval_block_0_bounds(cls, uow: AbstractUnitOfWork, df: pd.DataFrame):
        df_pat = df.copy()
        # Adiciona duracao dos patamares e calcula media ponderada dos
        # limites pela duracao
        df_blocks = cls.blocks_durations(uow)
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
        # Obtem data frame final com dados por patamar e patamar 0 (media)
        df = pd.concat([df_pat, df_pat0], ignore_index=True, join="inner")
        df.sort_values([HYDRO_CODE_COL, STAGE_COL, BLOCK_COL], inplace=True)
        return df.reset_index(drop=True)

    @classmethod
    def hydro_spilled_flow_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        # TODO: comportamento não é conforme o esperado pois o resultado de soleira
        # impresso no relato não é o que foi considerado no PL efetivamente, e sim
        # um cálculo pós-processamento com resultados da operação
        # # Substitui cota abaixo da soleira de vertedouro com o limite superior 0
        def __overwrite_with_spill_operation_status_bound(uow, df):
            spill_limits = Deck.hydro_operation_report_data(
                "considera_soleira_vertedouro", uow
            )
            df_spill_limits = spill_limits.loc[spill_limits["valor"]]
            for idx, row in df_spill_limits.iterrows():
                stage = row[STAGE_COL]
                scenario = row[SCENARIO_COL]
                hydro_code = row[HYDRO_CODE_COL]

                condition = (
                    (df[HYDRO_CODE_COL] == hydro_code)
                    & (df[STAGE_COL] == stage)
                    & (df[SCENARIO_COL] == scenario)
                )
                df[UPPER_BOUND_COL] = np.where(
                    condition, 0, df[UPPER_BOUND_COL]
                )
            return df

        name = "hydro_spilled_flow_bounds"
        hydro_spilled_flow_bounds = cls.DECK_DATA_CACHING.get(name)
        if hydro_spilled_flow_bounds is None:
            # Inicializa valores (liminf=0 e limsup=inf)
            df = cls.__initialize_df_hydro_bounds(uow)
            df[LOWER_BOUND_COL] = 0.00
            df[UPPER_BOUND_COL] = float("inf")

            # Obtem restricoes operativas de QVER
            df_constraints = cls._get_hydro_flow_operative_constraints(
                uow, "QVER"
            )
            # Sobrescreve com restricoes operativas
            df = cls.__overwrite_hydro_bounds_with_operative_constraints(
                df, df_constraints
            )
            # Adiciona patamar 0
            df = cls.__eval_block_0_bounds(uow, df)
            cls.DECK_DATA_CACHING[name] = df

        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def hydro_outflow_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "hydro_outflow_bounds"
        hydro_outflow_bounds = cls.DECK_DATA_CACHING.get(name)
        if hydro_outflow_bounds is None:
            # Inicializa valores (liminf=0 e limsup=inf)
            df = cls.__initialize_df_hydro_bounds(uow)
            df[LOWER_BOUND_COL] = 0.00
            df[UPPER_BOUND_COL] = float("inf")
            # Obtem restricoes operativas de QDEF
            df_constraints = cls._get_hydro_flow_operative_constraints(
                uow, "QDEF"
            )
            # Sobrescreve com restricoes operativas
            df = cls.__overwrite_hydro_bounds_with_operative_constraints(
                df, df_constraints
            )
            # Adiciona patamar 0
            df = cls.__eval_block_0_bounds(uow, df)
            cls.DECK_DATA_CACHING[name] = df

        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def hydro_turbined_flow_bounds(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        def _get_turbined_flow_bounds(uow, df) -> pd.DataFrame:
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
            # O arquivo avl_turb_max erroneamente nao
            # contem dados para o ultimo estagio. Quando a impressao
            df["vazao_turbinada_maxima_pl_m3s"] = df[
                "vazao_turbinada_maxima_pl_m3s"
            ].fillna(float("inf"))
            df[UPPER_BOUND_COL] = df["vazao_turbinada_maxima_pl_m3s"]
            df.drop(columns=["vazao_turbinada_maxima_pl_m3s"], inplace=True)
            return df

        name = "hydro_turbined_bounds"
        hydro_turbined_bounds = cls.DECK_DATA_CACHING.get(name)
        if hydro_turbined_bounds is None:
            # Inicializa valores (liminf=0 e limsup=inf)
            df = cls.__initialize_df_hydro_bounds(uow)
            df[LOWER_BOUND_COL] = 0.00
            df[UPPER_BOUND_COL] = float("inf")
            # Obtem turbinamento maximo considerado no PL pelo Decomp, que correspodne
            # ao min(Qmaxgerador,Qmaxturbina)
            df = _get_turbined_flow_bounds(uow, df)
            # Obtem restricoes operativas de QTUR
            df_constraints = cls._get_hydro_flow_operative_constraints(
                uow, "QTUR"
            )
            # Sobrescreve com restricoes operativas
            df = cls.__overwrite_hydro_bounds_with_operative_constraints(
                df, df_constraints
            )
            # Adiciona patamar 0
            df = cls.__eval_block_0_bounds(uow, df)
            cls.DECK_DATA_CACHING[name] = df

        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def thermal_generation_bounds(
        cls,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        name = "thermal_generation_bounds"
        thermal_generation_bounds = cls.DECK_DATA_CACHING.get(name)
        if thermal_generation_bounds is None:
            df = cls.dec_oper_usit(uow)
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

            cls.DECK_DATA_CACHING[name] = df[
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
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def exchange_bounds(
        cls,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        name = "exchange_bounds"
        exchange_bounds = cls.DECK_DATA_CACHING.get(name)
        if exchange_bounds is None:
            df = cls.dec_oper_interc(uow)
            df.rename(
                {
                    "capacidade_MW": UPPER_BOUND_COL,
                },
                axis=1,
                inplace=True,
            )
            df[LOWER_BOUND_COL] = 0
            cls.DECK_DATA_CACHING[name] = df[
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
        return cls.DECK_DATA_CACHING[name]
