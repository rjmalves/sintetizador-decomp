from idecomp.decomp import Dadger, Relato, InviabUnic, Hidr, Decomptim, Vazoes
from idecomp.decomp.dec_oper_usih import DecOperUsih
from idecomp.decomp.dec_oper_usit import DecOperUsit
from idecomp.decomp.dec_oper_gnl import DecOperGnl
from idecomp.decomp.dec_oper_ree import DecOperRee
from idecomp.decomp.dec_oper_sist import DecOperSist
from idecomp.decomp.dec_oper_interc import DecOperInterc
from idecomp.decomp.dec_eco_discr import DecEcoDiscr
from idecomp.decomp.modelos.dadger import DT
import logging
import numpy as np  # type: ignore
import pandas as pd  # type: ignore
from typing import Any, Optional, TypeVar, Type, Dict, List
from datetime import datetime, timedelta
from app.services.unitofwork import AbstractUnitOfWork
from app.model.execution.infeasibility import Infeasibility, InfeasibilityType
from app.internal.constants import (
    ITERATION_COL,
    STAGE_COL,
    SCENARIO_COL,
    BLOCK_COL,
    START_DATE_COL,
    END_DATE_COL,
    RUNTIME_COL,
    UNIT_COL,
    SUBMARKET_NAME_COL,
)


class Deck:

    T = TypeVar("T")
    logger: Optional[logging.Logger] = None

    DECK_DATA_CACHING: Dict[str, Any] = {}

    @classmethod
    def _get_dadger(cls, uow: AbstractUnitOfWork) -> Dadger:
        with uow:
            dadger = uow.files.get_dadger()
            return dadger

    @classmethod
    def _get_relato(cls, uow: AbstractUnitOfWork) -> Relato:
        with uow:
            relato = uow.files.get_relato()
            return relato

    @classmethod
    def _get_relato2(cls, uow: AbstractUnitOfWork) -> Relato:
        with uow:
            relato = uow.files.get_relato2()
            return relato

    @classmethod
    def _get_inviabunic(cls, uow: AbstractUnitOfWork) -> InviabUnic:
        with uow:
            inviabunic = uow.files.get_inviabunic()
            return inviabunic

    @classmethod
    def _get_decomptim(cls, uow: AbstractUnitOfWork) -> Decomptim:
        with uow:
            decomptim = uow.files.get_decomptim()
            return decomptim

    @classmethod
    def _get_vazoes(cls, uow: AbstractUnitOfWork) -> Vazoes:
        with uow:
            vazoes = uow.files.get_vazoes()
            return vazoes

    @classmethod
    def _get_hidr(cls, uow: AbstractUnitOfWork) -> Hidr:
        with uow:
            hidr = uow.files.get_hidr()
            return hidr

    @classmethod
    def _get_dec_eco_discr(cls, uow: AbstractUnitOfWork) -> DecEcoDiscr:
        with uow:
            dec = uow.files.get_dec_eco_discr()
            return dec

    @classmethod
    def _get_dec_oper_sist(cls, uow: AbstractUnitOfWork) -> DecOperSist:
        with uow:
            dec = uow.files.get_dec_oper_sist()
            return dec

    @classmethod
    def _get_dec_oper_ree(cls, uow: AbstractUnitOfWork) -> DecOperRee:
        with uow:
            dec = uow.files.get_dec_oper_ree()
            return dec

    @classmethod
    def _get_dec_oper_usih(cls, uow: AbstractUnitOfWork) -> DecOperUsih:
        with uow:
            dec = uow.files.get_dec_oper_usih()
            return dec

    @classmethod
    def _get_dec_oper_usit(cls, uow: AbstractUnitOfWork) -> DecOperUsit:
        with uow:
            dec = uow.files.get_dec_oper_usit()
            return dec

    @classmethod
    def _get_dec_oper_gnl(cls, uow: AbstractUnitOfWork) -> DecOperGnl:
        with uow:
            dec = uow.files.get_dec_oper_gnl()
            return dec

    @classmethod
    def _get_dec_oper_interc(cls, uow: AbstractUnitOfWork) -> DecOperInterc:
        with uow:
            dec = uow.files.get_dec_oper_interc()
            return dec

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
    def stored_energy_upper_bounds(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        name = "stored_energy_upper_bounds"
        if name not in cls.DECK_DATA_CACHING:
            df = cls._validate_data(
                cls.relato(uow).energia_armazenada_maxima_submercado,
                pd.DataFrame,
                "energia armazenada máxima por submercado",
            )
            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name]

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
            df = relato.relatorio_operacao_custos
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
                {"valor_esperado": np.float64, "desvio_padrao": np.float64}
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
            df = cls.relato(uow).convergencia
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
        if infeasibility.type is InfeasibilityType.DEFICIT:
            df_blocks = cls.blocks_durations(uow)
            durations = df_blocks.loc[
                df_blocks[STAGE_COL] == infeasibility.stage, "duracao"
            ].to_numpy()
            fracao = durations[infeasibility.block - 1] / np.sum(durations)
            violation_perc = infeasibility.violation * fracao

            max_stored_energy = cls.stored_energy_upper_bounds
            max_stored_energy_submarket = float(
                max_stored_energy.loc[
                    max_stored_energy[SUBMARKET_NAME_COL]
                    == infeasibility.submarket,
                    "energia_armazenada_maxima",
                ]
            )
            violation_perc = 100 * (
                infeasibility.violation * fracao / max_stored_energy_submarket
            )

            infeasibility.violation = violation_perc
            infeasibility.unit = "%\EARmax"
        return infeasibility

    @classmethod
    def infeasibilities(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        infeasibilities = cls.DECK_DATA_CACHING.get("infeasibilities")
        if infeasibilities is None:
            df_iter = cls.infeasibilities_iterations(uow)
            df_fs = cls.infeasibilities_final_simulation(uow)
            df_fs[ITERATION_COL] = -1
            df_infeas = pd.concat([df_iter, df_fs], ignore_index=True)
            infeasibilities_aux = []
            for _, linha in df_infeas.iterrows():
                infeasibility = Infeasibility.factory(
                    linha, cls._get_hidr(uow), cls._get_relato(uow)
                )
                infeasibility_posprocess = (
                    cls._posprocess_infeasibilities_units(infeasibility, uow)
                )
                infeasibilities_aux.append(infeasibility_posprocess)

            types: List[str] = []
            iterations: List[int] = []
            stages: List[int] = []
            scenarios: List[int] = []
            constraint_codes: List[int] = []
            violations: List[float] = []
            units: List[str] = []
            blocks: List[int] = []
            bounds: List[str] = []
            submarkets: List[str] = []

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
            df = cls._get_decomptim(uow).tempos_etapas
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
            cls.DECK_DATA_CACHING["probabilities"] = probabilities
        return probabilities

    @staticmethod
    def _stub_nodes_scenarios_v31_0_2(df: pd.DataFrame) -> pd.DataFrame:
        stages = df[STAGE_COL].unique().tolist()
        # Para todos os estágios antes do último, fixa cenário em 1
        df.loc[df[STAGE_COL].isin(stages[:-1]), SCENARIO_COL] = 1
        # Subtrai dos cenários o valor de n_semanas
        df.loc[df[STAGE_COL] == stages[-1], SCENARIO_COL] -= len(stages) - 1
        return df.copy()

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
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()

    @classmethod
    def stages_durations(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "stages_durations"
        df = cls.DECK_DATA_CACHING.get(name)
        if df is None:
            df = cls.dec_eco_discr(uow)
            df = df.loc[df[BLOCK_COL].isna()]
            df["duracao_acumulada"] = df["duracao"].cumsum()
            df[START_DATE_COL] = df.apply(
                lambda linha: cls.study_starting_date(uow)
                + timedelta(
                    hours=df.loc[df[STAGE_COL] < linha[STAGE_COL], "duracao"]
                    .to_numpy()
                    .sum()
                ),
                axis=1,
            )
            df[END_DATE_COL] = df.apply(
                lambda linha: linha[START_DATE_COL]
                + timedelta(hours=linha["duracao"]),
                axis=1,
            )
            df = df[
                [STAGE_COL, START_DATE_COL, END_DATE_COL, "numero_aberturas"]
            ].copy()
            cls.DECK_DATA_CACHING[name] = df
        return df

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
    def blocks(cls, uow: AbstractUnitOfWork) -> List[int]:
        name = "blocks"
        if name not in cls.DECK_DATA_CACHING:
            df = cls.dec_eco_discr(uow)
            cls.DECK_DATA_CACHING[name] = (
                df[BLOCK_COL].dropna().unique().tolist()
            )
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def blocks_durations(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "blocks_durations"
        if name not in cls.DECK_DATA_CACHING:
            df = cls.dec_eco_discr(uow)
            df = df.loc[
                ~df[BLOCK_COL].isna(), [STAGE_COL, BLOCK_COL, "duracao"]
            ].copy()

            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def _add_dates_to_df(
        cls, line: pd.Series, uow: AbstractUnitOfWork
    ) -> np.ndarray:
        df = cls.stages_durations(uow)
        return (
            df.loc[
                df[STAGE_COL] == line[STAGE_COL],
                [START_DATE_COL, END_DATE_COL],
            ]
            .to_numpy()
            .flatten()
        )

    @classmethod
    def dec_oper_sist(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_oper_sist"
        df = cls.DECK_DATA_CACHING.get(name)
        if df is None:
            df = cls._validate_data(
                cls._get_dec_oper_sist(uow).tabela,
                pd.DataFrame,
                name,
            )
            version = cls._validate_data(
                cls._get_dec_oper_sist(uow).versao,
                str,
                name,
            )
            if version <= "31.0.2":
                df = cls._stub_nodes_scenarios_v31_0_2(df)
            df[[START_DATE_COL, END_DATE_COL]] = df.apply(
                lambda line: cls._add_dates_to_df(line, uow),
                axis=1,
                result_type="expand",
            )
            df["geracao_termica_total_MW"] = (
                df["geracao_termica_MW"] + df["geracao_termica_antecipada_MW"]
            )
            df["itaipu_60MW"] = df["itaipu_60MW"].fillna(0.0)
            df["geracao_hidro_com_itaipu_MW"] = (
                df["geracao_hidroeletrica_MW"] + df["itaipu_60MW"]
            )
            df["demanda_liquida_MW"] = (
                df["demanda_MW"] - df["geracao_pequenas_usinas_MW"]
            )
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()

    @classmethod
    def dec_oper_ree(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_oper_ree"
        df = cls.DECK_DATA_CACHING.get(name)
        if df is None:
            df = cls._validate_data(
                cls._get_dec_oper_ree(uow).tabela,
                pd.DataFrame,
                name,
            )
            version = cls._validate_data(
                cls._get_dec_oper_ree(uow).versao,
                str,
                name,
            )
            if version <= "31.0.2":
                df = cls._stub_nodes_scenarios_v31_0_2(df)
            df[[START_DATE_COL, END_DATE_COL]] = df.apply(
                lambda line: cls._add_dates_to_df(line, uow),
                axis=1,
                result_type="expand",
            )
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()

    @classmethod
    def dec_oper_usih(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_oper_usih"
        df = cls.DECK_DATA_CACHING.get(name)
        if df is None:
            df = cls._validate_data(
                cls._get_dec_oper_usih(uow).tabela,
                pd.DataFrame,
                name,
            )
            version = cls._validate_data(
                cls._get_dec_oper_usih(uow).versao,
                str,
                name,
            )
            if version <= "31.0.2":
                df = cls._stub_nodes_scenarios_v31_0_2(df)
            df[[START_DATE_COL, END_DATE_COL]] = df.apply(
                lambda line: cls._add_dates_to_df(line, uow),
                axis=1,
                result_type="expand",
            )
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()

    @classmethod
    def dec_oper_usit(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_oper_usit"
        df = cls.DECK_DATA_CACHING.get(name)
        if df is None:
            df = cls._validate_data(
                cls._get_dec_oper_usit(uow).tabela,
                pd.DataFrame,
                name,
            )
            version = cls._validate_data(
                cls._get_dec_oper_usit(uow).versao,
                str,
                name,
            )
            if version <= "31.0.2":
                df = cls._stub_nodes_scenarios_v31_0_2(df)
            df[[START_DATE_COL, END_DATE_COL]] = df.apply(
                lambda line: cls._add_dates_to_df(line, uow),
                axis=1,
                result_type="expand",
            )
            df["geracao_percentual_maxima"] = (
                100 * df["geracao_MW"] / df["geracao_maxima_MW"]
            )
            filtro = df["geracao_maxima_MW"] != df["geracao_minima_MW"]
            df.loc[
                filtro,
                "geracao_percentual_flexivel",
            ] = (
                100
                * (
                    df.loc[
                        filtro,
                        "geracao_MW",
                    ]
                    - df.loc[
                        filtro,
                        "geracao_minima_MW",
                    ]
                )
                / (
                    df.loc[
                        filtro,
                        "geracao_maxima_MW",
                    ]
                    - df.loc[filtro, "geracao_minima_MW"]
                )
            )
            df.loc[~filtro, "geracao_percentual_flexivel"] = 100.0
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()

    @classmethod
    def dec_oper_gnl(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_oper_gnl"
        df = cls.DECK_DATA_CACHING.get(name)
        if df is None:
            df = cls._validate_data(
                cls._get_dec_oper_gnl(uow).tabela,
                pd.DataFrame,
                name,
            )
            version = cls._validate_data(
                cls._get_dec_oper_gnl(uow).versao,
                str,
                name,
            )
            if version <= "31.0.2":
                df = cls._stub_nodes_scenarios_v31_0_2(df)
            df[[START_DATE_COL, END_DATE_COL]] = df.apply(
                lambda line: cls._add_dates_to_df(line, uow),
                axis=1,
                result_type="expand",
            )
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()

    @classmethod
    def dec_oper_interc(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_oper_interc"
        df = cls.DECK_DATA_CACHING.get(name)
        if df is None:
            df = cls._validate_data(
                cls._get_dec_oper_interc(uow).tabela,
                pd.DataFrame,
                name,
            )
            version = cls._validate_data(
                cls._get_dec_oper_interc(uow).versao,
                str,
                name,
            )
            if version <= "31.0.2":
                df = cls._stub_nodes_scenarios_v31_0_2(df)
            df[[START_DATE_COL, END_DATE_COL]] = df.apply(
                lambda line: cls._add_dates_to_df(line, uow),
                axis=1,
                result_type="expand",
            )
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()
