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
from app.model.execution.inviabilidade import Inviabilidade
from app.internal.constants import (
    ITERATION_COL,
    STAGE_COL,
    SCENARIO_COL,
    BLOCK_COL,
    BLOCK_DURATION_COL,
    START_DATE_COL,
    END_DATE_COL,
    SUBMARKET_CODE_COL,
    EER_CODE_COL,
    HYDRO_CODE_COL,
    THERMAL_CODE_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    IV_SUBMARKET_CODE,
    VALUE_COL,
)

# from app.internal.constants import STRING_DF_TYPE


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
    def stored_energy_upper_bounds(cls, uow: AbstractUnitOfWork) -> float:
        name = "stored_energy_upper_bounds"
        if name not in cls.DECK_DATA_CACHING:
            df = cls._validate_data(
                cls.relato(uow).energia_armazenada_maxima_submercado,
                pd.DataFrame,
                "energia armazenada máxima do SIN",
            )
            cls.DECK_DATA_CACHING[name] = df["energia_armazenada_maxima"].sum()
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
    def convergence(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        convergence = cls.DECK_DATA_CACHING.get("convergence")
        if convergence is None:
            convergence = cls._validate_data(
                cls.relato(uow).convergencia,
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
    def infeasibilities(cls, uow: AbstractUnitOfWork) -> list:
        infeasibilities = cls.DECK_DATA_CACHING.get("infeasibilities")
        if infeasibilities is None:
            df_iter = cls.infeasibilities_iterations(uow)
            df_fs = cls.infeasibilities_final_simulation(uow)
            df_fs[ITERATION_COL] = -1
            df_infeas = pd.concat([df_iter, df_fs], ignore_index=True)
            infleasibilities_aux = []
            for _, linha in df_infeas.iterrows():
                infleasibilities_aux.append(
                    Inviabilidade.factory(
                        linha, cls._get_hidr(uow), cls._get_relato(uow)
                    )
                )
            infeasibilities = cls._validate_data(
                infleasibilities_aux,
                list,
                "inviabilidades",
            )
            cls.DECK_DATA_CACHING["infeasibilities"] = infeasibilities
        return infeasibilities

    @classmethod
    def execution_time_per_step(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        execution_time_per_step = cls.DECK_DATA_CACHING.get(
            "execution_time_per_step"
        )
        if execution_time_per_step is None:
            execution_time_per_step = cls._validate_data(
                cls._get_decomptim(uow).tempos_etapas,
                pd.DataFrame,
                "tempos de execução por etapa",
            )
            cls.DECK_DATA_CACHING["execution_time_per_step"] = (
                execution_time_per_step
            )
        return execution_time_per_step

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
                lambda line: line[VALUE_COL]
                / factors_df.at[line[STAGE_COL], VALUE_COL],
                axis=1,
            )
            cls.DECK_DATA_CACHING["expanded_probabilities"] = df
        return df

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
            df = df.rename(columns={"duracao": BLOCK_DURATION_COL})
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()

    @classmethod
    def blocks_durations(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
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
                lambda linha: cls.study_starting_date(uow)
                + timedelta(
                    hours=df.loc[
                        df[STAGE_COL] < linha[STAGE_COL], BLOCK_DURATION_COL
                    ]
                    .to_numpy()
                    .sum()
                ),
                axis=1,
            )
            df[END_DATE_COL] = df.apply(
                lambda linha: linha[START_DATE_COL]
                + timedelta(hours=linha[BLOCK_DURATION_COL]),
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
    def hydro_eer_submarket_map(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        name = "hydro_eer_submarket_map"
        mapping = cls.DECK_DATA_CACHING.get(name)
        if mapping is None:
            mapping = cls._validate_data(
                cls.relato(uow).uhes_rees_submercados,
                pd.DataFrame,
                "mapa UHE - REE - SBM",
            )
            mapping = mapping.set_index(HYDRO_CODE_COL)
            cls.DECK_DATA_CACHING[name] = mapping
        return mapping

    @classmethod
    def _add_dates_to_df(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        def _add_dates_to_df_internal(
            line: pd.Series, stages_durations: pd.DataFrame
        ) -> np.ndarray:
            return (
                stages_durations.loc[
                    stages_durations[STAGE_COL] == line[STAGE_COL],
                    [START_DATE_COL, END_DATE_COL],
                ]
                .to_numpy()
                .flatten()
            )

        stages_durations = cls.stages_durations(uow)
        df[[START_DATE_COL, END_DATE_COL]] = df.apply(
            lambda line: _add_dates_to_df_internal(line, stages_durations),
            axis=1,
            result_type="expand",
        )
        return df

    @classmethod
    def _add_stages_durations_to_df(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        def _add_durations_to_df_internal(
            line: pd.Series, stages_durations: pd.DataFrame
        ) -> np.ndarray:
            return stages_durations.at[line[STAGE_COL], BLOCK_DURATION_COL]

        stages_durations = cls.stages_durations(uow).set_index(STAGE_COL)
        df[BLOCK_COL] = 0
        df[BLOCK_DURATION_COL] = df.apply(
            lambda line: _add_durations_to_df_internal(line, stages_durations),
            axis=1,
        )
        return df

    @classmethod
    def _add_block_durations_to_df(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        def _add_durations_to_df_internal(
            line: pd.Series, blocks_durations: pd.DataFrame
        ) -> np.ndarray:
            if pd.isna(line[BLOCK_COL]):
                return np.nan
            else:
                return blocks_durations.loc[
                    (blocks_durations[STAGE_COL] == line[STAGE_COL])
                    & (blocks_durations[BLOCK_COL] == line[BLOCK_COL]),
                    BLOCK_DURATION_COL,
                ].iloc[0]

        blocks_durations = cls.blocks_durations(uow)
        df[BLOCK_DURATION_COL] = df.apply(
            lambda line: _add_durations_to_df_internal(line, blocks_durations),
            axis=1,
        )
        return df

    @classmethod
    def _fill_average_block_in_df(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """ """
        # Assume que as linhas da tabela são ordenadas por patamares,
        # na ordem [1, 2, 3, NaN, 1, 2, 3, NaN, 1, ...]
        num_blocks = len(cls.blocks(uow))
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

    @classmethod
    def _expand_scenarios_in_df_single_stochastic_stage(
        cls, df: pd.DataFrame, stage: int, num_scenarios: int
    ):
        deterministic_stages_df = df.loc[df[STAGE_COL] != stage].copy()
        stochastic_stage_df = df.loc[df[STAGE_COL] == stage].copy()
        expanded_df = pd.concat(
            [deterministic_stages_df] * num_scenarios, ignore_index=True
        )
        expanded_df[SCENARIO_COL] = np.repeat(
            np.arange(1, num_scenarios + 1), deterministic_stages_df.shape[0]
        )
        return pd.concat([expanded_df, stochastic_stage_df], ignore_index=True)

    @classmethod
    def _expand_scenarios_in_df(cls, df: pd.DataFrame) -> pd.DataFrame:
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
            df = cls._expand_scenarios_in_df_single_stochastic_stage(
                df, stage, num_scenarios
            )
        elif (len(stochastic_stages) == 0) or (len(deterministic_stages) == 0):
            pass
        else:
            raise RuntimeError("Formato dos cenários não reconhecido")
        return df

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
            df = cls._add_dates_to_df(df, uow)
            df = df.rename(columns={"duracao": BLOCK_DURATION_COL})
            df = cls._fill_average_block_in_df(df, uow)
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
            df = cls._expand_scenarios_in_df(df)
            df = df.sort_values(
                [SUBMARKET_CODE_COL, STAGE_COL, SCENARIO_COL, BLOCK_COL]
            ).reset_index(drop=True)
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
            df = cls._add_dates_to_df(df, uow)
            df = cls._add_stages_durations_to_df(df, uow)
            df = cls._expand_scenarios_in_df(df)
            df = df.sort_values(
                [EER_CODE_COL, STAGE_COL, SCENARIO_COL, BLOCK_COL]
            ).reset_index(drop=True)
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()

    @classmethod
    def _add_eer_sbm_to_df(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        # Assume a ordenação por estagio, cenario, patamar, usina
        hydro_order = df[HYDRO_CODE_COL].unique().tolist()
        num_blocks_with_average = len(cls.blocks(uow)) + 1
        num_tiles = df.shape[0] // (len(hydro_order) * num_blocks_with_average)
        map_df = cls.hydro_eer_submarket_map(uow)
        submarket_codes = map_df.loc[hydro_order, SUBMARKET_CODE_COL].to_numpy()
        eer_codes = map_df.loc[hydro_order, EER_CODE_COL].to_numpy()
        df[SUBMARKET_CODE_COL] = np.tile(
            np.repeat(submarket_codes, num_blocks_with_average), num_tiles
        )
        df[EER_CODE_COL] = np.tile(
            np.repeat(eer_codes, num_blocks_with_average), num_tiles
        )
        return df

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
            df = cls._add_dates_to_df(df, uow)
            df = cls._add_eer_sbm_to_df(df, uow)
            df = df.rename(columns={"duracao": BLOCK_DURATION_COL})
            df = cls._fill_average_block_in_df(df, uow)
            df = cls._expand_scenarios_in_df(df)
            df = df.sort_values(
                [HYDRO_CODE_COL, STAGE_COL, SCENARIO_COL, BLOCK_COL]
            ).reset_index(drop=True)
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
            df = cls._add_dates_to_df(df, uow)
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
            df = df.rename(columns={"duracao": BLOCK_DURATION_COL})
            df = cls._fill_average_block_in_df(df, uow)
            df = cls._expand_scenarios_in_df(df)
            df = df.sort_values(
                [THERMAL_CODE_COL, STAGE_COL, SCENARIO_COL, BLOCK_COL]
            ).reset_index(drop=True)
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
            df = cls._add_dates_to_df(df, uow)

            cls.DECK_DATA_CACHING[name] = df
        return df.copy()

    @classmethod
    def _add_iv_submarket_code(cls, df: pd.DataFrame) -> pd.DataFrame:
        for col in [EXCHANGE_SOURCE_CODE_COL, EXCHANGE_TARGET_CODE_COL]:
            df.loc[df[col].isna(), col] = IV_SUBMARKET_CODE
            df[col] = df[col].astype(int)
        return df

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
            df = cls._add_iv_submarket_code(df)
            df = cls._add_dates_to_df(df, uow)
            df = cls._add_block_durations_to_df(df, uow)
            df = cls._fill_average_block_in_df(df, uow)
            df = cls._expand_scenarios_in_df(df)
            df = df.sort_values(
                [
                    EXCHANGE_SOURCE_CODE_COL,
                    EXCHANGE_TARGET_CODE_COL,
                    STAGE_COL,
                    SCENARIO_COL,
                    BLOCK_COL,
                ]
            ).reset_index(drop=True)
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()

    @classmethod
    def _merge_relato_relato2_df_data(
        cls,
        relato_df: pd.DataFrame,
        relato2_df: pd.DataFrame,
        col: str,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        # Merge stage data
        relato_stages = relato_df[STAGE_COL].unique().tolist()
        relato2_stages = relato2_df[STAGE_COL].unique().tolist()
        stages = list(set(relato_stages + relato2_stages))
        # Merge scenario data
        relato_scenarios = relato_df[SCENARIO_COL].unique().tolist()
        relato2_scenarios = relato2_df[SCENARIO_COL].unique().tolist()
        scenarios = list(set(relato_scenarios + relato2_scenarios))
        # Create empty table
        start_dates = [Deck.stages_start_date(uow)[i - 1] for i in stages]
        end_dates = [Deck.stages_end_date(uow)[i - 1] for i in stages]
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
        df = cls._add_stages_durations_to_df(df, uow)
        return df

    @classmethod
    def operation_report_data(
        cls, col: str, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        relato_df = cls._validate_data(
            cls.relato(uow).relatorio_operacao_custos,
            pd.DataFrame,
            "relatório da operação do relato",
        )
        relato2_df = cls._validate_data(
            cls.relato2(uow).relatorio_operacao_custos,
            pd.DataFrame,
            "relatório da operação do relato2",
        )
        return cls._merge_relato_relato2_df_data(
            relato_df, relato2_df, col, uow
        )

    @classmethod
    def _add_eer_sbm_to_expanded_df(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        # Assume a ordenação por usina, estagio, cenario, patamar
        hydro_order = df[HYDRO_CODE_COL].unique().tolist()
        num_repeats = df.shape[0] // (len(hydro_order))
        map_df = cls.hydro_eer_submarket_map(uow)
        submarket_codes = map_df.loc[hydro_order, SUBMARKET_CODE_COL].to_numpy()
        eer_codes = map_df.loc[hydro_order, EER_CODE_COL].to_numpy()
        df[SUBMARKET_CODE_COL] = np.repeat(submarket_codes, num_repeats)
        df[EER_CODE_COL] = np.repeat(eer_codes, num_repeats)
        return df

    @classmethod
    def hydro_operation_report_data(
        cls, col: str, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        relato_df = cls._validate_data(
            cls.relato(uow).relatorio_operacao_uhe,
            pd.DataFrame,
            "relatório da operação das UHE do relato",
        )
        relato2_df = cls._validate_data(
            cls.relato2(uow).relatorio_operacao_uhe,
            pd.DataFrame,
            "relatório da operação das UHE do relato2",
        )
        relato_df = relato_df.loc[~pd.isna(relato_df["FPCGC"])]
        relato2_df = relato2_df.loc[~pd.isna(relato2_df["FPCGC"])]
        hydros = relato_df[HYDRO_CODE_COL].unique().tolist()
        dfs: List[pd.DataFrame] = []
        for hydro in hydros:
            df = cls._merge_relato_relato2_df_data(
                relato_df.loc[relato_df[HYDRO_CODE_COL] == hydro],
                relato2_df.loc[relato2_df[HYDRO_CODE_COL] == hydro],
                col,
                uow,
            )
            df[HYDRO_CODE_COL] = hydro
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)
        df = cls._add_eer_sbm_to_expanded_df(df, uow)
        return df
