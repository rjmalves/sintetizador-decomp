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

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.model.execution.inviabilidade import Inviabilidade

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
    def data_inicio_estudo(cls, uow: AbstractUnitOfWork) -> datetime:
        name = "data_inicio_estudo"
        if name not in cls.DECK_DATA_CACHING:
            dt_reg = cls._validate_data(
                cls.dadger(uow).dt,
                DT,
                "registro DT",
            )
            year = cls._validate_data(
                dt_reg.ano,
                int,
                "ano de inicio do estudo",
            )
            month = cls._validate_data(
                dt_reg.mes,
                int,
                "mes de inicio do estudo",
            )
            day = cls._validate_data(
                dt_reg.dia,
                int,
                "dia de inicio do estudo",
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
    def earmax_sin(cls, uow: AbstractUnitOfWork) -> float:
        name = "earmax_sin"
        if name not in cls.DECK_DATA_CACHING:
            df = cls._validate_data(
                cls.relato(uow).energia_armazenada_maxima_submercado,
                pd.DataFrame,
                "Earmax SIN",
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
    def convergencia(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        convergencia = cls.DECK_DATA_CACHING.get("convergencia")
        if convergencia is None:
            convergencia = cls._validate_data(
                cls.relato(uow).convergencia,
                pd.DataFrame,
                "convergencia",
            )
            cls.DECK_DATA_CACHING["convergencia"] = convergencia
        return convergencia

    @classmethod
    def inviabilidades_iteracoes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        inviabilidades_iteracoes = cls.DECK_DATA_CACHING.get(
            "inviabilidades_iteracoes"
        )
        if inviabilidades_iteracoes is None:
            inviabilidades_iteracoes = cls._validate_data(
                cls._get_inviabunic(uow).inviabilidades_iteracoes,
                pd.DataFrame,
                "inviabilidades_iteracoes",
            )
            cls.DECK_DATA_CACHING["inviabilidades_iteracoes"] = (
                inviabilidades_iteracoes
            )
        return inviabilidades_iteracoes

    @classmethod
    def inviabilidades_simulacao_final(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        inviabilidades_simulacao_final = cls.DECK_DATA_CACHING.get(
            "inviabilidades_simulacao_final"
        )
        if inviabilidades_simulacao_final is None:
            inviabilidades_simulacao_final = cls._validate_data(
                cls._get_inviabunic(uow).inviabilidades_simulacao_final,
                pd.DataFrame,
                "inviabilidades_simulacao_final",
            )
            cls.DECK_DATA_CACHING["inviabilidades_simulacao_final"] = (
                inviabilidades_simulacao_final
            )
        return inviabilidades_simulacao_final

    @classmethod
    def inviabilidades(cls, uow: AbstractUnitOfWork) -> list:
        inviabilidades = cls.DECK_DATA_CACHING.get("inviabilidades")
        if inviabilidades is None:
            df_iter = cls.inviabilidades_iteracoes(uow)
            df_sf = cls.inviabilidades_simulacao_final(uow)
            df_sf["iteracao"] = -1
            df_inviabs = pd.concat([df_iter, df_sf], ignore_index=True)
            inviabilidades_aux = []
            for _, linha in df_inviabs.iterrows():
                inviabilidades_aux.append(
                    Inviabilidade.factory(
                        linha, cls._get_hidr(uow), cls._get_relato(uow)
                    )
                )
            inviabilidades = cls._validate_data(
                inviabilidades_aux,
                list,
                "inviabilidades",
            )
            cls.DECK_DATA_CACHING["inviabilidades"] = inviabilidades
        return inviabilidades

    @classmethod
    def tempos_por_etapa(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        tempos_por_etapa = cls.DECK_DATA_CACHING.get("tempos_por_etapa")
        if tempos_por_etapa is None:
            tempos_por_etapa = cls._validate_data(
                cls._get_decomptim(uow).tempos_etapas,
                pd.DataFrame,
                "tempos_por_etapa",
            )
            cls.DECK_DATA_CACHING["tempos_por_etapa"] = tempos_por_etapa
        return tempos_por_etapa

    @classmethod
    def probabilidades(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        probabilidades = cls.DECK_DATA_CACHING.get("probabilidades")
        if probabilidades is None:
            probabilidades = cls._validate_data(
                cls._get_vazoes(uow).probabilidades,
                pd.DataFrame,
                "probabilidades",
            )
            cls.DECK_DATA_CACHING["probabilidades"] = probabilidades
        return probabilidades

    @staticmethod
    def _stub_nodes_scenarios_v31_0_2(df: pd.DataFrame) -> pd.DataFrame:
        estagios = df["estagio"].unique().tolist()
        # Para todos os estágios antes do último, fixa cenário em 1
        df.loc[df["estagio"].isin(estagios[:-1]), "cenario"] = 1
        # Subtrai dos cenários o valor de n_semanas
        df.loc[df["estagio"] == estagios[-1], "cenario"] -= len(estagios) - 1
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
        """
        - estagio (`int`)
        - data_inicio (`datetime`)
        - data_fim (`datetime`)
        - numero_aberturas (`int`)
        """
        name = "stages_durations"
        df = cls.DECK_DATA_CACHING.get(name)
        if df is None:
            df = cls.dec_eco_discr(uow)
            df = df.loc[df["patamar"].isna()]
            df["duracao_acumulada"] = df["duracao"].cumsum()
            df["data_inicio"] = df.apply(
                lambda linha: cls.data_inicio_estudo(uow)
                + timedelta(
                    hours=df.loc[df["estagio"] < linha["estagio"], "duracao"]
                    .to_numpy()
                    .sum()
                ),
                axis=1,
            )
            df["data_fim"] = df.apply(
                lambda linha: linha["data_inicio"]
                + timedelta(hours=linha["duracao"]),
                axis=1,
            )
            df = df[
                ["estagio", "data_inicio", "data_fim", "numero_aberturas"]
            ].copy()
            cls.DECK_DATA_CACHING[name] = df
        return df

    @classmethod
    def stages_start_date(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        name = "stages_start_date"
        dates = cls.DECK_DATA_CACHING.get(name)
        if dates is None:
            dates = cls.stages_durations(uow)["data_inicio"].tolist()
            cls.DECK_DATA_CACHING[name] = dates
        return dates

    @classmethod
    def stages_end_date(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        name = "stages_end_date"
        dates = cls.DECK_DATA_CACHING.get(name)
        if dates is None:
            dates = cls.stages_durations(uow)["data_fim"].tolist()
            cls.DECK_DATA_CACHING[name] = dates
        return dates

    @classmethod
    def blocks(cls, uow: AbstractUnitOfWork) -> List[int]:
        name = "blocks"
        if name not in cls.DECK_DATA_CACHING:
            df = cls.dec_eco_discr(uow)
            cls.DECK_DATA_CACHING[name] = (
                df["patamar"].dropna().unique().tolist()
            )
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def _add_dates_to_df(
        cls, line: pd.Series, uow: AbstractUnitOfWork
    ) -> np.ndarray:
        df = cls.stages_durations(uow)
        return (
            df.loc[
                df["estagio"] == line["estagio"],
                ["data_inicio", "data_fim"],
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
            df[["dataInicio", "dataFim"]] = df.apply(
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
            df[["dataInicio", "dataFim"]] = df.apply(
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
            df[["dataInicio", "dataFim"]] = df.apply(
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
            df[["dataInicio", "dataFim"]] = df.apply(
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
            df[["dataInicio", "dataFim"]] = df.apply(
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
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda line: cls._add_dates_to_df(line, uow),
                axis=1,
                result_type="expand",
            )
            cls.DECK_DATA_CACHING[name] = df
        return df.copy()
