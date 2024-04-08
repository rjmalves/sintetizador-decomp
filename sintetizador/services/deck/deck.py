from idecomp.decomp import Relato, InviabUnic, Hidr, Decomptim, Vazoes
import logging
import pandas as pd  # type: ignore
from typing import Any, Optional, TypeVar, Type, Dict

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.model.execution.inviabilidade import Inviabilidade

# from app.internal.constants import STRING_DF_TYPE


class Deck:

    T = TypeVar("T")
    logger: Optional[logging.Logger] = None

    DECK_DATA_CACHING: Dict[str, Any] = {}

    @classmethod
    def _get_relato(cls, uow: AbstractUnitOfWork) -> Relato:
        with uow:
            relato = uow.files.get_relato()
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
    def _validate_data(cls, data, type: Type[T], msg: str = "dados") -> T:
        if not isinstance(data, type):
            if cls.logger is not None:
                cls.logger.error(f"Erro na leitura de {msg}")
            raise RuntimeError()
        return data

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
