from typing import Callable, Dict, TypeVar

import numpy as np
import pandas as pd  # type: ignore

from app.internal.constants import (
    BLOCK_COL,
    HYDRO_CODE_COL,
    LOWER_BOUND_COL,
    STAGE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork


class OperationVariableBounds:
    """
    Entidade responsável por calcular os limites das variáveis de operação
    existentes nos arquivos de saída do DECOMP, que são processadas no
    processo de síntese da operação.
    """

    T = TypeVar("T")

    MAPPINGS: Dict[OperationSynthesis, Callable] = {
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._spilled_flow_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._outflow_bounds(df, uow),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._turbined_flow_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
    }

    @classmethod
    def _spilled_flow_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variávei de Vazão Vertida (QVER) para cada UHE.
        """

        df[VALUE_COL] = np.round(df[VALUE_COL], 2)
        df_bounds = Deck.hydro_spilled_flow_bounds(uow)
        df = pd.merge(
            df,
            df_bounds,
            how="left",
            on=[HYDRO_CODE_COL, STAGE_COL, BLOCK_COL],
        )
        return df

    @classmethod
    def _outflow_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variávei de Vazão Defluente (QDEF) para cada UHE.
        """

        df[VALUE_COL] = np.round(df[VALUE_COL], 2)
        df_bounds = Deck.hydro_outflow_bounds(uow)
        df = pd.merge(
            df,
            df_bounds,
            how="left",
            on=[HYDRO_CODE_COL, STAGE_COL, BLOCK_COL],
        )
        return df

    @classmethod
    def _turbined_flow_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variávei de Vazão Turbinada (QTUR) para cada UHE.
        """

        df[VALUE_COL] = np.round(df[VALUE_COL], 2)
        df_bounds = Deck.hydro_turbined_flow_bounds(uow)
        df = pd.merge(
            df,
            df_bounds,
            how="left",
            on=[HYDRO_CODE_COL, STAGE_COL, BLOCK_COL],
        )
        return df

    @classmethod
    def _lower_bounded_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior zero e superior
        infinito.
        """
        df[VALUE_COL] = np.round(df[VALUE_COL], 2)
        df[LOWER_BOUND_COL] = 0.0
        df[UPPER_BOUND_COL] = float("inf")

        return df

    @classmethod
    def _unbounded(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona os valores padrão para variáveis não limitadas.
        """
        df[LOWER_BOUND_COL] = -float("inf")
        df[UPPER_BOUND_COL] = float("inf")
        return df

    @classmethod
    def is_bounded(cls, s: OperationSynthesis) -> bool:
        """
        Verifica se uma determinada síntese possui limites implementados
        para adição ao DataFrame.
        """
        return s in cls.MAPPINGS

    @classmethod
    def resolve_bounds(
        cls,
        s: OperationSynthesis,
        df: pd.DataFrame,
        ordered_synthesis_entities: Dict[str, list],
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        """
        Adiciona colunas de limite inferior e superior a um DataFrame,
        calculando os valores necessários caso a variável seja limitada
        ou atribuindo -inf e +inf caso contrário.

        """
        if cls.is_bounded(s):
            return cls.MAPPINGS[s](df, uow, ordered_synthesis_entities)
        else:
            return cls._unbounded(df)
