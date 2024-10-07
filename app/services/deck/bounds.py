from typing import Callable, Dict, TypeVar, Optional, List

import numpy as np
import pandas as pd  # type: ignore

from app.internal.constants import (
    BLOCK_COL,
    HYDRO_CODE_COL,
    THERMAL_CODE_COL,
    LOWER_BOUND_COL,
    STAGE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
    SCENARIO_COL,
    SUBMARKET_CODE_COL,
    IDENTIFICATION_COLUMNS,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
)
from app.utils.operations import fast_group_df
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
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.USINA_TERMELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._thermal_generation_bounds(
            df, uow, entity_column=THERMAL_CODE_COL
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._thermal_generation_bounds(
            df, uow, entity_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._thermal_generation_bounds(
            df, uow, entity_column=None
        ),
        OperationSynthesis(
            Variable.INTERCAMBIO,
            SpatialResolution.PAR_SUBMERCADOS,
        ): lambda df, uow, _: OperationVariableBounds._exchange_bounds(
            df,
            uow,
        ),
    }

    @classmethod
    def _unbounded(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona os valores padrão para variáveis não limitadas.
        """
        df[LOWER_BOUND_COL] = -float("inf")
        df[UPPER_BOUND_COL] = float("inf")
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
    def _spilled_flow_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Vazão Vertida (QVER) para cada UHE.
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
        para a variável de Vazão Defluente (QDEF) para cada UHE.
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
        para a variável de Vazão Turbinada (QTUR) para cada UHE.
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
    def _group_thermal_bounds_df(
        cls,
        df: pd.DataFrame,
        grouping_column: Optional[str] = None,
        extract_columns: Optional[list[str]] = [VALUE_COL],
    ) -> pd.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de usina
        para uma síntese de SBMs ou para o SIN. A agregação
        tem como requisito que as variáveis fornecidas sejam em unidades
        cuja agregação seja possível apenas pela soma.
        """
        valid_grouping_columns = [
            THERMAL_CODE_COL,
            SUBMARKET_CODE_COL,
        ]
        grouping_column_map: Dict[str, List[str]] = {
            THERMAL_CODE_COL: [
                THERMAL_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            SUBMARKET_CODE_COL: [SUBMARKET_CODE_COL],
        }
        mapped_columns = (
            grouping_column_map[grouping_column] if grouping_column else []
        )
        grouping_columns = mapped_columns + [
            c
            for c in df.columns
            if c in IDENTIFICATION_COLUMNS and c not in valid_grouping_columns
        ]
        grouped_df = fast_group_df(
            df,
            grouping_columns,
            extract_columns,
            operation="sum",
        )
        return grouped_df

    @classmethod
    def _thermal_generation_bounds(
        cls,
        df: pd.DataFrame,
        uow: AbstractUnitOfWork,
        entity_column: Optional[str],
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Geração Térmica (GTER) para cada UHE, submercado e SIN.
        """
        df[VALUE_COL] = np.round(df[VALUE_COL], 2)
        df_bounds = Deck.thermal_generation_bounds(uow)
        if entity_column != THERMAL_CODE_COL:
            df_bounds = cls._group_thermal_bounds_df(
                df_bounds,
                entity_column,
                extract_columns=[LOWER_BOUND_COL, UPPER_BOUND_COL],
            )
        entity_column = [] if entity_column is None else [entity_column]
        df = pd.merge(
            df,
            df_bounds,
            how="left",
            on=[STAGE_COL, SCENARIO_COL, BLOCK_COL] + entity_column,
            suffixes=[None, "_bounds"],
        )
        df[LOWER_BOUND_COL] = df[LOWER_BOUND_COL].fillna(float(0))
        df[UPPER_BOUND_COL] = df[UPPER_BOUND_COL].fillna(float("inf"))
        df.drop(
            [c for c in df.columns if "_bounds" in c], axis=1, inplace=True
        )
        return df

    @classmethod
    def _exchange_bounds(
        cls,
        df: pd.DataFrame,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Intercâmbio (INT) para cada par de submercados.
        """
        df[VALUE_COL] = np.round(df[VALUE_COL], 2)
        df_bounds = Deck.exchange_bounds(uow)
        df = pd.merge(
            df,
            df_bounds,
            how="left",
            on=[
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                EXCHANGE_SOURCE_CODE_COL,
                EXCHANGE_TARGET_CODE_COL,
            ],
        )
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
