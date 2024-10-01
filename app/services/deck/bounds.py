import pandas as pd  # type: ignore
import numpy as np
from typing import Dict, Callable, TypeVar
from app.model.operation.operationsynthesis import OperationSynthesis
from app.services.unitofwork import AbstractUnitOfWork
from app.internal.constants import (
    UPPER_BOUND_COL,
    LOWER_BOUND_COL,
    VALUE_COL,
    STAGE_COL,
    SCENARIO_COL,
    HYDRO_CODE_COL,
    BLOCK_COL,
    LOWER_BOUND_UNIT_COL,
    UPPER_BOUND_UNIT_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.deck import Deck
from app.model.operation.unit import Unit


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
        )
    }

    @classmethod
    def _spilled_flow_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variávei de Vazão Vertida (QVER) para cada UHE.
        """

        def __overwrite_with_operative_constraints_bounds(uow, df):
            df_operative_constraints = Deck.hydro_spilled_flow_bounds(uow)
            for idx, row in df_operative_constraints.iterrows():
                stage = row[STAGE_COL]
                hydro_code = row[HYDRO_CODE_COL]
                block = row[BLOCK_COL]
                upper_bound = row[UPPER_BOUND_COL]
                lower_bound = row[LOWER_BOUND_COL]

                condition = (
                    (df[HYDRO_CODE_COL] == hydro_code)
                    & (df[STAGE_COL] == stage)
                    & (df[BLOCK_COL] == block)
                )
                df[LOWER_BOUND_COL] = np.where(
                    condition,
                    (
                        np.round(lower_bound, 2)
                        if ~np.isnan(lower_bound)
                        else 0.00
                    ),
                    df[LOWER_BOUND_COL],
                )
                df[UPPER_BOUND_COL] = np.where(
                    condition,
                    (
                        np.round(upper_bound, 2)
                        if ~np.isnan(upper_bound)
                        else float("inf")
                    ),
                    df[UPPER_BOUND_COL],
                )
            return df

        def __overwrite_with_spill_operation_status_bound(uow, df):
            spill_limits = Deck.hydro_operation_report_data(
                "considera_soleira_vertedouro", uow
            )
            df_spill_limits = spill_limits.loc[spill_limits["valor"] == True]
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

        # Inicializa valores (liminf=0 e limsup=inf)
        df[VALUE_COL] = np.round(df[VALUE_COL], 2)
        df[LOWER_BOUND_COL] = 0.00
        df[UPPER_BOUND_COL] = float("inf")

        # TODO - hydro spilled flow bounds não retorna valor para pat 0
        # Sobrescreve com restrições operativas
        df = __overwrite_with_operative_constraints_bounds(uow, df)

        # TODO: comportamento não é conforme o esperado pois o resultado de soleira
        # impresso no relato não é o que foi considerado no PL efetivamente, e sim
        # um cálculo pós-processamento com resultados da operação
        # # Substitui cota abaixo da soleira de vertedouro com o limite superior 0
        # df = __overwrite_with_spill_operation_status_bound(uow, df)

        return df

    @classmethod
    def _diverted_flow_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork, synthesis_unit: str
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Vazão Desviada (QDES) para cada UHE.
        """

        def __overwrite_with_operative_constraints_bounds(uow, df):
            return

        def __overwrite_with_registration_dara_bound(uow, df):
            return

        # Inicializa valores (liminf=0 e limsup=inf)
        df[VALUE_COL] = np.round(df[VALUE_COL], 2)
        df[LOWER_BOUND_COL] = 0.00
        df[UPPER_BOUND_COL] = float("inf")
        df[LOWER_BOUND_UNIT_COL] = Unit.m3s.value
        df[UPPER_BOUND_UNIT_COL] = Unit.m3s.value

        return df

    @classmethod
    def is_bounded(cls, s: OperationSynthesis) -> bool:
        """
        Verifica se uma determinada síntese possui limites implementados
        para adição ao DataFrame.
        """
        return s in cls.MAPPINGS

    @classmethod
    def _unbounded(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona os valores padrão para variáveis não limitadas.
        """
        df[LOWER_BOUND_COL] = -float("inf")
        df[UPPER_BOUND_COL] = float("inf")
        return df

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
