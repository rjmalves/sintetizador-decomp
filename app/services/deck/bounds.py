from logging import INFO, Logger
from typing import Callable, Dict, Optional, Tuple, TypeVar

import numpy as np
import pandas as pd  # type: ignore

from app.internal.constants import (
    BLOCK_COL,
    EER_CODE_COL,
    LOWER_BOUND_COL,
    SCENARIO_COL,
    STAGE_COL,
    SUBMARKET_CODE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.unit import Unit
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
    logger: Optional[Logger] = None

    MAPPINGS: Dict[OperationSynthesis, Callable] = {
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.MWmes.value,
            ordered_entities=entities,
            initial=True,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df, uow, synthesis_unit=Unit.MWmes.value, ordered_entities=entities
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
            initial=True,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_energy_bounds(
            df,
            uow,
            synthesis_unit=Unit.perc_modif.value,
            ordered_entities=entities,
        ),
    }

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _stored_energy_bounds(
        cls,
        df: pd.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list],
        entity_column: Optional[str] = None,
        initial: bool = False,
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Energia Armazenada Absoluta (EARM) e Energia
        Armazenada Percentual (EARP).
        """

        def _get_group_and_cast_bounds() -> Tuple[np.ndarray, np.ndarray]:
            upper_bound_df = Deck.stored_energy_upper_bounds_eer(uow)
            lower_bound_df = Deck.stored_energy_lower_bounds_eer(uow)
            if initial:
                num_stages = len(Deck.stages_start_date(uow))
                # Offset by 1 stage
                upper_bound_df[STAGE_COL] += 1
                upper_bound_df.loc[
                    upper_bound_df[STAGE_COL] == num_stages + 1, STAGE_COL
                ] = 1
                upper_bound_df.loc[
                    upper_bound_df[STAGE_COL] == 1, VALUE_COL
                ] = float("inf")
                upper_bound_df = upper_bound_df.sort_values([
                    STAGE_COL,
                    EER_CODE_COL,
                ])
                lower_bound_df[STAGE_COL] += 1
                lower_bound_df.loc[
                    lower_bound_df[STAGE_COL] == num_stages + 1, STAGE_COL
                ] = 1
                lower_bound_df.loc[
                    lower_bound_df[STAGE_COL] == 1, VALUE_COL
                ] = 0.0
                lower_bound_df = lower_bound_df.sort_values([
                    STAGE_COL,
                    EER_CODE_COL,
                ])
            upper_bounds = (
                upper_bound_df.groupby(grouping_columns, as_index=False)
                .sum(numeric_only=True)[VALUE_COL]
                .to_numpy()
            )
            lower_bounds = (
                lower_bound_df.groupby(grouping_columns, as_index=False)
                .sum(numeric_only=True)[VALUE_COL]
                .to_numpy()
            )
            if synthesis_unit == Unit.perc_modif.value:
                lower_bounds = lower_bounds / upper_bounds * 100.0
                upper_bounds = 100.0 * np.ones_like(lower_bounds)
            return lower_bounds, upper_bounds

        def _repeat_bounds_by_scenario(
            df: pd.DataFrame,
            lower_bounds: np.ndarray,
            upper_bounds: np.ndarray,
        ) -> pd.DataFrame:
            num_entities = (
                len(ordered_entities[entity_column]) if entity_column else 1
            )
            num_stages = len(ordered_entities[STAGE_COL])
            num_scenarios = len(ordered_entities[SCENARIO_COL])
            num_blocks = len(ordered_entities[BLOCK_COL])
            df[LOWER_BOUND_COL] = cls._repeats_data_by_scenario(
                lower_bounds,
                num_entities,
                num_stages,
                num_scenarios,
                num_blocks,
            )
            df[UPPER_BOUND_COL] = cls._repeats_data_by_scenario(
                upper_bounds,
                num_entities,
                num_stages,
                num_scenarios,
                num_blocks,
            )
            return df

        def _sort_and_round_bounds(df: pd.DataFrame) -> pd.DataFrame:
            num_digits = 1 if synthesis_unit == Unit.perc_modif.value else 0
            df[VALUE_COL] = np.round(df[VALUE_COL], num_digits)
            df = df.sort_values(grouping_columns)
            df[LOWER_BOUND_COL] = np.round(df[LOWER_BOUND_COL], num_digits)
            df[UPPER_BOUND_COL] = np.round(df[UPPER_BOUND_COL], num_digits)
            return df

        entity_column_list = [entity_column] if entity_column else []
        grouping_columns = entity_column_list + [STAGE_COL]
        lower_bounds, upper_bounds = _get_group_and_cast_bounds()
        df = _repeat_bounds_by_scenario(df, lower_bounds, upper_bounds)
        df = _sort_and_round_bounds(df)
        return df

    @classmethod
    def _repeats_data_by_scenario(
        cls,
        data: np.ndarray,
        num_entities: int,
        num_stages: int,
        num_scenarios: int,
        num_blocks: int,
    ):
        """
        Expande os dados cadastrais para cada cenário, mantendo a ordem dos
        patamares internamente.
        """
        data_cenarios = np.zeros((len(data) * num_scenarios,), dtype=np.float64)
        for i in range(num_entities):
            for j in range(num_stages):
                i_i = i * num_stages * num_blocks + j * num_blocks
                i_f = i_i + num_blocks
                data_cenarios[i_i * num_scenarios : i_f * num_scenarios] = (
                    np.tile(data[i_i:i_f], num_scenarios)
                )
        return data_cenarios

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
