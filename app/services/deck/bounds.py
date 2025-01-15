from logging import INFO, Logger
from typing import Callable, Dict, Optional, Tuple, TypeVar

import numpy as np
import pandas as pd  # type: ignore

from app.internal.constants import (
    BLOCK_COL,
    EER_CODE_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    HYDRO_CODE_COL,
    IDENTIFICATION_COLUMNS,
    LOWER_BOUND_COL,
    SCENARIO_COL,
    STAGE_COL,
    SUBMARKET_CODE_COL,
    THERMAL_CODE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.unit import Unit
from app.model.operation.variable import Variable
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.operations import fast_group_df


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
            Variable.VAZAO_EVAPORADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        # TODO - melhorar usando os valores de retirada como upper e lower
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        # TODO - melhorar usando os valores de desvio fornecidos
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.USINA_TERMELETRICA,
        ): lambda df,
        uow,
        _: OperationVariableBounds._thermal_generation_bounds(
            df, uow, entity_column=THERMAL_CODE_COL
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._thermal_generation_bounds(
            df, uow, entity_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        _: OperationVariableBounds._thermal_generation_bounds(
            df, uow, entity_column=None
        ),
        OperationSynthesis(
            Variable.CUSTO_GERACAO_TERMICA,
            SpatialResolution.USINA_TERMELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.CUSTO_GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.CUSTO_OPERACAO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.CUSTO_FUTURO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.GERACAO_USINAS_NAO_SIMULADAS,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.GERACAO_USINAS_NAO_SIMULADAS,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.INTERCAMBIO,
            SpatialResolution.PAR_SUBMERCADOS,
        ): lambda df, uow, _: OperationVariableBounds._exchange_bounds(
            df,
            uow,
        ),
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
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
            initial=True,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=None,
            initial=True,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=HYDRO_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=EER_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=SUBMARKET_CODE_COL,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df,
        uow,
        entities: OperationVariableBounds._stored_volume_bounds(
            df,
            uow,
            synthesis_unit=Unit.hm3_modif.value,
            ordered_entities=entities,
            entity_column=None,
        ),
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._hydro_generation_bounds(
            df, uow, entity_column=HYDRO_CODE_COL
        ),
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._hydro_generation_bounds(
            df, uow, entity_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._hydro_generation_bounds(
            df, uow, entity_column=None
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
    def _stored_volume_bounds(
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
        para as variáveis de Volume Armazenado Absoluto (VARM) e Volume
        Armazenado Percentual (VARP) para cada UHE.
        """

        def _get_group_and_cast_bounds() -> Tuple[np.ndarray, np.ndarray]:
            upper_bound_df = Deck.stored_volume_upper_bounds(uow)
            lower_bound_df = Deck.stored_volume_lower_bounds(uow)
            if initial:
                num_stages = len(Deck.stages_start_date(uow))
                # Offset by 1 stage
                upper_bound_df[STAGE_COL] += 1
                upper_bound_df.loc[
                    upper_bound_df[STAGE_COL] == num_stages + 1, STAGE_COL
                ] = 1
                upper_bound_df.loc[
                    (upper_bound_df[STAGE_COL] == 1)
                    & (~upper_bound_df[VALUE_COL].isna()),
                    VALUE_COL,
                ] = float("inf")
                upper_bound_df = upper_bound_df.sort_values([
                    STAGE_COL,
                    HYDRO_CODE_COL,
                ])
                lower_bound_df[STAGE_COL] += 1
                lower_bound_df.loc[
                    lower_bound_df[STAGE_COL] == num_stages + 1, STAGE_COL
                ] = 1
                lower_bound_df.loc[
                    (lower_bound_df[STAGE_COL] == 1)
                    & (~lower_bound_df[VALUE_COL].isna()),
                    VALUE_COL,
                ] = 0.0
                lower_bound_df = lower_bound_df.sort_values([
                    STAGE_COL,
                    HYDRO_CODE_COL,
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
            df = df.sort_values(grouping_columns)
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
            num_digits = 2

            df[VALUE_COL] = np.round(df[VALUE_COL], num_digits)
            df[LOWER_BOUND_COL] = np.round(df[LOWER_BOUND_COL], num_digits)
            df[UPPER_BOUND_COL] = np.round(df[UPPER_BOUND_COL], num_digits)
            return df

        entity_column_list = [entity_column] if entity_column else []
        grouping_columns = entity_column_list + [STAGE_COL]
        lower_bounds, upper_bounds = _get_group_and_cast_bounds()
        if entity_column != HYDRO_CODE_COL:
            df = cls._group_hydro_df(df, entity_column)
        df = _repeat_bounds_by_scenario(
            df,
            lower_bounds,
            upper_bounds,
        )
        df = _sort_and_round_bounds(df)
        return df

    @classmethod
    def _group_hydro_df(
        cls, df: pd.DataFrame, grouping_column: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de UHE
        para uma síntese de REEs, SBMs ou para o SIN. A agregação
        tem como requisito que as variáveis fornecidas sejam em unidades
        cuja agregação seja possível apenas pela soma.
        """
        valid_grouping_columns = [
            HYDRO_CODE_COL,
            EER_CODE_COL,
            SUBMARKET_CODE_COL,
        ]

        grouping_column_map: Dict[str, list[str]] = {
            HYDRO_CODE_COL: [
                HYDRO_CODE_COL,
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            EER_CODE_COL: [
                EER_CODE_COL,
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
            [VALUE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL],
            operation="sum",
        )

        return grouped_df

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
        extract_columns: list[str] = [VALUE_COL],
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
        grouping_column_map: Dict[str, list[str]] = {
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
        df_bounds = Deck.thermal_generation_bounds(uow)
        if entity_column != THERMAL_CODE_COL:
            df_bounds = cls._group_thermal_bounds_df(
                df_bounds,
                entity_column,
                extract_columns=[LOWER_BOUND_COL, UPPER_BOUND_COL],
            )
        entity_column_list = [] if entity_column is None else [entity_column]
        df = pd.merge(
            df,
            df_bounds,
            how="left",
            on=[STAGE_COL, SCENARIO_COL, BLOCK_COL] + entity_column_list,
            suffixes=[None, "_bounds"],
        )
        df[LOWER_BOUND_COL] = df[LOWER_BOUND_COL].fillna(float(0))
        df[UPPER_BOUND_COL] = df[UPPER_BOUND_COL].fillna(float("inf"))
        for col in [VALUE_COL, UPPER_BOUND_COL, LOWER_BOUND_COL]:
            df[col] = np.round(df[col], 2)
        df.drop([c for c in df.columns if "_bounds" in c], axis=1, inplace=True)
        return df

    @classmethod
    def _group_hydro_bounds_df(
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
            HYDRO_CODE_COL,
            SUBMARKET_CODE_COL,
        ]
        grouping_column_map: Dict[str, list[str]] = {
            HYDRO_CODE_COL: [
                HYDRO_CODE_COL,
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
    def _hydro_generation_bounds(
        cls,
        df: pd.DataFrame,
        uow: AbstractUnitOfWork,
        entity_column: Optional[str],
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Geração Hidráulica (GHID) para cada UHE, submercado e SIN.
        """
        df[VALUE_COL] = np.round(df[VALUE_COL], 2)
        df_bounds = Deck.hydro_generation_bounds(uow)

        if entity_column != HYDRO_CODE_COL:
            df_bounds = cls._group_hydro_bounds_df(
                df_bounds,
                entity_column,
                extract_columns=[LOWER_BOUND_COL, UPPER_BOUND_COL],
            )
        entity_column = [] if entity_column is None else [entity_column]
        df = pd.merge(
            df,
            df_bounds,
            how="left",
            on=[STAGE_COL, BLOCK_COL] + entity_column,
            suffixes=[None, "_bounds"],
        )

        df[LOWER_BOUND_COL] = df[LOWER_BOUND_COL].fillna(float(0))
        df[UPPER_BOUND_COL] = df[UPPER_BOUND_COL].fillna(float("inf"))
        df.drop([c for c in df.columns if "_bounds" in c], axis=1, inplace=True)

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
        for col in [VALUE_COL, UPPER_BOUND_COL, LOWER_BOUND_COL]:
            df[col] = np.round(df[col], 2)
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
            try:
                return cls.MAPPINGS[s](df, uow, ordered_synthesis_entities)
            except Exception:
                return cls._unbounded(df)
        else:
            return cls._unbounded(df)
