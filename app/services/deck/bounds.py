from logging import INFO, Logger
from typing import Any, Callable, Dict, Optional, TypeVar

import polars as pl

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


class OperationVariableBounds:
    """
    Entidade responsável por calcular os limites das variáveis de operação
    existentes nos arquivos de saída do DECOMP, que são processadas no
    processo de síntese da operação.
    """

    T = TypeVar("T")
    logger: Optional[Logger] = None

    MAPPINGS: Dict[OperationSynthesis, Callable[..., pl.DataFrame]] = {
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
        ): lambda df, uow, _: (
            OperationVariableBounds._thermal_generation_bounds(
                df, uow, entity_column=THERMAL_CODE_COL
            )
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: (
            OperationVariableBounds._thermal_generation_bounds(
                df, uow, entity_column=SUBMARKET_CODE_COL
            )
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: (
            OperationVariableBounds._thermal_generation_bounds(
                df, uow, entity_column=None
            )
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
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_energy_bounds(
                df,
                uow,
                synthesis_unit=Unit.MWmes.value,
                ordered_entities=entities,
                entity_column=EER_CODE_COL,
                initial=True,
            )
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_energy_bounds(
                df,
                uow,
                synthesis_unit=Unit.MWmes.value,
                ordered_entities=entities,
                entity_column=EER_CODE_COL,
            )
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_energy_bounds(
                df,
                uow,
                synthesis_unit=Unit.perc_modif.value,
                ordered_entities=entities,
                entity_column=EER_CODE_COL,
                initial=True,
            )
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_energy_bounds(
                df,
                uow,
                synthesis_unit=Unit.perc_modif.value,
                ordered_entities=entities,
                entity_column=EER_CODE_COL,
            )
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_energy_bounds(
                df,
                uow,
                synthesis_unit=Unit.MWmes.value,
                ordered_entities=entities,
                entity_column=SUBMARKET_CODE_COL,
                initial=True,
            )
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_energy_bounds(
                df,
                uow,
                synthesis_unit=Unit.MWmes.value,
                ordered_entities=entities,
                entity_column=SUBMARKET_CODE_COL,
            )
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_energy_bounds(
                df,
                uow,
                synthesis_unit=Unit.perc_modif.value,
                ordered_entities=entities,
                entity_column=SUBMARKET_CODE_COL,
                initial=True,
            )
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_energy_bounds(
                df,
                uow,
                synthesis_unit=Unit.perc_modif.value,
                ordered_entities=entities,
                entity_column=SUBMARKET_CODE_COL,
            )
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_energy_bounds(
                df,
                uow,
                synthesis_unit=Unit.MWmes.value,
                ordered_entities=entities,
                initial=True,
            )
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_energy_bounds(
                df,
                uow,
                synthesis_unit=Unit.MWmes.value,
                ordered_entities=entities,
            )
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_energy_bounds(
                df,
                uow,
                synthesis_unit=Unit.perc_modif.value,
                ordered_entities=entities,
                initial=True,
            )
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_energy_bounds(
                df,
                uow,
                synthesis_unit=Unit.perc_modif.value,
                ordered_entities=entities,
            )
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_volume_bounds(
                df,
                uow,
                synthesis_unit=Unit.hm3_modif.value,
                ordered_entities=entities,
                entity_column=HYDRO_CODE_COL,
                initial=True,
            )
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_volume_bounds(
                df,
                uow,
                synthesis_unit=Unit.hm3_modif.value,
                ordered_entities=entities,
                entity_column=EER_CODE_COL,
                initial=True,
            )
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_volume_bounds(
                df,
                uow,
                synthesis_unit=Unit.hm3_modif.value,
                ordered_entities=entities,
                entity_column=SUBMARKET_CODE_COL,
                initial=True,
            )
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_volume_bounds(
                df,
                uow,
                synthesis_unit=Unit.hm3_modif.value,
                ordered_entities=entities,
                entity_column=None,
                initial=True,
            )
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_volume_bounds(
                df,
                uow,
                synthesis_unit=Unit.hm3_modif.value,
                ordered_entities=entities,
                entity_column=HYDRO_CODE_COL,
            )
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_volume_bounds(
                df,
                uow,
                synthesis_unit=Unit.hm3_modif.value,
                ordered_entities=entities,
                entity_column=EER_CODE_COL,
            )
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_volume_bounds(
                df,
                uow,
                synthesis_unit=Unit.hm3_modif.value,
                ordered_entities=entities,
                entity_column=SUBMARKET_CODE_COL,
            )
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, entities: (
            OperationVariableBounds._stored_volume_bounds(
                df,
                uow,
                synthesis_unit=Unit.hm3_modif.value,
                ordered_entities=entities,
                entity_column=None,
            )
        ),
    }

    @classmethod
    def _log(cls, msg: str, level: int = INFO) -> None:
        if cls.logger:
            cls.logger.log(level, msg)

    @classmethod
    def _stored_energy_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list[Any]],
        entity_column: Optional[str] = None,
        initial: bool = False,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Energia Armazenada Absoluta (EARM) e Energia
        Armazenada Percentual (EARP).
        """
        entity_column_list = [entity_column] if entity_column else []
        grouping_columns = entity_column_list + [STAGE_COL]

        upper_bound_pl = pl.from_pandas(
            Deck.stored_energy_upper_bounds_eer(uow)
        )
        lower_bound_pl = pl.from_pandas(
            Deck.stored_energy_lower_bounds_eer(uow)
        )

        if initial:
            num_stages = len(Deck.stages_start_date(uow))
            # Offset STAGE_COL by 1; wrap stage num_stages+1 back to 1
            upper_bound_pl = upper_bound_pl.with_columns(
                (pl.col(STAGE_COL) + 1).alias(STAGE_COL)
            )
            upper_bound_pl = upper_bound_pl.with_columns(
                pl.when(pl.col(STAGE_COL) == num_stages + 1)
                .then(pl.lit(1))
                .otherwise(pl.col(STAGE_COL))
                .alias(STAGE_COL)
            )
            upper_bound_pl = upper_bound_pl.with_columns(
                pl.when(pl.col(STAGE_COL) == 1)
                .then(pl.lit(float("inf")))
                .otherwise(pl.col(VALUE_COL))
                .alias(VALUE_COL)
            )
            upper_bound_pl = upper_bound_pl.sort([STAGE_COL, EER_CODE_COL])

            lower_bound_pl = lower_bound_pl.with_columns(
                (pl.col(STAGE_COL) + 1).alias(STAGE_COL)
            )
            lower_bound_pl = lower_bound_pl.with_columns(
                pl.when(pl.col(STAGE_COL) == num_stages + 1)
                .then(pl.lit(1))
                .otherwise(pl.col(STAGE_COL))
                .alias(STAGE_COL)
            )
            lower_bound_pl = lower_bound_pl.with_columns(
                pl.when(pl.col(STAGE_COL) == 1)
                .then(pl.lit(0.0))
                .otherwise(pl.col(VALUE_COL))
                .alias(VALUE_COL)
            )
            lower_bound_pl = lower_bound_pl.sort([STAGE_COL, EER_CODE_COL])

        # Group by entity + stage and sum
        upper_grouped = upper_bound_pl.group_by(grouping_columns).agg(
            pl.col(VALUE_COL).sum().alias(UPPER_BOUND_COL)
        )
        lower_grouped = lower_bound_pl.group_by(grouping_columns).agg(
            pl.col(VALUE_COL).sum().alias(LOWER_BOUND_COL)
        )

        # Join lower and upper bounds together on grouping_columns
        bounds_df = lower_grouped.join(
            upper_grouped, on=grouping_columns, how="left"
        )

        if synthesis_unit == Unit.perc_modif.value:
            bounds_df = bounds_df.with_columns(
                (
                    pl.col(LOWER_BOUND_COL) / pl.col(UPPER_BOUND_COL) * 100.0
                ).alias(LOWER_BOUND_COL),
                pl.lit(100.0).alias(UPPER_BOUND_COL),
            )

        # Join bounds onto the main DataFrame using entity + stage columns
        df = df.join(bounds_df, on=grouping_columns, how="left")

        # Round values
        num_digits = 1 if synthesis_unit == Unit.perc_modif.value else 0
        df = df.with_columns(
            pl.col(VALUE_COL).round(num_digits),
            pl.col(LOWER_BOUND_COL).round(num_digits),
            pl.col(UPPER_BOUND_COL).round(num_digits),
        )
        df = df.sort(grouping_columns)
        return df

    @classmethod
    def _stored_volume_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        synthesis_unit: str,
        ordered_entities: Dict[str, list[Any]],
        entity_column: Optional[str] = None,
        initial: bool = False,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Armazenado Absoluto (VARM) e Volume
        Armazenado Percentual (VARP) para cada UHE.
        """
        entity_column_list = [entity_column] if entity_column else []
        grouping_columns = entity_column_list + [STAGE_COL]

        upper_bound_pl = pl.from_pandas(Deck.stored_volume_upper_bounds(uow))
        lower_bound_pl = pl.from_pandas(Deck.stored_volume_lower_bounds(uow))

        if initial:
            num_stages = len(Deck.stages_start_date(uow))
            # Offset STAGE_COL by 1; wrap stage num_stages+1 back to 1
            upper_bound_pl = upper_bound_pl.with_columns(
                (pl.col(STAGE_COL) + 1).alias(STAGE_COL)
            )
            upper_bound_pl = upper_bound_pl.with_columns(
                pl.when(pl.col(STAGE_COL) == num_stages + 1)
                .then(pl.lit(1))
                .otherwise(pl.col(STAGE_COL))
                .alias(STAGE_COL)
            )
            # Only set inf where VALUE_COL is not null (mirrors pandas ~isna() condition)
            upper_bound_pl = upper_bound_pl.with_columns(
                pl.when(
                    (pl.col(STAGE_COL) == 1) & pl.col(VALUE_COL).is_not_null()
                )
                .then(pl.lit(float("inf")))
                .otherwise(pl.col(VALUE_COL))
                .alias(VALUE_COL)
            )
            upper_bound_pl = upper_bound_pl.sort([STAGE_COL, HYDRO_CODE_COL])

            lower_bound_pl = lower_bound_pl.with_columns(
                (pl.col(STAGE_COL) + 1).alias(STAGE_COL)
            )
            lower_bound_pl = lower_bound_pl.with_columns(
                pl.when(pl.col(STAGE_COL) == num_stages + 1)
                .then(pl.lit(1))
                .otherwise(pl.col(STAGE_COL))
                .alias(STAGE_COL)
            )
            lower_bound_pl = lower_bound_pl.with_columns(
                pl.when(
                    (pl.col(STAGE_COL) == 1) & pl.col(VALUE_COL).is_not_null()
                )
                .then(pl.lit(0.0))
                .otherwise(pl.col(VALUE_COL))
                .alias(VALUE_COL)
            )
            lower_bound_pl = lower_bound_pl.sort([STAGE_COL, HYDRO_CODE_COL])

        # Group by entity + stage and sum
        upper_grouped = upper_bound_pl.group_by(grouping_columns).agg(
            pl.col(VALUE_COL).sum().alias(UPPER_BOUND_COL)
        )
        lower_grouped = lower_bound_pl.group_by(grouping_columns).agg(
            pl.col(VALUE_COL).sum().alias(LOWER_BOUND_COL)
        )

        bounds_df = lower_grouped.join(
            upper_grouped, on=grouping_columns, how="left"
        )

        if synthesis_unit == Unit.perc_modif.value:
            bounds_df = bounds_df.with_columns(
                (
                    pl.col(LOWER_BOUND_COL) / pl.col(UPPER_BOUND_COL) * 100.0
                ).alias(LOWER_BOUND_COL),
                pl.lit(100.0).alias(UPPER_BOUND_COL),
            )

        # Aggregate df if not at individual hydro plant level
        if entity_column != HYDRO_CODE_COL:
            df = cls._group_hydro_df(df, entity_column)

        # Join bounds onto the main DataFrame using entity + stage columns
        df = df.join(bounds_df, on=grouping_columns, how="left")

        # Round values
        num_digits = 2
        df = df.with_columns(
            pl.col(VALUE_COL).round(num_digits),
            pl.col(LOWER_BOUND_COL).round(num_digits),
            pl.col(UPPER_BOUND_COL).round(num_digits),
        )
        return df

    @classmethod
    def _group_hydro_df(
        cls, df: pl.DataFrame, grouping_column: Optional[str] = None
    ) -> pl.DataFrame:
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
        df_columns = df.columns
        grouping_columns = mapped_columns + [
            c
            for c in df_columns
            if c in IDENTIFICATION_COLUMNS and c not in valid_grouping_columns
        ]

        extract_columns = [VALUE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL]
        present_extract_columns = [
            c for c in extract_columns if c in df_columns
        ]

        grouped_df = df.group_by(grouping_columns).agg(
            [pl.col(c).sum() for c in present_extract_columns]
        )
        return grouped_df

    @classmethod
    def is_bounded(cls, s: OperationSynthesis) -> bool:
        return s in cls.MAPPINGS

    @classmethod
    def _unbounded(cls, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.lit(float("-inf")).cast(pl.Float64).alias(LOWER_BOUND_COL),
            pl.lit(float("inf")).cast(pl.Float64).alias(UPPER_BOUND_COL),
        )

    @classmethod
    def _lower_bounded_bounds(
        cls, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        return df.with_columns(
            pl.col(VALUE_COL).round(2),
            pl.lit(0.0).cast(pl.Float64).alias(LOWER_BOUND_COL),
            pl.lit(float("inf")).cast(pl.Float64).alias(UPPER_BOUND_COL),
        )

    @classmethod
    def _spilled_flow_bounds(
        cls, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        df = df.with_columns(pl.col(VALUE_COL).round(2))
        df_bounds = pl.from_pandas(Deck.hydro_spilled_flow_bounds(uow))
        return df.join(
            df_bounds,
            on=[HYDRO_CODE_COL, STAGE_COL, BLOCK_COL],
            how="left",
        )

    @classmethod
    def _outflow_bounds(
        cls, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        df = df.with_columns(pl.col(VALUE_COL).round(2))
        df_bounds = pl.from_pandas(Deck.hydro_outflow_bounds(uow))
        return df.join(
            df_bounds,
            on=[HYDRO_CODE_COL, STAGE_COL, BLOCK_COL],
            how="left",
        )

    @classmethod
    def _turbined_flow_bounds(
        cls, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        df = df.with_columns(pl.col(VALUE_COL).round(2))
        df_bounds = pl.from_pandas(Deck.hydro_turbined_flow_bounds(uow))
        return df.join(
            df_bounds,
            on=[HYDRO_CODE_COL, STAGE_COL, BLOCK_COL],
            how="left",
        )

    @classmethod
    def _group_thermal_bounds_df(
        cls,
        df: pl.DataFrame,
        grouping_column: Optional[str] = None,
        extract_columns: list[str] | None = None,
    ) -> pl.DataFrame:
        if extract_columns is None:
            extract_columns = [VALUE_COL]
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
        df_columns = df.columns
        grouping_columns = mapped_columns + [
            c
            for c in df_columns
            if c in IDENTIFICATION_COLUMNS and c not in valid_grouping_columns
        ]
        present_extract_columns = [
            c for c in extract_columns if c in df_columns
        ]
        grouped_df = df.group_by(grouping_columns).agg(
            [pl.col(c).sum() for c in present_extract_columns]
        )
        return grouped_df

    @classmethod
    def _thermal_generation_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        entity_column: Optional[str],
    ) -> pl.DataFrame:
        df_bounds = pl.from_pandas(Deck.thermal_generation_bounds(uow))
        if entity_column != THERMAL_CODE_COL:
            df_bounds = cls._group_thermal_bounds_df(
                df_bounds,
                entity_column,
                extract_columns=[LOWER_BOUND_COL, UPPER_BOUND_COL],
            )
        entity_column_list = [] if entity_column is None else [entity_column]
        join_keys = [STAGE_COL, BLOCK_COL] + entity_column_list

        # Rename bound columns to avoid collision if they already exist in df
        df_bounds_renamed = df_bounds.rename(
            {
                LOWER_BOUND_COL: f"{LOWER_BOUND_COL}_bounds",
                UPPER_BOUND_COL: f"{UPPER_BOUND_COL}_bounds",
            }
        )
        df = df.join(df_bounds_renamed, on=join_keys, how="left")
        df = df.with_columns(
            pl.col(f"{LOWER_BOUND_COL}_bounds")
            .fill_null(0.0)
            .alias(LOWER_BOUND_COL),
            pl.col(f"{UPPER_BOUND_COL}_bounds")
            .fill_null(float("inf"))
            .alias(UPPER_BOUND_COL),
        )
        df = df.drop([f"{LOWER_BOUND_COL}_bounds", f"{UPPER_BOUND_COL}_bounds"])
        df = df.with_columns(
            pl.col(VALUE_COL).round(2),
            pl.col(UPPER_BOUND_COL).round(2),
            pl.col(LOWER_BOUND_COL).round(2),
        )
        return df

    @classmethod
    def _exchange_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
    ) -> pl.DataFrame:
        df_bounds = pl.from_pandas(Deck.exchange_bounds(uow))
        df = df.join(
            df_bounds,
            on=[
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                EXCHANGE_SOURCE_CODE_COL,
                EXCHANGE_TARGET_CODE_COL,
            ],
            how="left",
        )
        df = df.with_columns(
            pl.col(VALUE_COL).round(2),
            pl.col(UPPER_BOUND_COL).round(2),
            pl.col(LOWER_BOUND_COL).round(2),
        )
        return df

    @classmethod
    def resolve_bounds(
        cls,
        s: OperationSynthesis,
        df: pl.DataFrame,
        ordered_synthesis_entities: Dict[str, list[Any]],
        uow: AbstractUnitOfWork,
    ) -> pl.DataFrame:
        if not cls.is_bounded(s):
            return cls._unbounded(df)
        try:
            return cls.MAPPINGS[s](df, uow, ordered_synthesis_entities)
        except Exception:
            return cls._unbounded(df)
