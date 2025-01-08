import logging
from logging import DEBUG, ERROR, INFO, WARNING
from traceback import print_exc
from typing import Callable, TypeVar

import pandas as pd  # type: ignore

from app.internal.constants import (
    BLOCK_COL,
    EER_CODE_COL,
    HYDRO_CODE_COL,
    IDENTIFICATION_COLUMNS,
    LOWER_BOUND_COL,
    OPERATION_SYNTHESIS_METADATA_OUTPUT,
    OPERATION_SYNTHESIS_STATS_ROOT,
    OPERATION_SYNTHESIS_SUBDIR,
    STRING_DF_TYPE,
    SUBMARKET_CODE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
    VARIABLE_COL,
)
from app.model.operation.operationsynthesis import (
    SUPPORTED_SYNTHESIS,
    SYNTHESIS_DEPENDENCIES,
    UNITS,
    OperationSynthesis,
)
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.bounds import OperationVariableBounds
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.operations import calc_statistics, fast_group_df
from app.utils.regex import match_variables_with_wildcards
from app.utils.timing import time_and_log


class OperationSynthetizer:
    T = TypeVar("T")
    logger: logging.Logger | None = None

    DEFAULT_OPERATION_SYNTHESIS_ARGS = SUPPORTED_SYNTHESIS

    # Todas as sínteses que forem dependências de outras sínteses
    # devem ser armazenadas em cache
    SYNTHESIS_TO_CACHE: list[OperationSynthesis] = list(
        set([p for pr in SYNTHESIS_DEPENDENCIES.values() for p in pr])
    )

    # Estratégias de cache para reduzir tempo total de síntese
    CACHED_SYNTHESIS: dict[OperationSynthesis, pd.DataFrame] = {}
    ORDERED_SYNTHESIS_ENTITIES: dict[OperationSynthesis, dict[str, list]] = {}

    # Estatísticas das sínteses são armazenadas separadamente
    SYNTHESIS_STATS: dict[SpatialResolution, list[pd.DataFrame]] = {}

    @classmethod
    def clear_cache(cls):
        """
        Limpa o cache de síntese de operação.
        """
        cls.CACHED_SYNTHESIS.clear()
        cls.ORDERED_SYNTHESIS_ENTITIES.clear()
        cls.SYNTHESIS_STATS.clear()

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _resolve(
        cls, synthesis: tuple[Variable, SpatialResolution]
    ) -> Callable:
        _rules: dict[
            tuple[Variable, SpatialResolution],
            Callable,
        ] = {
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_dec_oper_sist(uow, "cmo"),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_operation_report_block(
                uow, "geracao_termica"
            ),
            (
                Variable.CUSTO_OPERACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_operation_report_block(
                uow, "custo_presente"
            ),
            (
                Variable.CUSTO_FUTURO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_operation_report_block(
                uow, "custo_futuro"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda uow: cls._resolve_dec_oper_ree(uow, "earm_inicial_MWmes"),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda uow: cls._resolve_dec_oper_ree(
                uow, "earm_inicial_percentual"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls.__stub_block_0_dec_oper_sist(
                uow, "earm_inicial_MWmes"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls.__stub_block_0_dec_oper_sist(
                uow, "earm_inicial_percentual"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda uow: cls._resolve_dec_oper_ree(uow, "earm_final_MWmes"),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda uow: cls._resolve_dec_oper_ree(
                uow, "earm_final_percentual"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls.__stub_block_0_dec_oper_sist(
                uow, "earm_final_MWmes"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls.__stub_block_0_dec_oper_sist(
                uow, "earm_final_percentual"
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_dec_oper_sist(
                uow, "geracao_termica_total_MW"
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_hydro_generation_report_block(uow),
            (
                Variable.GERACAO_USINAS_NAO_SIMULADAS,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_dec_oper_sist(
                uow, "geracao_nao_simuladas_MW"
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda uow: cls._resolve_ena_coupling_eer(uow),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_ena_coupling_sbm(uow),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): lambda uow: cls._resolve_dec_oper_ree(
                uow,
                "ena_MWmes",
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_dec_oper_sist(
                uow,
                "ena_MWmes",
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_dec_oper_sist(uow, "demanda_MW"),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_dec_oper_sist(
                uow, "demanda_liquida_MW"
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_dec_oper_sist(uow, "deficit_MW"),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls.__stub_stored_volume_dec_oper_usih(
                uow, "volume_util_inicial_percentual"
            ),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls.__stub_stored_volume_dec_oper_usih(
                uow, "volume_util_final_percentual"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls.__stub_stored_volume_dec_oper_usih(
                uow, "volume_util_inicial_hm3"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls.__stub_stored_volume_dec_oper_usih(
                uow, "volume_util_final_hm3"
            ),
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_dec_oper_usih(
                uow, "vazao_incremental_m3s", blocks=[0]
            ),
            (
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_dec_oper_usih(
                uow, "vazao_afluente_m3s", blocks=[0]
            ),
            (
                Variable.VAZAO_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_dec_oper_usih(
                uow, "vazao_defluente_m3s"
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_dec_oper_usih(uow, "geracao_MW"),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_hydro_operation_report_block(
                uow, "vertimento_turbinavel"
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_hydro_operation_report_block(
                uow, "vertimento_nao_turbinavel"
            ),
            (
                Variable.VAZAO_TURBINADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_dec_oper_usih(
                uow, "vazao_turbinada_m3s"
            ),
            (
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_dec_oper_usih(uow, "vazao_vertida_m3s"),
            (
                Variable.VAZAO_DESVIADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_dec_oper_usih(
                uow, "vazao_desviada_m3s"
            ),
            (
                Variable.VAZAO_RETIRADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_dec_oper_usih(
                uow, "vazao_retirada_m3s"
            ),
            (
                Variable.VAZAO_EVAPORADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_dec_oper_usih(
                uow, "vazao_evaporada_m3s"
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
            ): lambda uow: cls._resolve_dec_oper_usit(uow, "geracao_MW"),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
            ): lambda uow: cls._resolve_dec_oper_usit(uow, "custo_geracao"),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
            ): lambda uow: cls._resolve_dec_oper_interc(
                uow, "intercambio_origem_MW"
            ),
            (
                Variable.INTERCAMBIO_LIQUIDO,
                SpatialResolution.PAR_SUBMERCADOS,
            ): lambda uow: cls._resolve_dec_oper_interc_net(
                uow, "intercambio_origem_MW"
            ),
        }
        return _rules[synthesis]

    @classmethod
    def _post_resolve_file(
        cls,
        df: pd.DataFrame,
        col: str,
    ) -> pd.DataFrame:
        if col not in df.columns:
            cls._log(f"Coluna {col} não encontrada no arquivo", WARNING)
            df[col] = 0.0
        df = df.rename(
            columns={
                col: VALUE_COL,
            }
        )
        cols = [c for c in df.columns if c in IDENTIFICATION_COLUMNS]
        df = df[cols + [VALUE_COL]]
        return df

    @classmethod
    def _resolve_dec_oper_sist(
        cls,
        uow: AbstractUnitOfWork,
        col: str,
    ):
        with time_and_log(
            message_root="Tempo para obtenção dos dados do dec_oper_sist",
            logger=cls.logger,
        ):
            df = Deck.dec_oper_sist(uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_dec_oper_ree(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do dec_oper_ree",
            logger=cls.logger,
        ):
            df = Deck.dec_oper_ree(uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_ena_coupling_eer(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados de ENA do relato",
            logger=cls.logger,
        ):
            df = Deck.eer_afluent_energy(uow)
            return df

    @classmethod
    def _resolve_ena_coupling_sbm(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados de ENA do relato",
            logger=cls.logger,
        ):
            df = Deck.sbm_afluent_energy(uow)
            return df

    @classmethod
    def _resolve_dec_oper_usih(
        cls, uow: AbstractUnitOfWork, col: str, blocks: list[int] | None = None
    ):
        with time_and_log(
            message_root="Tempo para obtenção dos dados do dec_oper_usih",
            logger=cls.logger,
        ):
            df = Deck.dec_oper_usih(uow)

            df = cls._post_resolve_file(df, col)
            if blocks:
                df = df.loc[df[BLOCK_COL].isin(blocks)].copy()
            return df

    @classmethod
    def _resolve_dec_oper_usit(
        cls,
        uow: AbstractUnitOfWork,
        col: str,
    ):
        with time_and_log(
            message_root="Tempo para obtenção dos dados do dec_oper_usit",
            logger=cls.logger,
        ):
            df = Deck.dec_oper_usit(uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_dec_oper_interc(
        cls,
        uow: AbstractUnitOfWork,
        col: str,
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do dec_oper_interc",
            logger=cls.logger,
        ):
            df = Deck.dec_oper_interc(uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_dec_oper_interc_net(
        cls,
        uow: AbstractUnitOfWork,
        col: str,
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do dec_oper_interc",
            logger=cls.logger,
        ):
            df = Deck.dec_oper_interc_net(uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_hydro_operation_report_block(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados dos relato e relato2",
            logger=cls.logger,
        ):
            return Deck.hydro_operation_report_data(col, uow)

    @classmethod
    def _resolve_hydro_generation_report_block(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados dos relato e relato2",
            logger=cls.logger,
        ):
            return Deck.energy_balance_report_data("geracao_hidraulica", uow)

    @classmethod
    def _resolve_operation_report_block(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados dos relato e relato2",
            logger=cls.logger,
        ):
            return Deck.operation_report_data(col, uow)

    @classmethod
    def _default_args(cls) -> list[str]:
        return cls.DEFAULT_OPERATION_SYNTHESIS_ARGS

    @classmethod
    def _match_wildcards(cls, variables: list[str]) -> list[str]:
        """
        Identifica se há variáveis de síntese que são suportadas
        dentro do padrão de wildcards (`*`) fornecidos.
        """
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: list[str],
    ) -> list[OperationSynthesis]:
        args_data = [OperationSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        for i, a in enumerate(args_data):
            if a is None:
                cls._log(f"Erro no argumento fornecido: {args[i]}")
                return []
        return valid_args

    @classmethod
    def _filter_valid_variables(
        cls, variables: list[OperationSynthesis], uow: AbstractUnitOfWork
    ) -> list[OperationSynthesis]:
        cls._log(f"Variáveis: {variables}")
        return variables

    @classmethod
    def _add_synthesis_dependencies(
        cls, synthesis: list[OperationSynthesis]
    ) -> list[OperationSynthesis]:
        """
        Adiciona objetos as dependências de síntese para uma lista de objetos
        de síntese que foram fornecidos.
        """

        def _add_synthesis_dependencies_recursive(
            current_synthesis: list[OperationSynthesis],
            todo_synthesis: OperationSynthesis,
        ):
            if todo_synthesis in SYNTHESIS_DEPENDENCIES.keys():
                for dep in SYNTHESIS_DEPENDENCIES[todo_synthesis]:
                    _add_synthesis_dependencies_recursive(
                        current_synthesis, dep
                    )
            if todo_synthesis not in current_synthesis:
                current_synthesis.append(todo_synthesis)

        result_synthesis: list[OperationSynthesis] = []
        for v in synthesis:
            _add_synthesis_dependencies_recursive(result_synthesis, v)
        return result_synthesis

    @classmethod
    def _get_unique_column_values_in_order(
        cls, df: pd.DataFrame, cols: list[str]
    ):
        """
        Extrai valores únicos na ordem em que aparecem para um
        conjunto de colunas de um DataFrame.
        """
        return {col: df[col].unique().tolist() for col in cols}

    @classmethod
    def _set_ordered_entities(
        cls, s: OperationSynthesis, entities: dict[str, list]
    ):
        """
        Armazena um conjunto de entidades ordenadas para uma síntese.
        """
        cls.ORDERED_SYNTHESIS_ENTITIES[s] = entities

    @classmethod
    def _get_ordered_entities(cls, s: OperationSynthesis) -> dict[str, list]:
        """
        Obtem um conjunto de entidades ordenadas para uma síntese.
        """
        return cls.ORDERED_SYNTHESIS_ENTITIES[s]

    @classmethod
    def _get_from_cache(cls, s: OperationSynthesis) -> pd.DataFrame:
        """
        Extrai o resultado de uma síntese da cache caso exista, lançando
        um erro caso contrário.
        """
        if s in cls.CACHED_SYNTHESIS.keys():
            cls._log(f"Lendo do cache - {str(s)}", DEBUG)
            res = cls.CACHED_SYNTHESIS.get(s)
            if res is None:
                cls._log(f"Erro na leitura do cache - {str(s)}", ERROR)
                raise RuntimeError()
            return res.copy()
        else:
            cls._log(f"Erro na leitura do cache - {str(s)}", ERROR)
            raise RuntimeError()

    @classmethod
    def __stub_EVER(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o cálculo da energia vertida a partir dos valores
        das energias vertidas turbinável e não-turbinável.
        """
        turbineable_synthesis = OperationSynthesis(
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            synthesis.spatial_resolution,
        )
        non_turbineable_synthesis = OperationSynthesis(
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            synthesis.spatial_resolution,
        )
        turbineable_df = cls._get_from_cache(turbineable_synthesis)
        spilled_df = cls._get_from_cache(non_turbineable_synthesis)

        spilled_df.loc[:, VALUE_COL] = (
            turbineable_df[VALUE_COL].to_numpy()
            + spilled_df[VALUE_COL].to_numpy()
        )
        return spilled_df

    @classmethod
    def _group_hydro_df(
        cls, df: pd.DataFrame, grouping_column: str | None = None
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

        grouping_column_map: dict[str, list[str]] = {
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
    def _group_submarket_df(
        cls, df: pd.DataFrame, grouping_column: str | None = None
    ) -> pd.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de SBM
        para o SIN. A agregação tem como requisito que as variáveis fornecidas
        sejam em unidades cuja agregação seja possível apenas pela soma.
        """
        valid_grouping_columns = [
            SUBMARKET_CODE_COL,
        ]

        grouping_column_map: dict[str, list[str]] = {
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
    def __stub_grouping_hydro(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o cálculo de uma variável agrupando a partir
        da extração feita por UHE.
        """
        hydro_synthesis = OperationSynthesis(
            synthesis.variable,
            SpatialResolution.USINA_HIDROELETRICA,
        )

        hydro_df = cls._get_from_cache(hydro_synthesis)

        grouping_col_map = {
            SpatialResolution.RESERVATORIO_EQUIVALENTE: EER_CODE_COL,
            SpatialResolution.SUBMERCADO: SUBMARKET_CODE_COL,
            SpatialResolution.SISTEMA_INTERLIGADO: None,
        }

        return cls._group_hydro_df(
            hydro_df,
            grouping_column=grouping_col_map[synthesis.spatial_resolution],
        )

    @classmethod
    def __stub_grouping_submarket(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o cálculo de uma variável agrupando a partir da extração
        feita por SBM.
        """
        submarket_synthesis = OperationSynthesis(
            synthesis.variable,
            SpatialResolution.SUBMERCADO,
        )

        hydro_df = cls._get_from_cache(submarket_synthesis)

        grouping_col_map = {
            SpatialResolution.SISTEMA_INTERLIGADO: None,
        }

        return cls._group_hydro_df(
            hydro_df,
            grouping_column=grouping_col_map[synthesis.spatial_resolution],
        )

    @classmethod
    def __stub_GHID_REE(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o cálculo da geração hidráulica a partir da extração
        feita por UHE.
        """
        hydro_synthesis = OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.USINA_HIDROELETRICA,
        )

        hydro_df = cls._get_from_cache(hydro_synthesis)

        grouping_col_map = {
            SpatialResolution.RESERVATORIO_EQUIVALENTE: EER_CODE_COL,
            SpatialResolution.SUBMERCADO: SUBMARKET_CODE_COL,
            SpatialResolution.SISTEMA_INTERLIGADO: None,
        }

        return cls._group_hydro_df(
            hydro_df,
            grouping_column=grouping_col_map[synthesis.spatial_resolution],
        )

    @classmethod
    def __stub_block_0_dec_oper_sist(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        df = cls._resolve_dec_oper_sist(uow, col)
        df = df.loc[(df[BLOCK_COL] == 0) & (~df[VALUE_COL].isna())].reset_index(
            drop=True
        )
        return df

    @classmethod
    def __stub_stored_volume_dec_oper_usih(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        df = cls._resolve_dec_oper_usih(uow, col)
        df = df.loc[(df[BLOCK_COL] == 0) & (~df[VALUE_COL].isna())].reset_index(
            drop=True
        )
        return df

    @classmethod
    def __stub_percent_SIN(
        cls, synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o cálculo de uma variável em percentual para o SIN a
        partir da extração feita por SBM.
        """
        earpi = Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL
        earmi = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL
        earpf = Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL
        earmf = Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL

        variable_map = {
            earpi: earmi,
            earpf: earmf,
        }
        absolute_submarket_synthesis = OperationSynthesis(
            variable_map[synthesis.variable],
            SpatialResolution.SUBMERCADO,
        )
        percent_submarket_synthesis = OperationSynthesis(
            synthesis.variable,
            SpatialResolution.SUBMERCADO,
        )

        # Groups absolute values
        absolute_df = cls._get_from_cache(absolute_submarket_synthesis)
        result_df = cls._group_submarket_df(
            absolute_df,
            grouping_column=None,
        )
        # Calculates capacity
        percent_df = cls._get_from_cache(percent_submarket_synthesis)
        percent_df[VALUE_COL] = (
            100
            * absolute_df[VALUE_COL].to_numpy()
            / percent_df[VALUE_COL].to_numpy()
        )
        capacity_df = cls._group_submarket_df(
            percent_df,
            grouping_column=None,
        )
        # Calculates percentage
        result_df[VALUE_COL] = 100 * (
            result_df[VALUE_COL] / capacity_df[VALUE_COL]
        )
        return result_df

    @classmethod
    def _stub_mappings(cls, s: OperationSynthesis) -> Callable | None:  # noqa
        """
        Obtem a função de resolução de cada síntese que foge ao
        fluxo de resolução padrão, por meio de um mapeamento de
        funções `stub` para cada variável e/ou resolução espacial.
        """
        f = None
        if s.variable == Variable.ENERGIA_VERTIDA:
            f = cls.__stub_EVER
        elif all([
            s.variable
            in [
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            ],
            s.spatial_resolution != SpatialResolution.USINA_HIDROELETRICA,
        ]):
            f = cls.__stub_grouping_hydro
        elif all([
            s.variable
            in [
                Variable.MERCADO,
                Variable.MERCADO_LIQUIDO,
                Variable.DEFICIT,
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                Variable.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
                Variable.GERACAO_HIDRAULICA,
                Variable.GERACAO_TERMICA,
                Variable.GERACAO_USINAS_NAO_SIMULADAS,
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            ],
            s.spatial_resolution == SpatialResolution.SISTEMA_INTERLIGADO,
        ]):
            f = cls.__stub_grouping_submarket
        elif all([
            s.variable == Variable.GERACAO_HIDRAULICA,
            s.spatial_resolution == SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ]):
            f = cls.__stub_GHID_REE
        elif all([
            s.variable
            in [
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            ],
            s.spatial_resolution == SpatialResolution.SISTEMA_INTERLIGADO,
        ]):
            f = cls.__stub_percent_SIN

        return f

    @classmethod
    def _resolve_stub(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> tuple[pd.DataFrame, bool]:
        """
        Realiza a resolução da síntese por meio de uma implementação
        alternativa ao fluxo natural de resolução (`stub`), caso esta seja
        uma variável que não possa ser resolvida diretamente a partir
        da extração de dados do DECOMP.
        """
        f = cls._stub_mappings(s)
        if f:
            df, is_stub = f(s, uow), True
        else:
            df, is_stub = pd.DataFrame(), False
        if is_stub:
            df = cls._post_resolve(df, s, uow)
            df = cls._resolve_bounds(s, df, uow)
        return df, is_stub

    @classmethod
    def __get_from_cache_if_exists(cls, s: OperationSynthesis) -> pd.DataFrame:
        """
        Obtém uma síntese da operação a partir da cache, caso esta
        exista. Caso contrário, retorna um DataFrame vazio.
        """
        if s in cls.CACHED_SYNTHESIS.keys():
            return cls._get_from_cache(s)
        else:
            return pd.DataFrame()

    @classmethod
    def __store_in_cache_if_needed(
        cls, s: OperationSynthesis, df: pd.DataFrame
    ):
        """
        Adiciona um DataFrame com os dados de uma síntese à cache
        caso esta seja uma variável que deva ser armazenada.
        """
        if s in cls.SYNTHESIS_TO_CACHE:
            with time_and_log(
                message_root="Tempo para armazenamento na cache",
                logger=cls.logger,
            ):
                cls.CACHED_SYNTHESIS[s] = df.copy()

    @classmethod
    def _resolve_bounds(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o cálculo dos limites superiores e inferiores para
        a síntese caso esta seja uma variável limitada.
        """
        with time_and_log(
            message_root="Tempo para calculo dos limites",
            logger=cls.logger,
        ):
            df = OperationVariableBounds.resolve_bounds(
                s,
                df,
                cls._get_ordered_entities(s),
                uow,
            )

        return df

    @classmethod
    def _post_resolve(
        cls,
        df: pd.DataFrame,
        s: OperationSynthesis,
        uow: AbstractUnitOfWork,
        early_hooks: list[Callable] = [],
        late_hooks: list[Callable] = [],
    ) -> pd.DataFrame:
        """
        Realiza pós-processamento após a resolução da extração
        de todos os dados de uma síntese.
        """
        with time_and_log(
            message_root="Tempo para compactacao dos dados", logger=cls.logger
        ):
            spatial_resolution = s.spatial_resolution

            for c in early_hooks:
                df = c(s, df, uow)

            df = df.sort_values(
                spatial_resolution.sorting_synthesis_df_columns
            ).reset_index(drop=True)

            entity_columns_order = cls._get_unique_column_values_in_order(
                df,
                spatial_resolution.sorting_synthesis_df_columns,
            )
            other_columns_order = cls._get_unique_column_values_in_order(
                df,
                spatial_resolution.non_entity_sorting_synthesis_df_columns,
            )
            cls._set_ordered_entities(
                s, {**entity_columns_order, **other_columns_order}
            )

            for c in late_hooks:
                df = c(s, df, uow)
        return df

    @classmethod
    def _resolve_synthesis(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza a resolução de uma síntese, opcionalmente adicionando
        limites superiores e inferiores aos valores de cada linha.
        """
        df = cls._resolve((s.variable, s.spatial_resolution))(uow)
        if df is not None:
            df = cls._post_resolve(df, s, uow)
            df = cls._resolve_bounds(s, df, uow)
        return df

    @classmethod
    def _export_metadata(
        cls,
        success_synthesis: list[OperationSynthesis],
        uow: AbstractUnitOfWork,
    ):
        """
        Cria um DataFrame com os metadados das variáveis de síntese
        e realiza a exportação para um arquivo de metadados.
        """
        metadata_df = pd.DataFrame(
            columns=[
                "chave",
                "nome_curto_variavel",
                "nome_longo_variavel",
                "nome_curto_agregacao",
                "nome_longo_agregacao",
                "unidade",
                "calculado",
                "limitado",
            ]
        )
        for s in success_synthesis:
            metadata_df.loc[metadata_df.shape[0]] = [
                str(s),
                s.variable.short_name,
                s.variable.long_name,
                s.spatial_resolution.value,
                s.spatial_resolution.long_name,
                UNITS[s].value if s in UNITS else "",
                s in SYNTHESIS_DEPENDENCIES,
                OperationVariableBounds.is_bounded(s),
            ]
        with uow:
            uow.export.synthetize_df(
                metadata_df, OPERATION_SYNTHESIS_METADATA_OUTPUT
            )

    @classmethod
    def _add_synthesis_stats(cls, s: OperationSynthesis, df: pd.DataFrame):
        """
        Adiciona um DataFrame com estatísticas de uma síntese ao
        DataFrame de estatísticas da agregação espacial em questão.
        """
        df[VARIABLE_COL] = s.variable.value

        if s.spatial_resolution not in cls.SYNTHESIS_STATS:
            cls.SYNTHESIS_STATS[s.spatial_resolution] = [df]
        else:
            cls.SYNTHESIS_STATS[s.spatial_resolution].append(df)

    @classmethod
    def _export_scenario_synthesis(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ):
        """
        Realiza a exportação dos dados para uma síntese da
        operação desejada. Opcionalmente, os dados são armazenados
        em cache para uso futuro e as estatísticas são adicionadas
        ao DataFrame de estatísticas da agregação espacial em questão.
        """
        filename = str(s)
        with time_and_log(
            message_root="Tempo para preparacao para exportacao",
            logger=cls.logger,
        ):
            df = df.sort_values(
                s.spatial_resolution.sorting_synthesis_df_columns
            ).reset_index(drop=True)
            probs_df = Deck.expanded_probabilities(uow)
            stats_df = calc_statistics(df, probs_df)
            cls._add_synthesis_stats(s, stats_df)
            cls.__store_in_cache_if_needed(s, df)
        with time_and_log(
            message_root="Tempo para exportacao dos dados", logger=cls.logger
        ):
            with uow:
                df = df[s.spatial_resolution.all_synthesis_df_columns]
                uow.export.synthetize_df(df, filename)

    @classmethod
    def _export_stats(
        cls,
        uow: AbstractUnitOfWork,
    ):
        """
        Realiza a exportação dos dados de estatísticas de síntese
        da operação. As estatísticas são exportadas para um arquivo
        único por agregação espacial, de nome
        `OPERACAO_{agregacao}`.
        """
        for res, dfs in cls.SYNTHESIS_STATS.items():
            with uow:
                df = pd.concat(dfs, ignore_index=True)
                df = df[[VARIABLE_COL] + res.all_synthesis_df_columns]
                df = df.astype({VARIABLE_COL: STRING_DF_TYPE})
                df = df.sort_values(
                    [VARIABLE_COL] + res.sorting_synthesis_df_columns
                ).reset_index(drop=True)
                uow.export.synthetize_df(
                    df, f"{OPERATION_SYNTHESIS_STATS_ROOT}_{res.value}"
                )

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: list[str], uow: AbstractUnitOfWork
    ) -> list[OperationSynthesis]:
        """
        Realiza o pré-processamento das variáveis de síntese fornecidas,
        filtrando as válidas para o caso em questão e adicionando dependências
        caso a síntese de operação de uma variável dependa de outra.
        """
        try:
            if len(variables) == 0:
                all_variables = cls._default_args()
            else:
                all_variables = cls._match_wildcards(variables)
            synthesis_variables = cls._process_variable_arguments(all_variables)
            valid_synthesis = cls._filter_valid_variables(
                synthesis_variables, uow
            )
            synthesis_with_dependencies = cls._add_synthesis_dependencies(
                valid_synthesis
            )
        except Exception as e:
            print_exc()
            cls._log(str(e), ERROR)
            cls._log("Erro no pré-processamento das variáveis", ERROR)
            synthesis_with_dependencies = []
        return synthesis_with_dependencies

    @classmethod
    def _synthetize_single_variable(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> OperationSynthesis | None:
        """
        Realiza a síntese de operação para uma variável
        fornecida.
        """
        filename = str(s)
        with time_and_log(
            message_root=f"Tempo para sintese de {filename}",
            logger=cls.logger,
        ):
            try:
                found_synthesis = False
                cls._log(f"Realizando sintese de {filename}")
                df = cls.__get_from_cache_if_exists(s)
                is_stub = cls._stub_mappings(s) is not None
                if df.empty:
                    df, is_stub = cls._resolve_stub(s, uow)
                    if not is_stub:
                        df = cls._resolve_synthesis(s, uow)
                if df is not None:
                    if not df.empty:
                        found_synthesis = True
                        cls._export_scenario_synthesis(s, df, uow)
                        return s
                if not found_synthesis:
                    cls._log(
                        "Nao foram encontrados dados"
                        + f" para a sintese de {filename}",
                        WARNING,
                    )
                return None
            except Exception as e:
                print_exc()
                cls._log(str(e), ERROR)
                cls._log(
                    f"Nao foi possível realizar a sintese de: {filename}",
                    ERROR,
                )
                return None

    @classmethod
    def synthetize(cls, variables: list[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
        Deck.logger = cls.logger
        OperationVariableBounds.logger = cls.logger
        uow.subdir = OPERATION_SYNTHESIS_SUBDIR
        with time_and_log(
            message_root="Tempo para sintese da operacao",
            logger=cls.logger,
        ):
            synthesis_with_dependencies = cls._preprocess_synthesis_variables(
                variables, uow
            )
            success_synthesis: list[OperationSynthesis] = []
            for s in synthesis_with_dependencies:
                r = cls._synthetize_single_variable(s, uow)
                if r:
                    success_synthesis.append(r)

            cls._export_stats(uow)
            cls._export_metadata(success_synthesis, uow)
