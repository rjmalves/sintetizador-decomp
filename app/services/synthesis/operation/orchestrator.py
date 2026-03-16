import logging
from logging import ERROR, INFO, WARNING
from traceback import print_exc
from typing import Callable, TypeVar

import pandas as pd  # type: ignore
import polars as pl

from app.internal.constants import (
    BLOCK_COL,
    OPERATION_SYNTHESIS_SUBDIR,
)
from app.model.operation.operationsynthesis import (
    SUPPORTED_SYNTHESIS,
    SYNTHESIS_DEPENDENCIES,
    OperationSynthesis,
)
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.bounds import OperationVariableBounds
from app.services.deck.deck import Deck
from app.services.synthesis.operation.cache import (
    get_from_cache,
    get_from_cache_if_exists,
    store_in_cache_if_needed,
)
from app.services.synthesis.operation.export import (
    add_synthesis_stats,
    export_metadata,
    export_scenario_synthesis,
    export_stats,
)
from app.services.synthesis.operation.pipeline import (
    get_ordered_entities,
    get_unique_column_values_in_order,
    post_resolve,
    post_resolve_file,
    set_ordered_entities,
)
from app.services.synthesis.operation.spatial import (
    group_hydro_df,
    group_submarket_df,
)
from app.services.synthesis.operation.stubs import (
    stub_mappings,
    stub_stored_volume_dec_oper_usih,
    stub_thermal_submarkets_dec_oper_sist,
    stub_valid_values_dec_oper_sist,
)
from app.services.unitofwork import AbstractUnitOfWork
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
    def _log(cls, msg: str, level: int = INFO) -> None:
        if cls.logger:
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
            ): lambda uow: cls.__stub_valid_values_dec_oper_sist(
                uow, "earm_inicial_MWmes", blocks=[0]
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls.__stub_valid_values_dec_oper_sist(
                uow, "earm_inicial_percentual", blocks=[0]
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
            ): lambda uow: cls.__stub_valid_values_dec_oper_sist(
                uow, "earm_final_MWmes", blocks=[0]
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls.__stub_valid_values_dec_oper_sist(
                uow, "earm_final_percentual", blocks=[0]
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls.__stub_thermal_submarkets_dec_oper_sist(
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
        return post_resolve_file(cls, df, col)

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
        return variables

    @classmethod
    def _add_synthesis_dependencies(
        cls, synthesis: list[OperationSynthesis]
    ) -> list[OperationSynthesis]:
        def _add_synthesis_dependencies_recursive(
            current_synthesis: list[OperationSynthesis],
            todo_synthesis: OperationSynthesis,
        ):
            if todo_synthesis in SYNTHESIS_DEPENDENCIES:
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
        return get_unique_column_values_in_order(df, cols)

    @classmethod
    def _set_ordered_entities(
        cls, s: OperationSynthesis, entities: dict[str, list]
    ):
        set_ordered_entities(cls, s, entities)

    @classmethod
    def _get_ordered_entities(cls, s: OperationSynthesis) -> dict[str, list]:
        return get_ordered_entities(cls, s)

    @classmethod
    def _get_from_cache(cls, s: OperationSynthesis) -> pd.DataFrame:
        return get_from_cache(cls, s)

    @classmethod
    def _group_hydro_df(
        cls, df: pd.DataFrame, grouping_column: str | None = None
    ) -> pd.DataFrame:
        return group_hydro_df(df, grouping_column)

    @classmethod
    def _group_submarket_df(
        cls, df: pd.DataFrame, grouping_column: str | None = None
    ) -> pd.DataFrame:
        return group_submarket_df(df, grouping_column)

    @classmethod
    def __stub_valid_values_dec_oper_sist(
        cls, uow: AbstractUnitOfWork, col: str, blocks: list[int] | None = None
    ) -> pd.DataFrame:
        return stub_valid_values_dec_oper_sist(cls, uow, col, blocks)

    @classmethod
    def __stub_stored_volume_dec_oper_usih(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        return stub_stored_volume_dec_oper_usih(cls, uow, col)

    @classmethod
    def __stub_thermal_submarkets_dec_oper_sist(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        return stub_thermal_submarkets_dec_oper_sist(cls, uow, col)

    @classmethod
    def _stub_mappings(cls, s: OperationSynthesis) -> Callable | None:
        return stub_mappings(cls, s)

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
        return get_from_cache_if_exists(cls, s)

    @classmethod
    def __store_in_cache_if_needed(
        cls, s: OperationSynthesis, df: pd.DataFrame
    ) -> None:
        store_in_cache_if_needed(cls, s, df)

    @classmethod
    def _resolve_bounds(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para calculo dos limites",
            logger=cls.logger,
        ):
            df_pl = pl.from_pandas(df)
            df_pl = OperationVariableBounds.resolve_bounds(
                s,
                df_pl,
                cls._get_ordered_entities(s),
                uow,
            )
            return df_pl.to_pandas()

    @classmethod
    def _post_resolve(
        cls,
        df: pd.DataFrame,
        s: OperationSynthesis,
        uow: AbstractUnitOfWork,
        early_hooks: list[Callable] | None = None,
        late_hooks: list[Callable] | None = None,
    ) -> pd.DataFrame:
        return post_resolve(cls, df, s, uow, early_hooks, late_hooks)

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
    ) -> None:
        export_metadata(cls, success_synthesis, uow)

    @classmethod
    def _add_synthesis_stats(
        cls, s: OperationSynthesis, df: pd.DataFrame
    ) -> None:
        add_synthesis_stats(cls, s, df)

    @classmethod
    def _export_scenario_synthesis(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> None:
        export_scenario_synthesis(cls, s, df, uow)

    @classmethod
    def _export_stats(cls, uow: AbstractUnitOfWork) -> None:
        export_stats(cls, uow)

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: list[str], uow: AbstractUnitOfWork
    ) -> list[OperationSynthesis]:
        try:
            all_variables = (
                cls._default_args()
                if not variables
                else cls._match_wildcards(variables)
            )
            synthesis_variables = cls._process_variable_arguments(all_variables)
            valid_synthesis = cls._filter_valid_variables(
                synthesis_variables, uow
            )
            return cls._add_synthesis_dependencies(valid_synthesis)
        except Exception as e:
            print_exc()
            cls._log(str(e), ERROR)
            cls._log("Erro no pré-processamento das variáveis", ERROR)
            return []

    @classmethod
    def _synthetize_single_variable(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> OperationSynthesis | None:
        filename = str(s)
        with time_and_log(
            message_root=f"Tempo para sintese de {filename}",
            logger=cls.logger,
        ):
            try:
                cls._log(f"Realizando sintese de {filename}")
                df = cls.__get_from_cache_if_exists(s)
                is_stub = cls._stub_mappings(s) is not None
                if df.empty:
                    df, is_stub = cls._resolve_stub(s, uow)
                    if not is_stub:
                        df = cls._resolve_synthesis(s, uow)
                if df is not None and not df.empty:
                    cls._export_scenario_synthesis(s, df, uow)
                    return s
                cls._log(
                    f"Nao foram encontrados dados para a sintese de {filename}",
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
