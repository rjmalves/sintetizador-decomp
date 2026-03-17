import logging
from logging import ERROR, INFO, WARNING
from traceback import print_exc
from typing import Any, Callable, TypeVar

import pandas as pd
import polars as pl

from app.internal.constants import (
    OPERATION_SYNTHESIS_SUBDIR,
)
from app.model.operation.operationsynthesis import (
    SUPPORTED_SYNTHESIS,
    SYNTHESIS_DEPENDENCIES,
    OperationSynthesis,
)
from app.model.operation.spatialresolution import SpatialResolution
from app.services.deck.bounds import OperationVariableBounds
from app.services.deck.deck import Deck
from app.services.synthesis.operation import resolution as _resolution_mod
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
    ORDERED_SYNTHESIS_ENTITIES: dict[
        OperationSynthesis, dict[str, list[Any]]
    ] = {}

    # Estatísticas das sínteses são armazenadas separadamente
    SYNTHESIS_STATS: dict[SpatialResolution, list[pd.DataFrame]] = {}

    @classmethod
    def clear_cache(cls) -> None:
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
    ) -> pd.DataFrame:
        return _resolution_mod.resolve_dec_oper_sist(uow, col, cls.logger)

    @classmethod
    def _resolve_dec_oper_usih(
        cls,
        uow: AbstractUnitOfWork,
        col: str,
        blocks: list[int] | None = None,
    ) -> pd.DataFrame:
        return _resolution_mod.resolve_dec_oper_usih(
            uow, col, blocks, cls.logger
        )

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
        ) -> None:
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
    ) -> dict[str, list[Any]]:
        return get_unique_column_values_in_order(df, cols)

    @classmethod
    def _set_ordered_entities(
        cls, s: OperationSynthesis, entities: dict[str, list[Any]]
    ) -> None:
        set_ordered_entities(cls, s, entities)

    @classmethod
    def _get_ordered_entities(
        cls, s: OperationSynthesis
    ) -> dict[str, list[Any]]:
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
    def _stub_mappings(
        cls, s: OperationSynthesis
    ) -> Callable[..., pd.DataFrame] | None:
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
        early_hooks: list[Callable[..., pd.DataFrame]] | None = None,
        late_hooks: list[Callable[..., pd.DataFrame]] | None = None,
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
        df = _resolution_mod.resolve_dispatch(
            (s.variable, s.spatial_resolution), cls.logger
        )(uow)
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
    def synthetize(cls, variables: list[str], uow: AbstractUnitOfWork) -> None:
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
