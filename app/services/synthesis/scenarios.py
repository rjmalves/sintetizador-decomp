from typing import Callable
import pandas as pd  # type: ignore
import logging
from logging import INFO, ERROR
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.timing import time_and_log
from app.utils.regex import match_variables_with_wildcards
from app.model.scenarios.variable import Variable
from app.model.scenarios.scenariosynthesis import (
    ScenarioSynthesis,
    SUPPORTED_SYNTHESIS,
)
from app.internal.constants import (
    SCENARIO_SYNTHESIS_SUBDIR,
    SCENARIO_SYNTHESIS_METADATA_OUTPUT,
)
from traceback import print_exc

from app.services.deck.deck import Deck


class ScenarioSynthetizer:
    DEFAULT_SCENARIO_SYNTHESIS_ARGS: list[str] = SUPPORTED_SYNTHESIS

    logger: logging.Logger | None = None

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _default_args(cls) -> list[str]:
        return cls.DEFAULT_SCENARIO_SYNTHESIS_ARGS

    @classmethod
    def _match_wildcards(cls, variables: list[str]) -> list[str]:
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_SCENARIO_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: list[str],
    ) -> list[ScenarioSynthesis]:
        args_data = [ScenarioSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        for i, a in enumerate(args_data):
            if a is None:
                cls._log(f"Erro no argumento fornecido: {args[i]}", ERROR)
                return []
        return valid_args

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: list[str], uow: AbstractUnitOfWork
    ) -> list[ScenarioSynthesis]:
        """
        Realiza o pré-processamento das variáveis de síntese fornecidas,
        filtrando as válidas para o caso em questão.
        """
        try:
            if len(variables) == 0:
                all_variables = cls._default_args()
            else:
                all_variables = cls._match_wildcards(variables)
            synthesis_variables = cls._process_variable_arguments(all_variables)
        except Exception as e:
            print_exc()
            cls._log(str(e), ERROR)
            cls._log("Erro no pré-processamento das variáveis", ERROR)
            synthesis_variables = []
        return synthesis_variables

    @classmethod
    def _resolve_probabilities(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.probabilities(uow)
        if df is None:
            cls._log("Erro na leitura do arquivo de vazões", ERROR)
            raise RuntimeError()
        return df

    @classmethod
    def _resolve(
        cls, s: ScenarioSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        rules: dict[Variable, Callable] = {
            Variable.PROBABILIDADES: cls._resolve_probabilities,
        }
        return rules[s.variable](uow)

    @classmethod
    def _export_metadata(
        cls,
        success_synthesis: list[ScenarioSynthesis],
        uow: AbstractUnitOfWork,
    ):
        metadata_df = pd.DataFrame(
            columns=[
                "chave",
                "nome_curto",
                "nome_longo",
            ]
        )
        for s in success_synthesis:
            metadata_df.loc[metadata_df.shape[0]] = [
                str(s),
                s.variable.short_name,
                s.variable.long_name,
            ]
        with uow:
            uow.export.synthetize_df(
                metadata_df, SCENARIO_SYNTHESIS_METADATA_OUTPUT
            )

    @classmethod
    def _synthetize_single_variable(
        cls, s: ScenarioSynthesis, uow: AbstractUnitOfWork
    ) -> ScenarioSynthesis | None:
        """
        Realiza a síntese de cenários para uma variável
        fornecida.
        """
        filename = str(s)
        with time_and_log(
            message_root=f"Tempo para sintese de {filename}",
            logger=cls.logger,
        ):
            try:
                cls._log(f"Realizando síntese de {filename}")
                df = cls._resolve(s, uow)
                if df is not None:
                    with uow:
                        uow.export.synthetize_df(df, filename)
                        return s
                return None
            except Exception as e:
                print_exc()
                cls._log(str(e), ERROR)
                return None

    @classmethod
    def synthetize(cls, variables: list[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
        uow.subdir = SCENARIO_SYNTHESIS_SUBDIR

        with time_and_log(
            message_root="Tempo para sintese dos cenarios", logger=cls.logger
        ):
            synthesis_variables = cls._preprocess_synthesis_variables(
                variables, uow
            )

            success_synthesis: list[ScenarioSynthesis] = []
            for s in synthesis_variables:
                r = cls._synthetize_single_variable(s, uow)
                if r:
                    success_synthesis.append(r)

            cls._export_metadata(success_synthesis, uow)
