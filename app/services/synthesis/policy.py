import logging
from logging import ERROR, INFO
from traceback import print_exc
from typing import Callable, Optional

import pandas as pd  # type: ignore

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    END_DATE_COL,
    STAGE_COL,
    START_DATE_COL,
    POLICY_SYNTHESIS_METADATA_OUTPUT,
    POLICY_SYNTHESIS_SUBDIR,
    VALUE_COL,
)
from app.model.policy.policysynthesis import (
    SUPPORTED_SYNTHESIS,
    PolicySynthesis,
)
from app.model.policy.variable import Variable
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.regex import match_variables_with_wildcards
from app.utils.timing import time_and_log


class PolicySynthetizer:
    DEFAULT_POLICY_SYNTHESIS_ARGS = SUPPORTED_SYNTHESIS

    logger: Optional[logging.Logger] = None

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _default_args(cls) -> list[str]:
        return cls.DEFAULT_POLICY_SYNTHESIS_ARGS

    @classmethod
    def _match_wildcards(cls, variables: list[str]) -> list[str]:
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_POLICY_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: list[str],
    ) -> list[PolicySynthesis]:
        args_data = [PolicySynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        for i, a in enumerate(valid_args):
            if a is None:
                cls._log(f"Erro no argumento fornecido: {args[i]}", ERROR)
                return []
        return valid_args

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: list[str], uow: AbstractUnitOfWork
    ) -> list[PolicySynthesis]:
        """
        Realiza o pré-processamento das variáveis de síntese fornecidas,
        filtrando as válidas para o caso em questão.
        """
        try:
            if len(variables) == 0:
                all_variables = cls._default_args()
            else:
                all_variables = cls._match_wildcards(variables)
            synthesis_variables = cls._process_variable_arguments(
                all_variables
            )
        except Exception as e:
            print_exc()
            cls._log(str(e), ERROR)
            cls._log("Erro no pré-processamento das variáveis", ERROR)
            synthesis_variables = []
        return synthesis_variables

    @classmethod
    def _resolve(
        cls, synthesis: PolicySynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        RULES: dict[Variable, Callable] = {
            Variable.CORTES: cls._resolve_cortes,
            Variable.ESTADOS: cls._resolve_estados,
        }
        return RULES[synthesis.variable](uow)

    @classmethod
    def _resolve_cortes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        # TODO
        return

    @classmethod
    def _resolve_estados(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        # TODO
        return

    @classmethod
    def _export_metadata(
        cls,
        success_synthesis: list[PolicySynthesis],
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
                metadata_df, POLICY_SYNTHESIS_METADATA_OUTPUT
            )

    @classmethod
    def _synthetize_single_variable(
        cls, s: PolicySynthesis, uow: AbstractUnitOfWork
    ) -> Optional[PolicySynthesis]:
        """
        Realiza a síntese de sistema para uma variável
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
        uow.subdir = POLICY_SYNTHESIS_SUBDIR

        with time_and_log(
            message_root="Tempo para sintese da politica", logger=cls.logger
        ):
            synthesis_variables = cls._preprocess_synthesis_variables(
                variables, uow
            )
            success_synthesis: list[PolicySynthesis] = []
            for s in synthesis_variables:
                r = cls._synthetize_single_variable(s, uow)
                if r:
                    success_synthesis.append(r)

            cls._export_metadata(success_synthesis, uow)
