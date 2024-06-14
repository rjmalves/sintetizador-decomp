from typing import Callable, Dict, List
import pandas as pd  # type: ignore

from app.services.unitofwork import AbstractUnitOfWork
from app.utils.log import Log
from app.model.scenarios.variable import Variable
from app.model.scenarios.scenariosynthesis import ScenarioSynthesis
from app.services.deck.deck import Deck


class ScenarioSynthetizer:
    # TODO - levar lista de argumentos suportados para o
    # arquivo da ScenarioSynthesis
    DEFAULT_SCENARIO_SYNTHESIS_ARGS: List[str] = [
        "PROBABILIDADES",
    ]

    @classmethod
    def _get_rule(cls, s: ScenarioSynthesis) -> Callable:
        rules: Dict[Variable, Callable] = {
            Variable.PROBABILIDADES: cls._resolve_probabilities,
        }
        return rules[s.variable]

    # TODO - padronizar o tipo de retorno de _default_args com
    # _process_variable_arguments
    @classmethod
    def _default_args(cls) -> List[str]:
        return cls.DEFAULT_SCENARIO_SYNTHESIS_ARGS

    # TODO - criar o método interno _log

    # TODO - padronizar a forma de logging com o uso do método interno _log
    # Manter o processamento para sinalizar erros.
    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[ScenarioSynthesis]:
        args_data = [ScenarioSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        logger = Log.log()
        for i, a in enumerate(args_data):
            if a is None:
                if logger is not None:
                    logger.error(f"Erro no argumento fornecido: {args[i]}")
                return []
        return valid_args

    # TODO - renomear para _filter_valid_variables
    # TODO - padronizar a forma de logging com o uso do método interno _log
    @classmethod
    def filter_valid_variables(
        cls, variables: List[ScenarioSynthesis]
    ) -> List[ScenarioSynthesis]:
        logger = Log.log()
        if logger is not None:
            logger.info(f"Variáveis: {variables}")
        return variables

    # TODO - atualizar idioma para inglês
    # TODO - padronizar a forma de logging com o uso do método interno _log
    # TODO - avaliar obter o dataframe já pós-processado direto do Deck
    @classmethod
    def _resolve_probabilities(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.probabilities(uow)

        if df is None:
            logger = Log.log()
            if logger is not None:
                logger.warning("Erro na leitura do arquivo de vazões")
            return pd.DataFrame(columns=["estagio", "cenario", "probabilidade"])
        df_subset = df[["estagio", "cenario", "probabilidade"]].drop_duplicates(
            ignore_index=True
        )
        return df_subset

    # TODO - criar _export_metadata para exportar metadados
    # TODO - modularizar a parte da síntese de uma variável
    # em uma função à parte (_synthetize_single_variable)

    # TODO - atualizar forma de logging
    # TODO - exportar metadados ao final
    # TODO - padronizar atribuições e chamadas com o do newave
    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        if len(variables) == 0:
            variables = cls._default_args()
        logger = Log.log()
        synthesis_variables = cls._process_variable_arguments(variables)
        valid_synthesis = cls.filter_valid_variables(synthesis_variables)
        for s in valid_synthesis:
            filename = str(s)
            if logger is not None:
                logger.info(f"Realizando síntese de {filename}")
            df = cls._get_rule(s)(uow)
            with uow:
                uow.export.synthetize_df(df, filename)
