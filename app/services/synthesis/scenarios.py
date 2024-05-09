from typing import Callable, Dict, List
import pandas as pd  # type: ignore

from app.services.unitofwork import AbstractUnitOfWork
from app.utils.log import Log
from app.model.scenarios.variable import Variable
from app.model.scenarios.scenariosynthesis import ScenarioSynthesis
from app.services.deck.deck import Deck


class ScenarioSynthetizer:
    DEFAULT_SCENARIO_SYNTHESIS_ARGS: List[str] = [
        "PROBABILIDADES",
    ]

    @classmethod
    def _get_rule(cls, s: ScenarioSynthesis) -> Callable:
        rules: Dict[Variable, Callable] = {
            Variable.PROBABILIDADES: cls._resolve_probabilities,
        }
        return rules[s]

    @classmethod
    def _default_args(cls) -> List[str]:
        return cls.DEFAULT_SCENARIO_SYNTHESIS_ARGS

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

    @classmethod
    def filter_valid_variables(
        cls, variables: List[ScenarioSynthesis]
    ) -> List[ScenarioSynthesis]:
        logger = Log.log()
        if logger is not None:
            logger.info(f"Variáveis: {variables}")
        return variables

    @classmethod
    def _resolve_probabilities(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:

        df = Deck.probabilidades(uow)

        if df is None:
            logger = Log.log()
            if logger is not None:
                logger.warning("Erro na leitura do arquivo de vazões")
            return pd.DataFrame(
                columns=["estagio", "cenario", "probabilidade"]
            )
        df_subset = df[
            ["estagio", "cenario", "probabilidade"]
        ].drop_duplicates(ignore_index=True)
        return df_subset

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
            df = cls._get_rule(s.variable)(uow)
            with uow:
                uow.export.synthetize_df(df, filename)
