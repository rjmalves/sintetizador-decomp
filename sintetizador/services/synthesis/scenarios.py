from typing import Callable, Dict, List, Optional
import pandas as pd  # type: ignore

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.scenarios.variable import Variable
from sintetizador.model.scenarios.scenariosynthesis import ScenarioSynthesis


class ScenarioSynthetizer:
    DEFAULT_SCENARIO_SYNTHESIS_ARGS: List[str] = [
        "PROBABILIDADES",
    ]

    def __init__(self) -> None:
        self.__uow: Optional[AbstractUnitOfWork] = None
        self.__rules: Dict[Variable, Callable] = {
            Variable.PROBABILIDADES: self._resolve_probabilities,
        }

    @property
    def uow(self) -> AbstractUnitOfWork:
        if self.__uow is None:
            raise RuntimeError()
        return self.__uow

    def _default_args(self) -> List[str]:
        return self.__class__.DEFAULT_SCENARIO_SYNTHESIS_ARGS

    def _process_variable_arguments(
        self,
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

    def filter_valid_variables(
        self, variables: List[ScenarioSynthesis]
    ) -> List[ScenarioSynthesis]:
        logger = Log.log()
        if logger is not None:
            logger.info(f"Variáveis: {variables}")
        return variables

    def _resolve_probabilities(self) -> pd.DataFrame:
        with self.uow:
            r = self.uow.files.get_relato()
            r2 = self.uow.files.get_relato2()

        df = (
            pd.concat(
                [r.balanco_energetico, r2.balanco_energetico],
                ignore_index=True,
            )
            if r2 is not None
            else r.balanco_energetico
        )
        df = df.rename(
            columns={
                "Estágio": "estagio",
                "Cenário": "cenario",
                "Probabilidade": "probabilidade",
            }
        )
        df_subset = df[
            ["estagio", "cenario", "probabilidade"]
        ].drop_duplicates(ignore_index=True)
        return df_subset

    def synthetize(self, variables: List[str], uow: AbstractUnitOfWork):
        self.__uow = uow
        if len(variables) == 0:
            variables = self._default_args()
        logger = Log.log()
        synthesis_variables = self._process_variable_arguments(variables)
        valid_synthesis = self.filter_valid_variables(synthesis_variables)
        for s in valid_synthesis:
            filename = str(s)
            if logger is not None:
                logger.info(f"Realizando síntese de {filename}")
            df = self.__rules[s.variable]()
            with self.uow:
                self.uow.export.synthetize_df(df, filename)
