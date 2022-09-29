from typing import Callable, Dict, List
import pandas as pd

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.scenarios.variable import Variable
from sintetizador.model.scenarios.scenariosynthesis import ScenarioSynthesis


class ScenarioSynthetizer:

    DEFAULT_SCENARIO_SYNTHESIS_ARGS: List[str] = [
        "PROBABILIDADES",
    ]

    def __init__(self, uow: AbstractUnitOfWork) -> None:
        self.__uow = uow
        self.__rules: Dict[Variable, Callable] = {
            Variable.PROBABILIDADES: self._resolve_probabilities,
         }

    def _default_args(self) -> List[ScenarioSynthesis]:
        return [
            ScenarioSynthesis.factory(a)
            for a in self.__class__.DEFAULT_SCENARIO_SYNTHESIS_ARGS
        ]

    def _process_variable_arguments(
        self,
        args: List[str],
    ) -> List[ScenarioSynthesis]:
        args_data = [ScenarioSynthesis.factory(c) for c in args]
        for i, a in enumerate(args_data):
            if a is None:
                Log.log(f"Erro no argumento fornecido: {args[i]}")
                return []
        return args_data

    def filter_valid_variables(
        self, variables: List[ScenarioSynthesis]
    ) -> List[ScenarioSynthesis]:
        Log.log().info(f"Variáveis: {variables}")
        return variables

    def _resolve_probabilities(self) -> pd.DataFrame:
        with self.__uow:
            r = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        
        df = pd.concat([r.balanco_energetico, r2.balanco_energetico], ignore_index=True) if r2 is not None else r.balanco_energetico
        df = df.rename(columns={"Estágio": "Estagio", "Cenário": "Cenario"})
        df_subset = df[["Estagio", "Cenario", "Probabilidade"]].drop_duplicates(ignore_index=True)
        return df_subset

    def synthetize(self, variables: List[str]):
        if len(variables) == 0:
            variables = self._default_args()
        else:
            variables = self._process_variable_arguments(variables)
        valid_synthesis = self.filter_valid_variables(variables)
        for s in valid_synthesis:
            filename = str(s)
            Log.log().info(f"Realizando síntese de {filename}")
            df = self.__rules[s.variable]()
            with self.__uow:
                self.__uow.export.synthetize_df(df, filename)
