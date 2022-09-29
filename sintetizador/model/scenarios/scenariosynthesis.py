from dataclasses import dataclass
from typing import Optional
from sintetizador.model.scenarios.variable import Variable


@dataclass
class ScenarioSynthesis:
    variable: Variable

    def __repr__(self) -> str:
        return self.variable.value

    @classmethod
    def factory(cls, synthesis: str) -> Optional["ScenarioSynthesis"]:
        return cls(
            Variable.factory(synthesis),
        )
