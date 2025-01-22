from dataclasses import dataclass
from app.model.scenarios.variable import Variable
from typing import Optional


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


SUPPORTED_SYNTHESIS: list[str] = ["PROBABILIDADES"]
