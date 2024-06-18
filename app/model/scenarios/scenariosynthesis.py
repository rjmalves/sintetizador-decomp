from dataclasses import dataclass
from app.model.scenarios.variable import Variable


@dataclass
class ScenarioSynthesis:
    variable: Variable

    def __repr__(self) -> str:
        return self.variable.value

    @classmethod
    def factory(cls, synthesis: str) -> "ScenarioSynthesis" | None:
        return cls(
            Variable.factory(synthesis),
        )


SUPPORTED_SYNTHESIS: list[str] = ["PROBABILIDADES"]
