from enum import Enum


class Variable(Enum):
    PROBABILIDADES = "PROBABILIDADES"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.PROBABILIDADES

    def __repr__(self) -> str:
        return self.value

    @property
    def short_name(self) -> str | None:
        SHORT_NAMES: dict[str, str] = {
            "PROBABILIDADES": "Probabilidades",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self) -> str | None:
        LONG_NAMES: dict[str, str] = {
            "PROBABILIDADES": "Probabilidades dos cenários de vazões",
        }
        return LONG_NAMES.get(self.value)
