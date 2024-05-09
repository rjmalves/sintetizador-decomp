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
