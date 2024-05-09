from enum import Enum


class Variable(Enum):
    EST = "EST"
    PAT = "PAT"
    SBM = "SBM"
    UTE = "UTE"
    UHE = "UHE"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.SBM

    def __repr__(self) -> str:
        return self.value
