from enum import Enum


class TemporalResolution(Enum):
    ESTAGIO = "EST"
    MES = "MES"
    SEMANA = "SEM"
    PATAMAR = "PAT"

    @classmethod
    def factory(cls, val: str) -> "TemporalResolution":
        for v in cls:
            if v.value == val:
                return v
        return cls.MES

    def __repr__(self):
        return self.value
