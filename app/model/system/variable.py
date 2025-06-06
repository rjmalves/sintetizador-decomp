from enum import Enum


class Variable(Enum):
    EST = "EST"
    PAT = "PAT"
    SBM = "SBM"
    REE = "REE"
    UTE = "UTE"
    CVU = "CVU"
    UHE = "UHE"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.SBM

    def __repr__(self) -> str:
        return self.value

    @property
    def short_name(self) -> str | None:
        SHORT_NAMES: dict[str, str] = {
            "EST": "EST",
            "PAT": "PAT",
            "SBM": "SBM",
            "REE": "REE",
            "UTE": "UTE",
            "CVU": "CVU",
            "UHE": "UHE",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self) -> str | None:
        LONG_NAMES: dict[str, str] = {
            "EST": "Estágios",
            "PAT": "Patamares",
            "SBM": "Submercados",
            "REE": "Reservatórios Equivalentes",
            "UTE": "Usinas Termelétricas",
            "CVU": "Custos das Usinas Termelétricas",
            "UHE": "Usinas Hidroelétricas",
        }
        return LONG_NAMES.get(self.value)
