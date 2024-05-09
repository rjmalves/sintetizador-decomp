from enum import Enum


class Variable(Enum):
    PROGRAMA = "PROGRAMA"
    CONVERGENCIA = "CONVERGENCIA"
    TEMPO_EXECUCAO = "TEMPO"
    CUSTOS = "CUSTOS"
    RECURSOS_JOB = "RECURSOS_JOB"
    RECURSOS_CLUSTER = "RECURSOS_CLUSTER"
    INVIABILIDADES = "INVIABILIDADES"
    INVIABILIDADES_CODIGO = "INVIABILIDADES_CODIGO"
    INVIABILIDADES_PATAMAR = "INVIABILIDADES_PATAMAR"
    INVIABILIDADES_PATAMAR_LIMITE = "INVIABILIDADES_PATAMAR_LIMITE"
    INVIABILIDADES_LIMITE = "INVIABILIDADES_LIMITE"
    INVIABILIDADES_SBM_PATAMAR = "INVIABILIDADES_SBM_PATAMAR"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.CONVERGENCIA

    def __repr__(self) -> str:
        return self.value
