from enum import Enum
from typing import Dict


class Variable(Enum):
    PROGRAMA = "PROGRAMA"
    CONVERGENCIA = "CONVERGENCIA"
    TEMPO_EXECUCAO = "TEMPO"
    CUSTOS = "CUSTOS"
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

    @property
    def short_name(self):
        SHORT_NAMES: Dict[str, str] = {
            "PROGRAMA": "PROGRAMA",
            "CONVERGENCIA": "CONVERGENCIA",
            "TEMPO": "TEMPO",
            "CUSTOS": "CUSTOS",
            "INVIABILIDADES": "IMVIABILIDADES",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self):
        LONG_NAMES: Dict[str, str] = {
            "PROGRAMA": "Modelo de Otimização",
            "CONVERGENCIA": "Convergência do Processo Iterativo",
            "TEMPO": "Tempo de Execução",
            "CUSTOS": "Composição de Custos da Solução",
            "INVIABILIDADES": "Violações das Restrições",
        }
        return LONG_NAMES.get(self.value)
