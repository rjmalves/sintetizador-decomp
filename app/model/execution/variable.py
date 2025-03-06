from enum import Enum
from typing import Dict


class Variable(Enum):
    PROGRAMA = "PROGRAMA"
    VERSAO = "VERSAO"
    TITULO = "TITULO"
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
    def short_name(self) -> str | None:
        SHORT_NAMES: Dict[str, str] = {
            "PROGRAMA": "PROGRAMA",
            "VERSAO": "VERSAO",
            "TITULO": "TITULO",
            "CONVERGENCIA": "CONVERGENCIA",
            "TEMPO": "TEMPO",
            "CUSTOS": "CUSTOS",
            "INVIABILIDADES": "INVIABILIDADES",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self) -> str | None:
        LONG_NAMES: Dict[str, str] = {
            "PROGRAMA": "Modelo de Otimização",
            "VERSAO": "Versão do Modelo",
            "TITULO": "Título do Estudo",
            "CONVERGENCIA": "Convergência do Processo Iterativo",
            "TEMPO": "Tempo de Execução",
            "INVIABILIDADES": "Violações das Restrições",
            "CUSTOS": "Composição de Custos da Solução",
        }
        return LONG_NAMES.get(self.value)
