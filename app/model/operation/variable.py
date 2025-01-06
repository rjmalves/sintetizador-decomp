from enum import Enum


class Variable(Enum):
    CUSTO_MARGINAL_OPERACAO = "CMO"
    VALOR_AGUA = "VAGUA"
    CUSTO_GERACAO_TERMICA = "CTER"
    CUSTO_OPERACAO = "COP"
    CUSTO_FUTURO = "CFU"
    ENERGIA_NATURAL_AFLUENTE_ABSOLUTA = "ENAA"
    ENERGIA_NATURAL_AFLUENTE_MLT = "ENAM"
    ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL = "EARMI"
    ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL = "EARPI"
    ENERGIA_ARMAZENADA_ABSOLUTA_FINAL = "EARMF"
    ENERGIA_ARMAZENADA_PERCENTUAL_FINAL = "EARPF"
    GERACAO_HIDRAULICA = "GHID"
    GERACAO_USINAS_NAO_SIMULADAS = "GUNS"
    GERACAO_USINAS_NAO_SIMULADAS_DISPONIVEL = "GUNSD"
    CORTE_GERACAO_USINAS_NAO_SIMULADAS = "CUNS"
    GERACAO_TERMICA = "GTER"
    ENERGIA_VERTIDA = "EVER"
    ENERGIA_VERTIDA_TURBINAVEL = "EVERT"
    ENERGIA_VERTIDA_NAO_TURBINAVEL = "EVERNT"
    VAZAO_AFLUENTE = "QAFL"
    VAZAO_DEFLUENTE = "QDEF"
    VAZAO_INCREMENTAL = "QINC"
    VAZAO_TURBINADA = "QTUR"
    VAZAO_VERTIDA = "QVER"
    VELOCIDADE_VENTO = "VENTO"
    VOLUME_ARMAZENADO_ABSOLUTO_INICIAL = "VARMI"
    VOLUME_ARMAZENADO_PERCENTUAL_INICIAL = "VARPI"
    VOLUME_ARMAZENADO_ABSOLUTO_FINAL = "VARMF"
    VOLUME_ARMAZENADO_PERCENTUAL_FINAL = "VARPF"
    INTERCAMBIO = "INT"
    INTERCAMBIO_LIQUIDO = "INTL"
    MERCADO = "MER"
    MERCADO_LIQUIDO = "MERL"
    DEFICIT = "DEF"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL

    def __repr__(self) -> str:
        return self.value

    @property
    def short_name(self) -> str | None:
        SHORT_NAMES: dict[str, str] = {
            "CMO": "CMO",
            "VAGUA": "VAGUA",
            "CTER": "Custo de GT",
            "COP": "COPER",
            "CFU": "CFU",
            "ENAA": "ENA",
            "ENAM": "ENA %MLT",
            "EARMI": "EAR Inicial",
            "EARPI": "EAR Percentual Inicial",
            "EARMF": "EAR Final",
            "EARPF": "EAR Percentual Final",
            "GHID": "GH",
            "GUNS": "Geração Não Simuladas",
            "HMON": "Cota de Montante",
            "HJUS": "Cota de Jusante",
            "HLIQ": "Queda Líquida",
            "GTER": "GT",
            "GEOL": "GEOL",
            "EVER": "EVER",
            "EVERT": "EVER Turbinável",
            "EVERNT": "EVER Não-Turbinável",
            "QAFL": "Vazão AFL",
            "QINC": "Vazão INC",
            "QDEF": "Vazão DEF",
            "QTUR": "Vazão TUR",
            "QVER": "Vazão VER",
            "QRET": "Vazão RET",
            "QDES": "Vazão DES",
            "VENTO": "Vel. Vento",
            "VARMI": "VAR Inicial",
            "VARPI": "VAR Percentual Inicial",
            "VARMF": "VAR Final",
            "VARPF": "VAR Percentual Final",
            "INT": "Intercâmbio",
            "INTL": "Intercâmbio Líquido",
            "MER": "Mercado",
            "MERL": "Mercado Líq.",
            "DEF": "Déficit",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self) -> str | None:
        LONG_NAMES: dict[str, str] = {
            "CMO": "Custo Marginal de Operação",
            "VAGUA": "Valor da Água",
            "CTER": "Custo de Geração Térmica",
            "COP": "Custo de Operação",
            "CFU": "Custo Futuro",
            "ENAA": "Energia Natural Afluente Absoluta",
            "ENAM": "Energia Natural Afluente Percentual MLT",
            "EARMI": "Energia Armazenada Absoluta Inicial",
            "EARPI": "Energia Armazenada Percentual Inicial",
            "EARMF": "Energia Armazenada Absoluta Final",
            "EARPF": "Energia Armazenada Percentual Final",
            "GHID": "Geração Hidráulica",
            "GUNS": "Geração de Usinas Não Simuladas",
            "HMON": "Cota de Montante",
            "HJUS": "Cota de Jusante",
            "HLIQ": "Queda Líquida",
            "GTER": "Geração Térmica",
            "GEOL": "Geração Eólica",
            "EVER": "Energia Vertida",
            "EVERT": "Energia Vertida Turbinável",
            "EVERNT": "Energia Vertida Não-Turbinável",
            "QAFL": "Vazão Afluente",
            "QINC": "Vazão Incremental",
            "QDEF": "Vazão Defluente",
            "QTUR": "Vazão Turbinada",
            "QVER": "Vazão Vertida",
            "QRET": "Vazão Retirada",
            "QDES": "Vazão Desviada",
            "VENTO": "Velocidade do Vento",
            "VARMI": "Volume Armazenado Absoluto Inicial",
            "VARPI": "Volume Armazenado Percentual Inicial",
            "VARMF": "Volume Armazenado Absoluto Final",
            "VARPF": "Volume Armazenado Percentual Final",
            "INT": "Intercâmbio de Energia",
            "INTL": "Intercâmbio Líquido de Energia",
            "MER": "Mercado de Energia",
            "MERL": "Mercado de Energia Líquido",
            "DEF": "Déficit",
        }
        return LONG_NAMES.get(self.value)
