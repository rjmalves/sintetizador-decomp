from abc import abstractmethod
import numpy as np
import pandas as pd  # type: ignore
from idecomp.decomp.hidr import Hidr
from idecomp.decomp.relato import Relato
from sintetizador.utils.log import Log


class Inviabilidade:
    NOME = ""

    def __init__(
        self,
        iteracao: int,
        estagio: int,
        cenario: int,
        mensagem_restricao: str,
        violacao: float,
        unidade: str,
    ):
        self._iteracao = iteracao
        self._estagio = estagio
        self._cenario = cenario
        self._mensagem_restricao = mensagem_restricao
        self._violacao = violacao
        self._unidade = unidade
        self._codigo = 0
        self._patamar = 0
        self._limite = ""
        self._submercado = ""
        self._violacao_percentual = 0.0

    def __str__(self) -> str:
        return (
            f"Inviabilidade - Estágio {self._estagio}"
            + f" - It {self._iteracao} - Cenário {self._cenario}"
            + f" - Viol. {self._violacao} {self._unidade}"
        )

    @staticmethod
    def factory(
        linha_inviab_unic: pd.Series, hidr: Hidr, relato: Relato
    ) -> "Inviabilidade":
        if "iteracao" in list(linha_inviab_unic.index):
            iteracao = int(linha_inviab_unic["iteracao"])
        else:
            iteracao = -1
        estagio = int(linha_inviab_unic["estagio"])
        cenario = int(linha_inviab_unic["cenario"])
        mensagem_restricao = str(linha_inviab_unic["restricao"])
        violacao = float(linha_inviab_unic["violacao"])
        unidade = str(linha_inviab_unic["unidade"])
        if "RESTRICAO ELETRICA" in mensagem_restricao:
            return InviabilidadeRE(
                iteracao,
                estagio,
                cenario,
                mensagem_restricao,
                violacao,
                unidade,
            )
        elif "RHA" in mensagem_restricao:
            return InviabilidadeRHA(
                iteracao,
                estagio,
                cenario,
                mensagem_restricao,
                violacao,
                unidade,
            )
        elif "RHQ" in mensagem_restricao:
            return InviabilidadeRHQ(
                iteracao,
                estagio,
                cenario,
                mensagem_restricao,
                violacao,
                unidade,
            )
        elif "IRRIGACAO" in mensagem_restricao:
            return InviabilidadeTI(
                iteracao,
                estagio,
                cenario,
                mensagem_restricao,
                violacao,
                unidade,
                hidr,
            )
        elif "VERT. PERIODO" in mensagem_restricao:
            return InviabilidadeVertimento(
                iteracao,
                estagio,
                cenario,
                mensagem_restricao,
                violacao,
                unidade,
                hidr,
            )
        elif "RHV" in mensagem_restricao:
            return InviabilidadeRHV(
                iteracao,
                estagio,
                cenario,
                mensagem_restricao,
                violacao,
                unidade,
            )
        elif "RHE" in mensagem_restricao:
            return InviabilidadeRHE(
                iteracao,
                estagio,
                cenario,
                mensagem_restricao,
                violacao,
                unidade,
            )
        elif "EVAPORACAO" in mensagem_restricao:
            return InviabilidadeEV(
                iteracao,
                estagio,
                cenario,
                mensagem_restricao,
                violacao,
                unidade,
                hidr,
            )
        elif "DEF. MINIMA" in mensagem_restricao:
            return InviabilidadeDEFMIN(
                iteracao,
                estagio,
                cenario,
                mensagem_restricao,
                violacao,
                unidade,
                hidr,
            )
        elif "FUNCAO DE PRODUCAO" in mensagem_restricao:
            return InviabilidadeFP(
                iteracao,
                estagio,
                cenario,
                mensagem_restricao,
                violacao,
                unidade,
                hidr,
            )
        elif "DEFICIT" in mensagem_restricao:
            return InviabilidadeDeficit(
                iteracao,
                estagio,
                cenario,
                mensagem_restricao,
                violacao,
                unidade,
                relato,
            )
        else:
            raise TypeError(f"Restrição {mensagem_restricao} não suportada")

    @abstractmethod
    def processa_mensagem(self, *args) -> list:
        pass


class InviabilidadeTI(Inviabilidade):
    NOME = "TI"

    def __init__(
        self,
        iteracao: int,
        estagio: int,
        cenario: int,
        mensagem_restricao: str,
        violacao: float,
        unidade: str,
        hidr: Hidr,
    ):
        super().__init__(
            iteracao, estagio, cenario, mensagem_restricao, violacao, unidade
        )
        dados = self.processa_mensagem(hidr)
        self._codigo = dados[0]
        self._nome_usina = dados[1]

    def __str__(self) -> str:
        return (
            f"TI {self._codigo} ({self._nome_usina}) "
            + f"- Estágio {self._estagio}"
            + f" - It {self._iteracao} - Cenário {self._cenario}"
            + f" - Viol. {self._violacao} {self._unidade}"
        )

    def processa_mensagem(self, *args) -> list:
        hidr: Hidr = args[0]
        nome = self._mensagem_restricao.split("IRRIGACAO, USINA")[1].strip()
        codigo = int(
            list(
                hidr.cadastro.loc[hidr.cadastro["nome_usina"] == nome, :].index
            )[0]
        )
        return [codigo, nome]


class InviabilidadeVertimento(Inviabilidade):
    NOME = "VERT"

    def __init__(
        self,
        iteracao: int,
        estagio: int,
        cenario: int,
        mensagem_restricao: str,
        violacao: float,
        unidade: str,
        hidr: Hidr,
    ):
        super().__init__(
            iteracao, estagio, cenario, mensagem_restricao, violacao, unidade
        )
        dados = self.processa_mensagem(hidr)
        self._codigo = dados[0]
        self._nome_usina = dados[1]
        self._patamar = dados[2]

    def __str__(self) -> str:
        return (
            f"VERT {self._codigo} ({self._nome_usina}) "
            + f"- Estágio {self._estagio} - Patamar {self._patamar}"
            + f" - It {self._iteracao} - Cenário {self._cenario}"
            + f" - Viol. {self._violacao} {self._unidade}"
        )

    def processa_mensagem(self, *args) -> list:
        hidr: Hidr = args[0]
        patamar = int(
            self._mensagem_restricao.split("USINA")[0]
            .split("PAT. ")[1]
            .strip()
        )
        nome = self._mensagem_restricao.split("USINA")[1].strip()
        codigo = int(
            list(
                hidr.cadastro.loc[hidr.cadastro["nome_usina"] == nome, :].index
            )[0]
        )
        return [codigo, nome, patamar]


class InviabilidadeRHA(Inviabilidade):
    NOME = "RHA"

    def __init__(
        self,
        iteracao: int,
        estagio: int,
        cenario: int,
        mensagem_restricao: str,
        violacao: float,
        unidade: str,
    ):
        super().__init__(
            iteracao, estagio, cenario, mensagem_restricao, violacao, unidade
        )
        dados = self.processa_mensagem()
        self._codigo = dados[0]
        self._limite = dados[1]

    def __str__(self) -> str:
        return (
            f"HA {self._codigo} ({self._limite}) "
            + f"- Estágio {self._estagio}"
            + f" - It {self._iteracao} - Cenário {self._cenario}"
            + f" - Viol. {self._violacao} {self._unidade}"
        )

    def processa_mensagem(self, *args) -> list:
        codigo = int(self._mensagem_restricao.split("RHA")[1].split(":")[0])
        limite = self._mensagem_restricao.split("(")[1].split(")")[0]
        return [codigo, limite]


class InviabilidadeRHQ(Inviabilidade):
    NOME = "RHQ"

    def __init__(
        self,
        iteracao: int,
        estagio: int,
        cenario: int,
        mensagem_restricao: str,
        violacao: float,
        unidade: str,
    ):
        super().__init__(
            iteracao, estagio, cenario, mensagem_restricao, violacao, unidade
        )
        dados = self.processa_mensagem()
        self._codigo = dados[0]
        self._patamar = dados[1]
        self._limite = dados[2]

    def __str__(self) -> str:
        return (
            f"HQ {self._codigo} Pat {self._patamar} ({self._limite}) "
            + f"- Estágio {self._estagio}"
            + f" - It {self._iteracao} - Cenário {self._cenario}"
            + f" - Viol. {self._violacao} {self._unidade}"
        )

    def processa_mensagem(self, *args) -> list:
        codigo = int(self._mensagem_restricao.split("RHQ")[1].split(":")[0])
        pat = int(self._mensagem_restricao.split("PATAMAR")[1].split("(")[0])
        limite = self._mensagem_restricao.split("(")[1].split(")")[0]
        return [codigo, pat, limite]


class InviabilidadeRHV(Inviabilidade):
    NOME = "RHV"

    def __init__(
        self,
        iteracao: int,
        estagio: int,
        cenario: int,
        mensagem_restricao: str,
        violacao: float,
        unidade: str,
    ):
        super().__init__(
            iteracao, estagio, cenario, mensagem_restricao, violacao, unidade
        )
        dados = self.processa_mensagem()
        self._codigo = dados[0]
        self._limite = dados[1]

    def __str__(self) -> str:
        return (
            f"HV {self._codigo} ({self._limite}) "
            + f"- Estágio {self._estagio}"
            + f" - It {self._iteracao} - Cenário {self._cenario}"
            + f" - Viol. {self._violacao} {self._unidade}"
        )

    def processa_mensagem(self, *args) -> list:
        codigo = int(self._mensagem_restricao.split("RHV")[1].split(":")[0])
        limite = self._mensagem_restricao.split("(")[1].split(")")[0]
        return [codigo, limite]


class InviabilidadeRHE(Inviabilidade):
    NOME = "RHE"

    def __init__(
        self,
        iteracao: int,
        estagio: int,
        cenario: int,
        mensagem_restricao: str,
        violacao: float,
        unidade: str,
    ):
        super().__init__(
            iteracao, estagio, cenario, mensagem_restricao, violacao, unidade
        )
        dados = self.processa_mensagem()
        self._codigo = dados[0]
        self._estagio = dados[1]
        self._limite = dados[2]

    def __str__(self) -> str:
        return (
            f"HE {self._codigo} ({self._limite}) "
            + f"- Estágio {self._estagio}"
            + f" - It {self._iteracao} - Cenário {self._cenario}"
            + f" - Viol. {self._violacao} {self._unidade}"
        )

    def processa_mensagem(self, *args) -> list:
        s_rhe = "RESTRICAO RHE - NUMERO"
        s_per = "PERIODO"
        codigo = int(self._mensagem_restricao.split(s_rhe)[1].split(",")[0])
        estagio = int(self._mensagem_restricao.split(s_per)[1].split("(")[0])
        limite = self._mensagem_restricao.split("(")[1].split(")")[0]
        return [codigo, estagio, limite]


class InviabilidadeRE(Inviabilidade):
    NOME = "RE"

    def __init__(
        self,
        iteracao: int,
        estagio: int,
        cenario: int,
        mensagem_restricao: str,
        violacao: float,
        unidade: str,
    ):
        super().__init__(
            iteracao, estagio, cenario, mensagem_restricao, violacao, unidade
        )
        dados = self.processa_mensagem()
        self._codigo = dados[0]
        self._patamar = dados[1]
        self._limite = dados[2]

    def __str__(self) -> str:
        return (
            f"RE {self._codigo} Pat {self._patamar} ({self._limite}) "
            + f"- Estágio {self._estagio}"
            + f" - It {self._iteracao} - Cenário {self._cenario}"
            + f" - Viol. {self._violacao} {self._unidade}"
        )

    def processa_mensagem(self, *args) -> list:
        r = "RESTRICAO ELETRICA"
        codigo = int(self._mensagem_restricao.split(r)[1].split("PATAMAR")[0])
        pat = int(self._mensagem_restricao.split("PATAMAR")[1].split("(")[0])
        limite = self._mensagem_restricao.split("(")[1].split(")")[0]
        return [codigo, pat, limite]


class InviabilidadeEV(Inviabilidade):
    NOME = "EV"

    def __init__(
        self,
        iteracao: int,
        estagio: int,
        cenario: int,
        mensagem_restricao: str,
        violacao: float,
        unidade: str,
        hidr: Hidr,
    ):
        super().__init__(
            iteracao, estagio, cenario, mensagem_restricao, violacao, unidade
        )
        dados = self.processa_mensagem(hidr)
        self._codigo = dados[0]
        self._nome_usina = dados[1]

    def __str__(self) -> str:
        return (
            f"EV {self._codigo} ({self._nome_usina}) "
            + f"- Estágio {self._estagio}"
            + f" - It {self._iteracao} - Cenário {self._cenario}"
            + f" - Viol. {self._violacao} {self._unidade}"
        )

    def processa_mensagem(self, *args) -> list:
        hidr: Hidr = args[0]
        nome = self._mensagem_restricao.split("EVAPORACAO, USINA")[1].strip()
        codigo = int(
            list(
                hidr.cadastro.loc[hidr.cadastro["nome_usina"] == nome, :].index
            )[0]
        )
        return [codigo, nome]


class InviabilidadeDEFMIN(Inviabilidade):
    NOME = "DEFMIN"

    def __init__(
        self,
        iteracao: int,
        estagio: int,
        cenario: int,
        mensagem_restricao: str,
        violacao: float,
        unidade: str,
        hidr: Hidr,
    ):
        super().__init__(
            iteracao, estagio, cenario, mensagem_restricao, violacao, unidade
        )
        dados = self.processa_mensagem(hidr)
        self._codigo = dados[0]
        self._usina = dados[1]
        self._patamar = dados[2]
        self._vazmin_hidr = dados[3]

    def __str__(self) -> str:
        return (
            f"DEFMIN {self._codigo} Pat {self._patamar} ({self._usina}) "
            + f"- Estágio {self._estagio}"
            + f" - It {self._iteracao} - Cenário {self._cenario}"
            + f" - Viol. {self._violacao} {self._unidade}"
        )

    def processa_mensagem(self, *args) -> list:
        hidr: Hidr = args[0]
        p = "PATAMAR"
        u = "USINA"
        pat = int(self._mensagem_restricao.split(p)[1].split(u)[0].strip())
        nome = self._mensagem_restricao.split(u)[1].strip()
        codigo = int(
            list(
                hidr.cadastro.loc[hidr.cadastro["nome_usina"] == nome, :].index
            )[0]
        )
        vazmin_hidr = int(
            list(
                hidr.cadastro.loc[
                    hidr.cadastro["nome_usina"] == nome,
                    "vazao_minima_historica",
                ]
            )[0]
        )
        return [codigo, nome, pat, vazmin_hidr]


class InviabilidadeFP(Inviabilidade):
    NOME = "FP"

    def __init__(
        self,
        iteracao: int,
        estagio: int,
        cenario: int,
        mensagem_restricao: str,
        violacao: float,
        unidade: str,
        hidr: Hidr,
    ):
        super().__init__(
            iteracao, estagio, cenario, mensagem_restricao, violacao, unidade
        )
        dados = self.processa_mensagem(hidr)
        self._codigo = dados[0]
        self._usina = dados[1]
        self._patamar = dados[2]

    def __str__(self) -> str:
        return (
            f"FP {self._codigo} Pat {self._patamar} ({self._usina}) "
            + f"- Estágio {self._estagio}"
            + f" - It {self._iteracao} - Cenário {self._cenario}"
            + f" - Viol. {self._violacao} {self._unidade}"
        )

    def processa_mensagem(self, *args) -> list:
        hidr: Hidr = args[0]
        p = "PATAMAR"
        u = "USINA"
        pat = int(self._mensagem_restricao.split(p)[1])
        nome = self._mensagem_restricao.split(u)[1].split(",")[0].strip()
        codigo = int(
            list(
                hidr.cadastro.loc[hidr.cadastro["nome_usina"] == nome, :].index
            )[0]
        )
        return [codigo, nome, pat]


class InviabilidadeDeficit(Inviabilidade):
    NOME = "DEFICIT"

    def __init__(
        self,
        iteracao: int,
        estagio: int,
        cenario: int,
        mensagem_restricao: str,
        violacao: float,
        unidade: str,
        relato: Relato,
    ):
        super().__init__(
            iteracao, estagio, cenario, mensagem_restricao, violacao, unidade
        )
        dados = self.processa_mensagem(relato)
        self._submercado = dados[0]
        self._patamar = dados[1]
        self._violacao_percentual = dados[2]

    def __str__(self) -> str:
        return (
            "DEF "
            + f"- Estágio {self._estagio}"
            + f" - It {self._iteracao} - Cenário {self._cenario}"
            + f" - Viol. {self._violacao} {self._unidade}"
        )

    def processa_mensagem(self, *args) -> list:
        relato: Relato = args[0]
        msg = self._mensagem_restricao
        subsis = msg.split("SUBSISTEMA ")[1].split(",")[0].strip()
        pat = int(msg.split("PATAMAR")[1].strip())
        # Tenta obter informações úteis do Relato
        # Duração dos patamares
        try:
            merc = relato.dados_mercado
            if merc is None:
                logger = Log.log()
                if logger is not None:
                    logger.error("Erro na obtenção dos dados de mercado")
                raise RuntimeError()
            cols_pat = [c for c in merc.columns if "patamar" in c]
            duracoes = merc.loc[
                (merc["estagio"] == self._estagio)
                & (merc["nome_submercado"] == subsis),
                cols_pat,
            ].to_numpy()[0]
            fracao = duracoes[pat - 1] / np.sum(duracoes)
            violacao_ponderada = self._violacao * fracao
        except ValueError:
            violacao_ponderada = 0
        # EARMax
        try:
            earmax = relato.energia_armazenada_maxima_submercado
            if earmax is None:
                logger = Log.log()
                if logger is not None:
                    logger.error("Erro na obtenção dos dados de EARmax")
                raise RuntimeError()
            earmax_subsis = float(
                earmax.loc[
                    earmax["nome_submercado"] == subsis,
                    "energia_armazenada_maxima",
                ]
            )
        except ValueError:
            earmax_subsis = np.inf
        # Calcula a violação em percentual
        violacao_percentual = 100 * (violacao_ponderada / earmax_subsis)
        return [subsis, pat, violacao_percentual]
