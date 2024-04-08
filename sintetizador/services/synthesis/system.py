from typing import Callable, Dict, List
import pandas as pd  # type: ignore
from datetime import datetime, timedelta

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.system.variable import Variable
from sintetizador.model.system.systemsynthesis import SystemSynthesis


FATOR_HM3_M3S = 1.0 / 2.63


class SystemSynthetizer:
    IDENTIFICATION_COLUMNS = [
        "dataInicio",
        "dataFim",
        "estagio",
        "submercado",
        "submercadoDe",
        "submercadoPara",
        "ree",
        "usina",
        "patamar",
    ]

    DEFAULT_SYSTEM_SYNTHESIS_ARGS: List[str] = [
        "EST",
        "PAT",
        "SBM",
        "UTE",
        "UHE",
    ]

    @classmethod
    def _default_args(cls) -> List[str]:
        return cls.DEFAULT_SYSTEM_SYNTHESIS_ARGS

    @classmethod
    def _get_rule(cls, s: SystemSynthesis) -> Callable:
        rules = Dict[Variable, Callable] = {
            Variable.EST: cls._resolve_EST,
            Variable.PAT: cls._resolve_PAT,
            Variable.SBM: cls._resolve_SBM,
            Variable.UTE: cls._resolve_UTE,
            Variable.UHE: cls._resolve_UHE,
        }
        return rules[s]

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[SystemSynthesis]:
        args_data = [SystemSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        logger = Log.log()
        for i, a in enumerate(valid_args):
            if a is None:
                if logger is not None:
                    logger.error(f"Erro no argumento fornecido: {args[i]}")
                return []
        return valid_args

    @classmethod
    def filter_valid_variables(
        cls, variables: List[SystemSynthesis]
    ) -> List[SystemSynthesis]:
        valid_variables: List[SystemSynthesis] = []
        # TODO - verificar existência de PEE
        eolica = False
        logger = Log.log()
        if logger is not None:
            logger.info(f"Caso com geração de cenários de eólica: {eolica}")
        for v in variables:
            valid_variables.append(v)
        if logger is not None:
            logger.info(f"Variáveis: {valid_variables}")
        return valid_variables

    @classmethod
    def _resolve_EST(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            dadger = uow.files.get_dadger()
        logger = Log.log()
        registro_dt = dadger.dt
        if registro_dt is None:
            if logger is not None:
                logger.error("Não foi encontrado registro DT")
            raise RuntimeError()
        ano, mes, dia = registro_dt.ano, registro_dt.mes, registro_dt.dia
        if ano is None or mes is None or dia is None:
            if logger is not None:
                logger.error("Erro no processamento do registro DT")
            raise RuntimeError()
        data_inicial = datetime(year=ano, month=mes, day=dia)
        dps = dadger.dp(codigo_submercado=1)
        if dps is None or isinstance(dps, pd.DataFrame):
            if logger is not None:
                logger.error("Não foi encontrado registros DP")
            raise RuntimeError()
        registros_dp = dps if isinstance(dps, list) else [dps]
        datas = [data_inicial]
        for dp in registros_dp:
            duracoes = dp.duracao
            datas.append(
                datas[-1]
                + timedelta(
                    hours=sum(duracoes if duracoes is not None else [])
                )
            )
        datas_iniciais = datas[:-1]
        datas_finais = datas[1:]
        return pd.DataFrame(
            data={
                "idEstagio": list(range(1, len(datas_iniciais) + 1)),
                "dataInicio": datas_iniciais,
                "dataFim": datas_finais,
            }
        )

    @classmethod
    def _resolve_PAT(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            dadger = uow.files.get_dadger()
        # Assume que sempre existe subsistema de id = 1
        dps = dadger.dp(codigo_submercado=1)
        logger = Log.log()
        if dps is None or isinstance(dps, pd.DataFrame):
            if logger is not None:
                logger.error("Não foi encontrado registros DP")
            raise RuntimeError()
        registros_dp = dps if isinstance(dps, list) else [dps]
        estagios = []
        pats = []
        horas = []
        for dp in registros_dp:
            n = dp.numero_patamares
            if n is None:
                if logger is not None:
                    logger.info("Erro na leitura do número de patamares")
                raise RuntimeError()
            duracoes = dp.duracao
            if duracoes is None:
                if logger is not None:
                    logger.info("Erro na leitura da duração dos patamares")
                raise RuntimeError()
            for i in range(n):
                estagios.append(dp.estagio)
                pats.append(i + 1)
                horas.append(duracoes[i])
        df = pd.DataFrame(
            data={"idEstagio": estagios, "patamar": pats, "duracao": horas}
        )
        return df

    @classmethod
    def _resolve_SBM(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            dadger = uow.files.get_dadger()
        logger = Log.log()
        sbs = dadger.sb()
        if isinstance(sbs, pd.DataFrame) or sbs is None:
            if logger is not None:
                logger.error("Erro no processamento dos registros SB")
            raise RuntimeError()
        registros_submercados = sbs if isinstance(sbs, list) else [sbs]
        return pd.DataFrame(
            data={
                "id": [s.codigo_submercado for s in registros_submercados],
                "nome": [s.nome_submercado for s in registros_submercados],
            }
        )

    @classmethod
    def _resolve_UTE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            dadger = uow.files.get_dadger()
        logger = Log.log()
        cts = dadger.ct()
        if isinstance(cts, pd.DataFrame) or cts is None:
            if logger is not None:
                logger.error("Erro no processamento dos registros CT")
            raise RuntimeError()
        registros_utes = cts if isinstance(cts, list) else [cts]
        dados: Dict[str, list] = {"id": [], "idSubmercado": [], "nome": []}
        for ct in registros_utes:
            if ct.codigo_usina not in dados["id"]:
                dados["id"].append(ct.codigo_usina)
                dados["idSubmercado"].append(ct.codigo_submercado)
                dados["nome"].append(ct.nome_usina)
        return pd.DataFrame(data=dados)

    @classmethod
    def _resolve_UHE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            dadger = uow.files.get_dadger()
            hidr = uow.files.get_hidr()
        logger = Log.log()
        uhs = dadger.uh()
        if isinstance(uhs, pd.DataFrame) or uhs is None:
            if logger is not None:
                logger.error("Erro no processamento dos registros UH")
            raise RuntimeError()
        registros_uhes = uhs if isinstance(uhs, list) else [uhs]
        dados: Dict[str, list] = {
            "id": [],
            "idREE": [],
            "nome": [],
            "posto": [],
            "volumeInicial": [],
        }
        for uh in registros_uhes:
            dados["id"].append(uh.codigo_usina)
            dados["idREE"].append(uh.codigo_ree)
            dados["nome"].append(
                hidr.cadastro.at[uh.codigo_usina, "nome_usina"]
            )
            dados["posto"].append(hidr.cadastro.at[uh.codigo_usina, "posto"])
            dados["volumeInicial"].append(uh.volume_inicial)
        return pd.DataFrame(data=dados)

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        logger = Log.log()
        if len(variables) == 0:
            variables = cls._default_args()
        synthesis_variables = cls._process_variable_arguments(variables)
        valid_synthesis = cls.filter_valid_variables(synthesis_variables)
        for s in valid_synthesis:
            filename = str(s)
            if logger is not None:
                logger.info(f"Realizando síntese de {filename}")
            df = cls._get_rule(s.variable)(uow)
            if df is not None:
                with uow:
                    uow.export.synthetize_df(df, filename)
