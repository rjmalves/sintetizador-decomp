from typing import Callable, Dict, List
import pandas as pd
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

    def __init__(self, uow: AbstractUnitOfWork) -> None:
        self.__uow = uow
        self.__rules: Dict[Variable, Callable] = {
            Variable.EST: self.__resolve_EST,
            Variable.PAT: self.__resolve_PAT,
            Variable.SBM: self.__resolve_SBM,
            Variable.UTE: self.__resolve_UTE,
            Variable.UHE: self.__resolve_UHE,
        }

    def _default_args(self) -> List[SystemSynthesis]:
        return [
            SystemSynthesis.factory(a)
            for a in self.__class__.DEFAULT_SYSTEM_SYNTHESIS_ARGS
        ]

    def _process_variable_arguments(
        self,
        args: List[str],
    ) -> List[SystemSynthesis]:
        args_data = [SystemSynthesis.factory(c) for c in args]
        for i, a in enumerate(args_data):
            if a is None:
                Log.log(f"Erro no argumento fornecido: {args[i]}")
                return []
        return args_data

    def filter_valid_variables(
        self, variables: List[SystemSynthesis]
    ) -> List[SystemSynthesis]:
        with self.__uow:
            dadger = self.__uow.files.get_dadger()
        valid_variables: List[SystemSynthesis] = []
        # TODO - verificar existência de PEE
        eolica = False
        Log.log().info(f"Caso com geração de cenários de eólica: {eolica}")
        for v in variables:
            if v.variable in [Variable.PEE] and not eolica:
                continue
            valid_variables.append(v)
        Log.log().info(f"Variáveis: {valid_variables}")
        return valid_variables

    def __resolve_EST(self) -> pd.DataFrame:
        with self.__uow:
            dadger = self.__uow.files.get_dadger()
        dps = dadger.dp(subsistema=1)
        data_inicial = datetime(
            year=dadger.dt.ano, month=dadger.dt.mes, day=dadger.dt.ia
        )
        datas = [data_inicial]
        for dp in dps:
            datas.append(datas[-1] + timedelta(hours=dp.duracoes.sum()))
        datas_iniciais = datas[:-1]
        datas_finais = datas[1:]
        return pd.DataFrame(
            data={
                "idEstagio": list(range(1, len(datas_iniciais) + 1)),
                "dataInicio": datas_iniciais,
                "dataFim": datas_finais,
            }
        )

    def __resolve_PAT(self) -> pd.DataFrame:
        with self.__uow:
            dadger = self.__uow.files.get_dadger()
        # Assume que sempre existe subsistema de id = 1
        dps = dadger.dp(subsistema=1)
        estagios = []
        pats = []
        horas = []
        for dp in dps:
            n = dp.num_patamares
            for i in range(n):
                estagios.append(dp.estagio)
                pats.append(i + 1)
                horas.append(dp.duracoes[i])
        df = pd.DataFrame(
            data={"idEstagio": estagios, "patamar": pats, "duracao": horas}
        )
        return df

    def __resolve_SBM(self) -> pd.DataFrame:
        with self.__uow:
            dadger = self.__uow.files.get_dadger()
        sbs = dadger.sb()
        return pd.DataFrame(
            data={"id": [s.codigo for s in sbs], "nome": [s.nome for s in sbs]}
        )

    def __resolve_UTE(self) -> pd.DataFrame:
        with self.__uow:
            dadger = self.__uow.files.get_dadger()

        cts = dadger.ct()
        dados = {"id": [], "idSubmercado": [], "nome": []}
        for ct in cts:
            if ct.codigo not in dados["id"]:
                dados["id"].append(ct.codigo)
                dados["idSubmercado"].append(ct.subsistema)
                dados["nome"].append(ct.nome)
        return pd.DataFrame(data=dados)

    def __resolve_UHE(self) -> pd.DataFrame:
        with self.__uow:
            dadger = self.__uow.files.get_dadger()
            hidr = self.__uow.files.get_hidr()

        uhs = dadger.uh()
        dados = {
            "id": [],
            "idREE": [],
            "nome": [],
            "posto": [],
            "volumeInicial": [],
        }
        for uh in uhs:
            dados["id"].append(uh.codigo)
            dados["idREE"].append(uh.ree)
            dados["nome"].append(uh.nome)
            dados["posto"].append(hidr.cadastro.at[uh.codigo, "Posto"])
            dados["volumeInicial"].append(uh.volume_inicial)
        return pd.DataFrame(data=dados)

    def synthetize(self, variables: List[str]):
        if len(variables) == 0:
            variables = self._default_args()
        else:
            variables = self._process_variable_arguments(variables)
        valid_synthesis = self.filter_valid_variables(variables)
        for s in valid_synthesis:
            filename = str(s)
            Log.log().info(f"Realizando síntese de {filename}")
            df = self.__rules[s.variable]()
            if df is not None:
                with self.__uow:
                    self.__uow.export.synthetize_df(df, filename)
