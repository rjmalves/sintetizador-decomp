from typing import Callable, Dict, List, Optional
import pandas as pd

from sintetizador.model.execution.inviabilidade import (
    Inviabilidade,
    InviabilidadeTI,
    InviabilidadeEV,
    InviabilidadeRHV,
    InviabilidadeRHE,
    InviabilidadeRHQ,
    InviabilidadeRHA,
    InviabilidadeRE,
    InviabilidadeDEFMIN,
    InviabilidadeFP,
    InviabilidadeDeficit,
)
from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.execution.variable import Variable
from sintetizador.model.execution.executionsynthesis import ExecutionSynthesis


class ExecutionSynthetizer:

    DEFAULT_EXECUTION_SYNTHESIS_ARGS: List[str] = [
        "CONVERGENCIA",
        "TEMPO",
        "INVIABILIDADES_CODIGO",
        "INVIABILIDADES_PATAMAR",
        "INVIABILIDADES_PATAMAR_LIMITE",
        "INVIABILIDADES_LIMITE",
        "INVIABILIDADES_SBM_PATAMAR",
    ]

    INVIABS_CODIGO = [InviabilidadeTI, InviabilidadeEV]
    INVIABS_PATAMAR = [InviabilidadeDEFMIN, InviabilidadeFP]
    INVIABS_PATAMAR_LIMITE = [InviabilidadeRHQ, InviabilidadeRE]
    INVIABS_LIMITE = [InviabilidadeRHV, InviabilidadeRHE, InviabilidadeRHA]
    INVIABS_SBM_PATAMAR = [
        InviabilidadeDeficit,
    ]

    def __init__(self, uow: AbstractUnitOfWork) -> None:
        self.__uow = uow
        self.__inviabilidades: Optional[List[Inviabilidade]] = None
        self.__rules: Dict[Variable, Callable] = {
            Variable.CONVERGENCIA: self._resolve_convergence,
            Variable.TEMPO_EXECUCAO: self._resolve_convergence,
            Variable.INVIABILIDADES_CODIGO: self._resolve_inviabilidades_codigo,
            Variable.INVIABILIDADES_PATAMAR: self._resolve_inviabilidades_patamar,
            Variable.INVIABILIDADES_PATAMAR_LIMITE: self._resolve_inviabilidades_patamar_limite,
            Variable.INVIABILIDADES_LIMITE: self._resolve_inviabilidades_limite,
            Variable.INVIABILIDADES_SBM_PATAMAR: self._resolve_inviabilidades_submercado_patamar,
        }

    def _default_args(self) -> List[ExecutionSynthesis]:
        return [
            ExecutionSynthesis.factory(a)
            for a in self.__class__.DEFAULT_EXECUTION_SYNTHESIS_ARGS
        ]

    def _process_variable_arguments(
        self,
        args: List[str],
    ) -> List[ExecutionSynthesis]:
        args_data = [ExecutionSynthesis.factory(c) for c in args]
        for i, a in enumerate(args_data):
            if a is None:
                Log.log(f"Erro no argumento fornecido: {args[i]}")
                return []
        return args_data

    def filter_valid_variables(
        self, variables: List[ExecutionSynthesis]
    ) -> List[ExecutionSynthesis]:
        with self.__uow:
            existe_inviabunic = self.__uow.files.get_inviabunic() is not None
        invs_vars = [
            Variable.INVIABILIDADES_CODIGO,
            Variable.INVIABILIDADES_PATAMAR,
            Variable.INVIABILIDADES_PATAMAR_LIMITE,
            Variable.INVIABILIDADES_LIMITE,
            Variable.INVIABILIDADES_SBM_PATAMAR,
        ]
        if not existe_inviabunic:
            variables = [v for v in variables if v.variable not in invs_vars]
        Log.log().info(f"Variáveis: {variables}")
        return variables

    def _resolve_convergence(self) -> pd.DataFrame:
        with self.__uow:
            relato = self.__uow.files.get_relato()
        df = relato.convergencia
        df_processed = df.rename(
            columns={
                "Iteração": "Iteracao",
                "Gap (%)": "Gap",
                "Tempo (s)": "Tempo",
                "Num. Inviab": "Inviabilidades",
                "Total Def. Demanda (MWmed)": "Deficit",
            }
        )
        df_processed.drop(
            columns=["Tot. Def. Niv. Seg. (MWmes)"], inplace=True
        )
        return df_processed

    def __resolve_inviabilidades(self) -> List[Inviabilidade]:
        with self.__uow:
            Log.log().info(f"Obtendo Inviabilidades")
            inviabunic = self.__uow.files.get_inviabunic()
            hidr = self.__uow.files.get_hidr()
            relato = self.__uow.files.get_relato()
        df_iter = inviabunic.inviabilidades_iteracoes
        df_sf = inviabunic.inviabilidades_simulacao_final
        df_sf["Iteração"] = -1
        df_inviabs = pd.concat([df_iter, df_sf], ignore_index=True)
        inviabilidades = []
        for _, linha in df_inviabs.iterrows():
            inviabilidades.append(Inviabilidade.factory(linha, hidr, relato))
        return inviabilidades

    @property
    def inviabilidades(self) -> List[Inviabilidade]:
        if self.__inviabilidades is None:
            self.__inviabilidades = self.__resolve_inviabilidades()
        return self.__inviabilidades

    def _resolve_inviabilidades_codigo(self) -> pd.DataFrame:
        inviabs_codigo = [
            i
            for i in self.inviabilidades
            if type(i) in self.__class__.INVIABS_CODIGO
        ]
        tipos: List[str] = []
        iteracoes: List[int] = []
        cenarios: List[int] = []
        estagios: List[int] = []
        codigos: List[int] = []
        violacoes: List[float] = []
        unidades: List[str] = []
        for i in inviabs_codigo:
            tipos.append(i.NOME)
            iteracoes.append(i._iteracao)
            cenarios.append(i._cenario)
            estagios.append(i._estagio)
            codigos.append(i._codigo)
            violacoes.append(i._violacao)
            unidades.append(i._unidade)

        return pd.DataFrame(
            data={
                "Tipo": tipos,
                "Iteracao": iteracoes,
                "Cenario": cenarios,
                "Estagio": estagios,
                "Codigo": codigos,
                "Violacao": violacoes,
                "Unidade": unidades,
            }
        )

    def _resolve_inviabilidades_patamar(self) -> pd.DataFrame:
        inviabs_codigo = [
            i
            for i in self.inviabilidades
            if type(i) in self.__class__.INVIABS_PATAMAR
        ]
        tipos: List[str] = []
        iteracoes: List[int] = []
        cenarios: List[int] = []
        estagios: List[int] = []
        codigos: List[int] = []
        violacoes: List[float] = []
        unidades: List[str] = []
        patamares: List[int] = []
        for i in inviabs_codigo:
            tipos.append(i.NOME)
            iteracoes.append(i._iteracao)
            cenarios.append(i._cenario)
            estagios.append(i._estagio)
            codigos.append(i._codigo)
            violacoes.append(i._violacao)
            unidades.append(i._unidade)
            patamares.append(i._patamar)

        return pd.DataFrame(
            data={
                "Tipo": tipos,
                "Iteracao": iteracoes,
                "Cenario": cenarios,
                "Estagio": estagios,
                "Codigo": codigos,
                "Violacao": violacoes,
                "Unidade": unidades,
                "Patamar": patamares,
            }
        )

    def _resolve_inviabilidades_patamar_limite(self) -> pd.DataFrame:
        inviabs_codigo = [
            i
            for i in self.inviabilidades
            if type(i) in self.__class__.INVIABS_PATAMAR_LIMITE
        ]
        tipos: List[str] = []
        iteracoes: List[int] = []
        cenarios: List[int] = []
        estagios: List[int] = []
        codigos: List[int] = []
        violacoes: List[float] = []
        unidades: List[str] = []
        patamares: List[int] = []
        limites: List[str] = []
        for i in inviabs_codigo:
            tipos.append(i.NOME)
            iteracoes.append(i._iteracao)
            cenarios.append(i._cenario)
            estagios.append(i._estagio)
            codigos.append(i._codigo)
            violacoes.append(i._violacao)
            unidades.append(i._unidade)
            patamares.append(i._patamar)
            limites.append(i._limite)

        return pd.DataFrame(
            data={
                "Tipo": tipos,
                "Iteracao": iteracoes,
                "Cenario": cenarios,
                "Estagio": estagios,
                "Codigo": codigos,
                "Violacao": violacoes,
                "Unidade": unidades,
                "Patamar": patamares,
                "Limite": limites,
            }
        )

    def _resolve_inviabilidades_limite(self) -> pd.DataFrame:
        inviabs_codigo = [
            i
            for i in self.inviabilidades
            if type(i) in self.__class__.INVIABS_LIMITE
        ]
        tipos: List[str] = []
        iteracoes: List[int] = []
        cenarios: List[int] = []
        estagios: List[int] = []
        codigos: List[int] = []
        violacoes: List[float] = []
        unidades: List[str] = []
        limites: List[str] = []
        for i in inviabs_codigo:
            tipos.append(i.NOME)
            iteracoes.append(i._iteracao)
            cenarios.append(i._cenario)
            estagios.append(i._estagio)
            codigos.append(i._codigo)
            violacoes.append(i._violacao)
            unidades.append(i._unidade)
            limites.append(i._limite)

        return pd.DataFrame(
            data={
                "Tipo": tipos,
                "Iteracao": iteracoes,
                "Cenario": cenarios,
                "Estagio": estagios,
                "Codigo": codigos,
                "Violacao": violacoes,
                "Unidade": unidades,
                "Limite": limites,
            }
        )

    def _resolve_inviabilidades_submercado_patamar(self) -> pd.DataFrame:
        inviabs_codigo = [
            i
            for i in self.inviabilidades
            if type(i) in self.__class__.INVIABS_SBM_PATAMAR
        ]
        tipos: List[str] = []
        iteracoes: List[int] = []
        cenarios: List[int] = []
        estagios: List[int] = []
        codigos: List[int] = []
        violacoes: List[float] = []
        unidades: List[str] = []
        submercados: List[str] = []
        patamares: List[str] = []
        for i in inviabs_codigo:
            tipos.append(i.NOME)
            iteracoes.append(i._iteracao)
            cenarios.append(i._cenario)
            estagios.append(i._estagio)
            codigos.append(i._codigo)
            violacoes.append(i._violacao_percentual)
            unidades.append(i._unidade)
            submercados.append(i._subsistema)
            patamares.append(i._patamar)

        return pd.DataFrame(
            data={
                "Tipo": tipos,
                "Iteracao": iteracoes,
                "Cenario": cenarios,
                "Estagio": estagios,
                "Codigo": codigos,
                "Violacao": violacoes,
                "Unidade": unidades,
                "Submercado": submercados,
                "Patamar": patamares,
            }
        )

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
            with self.__uow:
                self.__uow.export.synthetize_df(df, filename)
