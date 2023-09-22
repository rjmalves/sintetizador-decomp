from typing import Callable, Dict, List, Optional
from traceback import print_exc
import pandas as pd  # type: ignore
import numpy as np
import socket
import pathlib

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
from sintetizador.utils.fs import set_directory
from sintetizador.model.execution.variable import Variable
from sintetizador.model.execution.executionsynthesis import ExecutionSynthesis


class ExecutionSynthetizer:
    DEFAULT_EXECUTION_SYNTHESIS_ARGS: List[str] = [
        "PROGRAMA",
        "CONVERGENCIA",
        "TEMPO",
        "INVIABILIDADES",
        "CUSTOS",
        "RECURSOS_JOB",
        "RECURSOS_CLUSTER",
    ]

    INVIABS_CODIGO = [InviabilidadeTI, InviabilidadeEV]
    INVIABS_PATAMAR = [InviabilidadeDEFMIN, InviabilidadeFP]
    INVIABS_PATAMAR_LIMITE = [InviabilidadeRHQ, InviabilidadeRE]
    INVIABS_LIMITE = [InviabilidadeRHV, InviabilidadeRHE, InviabilidadeRHA]
    INVIABS_SBM_PATAMAR = [
        InviabilidadeDeficit,
    ]

    CONVERGENCE_FILE = "CONVERGENCIA"
    RUNTIME_FILE = "TEMPO"
    INVIABS_FILE = "INVIABILIDADES"

    def __init__(self) -> None:
        self.__uow: Optional[AbstractUnitOfWork] = None
        self.__inviabilidades: Optional[List[Inviabilidade]] = None
        self.__rules: Dict[Variable, Callable] = {
            Variable.PROGRAMA: self._resolve_program,
            Variable.CONVERGENCIA: self._resolve_convergence,
            Variable.TEMPO_EXECUCAO: self._resolve_tempo,
            Variable.CUSTOS: self._resolve_costs,
            Variable.RECURSOS_JOB: self._resolve_job_resources,
            Variable.RECURSOS_CLUSTER: self._resolve_cluster_resources,
            Variable.INVIABILIDADES: self._resolve_inviabilidades_completas,
        }

    @property
    def uow(self) -> AbstractUnitOfWork:
        if self.__uow is None:
            raise RuntimeError()
        return self.__uow

    def _default_args(self) -> List[str]:
        return self.__class__.DEFAULT_EXECUTION_SYNTHESIS_ARGS

    def _process_variable_arguments(
        self,
        args: List[str],
    ) -> List[ExecutionSynthesis]:
        args_data = [ExecutionSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        logger = Log.log()
        for i, a in enumerate(args_data):
            if a is None:
                if logger is not None:
                    logger.info(f"Erro no argumento fornecido: {args[i]}")
                return []
        return valid_args

    def filter_valid_variables(
        self, variables: List[ExecutionSynthesis]
    ) -> List[ExecutionSynthesis]:
        with self.uow:
            existe_inviabunic = self.uow.files.get_inviabunic() is not None
        invs_vars = [
            Variable.INVIABILIDADES_CODIGO,
            Variable.INVIABILIDADES_PATAMAR,
            Variable.INVIABILIDADES_PATAMAR_LIMITE,
            Variable.INVIABILIDADES_LIMITE,
            Variable.INVIABILIDADES_SBM_PATAMAR,
        ]
        if not existe_inviabunic:
            variables = [v for v in variables if v.variable not in invs_vars]
        logger = Log.log()
        if logger is not None:
            logger.info(f"Variáveis: {variables}")
        return variables

    def _resolve_program(self) -> pd.DataFrame:
        return pd.DataFrame(data={"programa": ["DECOMP"]})

    def _resolve_convergence(self) -> pd.DataFrame:
        with self.uow:
            relato = self.uow.files.get_relato()
        df = relato.convergencia
        logger = Log.log()
        if df is None:
            if logger is not None:
                logger.error("Bloco de convergência do relato não encontrado")
            raise RuntimeError()
        df_processed = df.rename(
            columns={
                "iteracao": "iter",
                "zinf": "zinf",
                "zsup": "zsup",
                "gap_percentual": "gap",
                "tempo": "tempo",
                "numero_inviabilidades ": "inviabilidades",
                "deficit_demanda_MWmed": "deficit",
                "inviabilidades_MWmed": "viol_MWmed",
                "inviabilidades_m3s": "viol_m3s",
                "inviabilidades_hm3": "viol_hm3",
            }
        )
        df_processed.drop(
            columns=["deficit_nivel_seguranca_MWmes"], inplace=True
        )
        df_processed.loc[1:, "tempo"] = (
            df_processed["tempo"].to_numpy()[1:]
            - df_processed["tempo"].to_numpy()[:-1]
        )
        df_processed["dZinf"] = df_processed["zinf"]
        df_processed.loc[1:, "dZinf"] = (
            df_processed["zinf"].to_numpy()[1:]
            - df_processed["zinf"].to_numpy()[:-1]
        )
        df_processed.loc[1:, "dZinf"] /= df_processed["zinf"].to_numpy()[:-1]
        df_processed.at[0, "dZinf"] = np.nan

        conv = self.uow.export.read_df(self.CONVERGENCE_FILE)
        if conv is None:
            df_processed["execucao"] = 0
            return df_processed
        else:
            df_processed["execucao"] = conv["execucao"].max() + 1
            return pd.concat([conv, df_processed], ignore_index=True)

    def _resolve_tempo(self) -> pd.DataFrame:
        with self.uow:
            decomptim = self.uow.files.get_decomptim()
        df = decomptim.tempos_etapas
        logger = Log.log()
        if df is None:
            if logger is not None:
                logger.error("Dados de tempo do decomp.tim não encontrados")
            raise RuntimeError()
        df = df.rename(columns={"Etapa": "etapa", "Tempo": "tempo"})

        df["tempo"] = df["tempo"].dt.total_seconds()

        tempo = self.uow.export.read_df(self.RUNTIME_FILE)
        if tempo is None:
            df["execucao"] = 0
            return df
        else:
            df["execucao"] = tempo["execucao"].max() + 1
            return pd.concat([tempo, df], ignore_index=True)

    def _resolve_costs(self) -> pd.DataFrame:
        with self.uow:
            relato = self.uow.files.get_relato()
        df = relato.relatorio_operacao_custos
        logger = Log.log()
        if df is None:
            if logger is not None:
                logger.error(
                    "Bloco de custos da operação do relato não encontrado"
                )
            raise RuntimeError()
        estagios = df["estagio"].unique()
        dfs: List[pd.DataFrame] = []
        for e in estagios:
            parcelas = [
                "GERACAO TERMICA",
                "DESVIO",
                "TURB RESERV",
                "TURB FIO",
                "VERTIMENTO RESERV",
                "VERTIMENTO FIO",
                "INTERCAMBIO",
            ]
            means = (
                df.loc[
                    df["estagio"] == e,
                    [
                        "geracao_termica",
                        "violacao_desvio",
                        "violacao_turbinamento_reservatorio",
                        "violacao_turbinamento_fio",
                        "penalidade_vertimento_reservatorio",
                        "penalidade_vertimento_fio",
                        "penalidade_intercambio",
                    ],
                ]
                .to_numpy()
                .flatten()
            )
            dfe = pd.DataFrame(
                data={
                    "parcela": parcelas,
                    "mean": means,
                    "std": [0.0] * len(means),
                }
            )
            dfe["estagio"] = e
            dfs.append(dfe)
        df_completo = pd.concat(dfs, ignore_index=True)
        df_completo = df_completo.astype(
            {"mean": np.float64, "std": np.float64}
        )
        df_completo = df_completo.groupby("parcela").sum()
        df_completo = df_completo.reset_index()
        return df_completo[["parcela", "mean", "std"]]

    def _resolve_job_resources(self) -> pd.DataFrame:
        # REGRA DE NEGOCIO: arquivos do hpc-job-monitor
        # monitor-job.parquet.gzip
        with self.uow:
            file = "monitor-job.parquet.gzip"
            if pathlib.Path(file).exists():
                try:
                    df = pd.read_parquet("monitor-job.parquet.gzip")
                except Exception as e:
                    logger = Log.log()
                    if logger is not None:
                        logger.info(
                            f"Erro ao acessar arquivo monitor-job.parquet.gzip: {str(e)}"
                        )
                    return None
                return df
            return None

    def _resolve_cluster_resources(self) -> pd.DataFrame:
        # Le o do job para saber tempo inicial e final
        df_job = None
        with self.uow:
            file = "monitor-job.parquet.gzip"
            if pathlib.Path(file).exists():
                try:
                    df_job = pd.read_parquet("monitor-job.parquet.gzip")
                except Exception as e:
                    logger = Log.log()
                    if logger is not None:
                        logger.info(
                            f"Erro ao acessar arquivo monitor-job.parquet.gzip: {str(e)}"
                        )
                    return None
        if df_job is None:
            return None
        jobTimeInstants = pd.to_datetime(
            df_job["timeInstant"], format="ISO8601"
        ).tolist()
        # REGRA DE NEGOCIO: arquivos do hpc-job-monitor
        # monitor-(hostname).parquet.gzip
        with set_directory(str(pathlib.Path.home())):
            file = f"monitor-{socket.gethostname()}.parquet.gzip"
            if pathlib.Path(file).exists():
                try:
                    df = pd.read_parquet(file)
                except Exception as e:
                    logger = Log.log()
                    if logger is not None:
                        logger.info(
                            f"Erro ao acessar arquivo {file}: {str(e)}"
                        )
                    return None
                df["timeInstant"] = pd.to_datetime(
                    df["timeInstant"], format="ISO8601"
                )
                return df.loc[
                    (df["timeInstant"] >= jobTimeInstants[0])
                    & (df["timeInstant"] <= jobTimeInstants[-1])
                ]
        return None

    def __resolve_inviabilidades(self) -> List[Inviabilidade]:
        with self.uow:
            logger = Log.log()
            if logger is not None:
                logger.info("Obtendo Inviabilidades")
            inviabunic = self.uow.files.get_inviabunic()
            hidr = self.uow.files.get_hidr()
            relato = self.uow.files.get_relato()
        df_iter = inviabunic.inviabilidades_iteracoes
        df_sf = inviabunic.inviabilidades_simulacao_final
        if df_iter is None or df_sf is None:
            if logger is not None:
                logger.warning("Não foram encontradas inviabilidades")
            return []
        df_sf["iteracao"] = -1
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

    def _resolve_inviabilidades_completas(self) -> pd.DataFrame:
        df = pd.concat(
            [
                self._resolve_inviabilidades_codigo(),
                self._resolve_inviabilidades_patamar(),
                self._resolve_inviabilidades_patamar_limite(),
                self._resolve_inviabilidades_limite(),
                self._resolve_inviabilidades_submercado_patamar(),
            ],
            ignore_index=True,
        )
        df = df.astype({"iteracao": int, "cenario": int, "estagio": int})
        inviabs = self.uow.export.read_df(self.INVIABS_FILE)
        if inviabs is None:
            df["execucao"] = 0
            return df
        else:
            df["execucao"] = inviabs["execucao"].max() + 1
            return pd.concat([inviabs, df], ignore_index=True)

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
                "tipo": tipos,
                "iteracao": iteracoes,
                "cenario": cenarios,
                "estagio": estagios,
                "codigo": codigos,
                "violacao": violacoes,
                "unidade": unidades,
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
                "tipo": tipos,
                "iteracao": iteracoes,
                "cenario": cenarios,
                "estagio": estagios,
                "codigo": codigos,
                "violacao": violacoes,
                "unidade": unidades,
                "patamar": patamares,
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
                "tipo": tipos,
                "iteracao": iteracoes,
                "cenario": cenarios,
                "estagio": estagios,
                "codigo": codigos,
                "violacao": violacoes,
                "unidade": unidades,
                "patamar": patamares,
                "limite": limites,
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
                "tipo": tipos,
                "iteracao": iteracoes,
                "cenario": cenarios,
                "estagio": estagios,
                "codigo": codigos,
                "violacao": violacoes,
                "unidade": unidades,
                "limite": limites,
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
        violacoes: List[float] = []
        unidades: List[str] = []
        submercados: List[str] = []
        patamares: List[int] = []
        for i in inviabs_codigo:
            tipos.append(i.NOME)
            iteracoes.append(i._iteracao)
            cenarios.append(i._cenario)
            estagios.append(i._estagio)
            violacoes.append(i._violacao_percentual)
            unidades.append(i._unidade)
            submercados.append(i._submercado)
            patamares.append(i._patamar)

        return pd.DataFrame(
            data={
                "tipo": tipos,
                "iteracao": iteracoes,
                "cenario": cenarios,
                "estagio": estagios,
                "violacao": violacoes,
                "unidade": unidades,
                "submercado": submercados,
                "patamar": patamares,
            }
        )

    def synthetize(self, variables: List[str], uow: AbstractUnitOfWork):
        self.__uow = uow
        logger = Log.log()
        if len(variables) == 0:
            variables = self._default_args()
        synthesis_variables = self._process_variable_arguments(variables)
        valid_synthesis = self.filter_valid_variables(synthesis_variables)
        for s in valid_synthesis:
            filename = str(s)
            if logger is not None:
                logger.info(f"Realizando síntese de {filename}")
            try:
                df = self.__rules[s.variable]()
            except Exception:
                print_exc()
                continue
            if df is not None:
                with self.uow:
                    self.uow.export.synthetize_df(df, filename)
