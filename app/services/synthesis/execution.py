from typing import Callable, Dict, List, Any
from traceback import print_exc
import pandas as pd  # type: ignore
import numpy as np
import socket
import pathlib

from app.model.execution.inviabilidade import (
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
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.log import Log
from app.utils.fs import set_directory
from app.model.execution.variable import Variable
from app.model.execution.executionsynthesis import ExecutionSynthesis


class ExecutionSynthetizer:
    # TODO - levar lista de argumentos suportados para o
    # arquivo da ExecutionSynthesis
    DEFAULT_EXECUTION_SYNTHESIS_ARGS: List[str] = [
        "PROGRAMA",
        "CONVERGENCIA",
        "TEMPO",
        "INVIABILIDADES",
        "CUSTOS",
        "RECURSOS_JOB",
        "RECURSOS_CLUSTER",
    ]

    # TODO - rever a forma como são processadas as inviabilidades
    # Criar uma classe única para processar as inviabilidades
    # com todas as possíveis inviabilidades e um Enum para diferenciar
    # TODO - não precisa de sínteses de inviabilidades separadas por grupo
    INVIABS_CODIGO = [InviabilidadeTI, InviabilidadeEV]
    INVIABS_PATAMAR = [InviabilidadeDEFMIN, InviabilidadeFP]
    INVIABS_PATAMAR_LIMITE = [InviabilidadeRHQ, InviabilidadeRE]
    INVIABS_LIMITE = [InviabilidadeRHV, InviabilidadeRHE, InviabilidadeRHA]
    INVIABS_SBM_PATAMAR = [
        InviabilidadeDeficit,
    ]

    # TODO - substituir por usar diretamente o valor da variável de síntese
    # (s.variable.value ou str(s))
    CONVERGENCE_FILE = "CONVERGENCIA"
    RUNTIME_FILE = "TEMPO"
    INVIABS_FILE = "INVIABILIDADES"

    @classmethod
    def _get_rule(cls, s: ExecutionSynthesis) -> Callable:
        rules: Dict[Variable, Callable] = {
            Variable.PROGRAMA: cls._resolve_program,
            Variable.CONVERGENCIA: cls._resolve_convergence,
            Variable.TEMPO_EXECUCAO: cls._resolve_tempo,
            Variable.CUSTOS: cls._resolve_costs,
            Variable.RECURSOS_JOB: cls._resolve_job_resources,
            Variable.RECURSOS_CLUSTER: cls._resolve_cluster_resources,
            Variable.INVIABILIDADES: cls._resolve_inviabilidades_completas,
        }
        return rules[s]

    # TODO - padronizar o tipo de retorno de _default_args com
    # _process_variable_arguments
    @classmethod
    def _default_args(cls) -> List[str]:
        return cls.DEFAULT_EXECUTION_SYNTHESIS_ARGS

    # TODO - criar o método interno _log

    # TODO - padronizar a forma de logging com o uso do método interno _log
    # Manter o processamento para sinalizar erros.
    @classmethod
    def _process_variable_arguments(
        cls,
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

    # TODO - renomear para _filter_valid_variables
    # Atualizar lógica considerando apenas uma síntese de inviabilidade
    # TODO - padronizar a forma de logging com o uso do método interno _log
    # TODO - alterar idioma para inglês
    @classmethod
    def filter_valid_variables(
        cls, variables: List[ExecutionSynthesis], uow: AbstractUnitOfWork
    ) -> List[ExecutionSynthesis]:
        existe_inviabunic = Deck._get_inviabunic(uow) is not None
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

    # TODO criar método _preprocess_synthesis_variables para juntar
    # todos os pré-processamentos

    # TODO - criar método _resolve para resolver cada variável de síntese

    @classmethod
    def _resolve_program(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return pd.DataFrame(data={"programa": ["DECOMP"]})

    # TODO - atualizar idioma para inglês
    # TODO - padronizar a forma de logging com o uso do método interno _log
    # TODO - rever se os nomes escolhidos são os mais adequados
    # (padronizar com o newave, deixar de abreviar)
    # TODO - Modularizar pós-processamentos dos dados para padronizar com o newave
    # TODO - Modularizar a parte de adicionar "execucao" no dataframe
    # TODO - avaliar obter o dataframe já pós-processado direto do Deck
    @classmethod
    def _resolve_convergence(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.convergencia(uow)
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

        conv = uow.export.read_df(cls.CONVERGENCE_FILE)
        if conv is None:
            df_processed["execucao"] = 0
            return df_processed
        else:
            df_processed["execucao"] = conv["execucao"].max() + 1
            return pd.concat([conv, df_processed], ignore_index=True)

    # TODO - atualizar idioma para inglês (_resolve_runtime)
    # TODO - padronizar a forma de logging com o uso do método interno _log
    # TODO - rever se os nomes escolhidos são os mais adequados
    # (padronizar com o newave, deixar de abreviar)
    # TODO - Modularizar a parte de adicionar "execucao" no dataframe
    # TODO - avaliar obter o dataframe já pós-processado direto do Deck
    @classmethod
    def _resolve_tempo(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.tempos_por_etapa(uow)
        logger = Log.log()
        if df is None:
            if logger is not None:
                logger.error("Dados de tempo do decomp.tim não encontrados")
            raise RuntimeError()
        df = df.rename(columns={"Etapa": "etapa", "Tempo": "tempo"})

        df["tempo"] = df["tempo"].dt.total_seconds()

        tempo = uow.export.read_df(cls.RUNTIME_FILE)
        if tempo is None:
            df["execucao"] = 0
            return df
        else:
            df["execucao"] = tempo["execucao"].max() + 1
            return pd.concat([tempo, df], ignore_index=True)

    # TODO - atualizar idioma para inglês (_resolve_cost)
    # TODO - padronizar a forma de logging com o uso do método interno _log
    # TODO - rever se os nomes escolhidos são os mais adequados
    # (padronizar com o newave, deixar de abreviar)
    # TODO - avaliar obter o dataframe já pós-processado direto do Deck
    @classmethod
    def _resolve_costs(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        relato = Deck.relato(uow)
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

    # TODO - descontinuar sínteses de recursos
    @classmethod
    def _resolve_job_resources(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        # REGRA DE NEGOCIO: arquivos do hpc-job-monitor
        # monitor-job.parquet.gzip
        with uow:
            file = "monitor-job.parquet.gzip"
            logger = Log.log()
            if pathlib.Path(file).exists():
                try:
                    df = pd.read_parquet(file)
                except Exception as e:
                    if logger is not None:
                        logger.info(
                            f"Erro ao acessar arquivo {file}: {str(e)}"
                        )
                    return None
                return df
            else:
                if logger is not None:
                    logger.warning(f"Arquivo {file} não encontrado")

            return None

    # TODO - descontinuar sínteses de recursos
    @classmethod
    def _resolve_cluster_resources(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        # Le o do job para saber tempo inicial e final
        df_job = cls._resolve_job_resources(uow)
        if df_job is None:
            return None
        jobTimeInstants = pd.to_datetime(
            df_job["timeInstant"], format="ISO8601"
        ).tolist()
        # REGRA DE NEGOCIO: arquivos do hpc-job-monitor
        # monitor-(hostname).parquet.gzip
        with set_directory(str(pathlib.Path.home())):
            file = f"monitor-{socket.gethostname()}.parquet.gzip"
            logger = Log.log()
            if pathlib.Path(file).exists():
                try:
                    df = pd.read_parquet(file)
                except Exception as e:
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
            else:
                if logger is not None:
                    logger.warning(f"Arquivo {file} não encontrado")
        return None

    # TODO - avaliar obter como um dataframe direto do Deck
    @classmethod
    def inviabilidades(cls, uow: AbstractUnitOfWork) -> List[Inviabilidade]:
        logger = Log.log()
        if logger is not None:
            logger.info("Obtendo Inviabilidades")
        inviabilidades = Deck.inviabilidades(uow)
        if inviabilidades is None:
            if logger is not None:
                logger.warning("Não foram encontradas inviabilidades")
            return []
        return inviabilidades

    # TODO - atualizar idioma para inglês (_resolve_infeasibilities)
    # TODO - padronizar a forma de logging com o uso do método interno _log
    # TODO - avaliar obter o dataframe já pós-processado direto do Deck
    # TODO - Modularizar a parte de adicionar "execucao" no dataframe
    @classmethod
    def _resolve_inviabilidades_completas(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        df = pd.concat(
            [
                cls._resolve_inviabilidades_codigo(uow),
                cls._resolve_inviabilidades_patamar(uow),
                cls._resolve_inviabilidades_patamar_limite(uow),
                cls._resolve_inviabilidades_limite(uow),
                cls._resolve_inviabilidades_submercado_patamar(uow),
            ],
            ignore_index=True,
        )
        df = df.astype({"iteracao": int, "cenario": int, "estagio": int})
        with uow:
            inviabs = uow.export.read_df(cls.INVIABS_FILE)
        if inviabs is None:
            df["execucao"] = 0
            return df
        else:
            df["execucao"] = inviabs["execucao"].max() + 1
            return pd.concat([inviabs, df], ignore_index=True)

    # TODO - descontinuar pois só vai existir uma Inviabilidade
    @classmethod
    def _resolve_inviabilidades_codigo(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        inviabs_codigo = [
            i for i in cls.inviabilidades(uow) if type(i) in cls.INVIABS_CODIGO
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

    # TODO - descontinuar pois só vai existir uma Inviabilidade
    @classmethod
    def _resolve_inviabilidades_patamar(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        inviabs_codigo = [
            i
            for i in cls.inviabilidades(uow)
            if type(i) in cls.INVIABS_PATAMAR
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

    # TODO - descontinuar pois só vai existir uma Inviabilidade
    @classmethod
    def _resolve_inviabilidades_patamar_limite(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        inviabs_codigo = [
            i
            for i in cls.inviabilidades(uow)
            if type(i) in cls.INVIABS_PATAMAR_LIMITE
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

    # TODO - descontinuar pois só vai existir uma Inviabilidade
    @classmethod
    def _resolve_inviabilidades_limite(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        inviabs_codigo = [
            i for i in cls.inviabilidades(uow) if type(i) in cls.INVIABS_LIMITE
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

    # TODO - descontinuar pois só vai existir uma Inviabilidade
    @classmethod
    def _resolve_inviabilidades_submercado_patamar(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        inviabs_codigo = [
            i
            for i in cls.inviabilidades(uow)
            if type(i) in cls.INVIABS_SBM_PATAMAR
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

    # TODO - criar _export_metadata para exportar metadados
    # TODO - modularizar a parte da síntese de uma variável
    # em uma função à parte (_synthetize_single_variable)

    # TODO - atualizar forma de logging
    # TODO - exportar metadados ao final
    # TODO - padronizar atribuições e chamadas com o do newave
    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        logger = Log.log()
        if len(variables) == 0:
            variables = cls._default_args()
        synthesis_variables = cls._process_variable_arguments(variables)
        valid_synthesis = cls.filter_valid_variables(synthesis_variables, uow)
        for s in valid_synthesis:
            filename = str(s)
            if logger is not None:
                logger.info(f"Realizando síntese de {filename}")
            try:
                df = cls._get_rule(s.variable)(uow)
            except Exception:
                print_exc()
                continue
            if df is not None:
                with uow:
                    uow.export.synthetize_df(df, filename)
