from typing import Callable, Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.temporalresolution import TemporalResolution
from sintetizador.model.operation.operationsynthesis import OperationSynthesis


class OperationSynthetizer:

    DEFAULT_OPERATION_SYNTHESIS_ARGS: List[str] = [
        "CMO_SBM_EST",
        "CTER_SIN_EST",
        "CTER_SBM_EST",
        "CTER_SBM_PAT",
        "COP_SIN_EST",
        "CFU_SIN_EST",
        "ENA_SBM_EST",
        "ENA_SIN_EST",
    ]

    def __init__(self, uow: AbstractUnitOfWork) -> None:
        self.__uow = uow
        self.__subsystems: Optional[List[str]] = None
        self.__patamares: Optional[List[str]] = None
        self.__stages_start_dates: Optional[List[datetime]] = None
        self.__stages_end_dates: Optional[List[datetime]] = None
        self.__rules: Dict[
            Tuple[Variable, SpatialResolution, TemporalResolution],
            pd.DataFrame,
        ] = {
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): self.__processa_bloco_relatorio_operacao_cmo(),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): self.__processa_bloco_relatorio_operacao("Geração Térmica"),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): self.__processa_bloco_relatorio_balanco_gt_estagio(),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): self.__processa_bloco_relatorio_balanco_gt_patamar(),
            (
                Variable.CUSTO_OPERACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): self.__processa_bloco_relatorio_operacao(
                "Custo Total no Estágio"
            ),
            (
                Variable.CUSTO_FUTURO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): self.__processa_bloco_relatorio_operacao("Custo Futuro"),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): self.__processa_bloco_relatorio_balanco_energetico(
                "ENA Absoluta"
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): self.__processa_bloco_relatorio_balanco_energetico_sin(
                "ENA Absoluta"
            ),
        }

    # def __processa_bloco_cmo(
    #     self, submercado: str, patamares: List[str]
    # ) -> pd.DataFrame:
    #     with self.__uow:
    #         r = self.__uow.files.get_relato()
    #         df = r.cmo_medio_subsistema
    #     stage_cols = [c for c in df.columns if "Estágio" in c]
    #     df_completo = pd.DataFrame()
    #     for p in patamares:
    #         cmos = (
    #             df.loc[
    #                 (df["Subsistema"] == submercado) & (df["Patamar"] == p),
    #                 stage_cols,
    #             ]
    #             .to_numpy()
    #             .flatten()
    #         )
    #         df_cmo = pd.DataFrame(data={"1": cmos})
    #         df_cmo["Data Inicio"] = self.stages_start_date
    #         df_cmo["Data Fim"] = self.stages_end_date
    #         df_cmo["Submercado"] = submercado
    #         df_completo = pd.concat(
    #             [
    #                 df_completo,
    #                 df_cmo[["Data Inicio", "Data Fim", "Submercado", "1"]],
    #             ],
    #             ignore_index=True,
    #         )
    #     return df_completo

    def __processa_bloco_relatorio_operacao_ute(
        self, col: str
    ) -> pd.DataFrame:
        with self.__uow:
            r1 = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        df1 = r1.relatorio_operacao_ute
        df2 = r2.relatorio_operacao_ute
        usinas_relatorio = df1["Usina"].unique()
        df_final = pd.DataFrame()
        for u in usinas_relatorio:
            df1_u = df1.loc[df1["Usina"] == u, :]
            df2_u = df2.loc[df2["Usina"] == u, :]
            df_u = self.__process_df_relato1_relato2(df1_u, df2_u, col)
            cols_df_u = df_u.columns.to_list()
            df_u["Usina"] = u
            df_final = pd.concat([df_final, df_u], ignore_index=True)
        df_final = df_final[["Usina"] + cols_df_u]
        return df_final

    def __processa_bloco_relatorio_operacao_uhe(
        self, col: str
    ) -> pd.DataFrame:
        with self.__uow:
            r1 = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        df1 = r1.relatorio_operacao_uhe
        df2 = r2.relatorio_operacao_uhe
        df1 = df1.loc[~pd.isna(df1["FPCGC"]), :]
        df2 = df2.loc[~pd.isna(df2["FPCGC"]), :]
        usinas_relatorio = df1["Usina"].unique()
        df_final = pd.DataFrame()
        for u in usinas_relatorio:
            df1_u = df1.loc[df1["Usina"] == u, :]
            df2_u = df2.loc[df2["Usina"] == u, :]
            df_u = self.__process_df_relato1_relato2(df1_u, df2_u, col)
            cols_df_u = df_u.columns.to_list()
            df_u["Usina"] = u
            df_final = pd.concat([df_final, df_u], ignore_index=True)
        df_final = df_final[["Usina"] + cols_df_u]
        return df_final

    def __processa_bloco_relatorio_balanco_energetico(
        self, col: str, pat: str = "Medio"
    ) -> pd.DataFrame:
        with self.__uow:
            r1 = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        df1 = r1.balanco_energetico
        df2 = r2.balanco_energetico
        subsis_balanco = df1["Subsistema"].unique()
        df_final = pd.DataFrame()
        for s in subsis_balanco:
            df1_s = df1.loc[
                (df1["Subsistema"] == s) & (df1["Patamar"] == pat), :
            ]
            df2_s = df2.loc[
                (df2["Subsistema"] == s) & (df2["Patamar"] == pat), :
            ]
            df_s = self.__process_df_relato1_relato2(df1_s, df2_s, col)
            cols_df_s = df_s.columns.to_list()
            df_s["Submercado"] = s
            df_final = pd.concat([df_final, df_s], ignore_index=True)
        df_final = df_final[["Submercado"] + cols_df_s]
        return df_final

    def __processa_bloco_relatorio_balanco_energetico_sin(
        self, col: str, pat: str = "Medio"
    ) -> pd.DataFrame:
        with self.__uow:
            r1 = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        df1 = r1.balanco_energetico
        df2 = r2.balanco_energetico
        subsis_balanco = df1["Subsistema"].unique()
        df_final = pd.DataFrame()
        for s in subsis_balanco:
            df1_s = df1.loc[
                (df1["Subsistema"] == s) & (df1["Patamar"] == pat), :
            ]
            df2_s = df2.loc[
                (df2["Subsistema"] == s) & (df2["Patamar"] == pat), :
            ]
            df_s = self.__process_df_relato1_relato2(df1_s, df2_s, col)
            if df_final.empty:
                df_final = df_s
            else:
                cols_cenarios = [
                    c
                    for c in df_s.columns
                    if c not in ["Estagio", "Data Inicio", "Data Fim"]
                ]
                df_final.loc[:, cols_cenarios] += df_s.loc[:, cols_cenarios]
        return df_final

    def __processa_bloco_relatorio_operacao(self, col: str) -> pd.DataFrame:
        with self.__uow:
            r1 = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        df1 = r1.relatorio_operacao_custos
        df2 = r2.relatorio_operacao_custos
        df_s = self.__process_df_relato1_relato2(df1, df2, col)
        return df_s

    def __processa_bloco_relatorio_operacao_cmo(self) -> pd.DataFrame:
        with self.__uow:
            r1 = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        df1 = r1.relatorio_operacao_custos
        df2 = r2.relatorio_operacao_custos
        df_final = pd.DataFrame()
        for s in self.subsystems:
            col = f"CMO {s}"
            df_s = self.__process_df_relato1_relato2(df1, df2, col)
            cols_df_s = df_s.columns.to_list()
            df_s["Submercado"] = s
            df_final = pd.concat([df_final, df_s], ignore_index=True)
        df_final = df_final[["Submercado"] + cols_df_s]
        return df_final

    def __processa_bloco_relatorio_balanco_gt_estagio(
        self, pat: str = "Medio"
    ) -> pd.DataFrame:
        df_gt = self.__processa_bloco_relatorio_balanco_energetico("Gter", pat)
        df_gtat = self.__processa_bloco_relatorio_balanco_energetico(
            "GterAT", pat
        )
        cols_cenarios = [
            c
            for c in df_gt.columns
            if c not in ["Submercado", "Estagio", "Data Inicio", "Data Fim"]
        ]
        df_gt.loc[:, cols_cenarios] += df_gtat.loc[:, cols_cenarios]
        return df_gt

    def __processa_bloco_relatorio_balanco_gt_patamar(self) -> pd.DataFrame:
        df_final = pd.DataFrame()
        for pat in self.patamares:
            df_pat = self.__processa_bloco_relatorio_balanco_gt_estagio(pat)
            cols_df_pat = df_pat.columns.to_list()
            df_pat["Patamar"] = pat
            df_final = pd.concat([df_final, df_pat], ignore_index=True)
        df_final = df_final[["Patamar"] + cols_df_pat]
        return df_final

    def __process_df_relato1_relato2(
        self, df1: pd.DataFrame, df2: pd.DataFrame, col: str
    ) -> pd.DataFrame:
        estagios_r1 = df1["Estágio"].unique().tolist()
        estagios_r2 = df2["Estágio"].unique().tolist()
        estagios = list(set(estagios_r1 + estagios_r2))
        start_dates = [self.stages_start_date[i - 1] for i in estagios]
        end_dates = [self.stages_end_date[i - 1] for i in estagios]
        scenarios = df2["Cenário"].unique()
        cols_scenarios = [str(c) for c in scenarios]
        empty_table = np.zeros((len(start_dates), len(scenarios)))
        df_processed = pd.DataFrame(empty_table, columns=cols_scenarios)
        df_processed["Estagio"] = estagios
        df_processed["Data Inicio"] = start_dates
        df_processed["Data Fim"] = end_dates
        for e in estagios_r1:
            df_processed.loc[
                df_processed["Estagio"] == e,
                cols_scenarios,
            ] = float(df1.loc[df1["Estágio"] == e, col])
        for e in estagios_r2:
            df_processed.loc[
                df_processed["Estagio"] == e,
                cols_scenarios,
            ] = df2.loc[df2["Estágio"] == e, col].to_numpy()
        return df_processed

    def _default_args(self) -> List[OperationSynthesis]:
        return [
            OperationSynthesis.factory(a)
            for a in self.__class__.DEFAULT_OPERATION_SYNTHESIS_ARGS
        ]

    def _process_variable_arguments(
        self,
        args: List[str],
    ) -> List[OperationSynthesis]:
        args_data = [OperationSynthesis.factory(c) for c in args]
        for i, a in enumerate(args_data):
            if a is None:
                Log.log(f"Erro no argumento fornecido: {args[i]}")
                return []
        return args_data

    def filter_valid_variables(
        self, variables: List[OperationSynthesis]
    ) -> List[OperationSynthesis]:
        Log.log().info(f"Variáveis: {variables}")
        return variables

    def __resolve_subsystems(self) -> List[str]:
        with self.__uow:
            Log.log().info(f"Obtendo subsistemas")
            dadger = self.__uow.files.get_dadger()
            sbs = dadger.sb()
        if sbs is None:
            return []
        if isinstance(sbs, list):
            return [s.nome for s in sbs]
        else:
            return [sbs.nome]

    @property
    def subsystems(self) -> List[str]:
        if self.__subsystems is None:
            self.__subsystems = self.__resolve_subsystems()
        return self.__subsystems

    def __resolve_patamares(self) -> List[str]:
        sample_sb = self.subsystems[0]
        with self.__uow:
            Log.log().info(f"Obtendo patamares")
            dadger = self.__uow.files.get_dadger()
            sb_code = dadger.sb(nome=sample_sb).codigo
            dps = dadger.dp(subsistema=sb_code, estagio=1)
        if dps is None:
            return []
        if isinstance(dps, list):
            return [str(p) for p in range(1, dps[0].num_patamares + 1)]
        else:
            return [str(p) for p in range(1, dps.num_patamares + 1)]

    @property
    def patamares(self) -> List[str]:
        if self.__patamares is None:
            self.__patamares = self.__resolve_patamares()
        return self.__patamares

    def __resolve_stages_start_date(self) -> List[datetime]:
        sample_sb = self.subsystems[0]
        with self.__uow:
            Log.log().info(f"Obtendo início dos estágios")
            dadger = self.__uow.files.get_dadger()
            sb_code = dadger.sb(nome=sample_sb).codigo
            dt = dadger.dt
            dps = dadger.dp(subsistema=sb_code)
        if dt is None:
            return []
        if dps is None:
            hours_stage = []
        elif isinstance(dps, list):
            hours_stage = [sum(dp.duracoes) for dp in dps]
        else:
            hours_stage = [sum(dps.duracoes)]
        first_stage = datetime(year=dt.ano, month=dt.mes, day=dt.dia)

        return [
            first_stage + timedelta(hours=sum(hours_stage[:i]))
            for i in range(len(hours_stage))
        ]

    @property
    def stages_start_date(self) -> List[datetime]:
        if self.__stages_start_dates is None:
            self.__stages_start_dates = self.__resolve_stages_start_date()
        return self.__stages_start_dates

    def __resolve_stages_end_date(self) -> List[datetime]:
        sample_sb = self.subsystems[0]
        with self.__uow:
            Log.log().info(f"Obtendo fim dos estágios")
            dadger = self.__uow.files.get_dadger()
            sb_code = dadger.sb(nome=sample_sb).codigo
            dt = dadger.dt
            dps = dadger.dp(subsistema=sb_code)
        if dt is None:
            return []
        if dps is None:
            hours_stage = []
        elif isinstance(dps, list):
            hours_stage = [sum(dp.duracoes) for dp in dps]
        else:
            hours_stage = [sum(dps.duracoes)]
        first_stage = datetime(year=dt.ano, month=dt.mes, day=dt.dia)

        return [
            first_stage + timedelta(hours=sum(hours_stage[:i]))
            for i in range(1, len(hours_stage) + 1)
        ]

    @property
    def stages_end_date(self) -> List[datetime]:
        if self.__stages_end_dates is None:
            self.__stages_end_dates = self.__resolve_stages_end_date()
        return self.__stages_end_dates

    def synthetize(self, variables: List[str]):
        if len(variables) == 0:
            variables = self._default_args()
        else:
            variables = self._process_variable_arguments(variables)
        valid_synthesis = self.filter_valid_variables(variables)
        Log.log().info(f"Variáveis: {valid_synthesis}")
        for s in valid_synthesis:
            filename = str(s)
            Log.log().info(f"Realizando síntese de {filename}")
            df = self.__rules[
                (s.variable, s.spatial_resolution, s.temporal_resolution)
            ]
            with self.__uow:
                self.__uow.export.synthetize_df(df, filename)
