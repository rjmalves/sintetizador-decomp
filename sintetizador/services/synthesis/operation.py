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

    PROBABILITIES_FILE = "PROBABILIDADES"

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

    DEFAULT_OPERATION_SYNTHESIS_ARGS: List[str] = [
        "CMO_SBM_EST",
        "CTER_SIN_EST",
        "COP_SIN_EST",
        "CFU_SIN_EST",
        "EARMI_SBM_EST",
        "EARMI_SIN_EST",
        "EARPI_SBM_EST",
        "EARPI_SIN_EST",
        "EARMF_SBM_EST",
        "EARMF_SIN_EST",
        "EARPF_SBM_EST",
        "EARPF_SIN_EST",
        "GTER_SBM_EST",
        "GTER_SBM_PAT",
        "GTER_SIN_EST",
        "GTER_SIN_PAT",
        "GHID_UHE_EST",
        "GHID_UHE_PAT",
        "GHID_SBM_EST",
        "GHID_SBM_PAT",
        "GHID_SIN_EST",
        "GHID_SIN_PAT",
        "ENAA_SBM_EST",
        "ENAA_SIN_EST",
        "ENAM_SBM_EST",
        "MER_SBM_EST",
        "MER_SBM_PAT",
        "MER_SIN_EST",
        "MER_SIN_PAT",
        "DEF_SBM_EST",
        "DEF_SBM_PAT",
        "DEF_SIN_EST",
        "DEF_SIN_PAT",
        "VARPI_UHE_EST",
        "VARPF_UHE_EST",
        "VARMI_UHE_EST",
        "VARMF_UHE_EST",
        "VARMI_REE_EST",
        "VARMF_REE_EST",
        "VARMI_SBM_EST",
        "VARMF_SBM_EST",
        "VARMI_SIN_EST",
        "VARMF_SIN_EST",
        "QAFL_UHE_EST",
        "QDEF_UHE_EST",
        "QTUR_UHE_EST",
        "QVER_UHE_EST",
        "EVERT_UHE_EST",
        "EVERNT_UHE_EST",
        "EVERT_REE_EST",
        "EVERNT_REE_EST",
        "EVERT_SBM_EST",
        "EVERNT_SBM_EST",
        "EVERT_SIN_EST",
        "EVERNT_SIN_EST",
        "GTER_UTE_EST",
        "GTER_UTE_PAT",
        "CTER_UTE_EST",
    ]

    def __init__(self, uow: AbstractUnitOfWork) -> None:
        self.__uow = uow
        self.__subsystems: Optional[List[str]] = None
        self.__patamares: Optional[List[str]] = None
        self.__horas_patamares: Optional[
            Dict[str, Dict[int, List[float]]]
        ] = None
        self.__stages_start_dates: Optional[List[datetime]] = None
        self.__stages_end_dates: Optional[List[datetime]] = None
        self.__utes: Optional[Dict[str, Dict[int, List[str]]]] = None
        self.__rules: Dict[
            Tuple[Variable, SpatialResolution, TemporalResolution],
            pd.DataFrame,
        ] = {
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_cmo(),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao(
                "Geração Térmica"
            ),
            (
                Variable.CUSTO_OPERACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao(
                "Custo Total no Estágio"
            ),
            (
                Variable.CUSTO_FUTURO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao(
                "Custo Futuro"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["Earm Inicial Absoluto"],
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["Earm Inicial Percentual"],
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["Earm Inicial Absoluto"],
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_energetico_earm_sin_percentual(
                "Earm Inicial Absoluto"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["Earm Final Absoluto"],
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["Earm Final Percentual"],
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["Earm Final Absoluto"],
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_energetico_earm_sin_percentual(
                "Earm Final Absoluto"
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["Gter", "GterAT"],
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["Gter", "GterAT"],
                self.patamares,
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["Gter", "GterAT"],
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["Gter", "GterAT"],
                self.patamares,
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["Ghid", "Itaipu60"],
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["Ghid", "Itaipu60"],
                self.patamares,
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["Ghid", "Itaipu60"],
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["Ghid", "Itaipu60"],
                self.patamares,
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["ENA Absoluta"],
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["ENA Absoluta"],
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_MLT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["ENA Percentual"],
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["Mercado"],
                self.patamares,
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["Mercado"],
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["Mercado"],
                self.patamares,
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["Mercado"],
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["Deficit"],
                self.patamares,
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["Deficit"],
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["Deficit"],
                self.patamares,
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["Deficit"],
            ),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "Volume Ini (% V.U)"
            ),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "Volume Fin (% V.U)"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_relatorio_operacao_uhe_csv(
                "volumeUtilInicialHm3"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_relatorio_operacao_uhe_csv(
                "volumeUtilFinalHm3"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_relatorio_operacao_uhe_csv(
                    "volumeUtilInicialHm3"
                ),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_relatorio_operacao_uhe_csv(
                    "volumeUtilFinalHm3"
                ),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_relatorio_operacao_uhe_csv(
                    "volumeUtilInicialHm3"
                ),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_relatorio_operacao_uhe_csv(
                    "volumeUtilFinalHm3"
                ),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_relatorio_operacao_uhe_csv(
                    "volumeUtilInicialHm3"
                ),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_relatorio_operacao_uhe_csv(
                    "volumeUtilFinalHm3"
                ),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "Qafl (m3/s)"
            ),
            (
                Variable.VAZAO_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "Qdef (m3/s)"
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_uhe_patamares(
                self.patamares
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "Geração Média"
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.PATAMAR,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_uhe_patamares(self.patamares),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe("Geração Média"),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "Vertimento Turbinável"
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "Vertimento Não-Turbinável"
            ),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe(
                    "Vertimento Turbinável"
                ),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe(
                    "Vertimento Não-Turbinável"
                ),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe(
                    "Vertimento Turbinável"
                ),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe(
                    "Vertimento Não-Turbinável"
                ),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe(
                    "Vertimento Turbinável"
                ),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe(
                    "Vertimento Não-Turbinável"
                ),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.VAZAO_TURBINADA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_relatorio_operacao_uhe_csv(
                "vazaoTurbinadaM3S"
            ),
            (
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_relatorio_operacao_uhe_csv(
                "vazaoVertidaM3S"
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_ute_patamares(
                self.patamares
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_ute(
                "Patamar Medio"
            ),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_ute("Custo"),
        }

    ##  ---------------  DADOS DA OPERACAO DAS UTE   --------------   ##

    def __calcula_geracao_media(self, df: pd.DataFrame) -> pd.DataFrame:
        def aux_calcula_media(linha: pd.Series):
            sb = linha["Subsistema"]
            e = linha["Estágio"]
            acumulado = 0.0
            for i, p in enumerate(self.patamares):
                acumulado += (
                    self.horas_patamares[sb][e][i] * linha[f"Patamar {p}"]
                )
            acumulado /= sum(self.horas_patamares[sb][e])
            return acumulado

        df["Patamar Medio"] = df.apply(aux_calcula_media, axis=1)
        return df

    def __processa_bloco_relatorio_operacao_ute(
        self, col: str
    ) -> pd.DataFrame:
        with self.__uow:
            r1 = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        df1 = self.__calcula_geracao_media(r1.relatorio_operacao_ute)
        if r2 is not None:
            df2 = self.__calcula_geracao_media(r2.relatorio_operacao_ute)
        else:
            df2 = pd.DataFrame(columns=df1.columns)
        # Elimina usinas com nome repetido
        df1 = df1.groupby(
            ["Estágio", "Cenário", "Probabilidade", "Subsistema", "Usina"],
            as_index=False,
        ).sum()
        df2 = df2.groupby(
            ["Estágio", "Cenário", "Probabilidade", "Subsistema", "Usina"],
            as_index=False,
        ).sum()
        df = pd.concat([df1, df2], ignore_index=True)
        scenarios_r1 = df1["Cenário"].unique().tolist()
        scenarios_r2 = df2["Cenário"].unique().tolist()
        estagios_r1 = df1["Estágio"].unique().tolist()
        scenarios = list(set(scenarios_r1 + scenarios_r2))
        cols_scenarios = [str(s) for s in scenarios]
        df_final = pd.DataFrame()
        for sb, estagios_utes in self.utes.items():
            df_sb = pd.DataFrame()
            for e, utes in estagios_utes.items():
                df_sb_e = pd.DataFrame(
                    np.zeros((len(utes), len(scenarios))),
                    columns=cols_scenarios,
                )
                df_sb_e["usina"] = utes
                df_sb_e["estagio"] = e
                df_sb_e["dataInicio"] = self.stages_start_date[e - 1]
                df_sb_e["dataFim"] = self.stages_end_date[e - 1]
                for u in utes:
                    filtro = (
                        (df["Estágio"] == e)
                        & (df["Subsistema"] == sb)
                        & (df["Usina"] == u)
                    )
                    valores_cenarios = df.loc[filtro, ["Cenário", col]]
                    for _, v in valores_cenarios.iterrows():
                        if e in estagios_r1:
                            df_sb_e.loc[
                                (df_sb_e["usina"] == u)
                                & (df_sb_e["estagio"] == e),
                                cols_scenarios,
                            ] += float(v[col])
                        else:
                            df_sb_e.loc[
                                (df_sb_e["usina"] == u)
                                & (df_sb_e["estagio"] == e),
                                str(int(v["Cenário"])),
                            ] += float(v[col])

                df_sb = pd.concat([df_sb, df_sb_e], ignore_index=True)
            df_final = pd.concat([df_final, df_sb], ignore_index=True)

        return df_final[
            ["usina", "estagio", "dataInicio", "dataFim"] + cols_scenarios
        ]

    def __processa_bloco_relatorio_ute_patamares(
        self, pats: List[str]
    ) -> pd.DataFrame:
        """
        Extrai informações de uma soma de colunas para um patamar e para o SIN.
        """
        df_final = pd.DataFrame()
        for p in pats:
            df_p = self.__processa_bloco_relatorio_operacao_ute(f"Patamar {p}")
            cols_df_p = df_p.columns.to_list()
            df_p["patamar"] = p
            df_final = pd.concat([df_final, df_p], ignore_index=True)
        df_final = df_final[["patamar"] + cols_df_p]
        return df_final

    ##  ---------------  DADOS DA OPERACAO DAS UHE   --------------   ##
    def __processa_bloco_relatorio_operacao_uhe(
        self, col: str
    ) -> pd.DataFrame:
        with self.__uow:
            r1 = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        df1 = r1.relatorio_operacao_uhe
        df2 = (
            r2.relatorio_operacao_uhe
            if r2 is not None
            else pd.DataFrame(columns=df1.columns)
        )
        df1 = df1.loc[~pd.isna(df1["FPCGC"]), :]
        df2 = df2.loc[~pd.isna(df2["FPCGC"]), :]
        usinas_relatorio = df1["Usina"].unique()
        df_final = pd.DataFrame()
        for u in usinas_relatorio:
            df1_u = df1.loc[df1["Usina"] == u, :]
            df2_u = df2.loc[df2["Usina"] == u, :]
            df_u = self.__process_df_relato1_relato2(df1_u, df2_u, col)
            cols_df_u = df_u.columns.to_list()
            df_u["usina"] = u
            df_final = pd.concat([df_final, df_u], ignore_index=True)
        df_final = df_final[["usina"] + cols_df_u]
        return df_final

    def __processa_bloco_relatorio_uhe_patamares(
        self, pats: List[str]
    ) -> pd.DataFrame:
        """
        Extrai informações de uma soma de colunas para um patamar e para o SIN.
        """
        df_final = pd.DataFrame()
        for p in pats:
            df_p = self.__processa_bloco_relatorio_operacao_uhe(
                f"Geração Pat {p}"
            )
            cols_df_p = df_p.columns.to_list()
            df_p["patamar"] = p
            df_final = pd.concat([df_final, df_p], ignore_index=True)
        df_final = df_final[["patamar"] + cols_df_p]
        return df_final

    def __processa_relatorio_operacao_uhe_csv(
        self, col: str, patamar=None
    ) -> pd.DataFrame:
        with self.__uow:
            df = self.__uow.files.get_dec_oper_usih().tabela
        if patamar is None:
            df = df.loc[pd.isna(df["patamar"])]
        else:
            df = df.loc[df["patamar"] == patamar]
        usinas_relatorio = df["nomeUsina"].unique()
        df_final = pd.DataFrame()
        for u in usinas_relatorio:
            df_u = self.__process_df_decomp_csv(
                df.loc[df["nomeUsina"] == u, :], col
            )
            cols_df_u = df_u.columns.to_list()
            df_u["usina"] = u
            df_final = pd.concat([df_final, df_u], ignore_index=True)
        df_final = df_final[["usina"] + cols_df_u]
        return df_final

    def __agrupa_uhes(
        self, df: pd.DataFrame, s: SpatialResolution
    ) -> pd.DataFrame:
        with self.__uow:
            relato = self.__uow.files.get_relato()
            uhes_rees = relato.uhes_rees_subsistemas

            df["group"] = df.apply(
                lambda linha: int(
                    uhes_rees.loc[
                        uhes_rees["Nome UHE"] == linha["usina"], "Numero REE"
                    ].tolist()[0]
                ),
                axis=1,
            )

            if s == SpatialResolution.RESERVATORIO_EQUIVALENTE:
                df["group"] = df.apply(
                    lambda linha: uhes_rees.loc[
                        uhes_rees["Numero REE"] == linha["group"], "Nome REE"
                    ].tolist()[0],
                    axis=1,
                )
            elif s == SpatialResolution.SUBMERCADO:
                df["group"] = df.apply(
                    lambda linha: uhes_rees.loc[
                        uhes_rees["Numero REE"] == linha["group"],
                        "Nome Subsistema",
                    ].tolist()[0],
                    axis=1,
                )
            elif s == SpatialResolution.SISTEMA_INTERLIGADO:
                df["group"] = 1

            cols_group = ["group"] + [
                c
                for c in df.columns
                if c in self.IDENTIFICATION_COLUMNS and c != "usina"
            ]
            df_group = df.groupby(cols_group).sum().reset_index()

            group_name = {
                SpatialResolution.RESERVATORIO_EQUIVALENTE: "ree",
                SpatialResolution.SUBMERCADO: "submercado",
            }
            if s == SpatialResolution.SISTEMA_INTERLIGADO:
                df_group = df_group.drop(columns=["group"])
            else:
                df_group = df_group.rename(columns={"group": group_name[s]})
            return df_group

    ##  ---------------  DADOS DO BALANCO ENERGETICO --------------   ##

    def __processa_bloco_relatorio_balanco_energetico_earm_sin_percentual(
        self, col: str
    ) -> pd.DataFrame:
        """
        Extrai informação de uma coluna do balanço energético, para um patamar
        e todos os submercados.
        """
        df = self.__processa_bloco_relatorio_balanco_energetico_sin(col)
        with self.__uow:
            earmax = (
                self.__uow.files.get_relato().energia_armazenada_maxima_subsistema
            )
        cte_earmax_sin = float(earmax["Earmax"].sum()) / 100.0
        cols_scenarios = [
            c
            for c in df.columns
            if c not in ["estagio", "dataInicio", "dataFim", "patamar"]
        ]
        df.loc[:, cols_scenarios] /= cte_earmax_sin
        return df

    def __processa_bloco_relatorio_balanco_energetico_submercado(
        self, col: str, pat: str = "Medio"
    ) -> pd.DataFrame:
        """
        Extrai informação de uma coluna do balanço energético, para um patamar
        e todos os submercados.
        """
        with self.__uow:
            r1 = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        df1 = r1.balanco_energetico
        df2 = (
            r2.balanco_energetico
            if r2 is not None
            else pd.DataFrame(columns=df1.columns)
        )
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
            if pat != "Medio":
                df_s["patamar"] = pat
            df_s["submercado"] = s
            df_final = pd.concat([df_final, df_s], ignore_index=True)
        cols_adic = (
            ["submercado", "patamar"] if pat != "Medio" else ["submercado"]
        )
        df_final = df_final[cols_adic + cols_df_s]
        return df_final

    def __processa_bloco_relatorio_balanco_energetico_sin(
        self, col: str, pat: str = "Medio"
    ) -> pd.DataFrame:
        """
        Extrai informação de uma coluna do balanço energético, para um patamar
        e agrega os valores para o SIN.
        """
        with self.__uow:
            r1 = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        df1 = r1.balanco_energetico
        df2 = (
            r2.balanco_energetico
            if r2 is not None
            else pd.DataFrame(columns=df1.columns)
        )
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
            if pat != "Medio":
                df_s["patamar"] = pat
            if df_final.empty:
                df_final = df_s
            else:
                cols_cenarios = [
                    c
                    for c in df_s.columns
                    if c not in ["estagio", "dataInicio", "dataFim", "patamar"]
                ]
                df_final.loc[:, cols_cenarios] += df_s.loc[:, cols_cenarios]
        cols_adic = ["patamar"] if pat != "Medio" else []
        return df_final[cols_adic + cols_df_s]

    def __processa_bloco_relatorio_balanco_estagio(
        self, function: Callable, cols: List[str], pats: List[str] = ["Medio"]
    ) -> pd.DataFrame:
        """
        Extrai informações de uma soma de colunas para um patamar e para o SIN.
        """
        return pd.concat(
            [
                self.__sum_df_scenarios_columns(
                    self.__process_columns_dfs(
                        function,
                        cols,
                        p,
                    )
                )
                for p in pats
            ],
            ignore_index=True,
        )

    def __processa_relatorio_operacao_ree_csv(self, col: str) -> pd.DataFrame:
        with self.__uow:
            df = self.__uow.files.get_dec_oper_ree().tabela
        df = df.loc[pd.isna(df["patamar"])]
        rees_relatorio = df["nomeRee"].unique()
        df_final = pd.DataFrame()
        for u in rees_relatorio:
            df_u = self.__process_df_decomp_csv(
                df.loc[df["nomeRee"] == u, :], col
            )
            cols_df_u = df_u.columns.to_list()
            df_u["ree"] = u
            df_final = pd.concat([df_final, df_u], ignore_index=True)
        df_final = df_final[["ree"] + cols_df_u]
        return df_final

    ##  ---------------  DADOS DA OPERACAO GERAL     --------------   ##

    def __processa_bloco_relatorio_operacao(self, col: str) -> pd.DataFrame:
        with self.__uow:
            r1 = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        df1 = r1.relatorio_operacao_custos
        df2 = (
            r2.relatorio_operacao_custos
            if r2 is not None
            else pd.DataFrame(columns=df1.columns)
        )
        df_s = self.__process_df_relato1_relato2(df1, df2, col)
        return df_s

    def __processa_bloco_relatorio_operacao_cmo(self) -> pd.DataFrame:
        with self.__uow:
            r1 = self.__uow.files.get_relato()
            r2 = self.__uow.files.get_relato2()
        df1 = r1.relatorio_operacao_custos
        df2 = (
            r2.relatorio_operacao_custos
            if r2 is not None
            else pd.DataFrame(columns=df1.columns)
        )
        df_final = pd.DataFrame()
        for s in self.subsystems:
            col = f"CMO {s}"
            df_s = self.__process_df_relato1_relato2(df1, df2, col)
            cols_df_s = df_s.columns.to_list()
            df_s["submercado"] = s
            df_final = pd.concat([df_final, df_s], ignore_index=True)
        df_final = df_final[["submercado"] + cols_df_s]
        return df_final

    ## -------------- FUNCOES GERAIS ------------- ##

    def __process_df_decomp_csv(
        self, df: pd.DataFrame, col: str
    ) -> pd.DataFrame:
        estagios = df["periodo"].unique().tolist()
        start_dates = [self.stages_start_date[i - 1] for i in estagios]
        end_dates = [self.stages_end_date[i - 1] for i in estagios]
        scenarios = df["cenario"].unique().tolist()
        cols_scenarios = [str(c) for c in scenarios]
        empty_table = np.zeros((len(start_dates), len(scenarios)))
        df_processed = pd.DataFrame(empty_table, columns=cols_scenarios)
        df_processed["estagio"] = estagios
        df_processed["dataInicio"] = start_dates
        df_processed["dataFim"] = end_dates
        for e in estagios:
            dados_estagio = df.loc[df["periodo"] == e, col]
            if dados_estagio.shape[0] == 1:
                df_processed.loc[
                    df_processed["estagio"] == e,
                    cols_scenarios,
                ] = float(df.loc[df["periodo"] == e, col])
            else:
                df_processed.loc[
                    df_processed["estagio"] == e,
                    cols_scenarios,
                ] = df.loc[df["periodo"] == e, col].to_numpy()
        df_processed = df_processed[
            ["estagio", "dataInicio", "dataFim"] + cols_scenarios
        ]
        return df_processed

    def __process_df_relato1_relato2(
        self, df1: pd.DataFrame, df2: pd.DataFrame, col: str
    ) -> pd.DataFrame:
        estagios_r1 = df1["Estágio"].unique().tolist()
        estagios_r2 = df2["Estágio"].unique().tolist()
        estagios = list(set(estagios_r1 + estagios_r2))
        start_dates = [self.stages_start_date[i - 1] for i in estagios]
        end_dates = [self.stages_end_date[i - 1] for i in estagios]
        scenarios_r1 = df1["Cenário"].unique().tolist()
        scenarios_r2 = df2["Cenário"].unique().tolist()
        scenarios = list(set(scenarios_r1 + scenarios_r2))
        cols_scenarios = [str(c) for c in scenarios]
        empty_table = np.zeros((len(start_dates), len(scenarios)))
        df_processed = pd.DataFrame(empty_table, columns=cols_scenarios)
        df_processed["estagio"] = estagios
        df_processed["dataInicio"] = start_dates
        df_processed["dataFim"] = end_dates
        for e in estagios_r1:
            df_processed.loc[
                df_processed["estagio"] == e,
                cols_scenarios,
            ] = float(df1.loc[df1["Estágio"] == e, col])
        for e in estagios_r2:
            df_processed.loc[
                df_processed["estagio"] == e,
                cols_scenarios,
            ] = df2.loc[df2["Estágio"] == e, col].to_numpy()
        df_processed = df_processed[
            ["estagio", "dataInicio", "dataFim"] + cols_scenarios
        ]
        return df_processed

    def __process_columns_dfs(
        self, function: Callable, cols: List[str], pat: str = "Medio"
    ) -> List[pd.DataFrame]:
        """
        Processa uma função de formatação de DataFrame para diversas colunas
        e um patamar fixo.
        """
        return [function(c, pat) for c in cols]

    def __sum_df_scenarios_columns(self, dfs: List[pd.DataFrame]):
        """
        Realiza a soma das colunas relativas a cenários em uma lista de
        DataFrames, preservando as demais colunas.
        """
        non_scenario_columns = [
            "patamar",
            "submercado",
            "estagio",
            "dataInicio",
            "dataFim",
        ]
        scenario_columns = [
            c for c in dfs[0].columns if c not in non_scenario_columns
        ]
        df_sum = dfs[0].copy()
        for i in range(1, len(dfs)):
            df_sum.loc[:, scenario_columns] += dfs[i].loc[:, scenario_columns]
        return df_sum

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

    def __resolve_horas_patamares(self) -> Dict[str, Dict[int, List[float]]]:
        with self.__uow:
            Log.log().info(f"Obtendo duração dos patamares")
            dadger = self.__uow.files.get_dadger()
        duracoes_patamares = {}
        for sb in self.subsystems:
            duracoes_patamares[sb] = {}
            sb_code = dadger.sb(nome=sb).codigo
            for e in range(1, len(self.stages_start_date) + 1):
                dps = dadger.dp(subsistema=sb_code, estagio=e)
                if dps is None:
                    duracoes_patamares[sb][e] = []
                if isinstance(dps, list):
                    duracoes_patamares[sb][e] = dps[0].duracoes
                else:
                    duracoes_patamares[sb][e] = dps.duracoes
        return duracoes_patamares

    @property
    def horas_patamares(self) -> Dict[str, Dict[int, List[float]]]:
        if self.__horas_patamares is None:
            self.__horas_patamares = self.__resolve_horas_patamares()
        return self.__horas_patamares

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

    def __resolve_utes(self) -> Dict[str, Dict[int, List[float]]]:
        with self.__uow:
            Log.log().info(f"Obtendo UTEs")
            dadger = self.__uow.files.get_dadger()
        utes = {}
        for sb in self.subsystems:
            utes[sb] = {}
            sb_code = dadger.sb(nome=sb).codigo
            for e in range(1, len(self.stages_start_date) + 1):
                cts = dadger.ct(subsistema=sb_code, estagio=e)
                if cts is None:
                    utes[sb][e] = []
                elif isinstance(cts, list):
                    utes[sb][e] = [c.nome for c in cts]
                else:
                    utes[sb][e] = [cts.nome]
        return utes

    @property
    def utes(self) -> Dict[str, Dict[int, List[str]]]:
        if self.__utes is None:
            self.__utes = self.__resolve_utes()
        return self.__utes

    def _processa_media(
        self, df: pd.DataFrame, probabilities: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        cols_cenarios = [
            col
            for col in df.columns.tolist()
            if col not in self.IDENTIFICATION_COLUMNS
        ]
        cols_cenarios = [
            c for c in cols_cenarios if c not in ["min", "max", "median"]
        ]
        cols_cenarios = [c for c in cols_cenarios if "p" not in c]
        estagios = [int(e) for e in df["estagio"].unique()]
        if probabilities is not None:
            df["mean"] = 0.0
            for e in estagios:
                df_estagio = probabilities.loc[
                    probabilities["estagio"] == e, :
                ]
                probabilidades = {
                    str(int(linha["cenario"])): linha["probabilidade"]
                    for _, linha in df_estagio.iterrows()
                }
                probabilidades = {
                    **probabilidades,
                    **{
                        c: 0.0
                        for c in cols_cenarios
                        if c not in probabilidades.keys()
                    },
                }
                df_cenarios_estagio = df.loc[
                    df["estagio"] == e, cols_cenarios
                ].mul(probabilidades, fill_value=0.0)
                df.loc[df["estagio"] == e, "mean"] = df_cenarios_estagio[
                    list(probabilidades.keys())
                ].sum(axis=1)
        else:
            df["mean"] = df[cols_cenarios].mean(axis=1)
        return df

    def _processa_quantis(
        self, df: pd.DataFrame, quantiles: List[float]
    ) -> pd.DataFrame:
        cols_cenarios = [
            col
            for col in df.columns.tolist()
            if col not in self.IDENTIFICATION_COLUMNS
        ]
        for q in quantiles:
            if q == 0:
                label = "min"
            elif q == 1:
                label = "max"
            elif q == 0.5:
                label = "median"
            else:
                label = f"p{int(100 * q)}"
            df[label] = df[cols_cenarios].quantile(q, axis=1)
        return df

    def _postprocess(
        self, df: pd.DataFrame, probabilities: Optional[pd.DataFrame]
    ) -> pd.DataFrame:
        df = self._processa_quantis(df, [0.05 * i for i in range(21)])
        df = self._processa_media(df, probabilities)
        cols_not_scenarios = [
            c for c in df.columns if c in self.IDENTIFICATION_COLUMNS
        ]
        cols_scenarios = [
            c for c in df.columns if c not in self.IDENTIFICATION_COLUMNS
        ]
        df = pd.melt(
            df,
            id_vars=cols_not_scenarios,
            value_vars=cols_scenarios,
            var_name="cenario",
            value_name="valor",
        )
        return df

    def synthetize(self, variables: List[str]):
        if len(variables) == 0:
            variables = self._default_args()
        else:
            variables = self._process_variable_arguments(variables)
        valid_synthesis = self.filter_valid_variables(variables)
        for s in valid_synthesis:
            filename = str(s)
            Log.log().info(f"Realizando síntese de {filename}")
            df = self.__rules[
                (s.variable, s.spatial_resolution, s.temporal_resolution)
            ]()
            if df is None:
                continue
            with self.__uow:
                probs = self.__uow.export.read_df(self.PROBABILITIES_FILE)
                df = self._postprocess(df, probs)
                self.__uow.export.synthetize_df(df, filename)
