from typing import Callable, Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
from traceback import print_exc
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
        "GEOL_SBM_EST",
        "GEOL_SBM_PAT",
        "GEOL_SIN_EST",
        "GEOL_SIN_PAT",
        "ENAA_SBM_EST",
        "ENAA_SIN_EST",
        "ENAM_SBM_EST",
        "MER_SBM_EST",
        "MER_SBM_PAT",
        "MER_SIN_EST",
        "MER_SIN_PAT",
        "MERL_SBM_EST",
        "MERL_SBM_PAT",
        "MERL_SIN_EST",
        "MERL_SIN_PAT",
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
        "QINC_UHE_EST",
        "QAFL_UHE_EST",
        "QDEF_UHE_EST",
        "QTUR_UHE_EST",
        "QVER_UHE_EST",
        "EVERT_UHE_EST",
        "EVERNT_UHE_EST",
        "EVER_UHE_EST",
        "EVERT_REE_EST",
        "EVERNT_REE_EST",
        "EVER_REE_EST",
        "EVERT_SBM_EST",
        "EVERNT_SBM_EST",
        "EVER_SBM_EST",
        "EVERT_SIN_EST",
        "EVERNT_SIN_EST",
        "EVER_SIN_EST",
        "GTER_UTE_EST",
        "GTER_UTE_PAT",
        "CTER_UTE_EST",
        "INT_SBP_EST",
        "INT_SBP_PAT",
    ]

    def __init__(self) -> None:
        self.__uow: Optional[AbstractUnitOfWork] = None
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
                ["energia_armazenada_inicial_MWmed"],
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["energia_armazenada_inicial_percentual"],
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["energia_armazenada_inicial_MWmed"],
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_energetico_earm_sin_percentual(
                "energia_armazenada_inicial_MWmed"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["energia_armazenada_final_MWmed"],
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["energia_armazenada_final_percentual"],
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["energia_armazenada_final_MWmed"],
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_energetico_earm_sin_percentual(
                "energia_armazenada_final_MWmed"
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["geracao_termica", "geracao_termica_antecipada"],
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["geracao_termica", "geracao_termica_antecipada"],
                self.patamares,
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["geracao_termica", "geracao_termica_antecipada"],
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["geracao_termica", "geracao_termica_antecipada"],
                self.patamares,
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["geracao_hidraulica", "geracao_itaipu_60hz"],
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["geracao_hidraulica", "geracao_itaipu_60hz"],
                self.patamares,
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["geracao_hidraulica", "geracao_itaipu_60hz"],
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["geracao_hidraulica", "geracao_itaipu_60hz"],
                self.patamares,
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["geracao_eolica"],
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["geracao_eolica"],
                self.patamares,
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["geracao_eolica"],
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["geracao_eolica"],
                self.patamares,
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["energia_natural_afluente_MWmed"],
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["energia_natural_afluente_MWmed"],
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_MLT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["energia_natural_afluente_percentual"],
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["mercado"],
                self.patamares,
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["mercado"],
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["mercado"],
                self.patamares,
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["mercado"],
            ),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__stub_mercl_sin(self.patamares),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__stub_mercl_sin(),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__stub_mercl_sbm(self.patamares),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__stub_mercl_sbm(),
            (
                Variable.DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["deficit"],
                self.patamares,
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_sin,
                ["deficit"],
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["deficit"],
                self.patamares,
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_balanco_estagio(
                self.__processa_bloco_relatorio_balanco_energetico_submercado,
                ["deficit"],
            ),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "volume_inicial_percentual"
            ),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "volume_final_percentual"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_relatorio_operacao_uhe_csv(
                "volume_util_inicial_hm3"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_relatorio_operacao_uhe_csv(
                "volume_util_final_hm3"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_relatorio_operacao_uhe_csv(
                    "volume_util_inicial_hm3"
                ),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_relatorio_operacao_uhe_csv(
                    "volume_util_final_hm3"
                ),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_relatorio_operacao_uhe_csv(
                    "volume_util_inicial_hm3"
                ),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_relatorio_operacao_uhe_csv(
                    "volume_util_final_hm3"
                ),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_relatorio_operacao_uhe_csv(
                    "volume_util_inicial_hm3"
                ),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_relatorio_operacao_uhe_csv(
                    "volume_util_final_hm3"
                ),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_relatorio_operacao_uhe_csv(
                "vazao_incremental_m3s"
            ),
            (
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "vazao_afluente_m3s"
            ),
            (
                Variable.VAZAO_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "vazao_defluente_m3s"
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
                "geracao_media"
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
                self.__processa_bloco_relatorio_operacao_uhe("geracao_media"),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "vertimento_turbinavel"
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_uhe(
                "vertimento_nao_turbinavel"
            ),
            (
                Variable.ENERGIA_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__stub_ever_uhes(),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe(
                    "vertimento_turbinavel"
                ),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe(
                    "vertimento_nao_turbinavel"
                ),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.ENERGIA_VERTIDA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__stub_ever_uhes(),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe(
                    "vertimento_turbinavel"
                ),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.ENERGIA_VERTIDA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__stub_ever_uhes(),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe(
                    "vertimento_nao_turbinavel"
                ),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe(
                    "vertimento_turbinavel"
                ),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__processa_bloco_relatorio_operacao_uhe(
                    "vertimento_nao_turbinavel"
                ),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.ENERGIA_VERTIDA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.__stub_ever_uhes(),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.VAZAO_TURBINADA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_relatorio_operacao_uhe_csv(
                "vazao_turbinada_m3s"
            ),
            (
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_relatorio_operacao_uhe_csv(
                "vazao_vertida_m3s"
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
                "geracao_patamar_Medio"
            ),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao_ute("Custo"),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_relatorio_intercambios_csv(
                "intercambio_origem_MW"
            ),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
                TemporalResolution.PATAMAR,
            ): lambda: self.__processa_relatorio_intercambios_csv(
                "intercambio_origem_MW", self.patamares
            ),
        }

    @property
    def uow(self) -> AbstractUnitOfWork:
        if self.__uow is None:
            raise RuntimeError()
        return self.__uow

    #  ---------------  DADOS DA OPERACAO DAS UTE   --------------   #

    def __calcula_geracao_media(self, df: pd.DataFrame) -> pd.DataFrame:
        def aux_calcula_media(linha: pd.Series):
            sb = linha["submercado"]
            e = linha["estagio"]
            acumulado = 0.0
            for i, p in enumerate(self.patamares):
                acumulado += (
                    self.horas_patamares[sb][e][i]
                    * linha[f"geracao_patamar_{p}"]
                )
            acumulado /= sum(self.horas_patamares[sb][e])
            return acumulado

        df["geracao_patamar_medio"] = df.apply(aux_calcula_media, axis=1)
        return df

    def __processa_bloco_relatorio_operacao_ute(
        self, col: str
    ) -> pd.DataFrame:
        with self.uow:
            r1 = self.uow.files.get_relato()
            r2 = self.uow.files.get_relato2()
        df1 = self.__calcula_geracao_media(r1.relatorio_operacao_ute)
        if r2 is not None:
            df2 = self.__calcula_geracao_media(r2.relatorio_operacao_ute)
        else:
            df2 = pd.DataFrame(columns=df1.columns)
        # Elimina usinas com nome repetido
        df1 = df1.groupby(
            [
                "estagio",
                "cenario",
                "probabilidade",
                "nome_submercado",
                "nome_usina",
            ],
            as_index=False,
        ).sum()
        df2 = df2.groupby(
            [
                "estagio",
                "cenario",
                "probabilidade",
                "nome_submercado",
                "nome_usina",
            ],
            as_index=False,
        ).sum()
        df = pd.concat([df1, df2], ignore_index=True)
        scenarios_r1 = df1["cenario"].unique().tolist()
        scenarios_r2 = df2["cenario"].unique().tolist()
        estagios_r1 = df1["estagio"].unique().tolist()
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
                        (df["estagio"] == e)
                        & (df["nome_submercado"] == sb)
                        & (df["nome_usina"] == u)
                    )
                    valores_cenarios = df.loc[filtro, ["cenario", col]]
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
                                str(int(v["cenario"])),
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
            df_p = self.__processa_bloco_relatorio_operacao_ute(
                f"geracao_patamar_{p}"
            )
            cols_df_p = df_p.columns.to_list()
            df_p["patamar"] = p
            df_final = pd.concat([df_final, df_p], ignore_index=True)
        df_final = df_final[["patamar"] + cols_df_p]
        return df_final

    #  ---------------  DADOS DA OPERACAO DAS UHE   --------------   #
    def __processa_bloco_relatorio_operacao_uhe(
        self, col: str
    ) -> pd.DataFrame:
        with self.uow:
            r1 = self.uow.files.get_relato()
            r2 = self.uow.files.get_relato2()
        df1 = r1.relatorio_operacao_uhe
        df2 = (
            r2.relatorio_operacao_uhe
            if r2 is not None
            else pd.DataFrame(columns=df1.columns)
        )
        df1 = df1.loc[~pd.isna(df1["FPCGC"]), :]
        df2 = df2.loc[~pd.isna(df2["FPCGC"]), :]
        usinas_relatorio = df1["nome_usina"].unique()
        df_final = pd.DataFrame()
        for u in usinas_relatorio:
            df1_u = df1.loc[df1["nome_usina"] == u, :]
            df2_u = df2.loc[df2["nome_usina"] == u, :]
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
                f"geracao_patamar_{p}"
            )
            cols_df_p = df_p.columns.to_list()
            df_p["patamar"] = p
            df_final = pd.concat([df_final, df_p], ignore_index=True)
        df_final = df_final[["patamar"] + cols_df_p]
        return df_final

    def __processa_relatorio_operacao_uhe_csv(
        self, col: str, patamar=None
    ) -> pd.DataFrame:
        with self.uow:
            df = self.uow.files.get_dec_oper_usih().tabela
        if patamar is None:
            df = df.loc[pd.isna(df["patamar"])]
        else:
            df = df.loc[df["patamar"] == patamar]
        usinas_relatorio = df["nome_usina"].unique()
        df_final = pd.DataFrame()
        for u in usinas_relatorio:
            df_u = self.__process_df_decomp_csv(
                df.loc[df["nome_usina"] == u, :], col
            )
            cols_df_u = df_u.columns.to_list()
            df_u["usina"] = u
            df_final = pd.concat([df_final, df_u], ignore_index=True)
        df_final = df_final[["usina"] + cols_df_u]
        return df_final

    def __processa_relatorio_intercambios_csv(
        self, col: str, patamar=None
    ) -> pd.DataFrame:
        def __processa_dados_intercambio(
            df_dados: pd.DataFrame, patamar: Optional[int]
        ) -> pd.DataFrame:
            if patamar is None:
                df = df_dados.loc[pd.isna(df_dados["patamar"])]
            else:
                df = df_dados.loc[df_dados["patamar"] == patamar]
            submercados = df["nome_submercado_de"].unique()
            df_final = pd.DataFrame()
            for s_de in submercados:
                for s_para in submercados:
                    if s_de == s_para:
                        continue
                    df_s = self.__process_df_decomp_csv(
                        df.loc[
                            (df["nome_submercado_de"] == s_de)
                            & (df["nome_submercado_para"] == s_para),
                            :,
                        ],
                        col,
                    )
                    cols_df_s = df_s.columns.to_list()
                    df_s["submercadoDe"] = s_de
                    df_s["submercadoPara"] = s_para
                    df_final = pd.concat([df_final, df_s], ignore_index=True)
            if patamar is not None:
                df_final["patamar"] = patamar
                df_final = df_final[
                    ["submercadoDe", "submercadoPara", "patamar"] + cols_df_s
                ]
            else:
                df_final = df_final[
                    ["submercadoDe", "submercadoPara"] + cols_df_s
                ]
            return df_final

        with self.uow:
            df = self.uow.files.get_dec_oper_interc().tabela
        if patamar is None:
            df_final = __processa_dados_intercambio(df, None)
            df = df.loc[pd.isna(df["patamar"])]
        else:
            patamares = (
                df["patamar"].loc[~df["patamar"].isna()].unique().tolist()
            )
            patamares = [int(p) for p in patamares]
            df_final = pd.DataFrame()
            for p in patamares:
                df_p = __processa_dados_intercambio(df, p)
                df_final = pd.concat([df_final, df_p], ignore_index=True)
        return df_final

    def __agrupa_uhes(
        self, df: pd.DataFrame, s: SpatialResolution
    ) -> pd.DataFrame:
        with self.uow:
            relato = self.uow.files.get_relato()
            uhes_rees = relato.uhes_rees_submercados

            df["group"] = df.apply(
                lambda linha: int(
                    uhes_rees.loc[
                        uhes_rees["nome_usina"] == linha["usina"], "codigo_ree"
                    ].iloc[0]
                ),
                axis=1,
            )

            if s == SpatialResolution.RESERVATORIO_EQUIVALENTE:
                df["group"] = df.apply(
                    lambda linha: uhes_rees.loc[
                        uhes_rees["codigo_ree"] == linha["group"], "nome_ree"
                    ].iloc[0],
                    axis=1,
                )
            elif s == SpatialResolution.SUBMERCADO:
                df["group"] = df.apply(
                    lambda linha: uhes_rees.loc[
                        uhes_rees["codigo_ree"] == linha["group"],
                        "nome_submercado",
                    ].iloc[0],
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

    def __stub_ever_uhes(self):
        evert = self.__processa_bloco_relatorio_operacao_uhe(
            "vertimento_turbinavel"
        )
        evernt = self.__processa_bloco_relatorio_operacao_uhe(
            "vertimento_nao_turbinavel"
        )
        cols_cenarios = [
            c
            for c in evert.columns
            if c
            not in ["usina", "estagio", "dataInicio", "dataFim", "patamar"]
        ]
        evert[cols_cenarios] += evernt[cols_cenarios]
        return evert.copy()

    #  ---------------  DADOS DO BALANCO ENERGETICO --------------   #

    def __processa_bloco_relatorio_balanco_energetico_earm_sin_percentual(
        self, col: str
    ) -> pd.DataFrame:
        """
        Extrai informação de uma coluna do balanço energético, para um patamar
        e todos os submercados.
        """
        df = self.__processa_bloco_relatorio_balanco_energetico_sin(col)
        with self.uow:
            earmax = (
                self.uow.files.get_relato().energia_armazenada_maxima_submercado
            )
        cte_earmax_sin = (
            float(earmax["energia_armazenada_maxima"].sum()) / 100.0
        )
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
        with self.uow:
            r1 = self.uow.files.get_relato()
            r2 = self.uow.files.get_relato2()
        df1 = r1.balanco_energetico
        df2 = (
            r2.balanco_energetico
            if r2 is not None
            else pd.DataFrame(columns=df1.columns)
        )
        subsis_balanco = df1["nome_submercado"].unique()
        df_final = pd.DataFrame()
        for s in subsis_balanco:
            df1_s = df1.loc[
                (df1["nome_submercado"] == s) & (df1["patamar"] == pat), :
            ]
            df2_s = df2.loc[
                (df2["nome_submercado"] == s) & (df2["patamar"] == pat), :
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
        with self.uow:
            r1 = self.uow.files.get_relato()
            r2 = self.uow.files.get_relato2()
        df1 = r1.balanco_energetico
        df2 = (
            r2.balanco_energetico
            if r2 is not None
            else pd.DataFrame(columns=df1.columns)
        )
        subsis_balanco = df1["nome_submercado"].unique()
        df_final = pd.DataFrame()
        for s in subsis_balanco:
            df1_s = df1.loc[
                (df1["nome_submercado"] == s) & (df1["patamar"] == pat), :
            ]
            df2_s = df2.loc[
                (df2["nome_submercado"] == s) & (df2["patamar"] == pat), :
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

    def __stub_mercl_sbm(self, patamares=["Medio"]):
        mercado = self.__processa_bloco_relatorio_balanco_estagio(
            self.__processa_bloco_relatorio_balanco_energetico_submercado,
            ["mercado"],
            patamares,
        )
        unsi = self.__processa_bloco_relatorio_balanco_estagio(
            self.__processa_bloco_relatorio_balanco_energetico_submercado,
            ["bacia"],
            patamares,
        )
        cols_cenarios = [
            c
            for c in mercado.columns
            if c
            not in [
                "submercado",
                "estagio",
                "dataInicio",
                "dataFim",
                "patamar",
            ]
        ]
        mercado[cols_cenarios] -= unsi[cols_cenarios]
        return mercado.copy()

    def __stub_mercl_sin(self, patamares=["Medio"]):
        mercado = self.__processa_bloco_relatorio_balanco_estagio(
            self.__processa_bloco_relatorio_balanco_energetico_sin,
            ["mercado"],
            patamares,
        )
        unsi = self.__processa_bloco_relatorio_balanco_estagio(
            self.__processa_bloco_relatorio_balanco_energetico_sin,
            ["bacia"],
            patamares,
        )

        cols_cenarios = [
            c
            for c in mercado.columns
            if c
            not in [
                "estagio",
                "dataInicio",
                "dataFim",
                "patamar",
            ]
        ]
        mercado[cols_cenarios] -= unsi[cols_cenarios]
        return mercado.copy()

    def __processa_relatorio_operacao_ree_csv(self, col: str) -> pd.DataFrame:
        with self.uow:
            df = self.uow.files.get_dec_oper_ree().tabela
        df = df.loc[pd.isna(df["patamar"])]
        rees_relatorio = df["nome_ree"].unique()
        df_final = pd.DataFrame()
        for u in rees_relatorio:
            df_u = self.__process_df_decomp_csv(
                df.loc[df["nome_ree"] == u, :], col
            )
            cols_df_u = df_u.columns.to_list()
            df_u["ree"] = u
            df_final = pd.concat([df_final, df_u], ignore_index=True)
        df_final = df_final[["ree"] + cols_df_u]
        return df_final

    #  ---------------  DADOS DA OPERACAO GERAL     --------------   #

    def __processa_bloco_relatorio_operacao(self, col: str) -> pd.DataFrame:
        with self.uow:
            r1 = self.uow.files.get_relato()
            r2 = self.uow.files.get_relato2()
        df1 = r1.relatorio_operacao_custos
        df2 = (
            r2.relatorio_operacao_custos
            if r2 is not None
            else pd.DataFrame(columns=df1.columns)
        )
        df_s = self.__process_df_relato1_relato2(df1, df2, col)
        return df_s

    def __processa_bloco_relatorio_operacao_cmo(self) -> pd.DataFrame:
        with self.uow:
            r1 = self.uow.files.get_relato()
            r2 = self.uow.files.get_relato2()
        df1 = r1.relatorio_operacao_custos
        df2 = (
            r2.relatorio_operacao_custos
            if r2 is not None
            else pd.DataFrame(columns=df1.columns)
        )
        df_final = pd.DataFrame()
        for s in self.subsystems:
            col = f"cmo_{s}"
            df_s = self.__process_df_relato1_relato2(df1, df2, col)
            cols_df_s = df_s.columns.to_list()
            df_s["submercado"] = s
            df_final = pd.concat([df_final, df_s], ignore_index=True)
        df_final = df_final[["submercado"] + cols_df_s]
        return df_final

    # -------------- FUNCOES GERAIS ------------- #

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
                ] = float(df.loc[df["periodo"] == e, col].iloc[0])
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
        estagios_r1 = df1["estagio"].unique().tolist()
        estagios_r2 = df2["estagio"].unique().tolist()
        estagios = list(set(estagios_r1 + estagios_r2))
        start_dates = [self.stages_start_date[i - 1] for i in estagios]
        end_dates = [self.stages_end_date[i - 1] for i in estagios]
        scenarios_r1 = df1["cenario"].unique().tolist()
        scenarios_r2 = df2["cenario"].unique().tolist()
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
            ] = float(df1.loc[df1["estagio"] == e, col].iloc[0])
        for e in estagios_r2:
            df_processed.loc[
                df_processed["estagio"] == e,
                cols_scenarios,
            ] = df2.loc[df2["estagio"] == e, col].to_numpy()
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
        logger = Log.log()
        for i, a in enumerate(args_data):
            if a is None:
                if logger is not None:
                    logger.info(f"Erro no argumento fornecido: {args[i]}")
                return []
        return args_data

    def filter_valid_variables(
        self, variables: List[OperationSynthesis]
    ) -> List[OperationSynthesis]:
        logger = Log.log()
        if logger is not None:
            logger.info(f"Variáveis: {variables}")
        return variables

    def __resolve_subsystems(self) -> List[str]:
        with self.uow:
            logger = Log.log()
            if logger is not None:
                logger.info("Obtendo subsistemas")
            dadger = self.uow.files.get_dadger()
            sbs = dadger.sb()
        if sbs is None:
            return []
        if isinstance(sbs, list):
            return [s.nome_submercado for s in sbs]
        else:
            return [sbs.nome_submercado]

    @property
    def subsystems(self) -> List[str]:
        if self.__subsystems is None:
            self.__subsystems = self.__resolve_subsystems()
        return self.__subsystems

    def __resolve_patamares(self) -> List[str]:
        sample_sb = self.subsystems[0]
        with self.uow:
            logger = Log.log()
            if logger is not None:
                logger.info("Obtendo patamares")
            dadger = self.uow.files.get_dadger()
            sb_code = dadger.sb(nome_submercado=sample_sb).codigo_submercado
            dps = dadger.dp(codigo_submercado=sb_code, estagio=1)
        if dps is None:
            return []
        if isinstance(dps, list):
            return [str(p) for p in range(1, dps[0].numero_patamares + 1)]
        else:
            return [str(p) for p in range(1, dps.numero_patamares + 1)]

    @property
    def patamares(self) -> List[str]:
        if self.__patamares is None:
            self.__patamares = self.__resolve_patamares()
        return self.__patamares

    def __resolve_horas_patamares(self) -> Dict[str, Dict[int, List[float]]]:
        with self.uow:
            logger = Log.log()
            if logger is not None:
                logger.info("Obtendo duração dos patamares")
            dadger = self.uow.files.get_dadger()
        duracoes_patamares = {}
        for sb in self.subsystems:
            duracoes_patamares[sb] = {}
            sb_code = dadger.sb(nome_submercado=sb).codigo_submercado
            for e in range(1, len(self.stages_start_date) + 1):
                dps = dadger.dp(codigo_submercado=sb_code, estagio=e)
                if dps is None:
                    duracoes_patamares[sb][e] = []
                if isinstance(dps, list):
                    duracoes_patamares[sb][e] = dps[0].duracao
                else:
                    duracoes_patamares[sb][e] = dps.duracao
        return duracoes_patamares

    @property
    def horas_patamares(self) -> Dict[str, Dict[int, List[float]]]:
        if self.__horas_patamares is None:
            self.__horas_patamares = self.__resolve_horas_patamares()
        return self.__horas_patamares

    def __resolve_stages_start_date(self) -> List[datetime]:
        sample_sb = self.subsystems[0]
        with self.uow:
            logger = Log.log()
            if logger is not None:
                logger.info("Obtendo início dos estágios")
            dadger = self.uow.files.get_dadger()
            sb_code = dadger.sb(nome_submercado=sample_sb).codigo_submercado
            dt = dadger.dt
            dps = dadger.dp(codigo_submercado=sb_code)
        if dt is None:
            return []
        if dps is None:
            hours_stage = []
        elif isinstance(dps, list):
            hours_stage = [sum(dp.duracao) for dp in dps]
        else:
            hours_stage = [sum(dps.duracao)]
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
        with self.uow:
            logger = Log.log()
            if logger is not None:
                logger.info("Obtendo fim dos estágios")
            dadger = self.uow.files.get_dadger()
            sb_code = dadger.sb(nome_submercado=sample_sb).codigo_submercado
            dt = dadger.dt
            dps = dadger.dp(codigo_submercado=sb_code)
        if dt is None:
            return []
        if dps is None:
            hours_stage = []
        elif isinstance(dps, list):
            hours_stage = [sum(dp.duracao) for dp in dps]
        else:
            hours_stage = [sum(dps.duracao)]
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
        with self.uow:
            logger = Log.log()
            if logger is not None:
                logger.info("Obtendo UTEs")
            dadger = self.uow.files.get_dadger()
        utes = {}
        for sb in self.subsystems:
            utes[sb] = {}
            sb_code = dadger.sb(nome_submercado=sb).codigo_submercado
            for e in range(1, len(self.stages_start_date) + 1):
                cts = dadger.ct(subsistema=sb_code, estagio=e)
                if cts is None:
                    utes[sb][e] = []
                elif isinstance(cts, list):
                    utes[sb][e] = [c.nome_submercado for c in cts]
                else:
                    utes[sb][e] = [cts.nome_submercado]
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

    def synthetize(self, variables: List[str], uow: AbstractUnitOfWork):
        self.__uow = uow
        logger = Log.log()
        if len(variables) == 0:
            variables = self._default_args()
        else:
            variables = self._process_variable_arguments(variables)
        valid_synthesis = self.filter_valid_variables(variables)
        for s in valid_synthesis:
            filename = str(s)

            if logger is not None:
                logger.info(f"Realizando síntese de {filename}")
            try:
                df = self.__rules[
                    (s.variable, s.spatial_resolution, s.temporal_resolution)
                ]()
            except Exception:
                print_exc()
                continue
            if df is None:
                continue
            with self.uow:
                probs = self.uow.export.read_df(self.PROBABILITIES_FILE)
                df = self._postprocess(df, probs)
                self.uow.export.synthetize_df(df, filename)
