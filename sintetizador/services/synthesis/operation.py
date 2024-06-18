from typing import Dict, List, Tuple, Optional
import pandas as pd  # type: ignore
import numpy as np
from traceback import print_exc
from datetime import datetime, timedelta
from idecomp.decomp.dadger import Dadger

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
        "VAGUA_UHE_EST",
        "CMO_SBM_EST",
        "CMO_SBM_PAT",
        "CTER_SIN_EST",
        "COP_SIN_EST",
        "CFU_SIN_EST",
        "EARMI_REE_EST",
        "EARMI_SBM_EST",
        "EARMI_SIN_EST",
        "EARPI_REE_EST",
        "EARPI_SBM_EST",
        "EARPI_SIN_EST",
        "EARMF_REE_EST",
        "EARMF_SBM_EST",
        "EARMF_SIN_EST",
        "EARPF_REE_EST",
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
        self.__patamares: Optional[List[int]] = None
        self.__stages_durations: Optional[pd.DataFrame] = None
        self.__earmax_sin: Optional[float] = None
        self.__dadger: Optional[Dadger] = None
        self.__data_inicio_estudo: Optional[datetime] = None
        self.__dec_eco_discr: Optional[pd.DataFrame] = None
        self.__dec_oper_sist: Optional[pd.DataFrame] = None
        self.__dec_oper_ree: Optional[pd.DataFrame] = None
        self.__dec_oper_usih: Optional[pd.DataFrame] = None
        self.__dec_oper_usit: Optional[pd.DataFrame] = None
        self.__dec_oper_gnl: Optional[pd.DataFrame] = None
        self.__dec_oper_interc: Optional[pd.DataFrame] = None
        self.__valor_agua: Optional[pd.DataFrame] = None
        self.__rules: Dict[
            Tuple[Variable, SpatialResolution, TemporalResolution],
            pd.DataFrame,
        ] = {
            (
                Variable.VALOR_AGUA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_valor_agua(),
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_sist("cmo"),
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.processa_dec_oper_sist("cmo", self.patamares),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao(
                "geracao_termica"
            ),
            (
                Variable.CUSTO_OPERACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao(
                "custo_presente"
            ),
            (
                Variable.CUSTO_FUTURO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_bloco_relatorio_operacao(
                "custo_futuro"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_ree("earm_inicial_MWmes"),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_ree("earm_inicial_percentual"),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_sist("earm_inicial_MWmes"),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_sist("earm_inicial_percentual"),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist("earm_inicial_MWmes")
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.stub_earmax_sin(
                self.__agrupa_submercados(
                    self.processa_dec_oper_sist("earm_inicial_MWmes")
                )
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_ree("earm_final_MWmes"),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_ree("earm_final_percentual"),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_sist("earm_final_MWmes"),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_sist("earm_final_percentual"),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist("earm_final_MWmes")
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.stub_earmax_sin(
                self.__agrupa_submercados(
                    self.processa_dec_oper_sist("earm_final_MWmes")
                )
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_sist("geracao_termica_total_MW"),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.processa_dec_oper_sist(
                "geracao_termica_total_MW",
                self.patamares,
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist(
                    "geracao_termica_total_MW",
                )
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist(
                    "geracao_termica_total_MW",
                    self.patamares,
                )
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_sist(
                "geracao_hidro_com_itaipu_MW"
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.processa_dec_oper_sist(
                "geracao_hidro_com_itaipu_MW", self.patamares
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist("geracao_hidro_com_itaipu_MW")
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist(
                    "geracao_hidro_com_itaipu_MW",
                    self.patamares,
                ),
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_sist(
                "geracao_eolica_MW",
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.processa_dec_oper_sist(
                "geracao_eolica_MW",
                self.patamares,
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist(
                    "geracao_eolica_MW",
                ),
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist(
                    "geracao_eolica_MW",
                    self.patamares,
                ),
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_ree(
                "ena_MWmes",
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_sist(
                "ena_MWmes",
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist(
                    "ena_MWmes",
                ),
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist("demanda_MW", self.patamares),
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist("demanda_MW"),
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.processa_dec_oper_sist(
                "demanda_MW", self.patamares
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_sist("demanda_MW"),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist(
                    "demanda_liquida_MW", self.patamares
                )
            ),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist("demanda_liquida_MW")
            ),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.processa_dec_oper_sist(
                "demanda_liquida_MW", self.patamares
            ),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_sist("demanda_liquida_MW"),
            (
                Variable.DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist("deficit_MW", self.patamares)
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_submercados(
                self.processa_dec_oper_sist("deficit_MW")
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda: self.processa_dec_oper_sist(
                "deficit_MW", self.patamares
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_sist("deficit_MW"),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_usih(
                "volume_util_inicial_percentual"
            ),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_usih(
                "volume_util_final_percentual"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_usih("volume_util_inicial_hm3"),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_usih("volume_util_final_hm3"),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.processa_dec_oper_usih("volume_util_inicial_hm3"),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.processa_dec_oper_usih("volume_util_final_hm3"),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.processa_dec_oper_usih("volume_util_inicial_hm3"),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.processa_dec_oper_usih("volume_util_final_hm3"),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.processa_dec_oper_usih("volume_util_inicial_hm3"),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.processa_dec_oper_usih("volume_util_final_hm3"),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_usih("vazao_incremental_m3s"),
            (
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_usih("vazao_afluente_m3s"),
            (
                Variable.VAZAO_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_usih("vazao_defluente_m3s"),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda: self.processa_dec_oper_usih(
                "geracao_MW", self.patamares
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_usih("geracao_MW"),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.PATAMAR,
            ): lambda: self.__agrupa_uhes(
                self.processa_dec_oper_usih("geracao_MW", self.patamares),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__agrupa_uhes(
                self.processa_dec_oper_usih("geracao_MW"),
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
            ): lambda: self.processa_dec_oper_usih("vazao_turbinada_m3s"),
            (
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_usih("vazao_vertida_m3s"),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda: self.processa_dec_oper_usit(
                "geracao_MW", self.patamares
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_usit("geracao_MW"),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_usit("custo_geracao"),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
                TemporalResolution.ESTAGIO,
            ): lambda: self.processa_dec_oper_interc("intercambio_origem_MW"),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
                TemporalResolution.PATAMAR,
            ): lambda: self.processa_dec_oper_interc(
                "intercambio_origem_MW", self.patamares
            ),
        }

    @property
    def uow(self) -> AbstractUnitOfWork:
        if self.__uow is None:
            raise RuntimeError()
        return self.__uow

    def get_dadger(self) -> Dadger:
        if self.__dadger is None:
            self.__dadger = self.uow.files.get_dadger()
        return self.__dadger

    def get_dec_eco_discr(self) -> pd.DataFrame:
        if self.__dec_eco_discr is None:
            with self.uow:
                arq_discr = self.uow.files.get_dec_eco_discr()
            df = arq_discr.tabela
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do arquivo dec_eco_discr.csv"
                    )
                raise RuntimeError()
            self.__dec_eco_discr = df
        return self.__dec_eco_discr

    def get_dec_oper_sist(self) -> pd.DataFrame:
        if self.__dec_oper_sist is None:
            with self.uow:
                arq_oper = self.uow.files.get_dec_oper_sist()
            df = arq_oper.tabela
            versao = arq_oper.versao
            if versao is not None:
                if versao <= "31.0.2":
                    df = self.__stub_cenarios_nos_v31_0_2(df)
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do arquivo dec_oper_sist.csv"
                    )
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: self.adiciona_datas_df(linha),
                axis=1,
                result_type="expand",
            )
            df["geracao_termica_total_MW"] = (
                df["geracao_termica_MW"] + df["geracao_termica_antecipada_MW"]
            )
            df["itaipu_60MW"].fillna(0.0, inplace=True)
            df["geracao_hidro_com_itaipu_MW"] = (
                df["geracao_hidroeletrica_MW"] + df["itaipu_60MW"]
            )
            df["demanda_liquida_MW"] = (
                df["demanda_MW"] - df["geracao_pequenas_usinas_MW"]
            )
            self.__dec_oper_sist = df
        return self.__dec_oper_sist

    def get_valor_agua(self) -> pd.DataFrame:
        if self.__valor_agua is None:
            with self.uow:
                arq_custos = self.uow.files.get_custos()
            df = arq_custos.relatorio_variaveis_duais
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do arquivo custos.rvx"
                    )
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: self.adiciona_datas_df(linha),
                axis=1,
                result_type="expand",
            )
            self.__valor_agua = df
        return self.__valor_agua

    def get_dec_oper_ree(self) -> pd.DataFrame:
        if self.__dec_oper_ree is None:
            with self.uow:
                arq_oper = self.uow.files.get_dec_oper_ree()
            df = arq_oper.tabela
            versao = arq_oper.versao
            if versao is not None:
                if versao <= "31.0.2":
                    df = self.__stub_cenarios_nos_v31_0_2(df)
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error("Erro na leitura do arquivo dec_oper_ree.csv")
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: self.adiciona_datas_df(linha),
                axis=1,
                result_type="expand",
            )
            self.__dec_oper_ree = df
        return self.__dec_oper_ree

    def get_dec_oper_usih(self) -> pd.DataFrame:
        if self.__dec_oper_usih is None:
            with self.uow:
                arq_oper = self.uow.files.get_dec_oper_usih()
            df = arq_oper.tabela
            versao = arq_oper.versao
            if versao is not None:
                if versao <= "31.0.2":
                    df = self.__stub_cenarios_nos_v31_0_2(df)
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do arquivo dec_oper_usih.csv"
                    )
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: self.adiciona_datas_df(linha),
                axis=1,
                result_type="expand",
            )
            self.__dec_oper_usih = df
        return self.__dec_oper_usih

    def get_dec_oper_usit(self) -> pd.DataFrame:
        if self.__dec_oper_usit is None:
            with self.uow:
                arq_oper = self.uow.files.get_dec_oper_usit()
            df = arq_oper.tabela
            versao = arq_oper.versao
            if versao is not None:
                if versao <= "31.0.2":
                    df = self.__stub_cenarios_nos_v31_0_2(df)
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do arquivo dec_oper_usit.csv"
                    )
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: self.adiciona_datas_df(linha),
                axis=1,
                result_type="expand",
            )
            df["geracao_percentual_maxima"] = (
                100 * df["geracao_MW"] / df["geracao_maxima_MW"]
            )
            filtro = df["geracao_maxima_MW"] != df["geracao_minima_MW"]
            df.loc[
                filtro,
                "geracao_percentual_flexivel",
            ] = (
                100
                * (
                    df.loc[
                        filtro,
                        "geracao_MW",
                    ]
                    - df.loc[
                        filtro,
                        "geracao_minima_MW",
                    ]
                )
                / (
                    df.loc[
                        filtro,
                        "geracao_maxima_MW",
                    ]
                    - df.loc[filtro, "geracao_minima_MW"]
                )
            )
            df.loc[~filtro, "geracao_percentual_flexivel"] = 100.0
            self.__dec_oper_usit = df
        return self.__dec_oper_usit

    def get_dec_oper_gnl(self) -> pd.DataFrame:
        if self.__dec_oper_gnl is None:
            with self.uow:
                arq_oper = self.uow.files.get_dec_oper_gnl()
            df = arq_oper.tabela
            versao = arq_oper.versao
            if versao is not None:
                if versao <= "31.0.2":
                    df = self.__stub_cenarios_nos_v31_0_2(df)
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error("Erro na leitura do arquivo dec_oper_gnl.csv")
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: self.adiciona_datas_df(linha),
                axis=1,
                result_type="expand",
            )
            self.__dec_oper_gnl = df
        return self.__dec_oper_gnl

    def get_dec_oper_interc(self) -> pd.DataFrame:
        if self.__dec_oper_interc is None:
            with self.uow:
                arq_oper = self.uow.files.get_dec_oper_interc()
            df = arq_oper.tabela
            versao = arq_oper.versao
            if versao is not None:
                if versao <= "31.0.2":
                    df = self.__stub_cenarios_nos_v31_0_2(df)
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do arquivo dec_oper_interc.csv"
                    )
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: self.adiciona_datas_df(linha),
                axis=1,
                result_type="expand",
            )
            self.__dec_oper_interc = df
        return self.__dec_oper_interc

    @staticmethod
    def __stub_cenarios_nos_v31_0_2(df: pd.DataFrame) -> pd.DataFrame:
        estagios = df["estagio"].unique().tolist()
        # Para todos os estágios antes do último, fixa cenário em 1
        df.loc[df["estagio"].isin(estagios[:-1]), "cenario"] = 1
        # Subtrai dos cenários o valor de n_semanas
        df.loc[df["estagio"] == estagios[-1], "cenario"] -= len(estagios) - 1
        return df.copy()

    @property
    def data_inicio_estudo(self) -> datetime:
        if self.__data_inicio_estudo is None:
            logger = Log.log()
            registro_dt = self.get_dadger().dt
            if registro_dt is None:
                if logger is not None:
                    logger.error("Não foi encontrado registro DT")
                raise RuntimeError()
            ano, mes, dia = registro_dt.ano, registro_dt.mes, registro_dt.dia
            if ano is None or mes is None or dia is None:
                if logger is not None:
                    logger.error("Erro no processamento do registro DT")
                raise RuntimeError()
            self.__data_inicio_estudo = datetime(ano, mes, dia)
        return self.__data_inicio_estudo

    @property
    def patamares(self) -> List[int]:
        if self.__patamares is None:
            df = self.get_dec_eco_discr()
            self.__patamares = df["patamar"].dropna().unique().tolist()
        return self.__patamares

    @property
    def stages_durations(self) -> pd.DataFrame:
        """
        - estagio (`int`)
        - data_inicio (`datetime`)
        - data_fim (`datetime`)
        - numero_aberturas (`int`)
        """
        if self.__stages_durations is None:
            df = self.get_dec_eco_discr()
            df = df.loc[df["patamar"].isna()]
            df["duracao_acumulada"] = df["duracao"].cumsum()
            df["data_inicio"] = df.apply(
                lambda linha: self.data_inicio_estudo
                + timedelta(
                    hours=df.loc[df["estagio"] < linha["estagio"], "duracao"]
                    .to_numpy()
                    .sum()
                ),
                axis=1,
            )
            df["data_fim"] = df.apply(
                lambda linha: linha["data_inicio"]
                + timedelta(hours=linha["duracao"]),
                axis=1,
            )
            self.__stages_durations = df[
                ["estagio", "data_inicio", "data_fim", "numero_aberturas"]
            ].copy()
        return self.__stages_durations

    @property
    def stages_start_date(self) -> List[datetime]:
        return self.stages_durations["data_inicio"].tolist()

    @property
    def stages_end_date(self) -> List[datetime]:
        return self.stages_durations["data_fim"].tolist()

    @property
    def earmax_sin(self) -> float:
        if self.__earmax_sin is None:
            with self.uow:
                earmax = (
                    self.uow.files.get_relato().energia_armazenada_maxima_submercado
                )
            if earmax is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do bloco de EARMax do relato"
                    )
                raise RuntimeError()
            self.__earmax_sin = earmax["energia_armazenada_maxima"].sum()
        return self.__earmax_sin

    def stub_earmax_sin(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_cenarios = [c for c in df.columns if str(c).isnumeric()]
        df[cols_cenarios] *= 100.0 / self.earmax_sin
        return df.copy()

    def adiciona_datas_df(self, linha: pd.Series) -> np.ndarray:
        return (
            self.stages_durations.loc[
                self.stages_durations["estagio"] == linha["estagio"],
                ["data_inicio", "data_fim"],
            ]
            .to_numpy()
            .flatten()
        )

    def processa_valor_agua(self):
        df = self.get_valor_agua().copy()
        df = df.rename(
            columns={
                "pih": "valor"
            }
        )
        df = df.fillna(0.0)
        df = df.astype({"cenario": str})
        print("1: ",df)
        df = df.ffill(axis=1)
        print("3: ",df)
        return df.copy()

    def processa_dec_oper_sist(
        self, col: str, patamares: Optional[List[int]] = None
    ):
        df = self.get_dec_oper_sist().copy()
        if patamares is None:
            df = df.loc[df["patamar"].isna()]
            cols = [
                "submercado",
                "estagio",
                "dataInicio",
                "dataFim",
                "cenario",
                "valor",
            ]
        else:
            df = df.loc[df["patamar"].isin(patamares)]
            df = df.astype({"patamar": int})
            cols = [
                "submercado",
                "estagio",
                "dataInicio",
                "dataFim",
                "patamar",
                "cenario",
                "valor",
            ]
        if col not in df.columns:
            logger = Log.log()
            if logger is not None:
                logger.warning(f"Coluna {col} não encontrada no arquivo")
            df[col] = 0.0
        df = df.rename(
            columns={
                "nome_submercado": "submercado",
                col: "valor",
            }
        )
        df = df[cols]
        df = df.fillna(0.0)
        df["submercado"] = pd.Categorical(
            values=df["submercado"],
            categories=df["submercado"].unique().tolist(),
            ordered=True,
        )
        df.sort_values(["submercado", "estagio", "cenario"], inplace=True)
        df = df.astype({"cenario": str})
        df = df.pivot_table(
            "valor",
            index=[c for c in cols if c not in ["valor", "cenario"]],
            columns="cenario",
        ).reset_index()
        df = df.ffill(axis=1)
        df = df.astype({"submercado": str})
        return df.copy()

    def processa_dec_oper_ree(self, col: str):
        df = self.get_dec_oper_ree().copy()
        cols = [
            "ree",
            "estagio",
            "dataInicio",
            "dataFim",
            "cenario",
            "valor",
        ]
        if col not in df.columns:
            logger = Log.log()
            if logger is not None:
                logger.warning(f"Coluna {col} não encontrada no arquivo")
            df[col] = 0.0
        df = df.rename(
            columns={
                "nome_ree": "ree",
                col: "valor",
            }
        )
        df = df[cols]
        df = df.fillna(0.0)
        df["ree"] = pd.Categorical(
            values=df["ree"],
            categories=df["ree"].unique().tolist(),
            ordered=True,
        )
        df.sort_values(["ree", "estagio", "cenario"], inplace=True)
        df = df.astype({"cenario": str})
        df = df.pivot_table(
            "valor",
            index=[c for c in cols if c not in ["valor", "cenario"]],
            columns="cenario",
        ).reset_index()
        df = df.ffill(axis=1)
        df = df.astype({"ree": str})
        return df.copy()

    def processa_dec_oper_usih(
        self, col: str, patamares: Optional[List[int]] = None
    ):
        df = self.get_dec_oper_usih().copy()
        if patamares is None:
            df = df.loc[df["patamar"].isna()]
            cols = [
                "usina",
                "estagio",
                "dataInicio",
                "dataFim",
                "cenario",
                "valor",
            ]
        else:
            df = df.loc[df["patamar"].isin(patamares)]
            df = df.astype({"patamar": int})
            cols = [
                "usina",
                "estagio",
                "dataInicio",
                "dataFim",
                "patamar",
                "cenario",
                "valor",
            ]
        if col not in df.columns:
            logger = Log.log()
            if logger is not None:
                logger.warning(f"Coluna {col} não encontrada no arquivo")
            df[col] = 0.0
        df = df.rename(
            columns={
                "nome_usina": "usina",
                col: "valor",
            }
        )
        df = df[cols]
        df = df.fillna(0.0)
        df["usina"] = pd.Categorical(
            values=df["usina"],
            categories=df["usina"].unique().tolist(),
            ordered=True,
        )
        df.sort_values(["usina", "estagio", "cenario"], inplace=True)
        df = df.astype({"cenario": str})
        df = df.pivot_table(
            "valor",
            index=[c for c in cols if c not in ["valor", "cenario"]],
            columns="cenario",
        ).reset_index()
        df = df.ffill(axis=1)
        df = df.astype({"usina": str})
        return df.copy()

    def processa_dec_oper_usit(
        self, col: str, patamares: Optional[List[int]] = None
    ):
        df = self.get_dec_oper_usit().copy()
        if patamares is None:
            df = df.loc[df["patamar"].isna()]
            cols = [
                "usina",
                "estagio",
                "dataInicio",
                "dataFim",
                "cenario",
                "valor",
            ]
        else:
            df = df.loc[df["patamar"].isin(patamares)]
            df = df.astype({"patamar": int})
            cols = [
                "usina",
                "estagio",
                "dataInicio",
                "dataFim",
                "patamar",
                "cenario",
                "valor",
            ]
        if col not in df.columns:
            logger = Log.log()
            if logger is not None:
                logger.warning(f"Coluna {col} não encontrada no arquivo")
            df[col] = 0.0
        df = df.rename(
            columns={
                "nome_usina": "usina",
                col: "valor",
            }
        )
        df = df[cols]
        df = df.fillna(0.0)
        df["usina"] = pd.Categorical(
            values=df["usina"],
            categories=df["usina"].unique().tolist(),
            ordered=True,
        )
        df.sort_values(["usina", "estagio", "cenario"], inplace=True)
        df = df.astype({"cenario": str})
        df = df.pivot_table(
            "valor",
            index=[c for c in cols if c not in ["valor", "cenario"]],
            columns="cenario",
        ).reset_index()
        df = df.ffill(axis=1)
        df = df.astype({"usina": str})
        return df.copy()

    def processa_dec_oper_interc(
        self, col: str, patamares: Optional[List[int]] = None
    ):
        df = self.get_dec_oper_interc().copy()
        if patamares is None:
            df = df.loc[df["patamar"].isna()]
            cols = [
                "submercadoDe",
                "submercadoPara",
                "estagio",
                "dataInicio",
                "dataFim",
                "cenario",
                "valor",
            ]
        else:
            df = df.loc[df["patamar"].isin(patamares)]
            df = df.astype({"patamar": int})
            cols = [
                "submercadoDe",
                "submercadoPara",
                "estagio",
                "dataInicio",
                "dataFim",
                "patamar",
                "cenario",
                "valor",
            ]
        df = df.rename(
            columns={
                "nome_submercado_de": "submercadoDe",
                "nome_submercado_para": "submercadoPara",
                col: "valor",
            }
        )
        df = df[cols]
        df = df.fillna(0.0)
        df["submercadoDe"] = pd.Categorical(
            values=df["submercadoDe"],
            categories=df["submercadoDe"].unique().tolist(),
            ordered=True,
        )
        df["submercadoPara"] = pd.Categorical(
            values=df["submercadoPara"],
            categories=df["submercadoPara"].unique().tolist(),
            ordered=True,
        )
        df.sort_values(
            [
                "submercadoDe",
                "submercadoPara",
                "estagio",
                "cenario",
            ],
            inplace=True,
        )
        df = df.astype({"cenario": str})
        df = df.pivot_table(
            "valor",
            index=[c for c in cols if c not in ["valor", "cenario"]],
            columns="cenario",
        ).reset_index()
        df = df.ffill(axis=1)
        df = df.astype({"submercadoDe": str, "submercadoPara": str})
        return df.copy()

    def __agrupa_submercados(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_group = [
            c
            for c in df.columns
            if c in self.IDENTIFICATION_COLUMNS and c != "submercado"
        ]
        df_group = (
            df.groupby(cols_group)
            .sum()
            .reset_index()
            .drop(columns=["submercado"])
        )
        return df_group

    def __agrupa_uhes(
        self, df: pd.DataFrame, s: SpatialResolution
    ) -> pd.DataFrame:
        with self.uow:
            relato = self.uow.files.get_relato()
            uhes_rees = relato.uhes_rees_submercados
            if uhes_rees is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura da relação entre UHEs"
                        + " e REEs no relato."
                    )
                raise RuntimeError()
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

    #  ---------------  DADOS DA OPERACAO DAS UHE   --------------   #
    # Não existe informação de energia vertida no dec_oper_usih.csv,
    # por isso ainda são extraídas do relato.
    def __processa_bloco_relatorio_operacao_uhe(
        self, col: str
    ) -> pd.DataFrame:
        with self.uow:
            r1 = self.uow.files.get_relato()
            r2 = self.uow.files.get_relato2()
        logger = Log.log()
        df1 = r1.relatorio_operacao_uhe
        if df1 is None:
            if logger is not None:
                logger.error(
                    "Erro na leitura do relatório de"
                    + " operação das UHE do relato."
                )
            raise RuntimeError()
        df2 = (
            r2.relatorio_operacao_uhe
            if r2.relatorio_operacao_uhe is not None
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

    #  ---------------  DADOS DA OPERACAO GERAL     --------------   #

    def __processa_bloco_relatorio_operacao(self, col: str) -> pd.DataFrame:
        with self.uow:
            r1 = self.uow.files.get_relato()
            r2 = self.uow.files.get_relato2()
        logger = Log.log()
        df1 = r1.relatorio_operacao_custos
        if df1 is None:
            if logger is not None:
                logger.error(
                    "Erro na leitura do relatório de"
                    + " custos de operação do relato."
                )
            raise RuntimeError()
        df2 = (
            r2.relatorio_operacao_custos
            if r2.relatorio_operacao_custos is not None
            else pd.DataFrame(columns=df1.columns)
        )
        df_s = self.__process_df_relato1_relato2(df1, df2, col)
        return df_s

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

    def _default_args(self) -> List[str]:
        return self.__class__.DEFAULT_OPERATION_SYNTHESIS_ARGS

    def _process_variable_arguments(
        self,
        args: List[str],
    ) -> List[OperationSynthesis]:
        args_data = [OperationSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        logger = Log.log()
        for i, a in enumerate(args_data):
            if a is None:
                if logger is not None:
                    logger.info(f"Erro no argumento fornecido: {args[i]}")
                return []
        return valid_args

    def filter_valid_variables(
        self, variables: List[OperationSynthesis]
    ) -> List[OperationSynthesis]:
        logger = Log.log()
        if logger is not None:
            logger.info(f"Variáveis: {variables}")
        return variables

    def _processa_media(
        self, df: pd.DataFrame, probabilities: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        cols_cenarios = [
            col
            for col in df.columns.tolist()
            if col not in self.IDENTIFICATION_COLUMNS
        ]
        cols_cenarios = [c for c in cols_cenarios if c.isnumeric()]
        estagios = [int(e) for e in df["estagio"].unique()]
        if probabilities is not None:
            if len(probabilities["cenario"].unique().tolist()) != len(
                cols_cenarios
            ):
                logger = Log.log()
                if logger is not None:
                    logger.warning(
                        "Número de cenários informados nos"
                        + " relatos difere do número encontrado no"
                        + " arquivo do dado em questão. Considerados"
                        + " cenários equiprováveis."
                    )
                df["mean"] = df[cols_cenarios].mean(axis=1)
            else:
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
                    df.loc[df["estagio"] == e, "mean"] = (
                        df_cenarios_estagio.sum(axis=1).astype(np.float64)
                    )
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
        synthesis_variables = self._process_variable_arguments(variables)
        valid_synthesis = self.filter_valid_variables(synthesis_variables)
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
