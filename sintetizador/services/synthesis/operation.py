from typing import Dict, List, Tuple, Optional, Any, Callable
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

    DECK_FILES: Dict[str, Any] = {}

    def __init__(self) -> None:
        self.__uow: Optional[AbstractUnitOfWork] = None
        self.__patamares: Optional[List[int]] = None
        self.__stages_durations: Optional[pd.DataFrame] = None
        self.__earmax_sin: Optional[float] = None
        self.__dadger: Optional[Dadger] = None
        self.__data_inicio_estudo: Optional[datetime] = None
        self.__dec_eco_discr: Optional[pd.DataFrame] = None
        self.__dec_oper_ree: Optional[pd.DataFrame] = None
        self.__dec_oper_usih: Optional[pd.DataFrame] = None
        self.__dec_oper_usit: Optional[pd.DataFrame] = None
        self.__dec_oper_gnl: Optional[pd.DataFrame] = None
        self.__dec_oper_interc: Optional[pd.DataFrame] = None

    @property
    def uow(self) -> AbstractUnitOfWork:
        if self.__uow is None:
            raise RuntimeError()
        return self.__uow

    @classmethod
    def _get_rule(
        cls, synthesis: Tuple[Variable, SpatialResolution, TemporalResolution]
    ) -> Callable:
        _rules: Dict[
            Tuple[Variable, SpatialResolution, TemporalResolution],
            Callable,
        ] = {
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_sist(uow, "cmo"),
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow, "cmo", cls.get_patamares(uow)
            ),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._processa_bloco_relatorio_operacao(
                uow, "geracao_termica"
            ),
            (
                Variable.CUSTO_OPERACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._processa_bloco_relatorio_operacao(
                uow, "custo_presente"
            ),
            (
                Variable.CUSTO_FUTURO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._processa_bloco_relatorio_operacao(
                uow, "custo_futuro"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_ree(
                uow, "earm_inicial_MWmes"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_ree(
                uow, "earm_inicial_percentual"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow, "earm_inicial_MWmes"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow, "earm_inicial_percentual"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(uow, "earm_inicial_MWmes")
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.stub_earmax_sin(
                uow,
                cls._agrupa_submercados(
                    cls.processa_dec_oper_sist(uow, "earm_inicial_MWmes")
                ),
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_ree(uow, "earm_final_MWmes"),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_ree(
                uow, "earm_final_percentual"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_sist(uow, "earm_final_MWmes"),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow, "earm_final_percentual"
            ),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(uow, "earm_final_MWmes")
            ),
            (
                Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.stub_earmax_sin(
                uow,
                cls._agrupa_submercados(
                    cls.processa_dec_oper_sist(uow, "earm_final_MWmes")
                ),
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow, "geracao_termica_total_MW"
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow,
                "geracao_termica_total_MW",
                cls.get_patamares(uow),
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(
                    uow,
                    "geracao_termica_total_MW",
                )
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(
                    uow,
                    "geracao_termica_total_MW",
                    cls.get_patamares(uow),
                )
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow, "geracao_hidro_com_itaipu_MW"
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow, "geracao_hidro_com_itaipu_MW", cls.get_patamares(uow)
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(uow, "geracao_hidro_com_itaipu_MW")
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(
                    uow,
                    "geracao_hidro_com_itaipu_MW",
                    cls.get_patamares(uow),
                ),
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow,
                "geracao_eolica_MW",
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow,
                "geracao_eolica_MW",
                cls.get_patamares(uow),
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(
                    uow,
                    "geracao_eolica_MW",
                ),
            ),
            (
                Variable.GERACAO_EOLICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(
                    uow,
                    "geracao_eolica_MW",
                    cls.get_patamares(uow),
                ),
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_ree(
                uow,
                "ena_MWmes",
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow,
                "ena_MWmes",
            ),
            (
                Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(
                    uow,
                    "ena_MWmes",
                ),
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(
                    uow, "demanda_MW", cls.get_patamares(uow)
                ),
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(uow, "demanda_MW"),
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow, "demanda_MW", cls.get_patamares(uow)
            ),
            (
                Variable.MERCADO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_sist(uow, "demanda_MW"),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(
                    uow, "demanda_liquida_MW", cls.get_patamares(uow)
                )
            ),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(uow, "demanda_liquida_MW")
            ),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow, "demanda_liquida_MW", cls.get_patamares(uow)
            ),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow, "demanda_liquida_MW"
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(
                    uow, "deficit_MW", cls.get_patamares(uow)
                )
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_submercados(
                cls.processa_dec_oper_sist(uow, "deficit_MW")
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls.processa_dec_oper_sist(
                uow, "deficit_MW", cls.get_patamares(uow)
            ),
            (
                Variable.DEFICIT,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_sist(uow, "deficit_MW"),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_usih(
                uow, "volume_util_inicial_percentual"
            ),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_usih(
                uow, "volume_util_final_percentual"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_usih(
                uow, "volume_util_inicial_hm3"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_usih(
                uow, "volume_util_final_hm3"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls.processa_dec_oper_usih(uow, "volume_util_inicial_hm3"),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls.processa_dec_oper_usih(uow, "volume_util_final_hm3"),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls.processa_dec_oper_usih(uow, "volume_util_inicial_hm3"),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls.processa_dec_oper_usih(uow, "volume_util_final_hm3"),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls.processa_dec_oper_usih(uow, "volume_util_inicial_hm3"),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls.processa_dec_oper_usih(uow, "volume_util_final_hm3"),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_usih(
                uow, "vazao_incremental_m3s"
            ),
            (
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_usih(
                uow, "vazao_afluente_m3s"
            ),
            (
                Variable.VAZAO_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_usih(
                uow, "vazao_defluente_m3s"
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls.processa_dec_oper_usih(
                uow, "geracao_MW", cls.get_patamares(uow)
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_usih(uow, "geracao_MW"),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls.processa_dec_oper_usih(
                    uow, "geracao_MW", cls.get_patamares(uow)
                ),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls.processa_dec_oper_usih(uow, "geracao_MW"),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._processa_bloco_relatorio_operacao_uhe(
                uow, "vertimento_turbinavel"
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._processa_bloco_relatorio_operacao_uhe(
                uow, "vertimento_nao_turbinavel"
            ),
            (
                Variable.ENERGIA_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._stub_ever_uhes(uow),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls._processa_bloco_relatorio_operacao_uhe(
                    uow, "vertimento_turbinavel"
                ),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls._processa_bloco_relatorio_operacao_uhe(
                    uow, "vertimento_nao_turbinavel"
                ),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.ENERGIA_VERTIDA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls._stub_ever_uhes(uow),
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls._processa_bloco_relatorio_operacao_uhe(
                    uow, "vertimento_turbinavel"
                ),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.ENERGIA_VERTIDA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls._stub_ever_uhes(uow),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls._processa_bloco_relatorio_operacao_uhe(
                    uow, "vertimento_nao_turbinavel"
                ),
                SpatialResolution.SUBMERCADO,
            ),
            (
                Variable.ENERGIA_VERTIDA_TURBINAVEL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls._processa_bloco_relatorio_operacao_uhe(
                    uow, "vertimento_turbinavel"
                ),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls._processa_bloco_relatorio_operacao_uhe(
                    uow, "vertimento_nao_turbinavel"
                ),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.ENERGIA_VERTIDA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls._agrupa_uhes(
                uow,
                cls._stub_ever_uhes(uow),
                SpatialResolution.SISTEMA_INTERLIGADO,
            ),
            (
                Variable.VAZAO_TURBINADA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_usih(
                uow, "vazao_turbinada_m3s"
            ),
            (
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_usih(
                uow, "vazao_vertida_m3s"
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls.processa_dec_oper_usit(
                uow, "geracao_MW", cls.get_patamares(uow)
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_usit(uow, "geracao_MW"),
            (
                Variable.CUSTO_GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_usit(uow, "custo_geracao"),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
                TemporalResolution.ESTAGIO,
            ): lambda uow: cls.processa_dec_oper_interc(
                uow, "intercambio_origem_MW"
            ),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
                TemporalResolution.PATAMAR,
            ): lambda uow: cls.processa_dec_oper_interc(
                uow, "intercambio_origem_MW", cls.get_patamares(uow)
            ),
        }

        return _rules[synthesis]

    @classmethod
    def get_dadger(cls, uow: AbstractUnitOfWork) -> Dadger:
        name = "dadger"
        if name not in cls.DECK_FILES:
            with uow:
                cls.DECK_FILES[name] = uow.files.get_dadger()
        return cls.DECK_FILES[name]

    @classmethod
    def get_dec_eco_discr(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_eco_discr"
        if name not in cls.DECK_FILES:
            with uow:
                arq_discr = uow.files.get_dec_eco_discr()
            df = arq_discr.tabela
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do arquivo dec_eco_discr.csv"
                    )
                raise RuntimeError()
            cls.DECK_FILES[name] = df
        return cls.DECK_FILES[name].copy()

    @classmethod
    def get_dec_oper_sist(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_oper_sist"
        if name not in cls.DECK_FILES:
            with uow:
                arq_oper = uow.files.get_dec_oper_sist()
            df = arq_oper.tabela
            versao = arq_oper.versao
            if versao is not None:
                if versao <= "31.0.2":
                    df = cls._stub_cenarios_nos_v31_0_2(df)
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do arquivo dec_oper_sist.csv"
                    )
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: cls.adiciona_datas_df(linha, uow),
                axis=1,
                result_type="expand",
            )
            df["geracao_termica_total_MW"] = (
                df["geracao_termica_MW"] + df["geracao_termica_antecipada_MW"]
            )
            df["itaipu_60MW"] = df["itaipu_60MW"].fillna(0.0)
            df["geracao_hidro_com_itaipu_MW"] = (
                df["geracao_hidroeletrica_MW"] + df["itaipu_60MW"]
            )
            df["demanda_liquida_MW"] = (
                df["demanda_MW"] - df["geracao_pequenas_usinas_MW"]
            )
            cls.DECK_FILES[name] = df
        return cls.DECK_FILES[name].copy()

    @classmethod
    def get_dec_oper_ree(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_oper_ree"
        if name not in cls.DECK_FILES:
            with uow:
                arq_oper = uow.files.get_dec_oper_ree()
            df = arq_oper.tabela
            versao = arq_oper.versao
            if versao is not None:
                if versao <= "31.0.2":
                    df = cls._stub_cenarios_nos_v31_0_2(df)
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error("Erro na leitura do arquivo dec_oper_ree.csv")
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: cls.adiciona_datas_df(linha, uow),
                axis=1,
                result_type="expand",
            )
            cls.DECK_FILES[name] = df
        return cls.DECK_FILES[name].copy()

    @classmethod
    def get_dec_oper_usih(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_oper_usih"
        if name not in cls.DECK_FILES:
            with uow:
                arq_oper = uow.files.get_dec_oper_usih()
            df = arq_oper.tabela
            versao = arq_oper.versao
            if versao is not None:
                if versao <= "31.0.2":
                    df = cls._stub_cenarios_nos_v31_0_2(df)
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do arquivo dec_oper_usih.csv"
                    )
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: cls.adiciona_datas_df(linha, uow),
                axis=1,
                result_type="expand",
            )
            cls.DECK_FILES[name] = df
        return cls.DECK_FILES[name].copy()

    @classmethod
    def get_dec_oper_usit(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_oper_usit"
        if name not in cls.DECK_FILES:
            with uow:
                arq_oper = uow.files.get_dec_oper_usit()
            df = arq_oper.tabela
            versao = arq_oper.versao
            if versao is not None:
                if versao <= "31.0.2":
                    df = cls._stub_cenarios_nos_v31_0_2(df)
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do arquivo dec_oper_usit.csv"
                    )
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: cls.adiciona_datas_df(linha, uow),
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
            cls.DECK_FILES[name] = df
        return cls.DECK_FILES[name]

    @classmethod
    def get_dec_oper_gnl(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_oper_gnl"
        if name not in cls.DECK_FILES:
            with uow:
                arq_oper = uow.files.get_dec_oper_gnl()
            df = arq_oper.tabela
            versao = arq_oper.versao
            if versao is not None:
                if versao <= "31.0.2":
                    df = cls._stub_cenarios_nos_v31_0_2(df)
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error("Erro na leitura do arquivo dec_oper_gnl.csv")
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: cls.adiciona_datas_df(linha, uow),
                axis=1,
                result_type="expand",
            )
            cls.DECK_FILES[name] = df
        return cls.DECK_FILES[name]

    @classmethod
    def get_dec_oper_interc(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "dec_oper_interc"
        if name not in cls.DECK_FILES:
            with uow:
                arq_oper = uow.files.get_dec_oper_interc()
            df = arq_oper.tabela
            versao = arq_oper.versao
            if versao is not None:
                if versao <= "31.0.2":
                    df = cls._stub_cenarios_nos_v31_0_2(df)
            if df is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do arquivo dec_oper_interc.csv"
                    )
                raise RuntimeError()
            df[["dataInicio", "dataFim"]] = df.apply(
                lambda linha: cls.adiciona_datas_df(linha, uow),
                axis=1,
                result_type="expand",
            )
            cls.DECK_FILES[name] = df
        return cls.DECK_FILES[name]

    @staticmethod
    def _stub_cenarios_nos_v31_0_2(df: pd.DataFrame) -> pd.DataFrame:
        estagios = df["estagio"].unique().tolist()
        # Para todos os estágios antes do último, fixa cenário em 1
        df.loc[df["estagio"].isin(estagios[:-1]), "cenario"] = 1
        # Subtrai dos cenários o valor de n_semanas
        df.loc[df["estagio"] == estagios[-1], "cenario"] -= len(estagios) - 1
        return df.copy()

    @classmethod
    def get_data_inicio_estudo(cls, uow: AbstractUnitOfWork) -> datetime:
        name = "data_inicio_estudo"
        if name not in cls.DECK_FILES:
            logger = Log.log()
            registro_dt = cls.get_dadger(uow).dt
            if registro_dt is None:
                if logger is not None:
                    logger.error("Não foi encontrado registro DT")
                raise RuntimeError()
            ano, mes, dia = registro_dt.ano, registro_dt.mes, registro_dt.dia
            if ano is None or mes is None or dia is None:
                if logger is not None:
                    logger.error("Erro no processamento do registro DT")
                raise RuntimeError()
            cls.DECK_FILES[name] = datetime(ano, mes, dia)
        return cls.DECK_FILES[name]

    @classmethod
    def get_patamares(cls, uow: AbstractUnitOfWork) -> List[int]:
        name = "patamares"
        if name not in cls.DECK_FILES:
            df = cls.get_dec_eco_discr(uow)
            cls.DECK_FILES[name] = df["patamar"].dropna().unique().tolist()
        return cls.DECK_FILES[name]

    @classmethod
    def get_stages_durations(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        """
        - estagio (`int`)
        - data_inicio (`datetime`)
        - data_fim (`datetime`)
        - numero_aberturas (`int`)
        """
        name = "stages_durations"
        if name not in cls.DECK_FILES:
            df = cls.get_dec_eco_discr(uow)
            df = df.loc[df["patamar"].isna()]
            df["duracao_acumulada"] = df["duracao"].cumsum()
            df["data_inicio"] = df.apply(
                lambda linha: cls.get_data_inicio_estudo(uow)
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
            cls.DECK_FILES[name] = df[
                ["estagio", "data_inicio", "data_fim", "numero_aberturas"]
            ].copy()
        return cls.DECK_FILES[name]

    @classmethod
    def get_stages_start_date(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        return cls.get_stages_durations(uow)["data_inicio"].tolist()

    @classmethod
    def get_stages_end_date(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        return cls.get_stages_durations(uow)["data_fim"].tolist()

    @classmethod
    def get_earmax_sin(cls, uow: AbstractUnitOfWork) -> float:
        name = "earmax_sin"
        if name not in cls.DECK_FILES:
            with uow:
                earmax = (
                    uow.files.get_relato().energia_armazenada_maxima_submercado
                )
            if earmax is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro na leitura do bloco de EARMax do relato"
                    )
                raise RuntimeError()
            cls.DECK_FILES[name] = earmax["energia_armazenada_maxima"].sum()
        return cls.DECK_FILES[name]

    @classmethod
    def stub_earmax_sin(
        cls, uow: AbstractUnitOfWork, df: pd.DataFrame
    ) -> pd.DataFrame:
        cols_cenarios = [c for c in df.columns if str(c).isnumeric()]
        df[cols_cenarios] *= 100.0 / cls.get_earmax_sin(uow)
        return df.copy()

    @classmethod
    def adiciona_datas_df(
        cls, linha: pd.Series, uow: AbstractUnitOfWork
    ) -> np.ndarray:
        df = cls.get_stages_durations(uow)
        return (
            df.loc[
                df["estagio"] == linha["estagio"],
                ["data_inicio", "data_fim"],
            ]
            .to_numpy()
            .flatten()
        )

    @classmethod
    def processa_dec_oper_sist(
        cls,
        uow: AbstractUnitOfWork,
        col: str,
        patamares: Optional[List[int]] = None,
    ):
        df = cls.get_dec_oper_sist(uow).copy()
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
            observed=False,
        ).reset_index()
        df = df.ffill(axis=1)
        df = df.astype({"submercado": str})
        return df.copy()

    @classmethod
    def processa_dec_oper_ree(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        df = cls.get_dec_oper_ree(uow).copy()
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
            observed=False,
        ).reset_index()
        df = df.ffill(axis=1)
        df = df.astype({"ree": str})
        return df.copy()

    @classmethod
    def processa_dec_oper_usih(
        cls,
        uow: AbstractUnitOfWork,
        col: str,
        patamares: Optional[List[int]] = None,
    ):
        df = cls.get_dec_oper_usih(uow).copy()
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
            observed=False,
        ).reset_index()
        df = df.ffill(axis=1)
        df = df.astype({"usina": str})
        return df.copy()

    @classmethod
    def processa_dec_oper_usit(
        cls,
        uow: AbstractUnitOfWork,
        col: str,
        patamares: Optional[List[int]] = None,
    ):
        df = cls.get_dec_oper_usit(uow).copy()
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
            observed=False,
        ).reset_index()
        df = df.ffill(axis=1)
        df = df.astype({"usina": str})
        return df.copy()

    @classmethod
    def processa_dec_oper_interc(
        cls,
        uow: AbstractUnitOfWork,
        col: str,
        patamares: Optional[List[int]] = None,
    ) -> pd.DataFrame:
        df = cls.get_dec_oper_interc(uow).copy()
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
            observed=False,
        ).reset_index()
        df = df.ffill(axis=1)
        df = df.astype({"submercadoDe": str, "submercadoPara": str})
        return df.copy()

    @classmethod
    def _agrupa_submercados(cls, df: pd.DataFrame) -> pd.DataFrame:
        cols_group = [
            c
            for c in df.columns
            if c in cls.IDENTIFICATION_COLUMNS and c != "submercado"
        ]
        df_group = (
            df.groupby(cols_group)
            .sum()
            .reset_index()
            .drop(columns=["submercado"])
        )
        return df_group

    @classmethod
    def _agrupa_uhes(
        cls, uow: AbstractUnitOfWork, df: pd.DataFrame, s: SpatialResolution
    ) -> pd.DataFrame:
        with uow:
            relato = uow.files.get_relato()
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
                if c in cls.IDENTIFICATION_COLUMNS and c != "usina"
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
    @classmethod
    def _processa_bloco_relatorio_operacao_uhe(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with uow:
            r1 = uow.files.get_relato()
            r2 = uow.files.get_relato2()
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
            df_u = cls._process_df_relato1_relato2(df1_u, df2_u, col, uow)
            cols_df_u = df_u.columns.to_list()
            df_u["usina"] = u
            df_final = pd.concat([df_final, df_u], ignore_index=True)
        df_final = df_final[["usina"] + cols_df_u]
        return df_final

    @classmethod
    def _stub_ever_uhes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        evert = cls._processa_bloco_relatorio_operacao_uhe(
            uow, "vertimento_turbinavel"
        )
        evernt = cls._processa_bloco_relatorio_operacao_uhe(
            uow, "vertimento_nao_turbinavel"
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

    @classmethod
    def _processa_bloco_relatorio_operacao(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with uow:
            r1 = uow.files.get_relato()
            r2 = uow.files.get_relato2()
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
        df_s = cls._process_df_relato1_relato2(df1, df2, col, uow)
        return df_s

    @classmethod
    def _process_df_relato1_relato2(
        cls,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        col: str,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        estagios_r1 = df1["estagio"].unique().tolist()
        estagios_r2 = df2["estagio"].unique().tolist()
        estagios = list(set(estagios_r1 + estagios_r2))
        start_dates = [cls.get_stages_start_date(uow)[i - 1] for i in estagios]
        end_dates = [cls.get_stages_end_date(uow)[i - 1] for i in estagios]
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

    @classmethod
    def _default_args(cls) -> List[str]:
        return cls.DEFAULT_OPERATION_SYNTHESIS_ARGS

    @classmethod
    def _process_variable_arguments(
        cls,
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

    @classmethod
    def filter_valid_variables(
        cls, variables: List[OperationSynthesis]
    ) -> List[OperationSynthesis]:
        logger = Log.log()
        if logger is not None:
            logger.info(f"Variáveis: {variables}")
        return variables

    @classmethod
    def _processa_media(
        cls, df: pd.DataFrame, probabilities: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        cols_cenarios = [
            col
            for col in df.columns.tolist()
            if col not in cls.IDENTIFICATION_COLUMNS
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

    @classmethod
    def _processa_quantis(
        cls, df: pd.DataFrame, quantiles: List[float]
    ) -> pd.DataFrame:
        cols_cenarios = [
            col
            for col in df.columns.tolist()
            if col not in cls.IDENTIFICATION_COLUMNS
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

    @classmethod
    def _postprocess(
        cls, df: pd.DataFrame, probabilities: Optional[pd.DataFrame]
    ) -> pd.DataFrame:
        df = cls._processa_quantis(df, [0.05 * i for i in range(21)])
        df = cls._processa_media(df, probabilities)
        cols_not_scenarios = [
            c for c in df.columns if c in cls.IDENTIFICATION_COLUMNS
        ]
        cols_scenarios = [
            c for c in df.columns if c not in cls.IDENTIFICATION_COLUMNS
        ]
        df = pd.melt(
            df,
            id_vars=cols_not_scenarios,
            value_vars=cols_scenarios,
            var_name="cenario",
            value_name="valor",
        )
        return df

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
            try:
                df = cls._get_rule(
                    (s.variable, s.spatial_resolution, s.temporal_resolution)
                )(uow)
            except Exception:
                print_exc()
                continue
            if df is None:
                continue
            with uow:
                probs = uow.export.read_df(cls.PROBABILITIES_FILE)
                df = cls._postprocess(df, probs)
                uow.export.synthetize_df(df, filename)
