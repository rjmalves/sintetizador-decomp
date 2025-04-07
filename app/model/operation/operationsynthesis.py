from dataclasses import dataclass
from typing import Optional

from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.unit import Unit
from app.model.operation.variable import Variable


@dataclass
class OperationSynthesis:
    variable: Variable
    spatial_resolution: SpatialResolution

    def __repr__(self) -> str:
        return "_".join(
            [
                self.variable.value,
                self.spatial_resolution.value,
            ]
        )

    def __hash__(self) -> int:
        return hash(
            f"{self.variable.value}_" + f"{self.spatial_resolution.value}_"
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, OperationSynthesis):
            return False
        else:
            return all(
                [
                    self.variable == o.variable,
                    self.spatial_resolution == o.spatial_resolution,
                ]
            )

    @classmethod
    def factory(cls, synthesis: str) -> Optional["OperationSynthesis"]:
        data = synthesis.split("_")
        if len(data) != 2:
            return None
        else:
            return cls(
                Variable.factory(data[0]),
                SpatialResolution.factory(data[1]),
            )


SUPPORTED_SYNTHESIS: list[str] = [
    "CMO_SBM",
    "CTER_UTE",
    "CTER_SIN",
    "COP_SIN",
    "CFU_SIN",
    "EARMI_REE",
    "EARMI_SBM",
    "EARMI_SIN",
    "EARPI_REE",
    "EARPI_SBM",
    "EARPI_SIN",
    "EARMF_REE",
    "EARMF_SBM",
    "EARMF_SIN",
    "EARPF_REE",
    "EARPF_SBM",
    "EARPF_SIN",
    "GTER_SBM",
    "GTER_SIN",
    "GHID_UHE",
    "GHID_SBM",
    "GHID_SIN",
    "GUNS_SBM",
    "GUNS_SIN",
    "ENAA_REE",
    "ENAA_SBM",
    "ENAA_SIN",
    "ENAC_REE",
    "ENAC_SBM",
    "ENAC_SIN",
    "MER_SBM",
    "MER_SIN",
    "MERL_SBM",
    "MERL_SIN",
    "DEF_SBM",
    "DEF_SIN",
    "VARPI_UHE",
    "VARPF_UHE",
    "VARMI_UHE",
    "VARMF_UHE",
    "VARMI_REE",
    "VARMF_REE",
    "VARMI_SBM",
    "VARMF_SBM",
    "VARMI_SIN",
    "VARMF_SIN",
    "QINC_UHE",
    "QAFL_UHE",
    "QDEF_UHE",
    "QTUR_UHE",
    "QVER_UHE",
    "QRET_UHE",
    "QEVP_UHE",
    "QDES_UHE",
    "EVERT_UHE",
    "EVERNT_UHE",
    "EVER_UHE",
    "EVERT_REE",
    "EVERNT_REE",
    "EVER_REE",
    "EVERT_SBM",
    "EVERNT_SBM",
    "EVER_SBM",
    "EVERT_SIN",
    "EVERNT_SIN",
    "EVER_SIN",
    "GTER_UTE",
    "INT_SBP",
    "INTL_SBP",
]

SYNTHESIS_DEPENDENCIES: dict[OperationSynthesis, list[OperationSynthesis]] = {
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SpatialResolution.SUBMERCADO,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SUBMERCADO,
        ),
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.GERACAO_TERMICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.GERACAO_USINAS_NAO_SIMULADAS,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.GERACAO_USINAS_NAO_SIMULADAS,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.MERCADO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.MERCADO,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.MERCADO_LIQUIDO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.MERCADO_LIQUIDO,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.DEFICIT,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.DEFICIT,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_TURBINAVEL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_TURBINAVEL,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_TURBINAVEL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            SpatialResolution.SUBMERCADO,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_TURBINAVEL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ),
    ],
}

UNITS: dict[OperationSynthesis, Unit] = {
    OperationSynthesis(
        Variable.CUSTO_MARGINAL_OPERACAO, SpatialResolution.SUBMERCADO
    ): Unit.RS_MWh,
    OperationSynthesis(
        Variable.VALOR_AGUA, SpatialResolution.RESERVATORIO_EQUIVALENTE
    ): Unit.RS_MWh,
    OperationSynthesis(
        Variable.VALOR_AGUA, SpatialResolution.USINA_HIDROELETRICA
    ): Unit.RS_hm3,
    OperationSynthesis(
        Variable.CUSTO_GERACAO_TERMICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.kRS,
    OperationSynthesis(
        Variable.CUSTO_GERACAO_TERMICA,
        SpatialResolution.USINA_TERMELETRICA,
    ): Unit.kRS,
    OperationSynthesis(
        Variable.CUSTO_OPERACAO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.kRS,
    OperationSynthesis(
        Variable.CUSTO_FUTURO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.kRS,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.perc,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.perc,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.perc,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.perc,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.perc,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.perc,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.GERACAO_USINAS_NAO_SIMULADAS,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.GERACAO_USINAS_NAO_SIMULADAS,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.GERACAO_TERMICA,
        SpatialResolution.USINA_TERMELETRICA,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.GERACAO_TERMICA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.GERACAO_TERMICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_TURBINAVEL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_TURBINAVEL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_TURBINAVEL,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_TURBINAVEL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_NAO_TURBINAVEL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.VAZAO_TURBINADA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_TURBINADA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_TURBINADA,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_TURBINADA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_VERTIDA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_VERTIDA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_VERTIDA,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_VERTIDA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_AFLUENTE,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_AFLUENTE,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_AFLUENTE,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_AFLUENTE,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DESVIADA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DESVIADA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DESVIADA,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DESVIADA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_RETIRADA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_RETIRADA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_RETIRADA,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_RETIRADA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_EVAPORADA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_EVAPORADA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_EVAPORADA,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_EVAPORADA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.perc,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.perc,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.INTERCAMBIO,
        SpatialResolution.PAR_SUBMERCADOS,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.INTERCAMBIO_LIQUIDO,
        SpatialResolution.PAR_SUBMERCADOS,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.DEFICIT,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.DEFICIT,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.MERCADO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.MERCADO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.MERCADO_LIQUIDO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmed,
    OperationSynthesis(
        Variable.MERCADO_LIQUIDO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmed,
}
