import logging
from typing import Callable

import pandas as pd

from app.internal.constants import BLOCK_COL
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.timing import time_and_log


def resolve_dec_oper_sist(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do dec_oper_sist",
        logger=logger,
    ):
        df = Deck.dec_oper_sist(uow)
        return post_resolve_file(None, df, col, logger)


def resolve_dec_oper_ree(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do dec_oper_ree",
        logger=logger,
    ):
        df = Deck.dec_oper_ree(uow)
        return post_resolve_file(None, df, col, logger)


def resolve_ena_coupling_eer(
    uow: AbstractUnitOfWork,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    with time_and_log(
        message_root="Tempo para obtenção dos dados de ENA do relato",
        logger=logger,
    ):
        return Deck.eer_afluent_energy(uow)


def resolve_ena_coupling_sbm(
    uow: AbstractUnitOfWork,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    with time_and_log(
        message_root="Tempo para obtenção dos dados de ENA do relato",
        logger=logger,
    ):
        return Deck.sbm_afluent_energy(uow)


def resolve_dec_oper_usih(
    uow: AbstractUnitOfWork,
    col: str,
    blocks: list[int] | None = None,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do dec_oper_usih",
        logger=logger,
    ):
        df = Deck.dec_oper_usih(uow)
        df = post_resolve_file(None, df, col, logger)
        if blocks:
            df = df.loc[df[BLOCK_COL].isin(blocks)].copy()
        return df


def resolve_dec_oper_usit(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do dec_oper_usit",
        logger=logger,
    ):
        df = Deck.dec_oper_usit(uow)
        return post_resolve_file(None, df, col, logger)


def resolve_dec_oper_interc(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do dec_oper_interc",
        logger=logger,
    ):
        df = Deck.dec_oper_interc(uow)
        return post_resolve_file(None, df, col, logger)


def resolve_dec_oper_interc_net(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do dec_oper_interc",
        logger=logger,
    ):
        df = Deck.dec_oper_interc_net(uow)
        return post_resolve_file(None, df, col, logger)


def resolve_hydro_operation_report_block(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    with time_and_log(
        message_root="Tempo para obtenção dos dados dos relato e relato2",
        logger=logger,
    ):
        return Deck.hydro_operation_report_data(col, uow)


def resolve_hydro_generation_report_block(
    uow: AbstractUnitOfWork,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    with time_and_log(
        message_root="Tempo para obtenção dos dados dos relato e relato2",
        logger=logger,
    ):
        return Deck.energy_balance_report_data("geracao_hidraulica", uow)


def resolve_operation_report_block(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    with time_and_log(
        message_root="Tempo para obtenção dos dados dos relato e relato2",
        logger=logger,
    ):
        return Deck.operation_report_data(col, uow)


def resolve_dispatch(
    synthesis: tuple[Variable, SpatialResolution],
    logger: logging.Logger | None = None,
) -> Callable[[AbstractUnitOfWork], pd.DataFrame]:
    """Retorna a função de resolução correspondente à síntese fornecida."""
    V = Variable
    SR = SpatialResolution

    def sist(col: str) -> Callable[[AbstractUnitOfWork], pd.DataFrame]:
        return lambda uow: resolve_dec_oper_sist(uow, col, logger)

    def ree(col: str) -> Callable[[AbstractUnitOfWork], pd.DataFrame]:
        return lambda uow: resolve_dec_oper_ree(uow, col, logger)

    def usih(
        col: str, blocks: list[int] | None = None
    ) -> Callable[[AbstractUnitOfWork], pd.DataFrame]:
        return lambda uow: resolve_dec_oper_usih(uow, col, blocks, logger)

    def usit(col: str) -> Callable[[AbstractUnitOfWork], pd.DataFrame]:
        return lambda uow: resolve_dec_oper_usit(uow, col, logger)

    def interc(col: str) -> Callable[[AbstractUnitOfWork], pd.DataFrame]:
        return lambda uow: resolve_dec_oper_interc(uow, col, logger)

    def interc_net(col: str) -> Callable[[AbstractUnitOfWork], pd.DataFrame]:
        return lambda uow: resolve_dec_oper_interc_net(uow, col, logger)

    def hydro_op(col: str) -> Callable[[AbstractUnitOfWork], pd.DataFrame]:
        return lambda uow: resolve_hydro_operation_report_block(
            uow, col, logger
        )

    def op_report(col: str) -> Callable[[AbstractUnitOfWork], pd.DataFrame]:
        return lambda uow: resolve_operation_report_block(uow, col, logger)

    _rules: dict[
        tuple[Variable, SpatialResolution],
        Callable[[AbstractUnitOfWork], pd.DataFrame],
    ] = {
        (V.CUSTO_MARGINAL_OPERACAO, SR.SUBMERCADO): sist("cmo"),
        (V.CUSTO_GERACAO_TERMICA, SR.SISTEMA_INTERLIGADO): op_report(
            "geracao_termica"
        ),
        (V.CUSTO_OPERACAO, SR.SISTEMA_INTERLIGADO): op_report("custo_presente"),
        (V.CUSTO_FUTURO, SR.SISTEMA_INTERLIGADO): op_report("custo_futuro"),
        (
            V.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
            SR.RESERVATORIO_EQUIVALENTE,
        ): ree("earm_inicial_MWmes"),
        (
            V.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
            SR.RESERVATORIO_EQUIVALENTE,
        ): ree("earm_inicial_percentual"),
        (V.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL, SR.SUBMERCADO): lambda uow: (
            _stub_valid_values_sist(uow, "earm_inicial_MWmes", [0], logger)
        ),
        (V.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL, SR.SUBMERCADO): lambda uow: (
            _stub_valid_values_sist(uow, "earm_inicial_percentual", [0], logger)
        ),
        (
            V.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SR.RESERVATORIO_EQUIVALENTE,
        ): ree("earm_final_MWmes"),
        (
            V.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SR.RESERVATORIO_EQUIVALENTE,
        ): ree("earm_final_percentual"),
        (V.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL, SR.SUBMERCADO): lambda uow: (
            _stub_valid_values_sist(uow, "earm_final_MWmes", [0], logger)
        ),
        (V.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL, SR.SUBMERCADO): lambda uow: (
            _stub_valid_values_sist(uow, "earm_final_percentual", [0], logger)
        ),
        (V.GERACAO_TERMICA, SR.SUBMERCADO): lambda uow: (
            _stub_thermal_submarkets_sist(
                uow, "geracao_termica_total_MW", logger
            )
        ),
        (V.GERACAO_HIDRAULICA, SR.SUBMERCADO): lambda uow: (
            resolve_hydro_generation_report_block(uow, logger)
        ),
        (V.GERACAO_USINAS_NAO_SIMULADAS, SR.SUBMERCADO): sist(
            "geracao_nao_simuladas_MW"
        ),
        (
            V.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO,
            SR.RESERVATORIO_EQUIVALENTE,
        ): lambda uow: resolve_ena_coupling_eer(uow, logger),
        (V.ENERGIA_NATURAL_AFLUENTE_ACOPLAMENTO, SR.SUBMERCADO): lambda uow: (
            resolve_ena_coupling_sbm(uow, logger)
        ),
        (
            V.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
            SR.RESERVATORIO_EQUIVALENTE,
        ): ree("ena_MWmes"),
        (V.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA, SR.SUBMERCADO): sist("ena_MWmes"),
        (V.MERCADO, SR.SUBMERCADO): sist("demanda_MW"),
        (V.MERCADO_LIQUIDO, SR.SUBMERCADO): sist("demanda_liquida_MW"),
        (V.DEFICIT, SR.SUBMERCADO): sist("deficit_MW"),
        (
            V.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SR.USINA_HIDROELETRICA,
        ): lambda uow: _stub_stored_volume_usih(
            uow, "volume_util_inicial_percentual", logger
        ),
        (
            V.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SR.USINA_HIDROELETRICA,
        ): lambda uow: _stub_stored_volume_usih(
            uow, "volume_util_final_percentual", logger
        ),
        (
            V.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SR.USINA_HIDROELETRICA,
        ): lambda uow: _stub_stored_volume_usih(
            uow, "volume_util_inicial_hm3", logger
        ),
        (
            V.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SR.USINA_HIDROELETRICA,
        ): lambda uow: _stub_stored_volume_usih(
            uow, "volume_util_final_hm3", logger
        ),
        (V.VAZAO_INCREMENTAL, SR.USINA_HIDROELETRICA): usih(
            "vazao_incremental_m3s", [0]
        ),
        (V.VAZAO_AFLUENTE, SR.USINA_HIDROELETRICA): usih(
            "vazao_afluente_m3s", [0]
        ),
        (V.VAZAO_DEFLUENTE, SR.USINA_HIDROELETRICA): usih(
            "vazao_defluente_m3s"
        ),
        (V.GERACAO_HIDRAULICA, SR.USINA_HIDROELETRICA): usih("geracao_MW"),
        (V.ENERGIA_VERTIDA_TURBINAVEL, SR.USINA_HIDROELETRICA): hydro_op(
            "vertimento_turbinavel"
        ),
        (V.ENERGIA_VERTIDA_NAO_TURBINAVEL, SR.USINA_HIDROELETRICA): hydro_op(
            "vertimento_nao_turbinavel"
        ),
        (V.VAZAO_TURBINADA, SR.USINA_HIDROELETRICA): usih(
            "vazao_turbinada_m3s"
        ),
        (V.VAZAO_VERTIDA, SR.USINA_HIDROELETRICA): usih("vazao_vertida_m3s"),
        (V.VAZAO_DESVIADA, SR.USINA_HIDROELETRICA): usih("vazao_desviada_m3s"),
        (V.VAZAO_RETIRADA, SR.USINA_HIDROELETRICA): usih("vazao_retirada_m3s"),
        (V.VAZAO_EVAPORADA, SR.USINA_HIDROELETRICA): usih(
            "vazao_evaporada_m3s"
        ),
        (V.GERACAO_TERMICA, SR.USINA_TERMELETRICA): usit("geracao_MW"),
        (V.CUSTO_GERACAO_TERMICA, SR.USINA_TERMELETRICA): usit("custo_geracao"),
        (V.INTERCAMBIO, SR.PAR_SUBMERCADOS): interc("intercambio_origem_MW"),
        (V.INTERCAMBIO_LIQUIDO, SR.PAR_SUBMERCADOS): interc_net(
            "intercambio_origem_MW"
        ),
    }
    return _rules[synthesis]


# ---------------------------------------------------------------------------
# Internal helpers for inline stubs within the dispatch table.
# These replicate the logic from stubs.py without requiring a cls reference,
# to keep the dispatch table self-contained in this module.
# ---------------------------------------------------------------------------


def _stub_valid_values_sist(
    uow: AbstractUnitOfWork,
    col: str,
    blocks: list[int] | None = None,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    from app.internal.constants import VALUE_COL

    df = resolve_dec_oper_sist(uow, col, logger)
    filters = ~df[VALUE_COL].isna()
    if blocks:
        filters &= df[BLOCK_COL].isin(blocks)
    return df.loc[filters].reset_index(drop=True)


def _stub_stored_volume_usih(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    from app.internal.constants import VALUE_COL

    df = resolve_dec_oper_usih(uow, col, logger=logger)
    return df.loc[(df[BLOCK_COL] == 0) & (~df[VALUE_COL].isna())].reset_index(
        drop=True
    )


def _stub_thermal_submarkets_sist(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    from app.internal.constants import SUBMARKET_CODE_COL

    df = resolve_dec_oper_sist(uow, col, logger)
    thermals = Deck.thermals(uow)
    submarkets = thermals[SUBMARKET_CODE_COL].unique().tolist()
    return df.loc[df[SUBMARKET_CODE_COL].isin(submarkets)].reset_index(
        drop=True
    )
