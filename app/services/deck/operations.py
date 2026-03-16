from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

import numpy as np
import pandas as pd
from cfinterface.components.register import Register
from idecomp.decomp.dadger import ACVOLMIN

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    COEF_TYPE_COL,
    COEF_VALUE_COL,
    CUT_INDEX_COL,
    EER_CODE_COL,
    ENTITY_INDEX_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    GTER_COEF_CODE,
    HYDRO_CODE_COL,
    IV_SUBMARKET_CODE,
    ITERATION_COL,
    LAG_COL,
    QDEF_COEF_CODE,
    RHS_COEF_CODE,
    SCENARIO_COL,
    STAGE_COL,
    STATE_VALUE_COL,
    SUBMARKET_CODE_COL,
    THERMAL_CODE_COL,
    VARM_COEF_CODE,
)
from app.model.policy.unit import Unit
from app.services.deck import processing
from app.utils.operations import cast_ac_fields_to_stage

if TYPE_CHECKING:
    from app.services.unitofwork import AbstractUnitOfWork


def _stub_nodes_scenarios_v31_0_2(df: pd.DataFrame) -> pd.DataFrame:
    stages = df[STAGE_COL].unique().tolist()
    df.loc[df[STAGE_COL].isin(stages[:-1]), SCENARIO_COL] = 1
    df.loc[df[STAGE_COL] == stages[-1], SCENARIO_COL] -= len(stages) - 1
    return df.copy()


def _add_iv_submarket_code(df: pd.DataFrame) -> pd.DataFrame:
    for col in [EXCHANGE_SOURCE_CODE_COL, EXCHANGE_TARGET_CODE_COL]:
        df.loc[df[col].isna(), col] = IV_SUBMARKET_CODE
        df[col] = df[col].astype(int)
    return df


def _add_eer_sbm_to_df(
    df: pd.DataFrame, uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    # Assume a ordenação por estagio, cenario, patamar, usina
    from app.services.deck.deck import Deck

    hydro_order = df[HYDRO_CODE_COL].unique().tolist()
    num_blocks_with_average = len(Deck.blocks(uow)) + 1
    num_tiles = df.shape[0] // (len(hydro_order) * num_blocks_with_average)
    map_df = Deck.hydro_eer_submarket_map(uow)
    submarket_codes = map_df.loc[hydro_order, SUBMARKET_CODE_COL].to_numpy()
    eer_codes = map_df.loc[hydro_order, EER_CODE_COL].to_numpy()
    df[SUBMARKET_CODE_COL] = np.tile(
        np.repeat(submarket_codes, num_blocks_with_average), num_tiles
    )
    df[EER_CODE_COL] = np.tile(
        np.repeat(eer_codes, num_blocks_with_average), num_tiles
    )
    return df


def _eval_net_exchange(
    df: pd.DataFrame, uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    from app.services.deck.deck import Deck

    dadger = Deck.dadger(uow)
    registers = dadger.ia()
    registers = Deck._validate_data(registers, list, "registros IA")
    for r in registers:
        direct_filter = (df["nome_submercado_de"] == r.nome_submercado_de) & (
            df["nome_submercado_para"] == r.nome_submercado_para
        )
        reverse_filter = (
            df["nome_submercado_de"] == r.nome_submercado_para
        ) & (df["nome_submercado_para"] == r.nome_submercado_de)
        if df.loc[reverse_filter].empty:
            continue
        for col in ["intercambio_origem_MW", "intercambio_destino_MW"]:
            df.loc[direct_filter, col] -= df.loc[reverse_filter, col].to_numpy()
        df = df.drop(index=df.loc[reverse_filter].index)
    return df


def dec_oper_sist(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "dec_oper_sist"
    df = cache.get(name)
    if df is None:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck._get_dec_oper_sist(uow).tabela,
            pd.DataFrame,
            name,
        )
        version = Deck._validate_data(
            Deck._get_dec_oper_sist(uow).versao,
            str,
            name,
        )
        if version <= "31.0.2":
            df = _stub_nodes_scenarios_v31_0_2(df)
        df = processing.add_dates_to_df(df, uow)
        df = df.rename(columns={"duracao": BLOCK_DURATION_COL})
        df = processing.fill_average_block_in_df(df, uow)
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
        df["geracao_nao_simuladas_MW"] = (
            df["geracao_pequenas_usinas_MW"] + df["geracao_eolica_MW"]
        )
        df = processing.expand_scenarios_in_df(df)
        df = df.sort_values(
            [
                SUBMARKET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
            ]
        ).reset_index(drop=True)
        cache[name] = df
    return df.copy()


def dec_oper_ree(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "dec_oper_ree"
    df = cache.get(name)
    if df is None:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck._get_dec_oper_ree(uow).tabela,
            pd.DataFrame,
            name,
        )
        version = Deck._validate_data(
            Deck._get_dec_oper_ree(uow).versao,
            str,
            name,
        )
        if version <= "31.0.2":
            df = _stub_nodes_scenarios_v31_0_2(df)
        df = processing.add_dates_to_df(df, uow)
        df = processing.add_stages_durations_to_df(df, uow)
        df = processing.expand_scenarios_in_df(df)
        df = df.sort_values(
            [
                EER_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
            ]
        ).reset_index(drop=True)
        cache[name] = df
    return df.copy()


def dec_oper_usih(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    def _cast_volumes_to_absolute(
        df: pd.DataFrame, uow: "AbstractUnitOfWork"
    ) -> pd.DataFrame:
        from app.services.deck.deck import Deck

        hidr_df = Deck._validate_data(
            Deck._get_hidr(uow).cadastro,
            pd.DataFrame,
            "hidr",
        )
        codes = df[HYDRO_CODE_COL].unique()
        volume_columns = [
            "volume_util_maximo_hm3",
            "volume_util_inicial_hm3",
            "volume_util_final_hm3",
        ]
        df["volume_minimo_hm3"] = 0.0
        dadger = Deck.dadger(uow)
        stage_start_dates = Deck.stages_start_date(uow)
        stage_end_dates = Deck.stages_end_date(uow)
        num_stages = len(stage_start_dates)
        for hydro_code in codes:
            regulation = hidr_df.at[hydro_code, "tipo_regulacao"]
            min_volume = hidr_df.at[hydro_code, "volume_minimo"]
            for stage in range(1, num_stages + 1):
                min_volume_ac = dadger.ac(hydro_code, ACVOLMIN)
                if isinstance(min_volume_ac, Register):
                    min_volume_ac = [min_volume_ac]
                elif min_volume_ac is None:
                    min_volume_ac = []
                if min_volume_ac:
                    previous_acs = [
                        ac
                        for ac in min_volume_ac
                        if stage
                        >= cast_ac_fields_to_stage(
                            ac, stage_start_dates, stage_end_dates
                        )
                    ]
                    if len(previous_acs) > 0:
                        min_volume = previous_acs[-1].volume  # type: ignore
                filters = (df[HYDRO_CODE_COL] == hydro_code) & (
                    df[STAGE_COL] == stage
                )
                max_volume = (
                    df.loc[
                        filters,
                        "volume_util_maximo_hm3",
                    ].iloc[0]
                    + min_volume
                )
                is_run_of_river = (min_volume >= max_volume) or (
                    regulation not in ["M", "S"]
                )
                min_volume = np.nan if is_run_of_river else min_volume
                df.loc[filters, "volume_minimo_hm3"] = min_volume
                df.loc[filters, volume_columns] += min_volume
        return df

    name = "dec_oper_usih"
    df = cache.get(name)
    if df is None:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck._get_dec_oper_usih(uow).tabela,
            pd.DataFrame,
            name,
        )
        df = _cast_volumes_to_absolute(df, uow)
        version = Deck._validate_data(
            Deck._get_dec_oper_usih(uow).versao,
            str,
            name,
        )
        if version <= "31.0.2":
            df = _stub_nodes_scenarios_v31_0_2(df)
        df = processing.add_dates_to_df_merge(df, uow)
        df = _add_eer_sbm_to_df(df, uow)
        df = df.rename(columns={"duracao": BLOCK_DURATION_COL})
        df = processing.fill_average_block_in_df(df, uow)
        df = processing.expand_scenarios_in_df(df)
        df = df.sort_values(
            [
                HYDRO_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
            ]
        ).reset_index(drop=True)
        cache[name] = df
    return df.copy()


def dec_oper_usit(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "dec_oper_usit"
    df = cache.get(name)
    if df is None:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck._get_dec_oper_usit(uow).tabela,
            pd.DataFrame,
            name,
        )
        version = Deck._validate_data(
            Deck._get_dec_oper_usit(uow).versao,
            str,
            name,
        )
        if version <= "31.0.2":
            df = _stub_nodes_scenarios_v31_0_2(df)
        df = processing.add_dates_to_df_merge(df, uow)
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
                df.loc[filtro, "geracao_MW"]
                - df.loc[filtro, "geracao_minima_MW"]
            )
            / (
                df.loc[filtro, "geracao_maxima_MW"]
                - df.loc[filtro, "geracao_minima_MW"]
            )
        )
        df.loc[~filtro, "geracao_percentual_flexivel"] = 100.0
        df = df.rename(columns={"duracao": BLOCK_DURATION_COL})
        df = processing.fill_average_block_in_df(df, uow)
        df = processing.expand_scenarios_in_df(df)
        df = df.sort_values(
            [
                THERMAL_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
            ]
        ).reset_index(drop=True)
        cache[name] = df
    return df.copy()


def dec_oper_gnl(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "dec_oper_gnl"
    df = cache.get(name)
    if df is None:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck._get_dec_oper_gnl(uow).tabela,
            pd.DataFrame,
            name,
        )
        version = Deck._validate_data(
            Deck._get_dec_oper_gnl(uow).versao,
            str,
            name,
        )
        if version <= "31.0.2":
            df = _stub_nodes_scenarios_v31_0_2(df)
        df = processing.add_dates_to_df(df, uow)
        cache[name] = df
    return df.copy()


def dec_oper_interc(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "dec_oper_interc"
    df = cache.get(name)
    if df is None:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck._get_dec_oper_interc(uow).tabela,
            pd.DataFrame,
            name,
        )
        version = Deck._validate_data(
            Deck._get_dec_oper_interc(uow).versao,
            str,
            name,
        )
        if version <= "31.0.2":
            df = _stub_nodes_scenarios_v31_0_2(df)
        df = _add_iv_submarket_code(df)
        df = processing.add_dates_to_df(df, uow)
        df = processing.add_block_durations_to_df(df, uow)
        df = processing.fill_average_block_in_df(df, uow)
        df = processing.expand_scenarios_in_df(df)
        df = df.sort_values(
            [
                EXCHANGE_SOURCE_CODE_COL,
                EXCHANGE_TARGET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
            ]
        ).reset_index(drop=True)
        cache[name] = df
    return df.copy()


def dec_oper_interc_net(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "dec_oper_interc_net"
    df = cache.get(name)
    if df is None:
        from app.services.deck.deck import Deck

        df = Deck.dec_oper_interc(uow)
        df = _eval_net_exchange(df, uow)
        df = df.sort_values(
            [
                EXCHANGE_SOURCE_CODE_COL,
                EXCHANGE_TARGET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
            ]
        ).reset_index(drop=True)
        cache[name] = df
    return df.copy()


def avl_turb_max(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "avl_turb_max"
    df = cache.get(name)
    if df is None:
        from app.services.deck.deck import Deck

        df = Deck._validate_data(
            Deck._get_avl_turb_max(uow).tabela,
            pd.DataFrame,
            name,
        )
        df = df.loc[~(df[STAGE_COL].isna())]
        df = df.sort_values(
            [
                HYDRO_CODE_COL,
                STAGE_COL,
            ]
        ).reset_index(drop=True)
        cache[name] = df
    return df.copy()


def _dec_fcf_cortes_per_stage(
    cache: Dict[str, Any], stage: int, uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = f"dec_fcf_cortes_{str(stage).zfill(3)}"
    df: Optional[pd.DataFrame] = cache.get(name)
    if df is None:
        from app.services.deck.deck import Deck

        dec_fcf_cortes = Deck._get_dec_fcf_cortes(stage, uow)
        if dec_fcf_cortes is not None:
            df = Deck._validate_data(
                dec_fcf_cortes.tabela,
                pd.DataFrame,
                name,
            )
            df = df.rename(
                {
                    "indice_iteracao": ITERATION_COL,
                    "indice_lag": LAG_COL,
                    "indice_patamar": BLOCK_COL,
                    "indice_entidade": ENTITY_INDEX_COL,
                    "valor_coeficiente": COEF_VALUE_COL,
                    "ponto_consultado": STATE_VALUE_COL,
                },
                axis=1,
            )
            MAP_COEF_CODE = {
                "VARM": str(VARM_COEF_CODE),
                "-": str(VARM_COEF_CODE),
                "RHS": str(RHS_COEF_CODE),
                "GTERF": str(GTER_COEF_CODE),
                "QDEFP": str(QDEF_COEF_CODE),
            }
            df[COEF_TYPE_COL] = df["tipo_coeficiente"].replace(MAP_COEF_CODE)
            df[COEF_TYPE_COL] = df[COEF_TYPE_COL].astype(int)
            df[STAGE_COL] = df.shape[0] * [stage]
            df[SCENARIO_COL] = df.shape[0] * [np.nan]
            df.drop(columns=["tipo_entidade", "nome_entidade"], inplace=True)
            num_iterations = df[ITERATION_COL].max()
            num_elements = len(
                df.loc[
                    df[ITERATION_COL] == num_iterations, ITERATION_COL
                ].tolist()
            )
            df[CUT_INDEX_COL] = np.repeat(
                list(range(num_iterations, 0, -1)), num_elements
            )
            cache[name] = df
            return df.copy()
        else:
            return pd.DataFrame()
    return df.copy()


def dec_fcf_cortes(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "dec_fcf_cortes"
    df = cache.get(name)
    if df is None:
        from app.services.deck.deck import Deck

        df = pd.DataFrame()
        # TODO melhorar logica para pegar dados de nos que geram cortes
        # a partir do mapcut. A logica atual funciona apenas para casos
        # com moldes de PMO
        cut_stages = list(range(1, Deck.num_stages(uow)))
        for stage in cut_stages:
            df_stage = _dec_fcf_cortes_per_stage(cache, stage, uow)
            df = pd.concat([df, df_stage], ignore_index=True)
        df = df.reset_index(drop=True)
        cache[name] = df
    return df.copy()


def cortes(cache: Dict[str, Any], uow: "AbstractUnitOfWork") -> pd.DataFrame:
    name = "cortes"
    df = cache.get(name)
    if df is None:
        df = dec_fcf_cortes(cache, uow)
        df = df[
            [
                STAGE_COL,
                CUT_INDEX_COL,
                ITERATION_COL,
                SCENARIO_COL,
                COEF_TYPE_COL,
                ENTITY_INDEX_COL,
                LAG_COL,
                BLOCK_COL,
                COEF_VALUE_COL,
                STATE_VALUE_COL,
            ]
        ]
        df = df.reset_index(drop=True)
        cache[name] = df
    return df.copy()


def variaveis_cortes(
    cache: Dict[str, Any], uow: "AbstractUnitOfWork"
) -> pd.DataFrame:
    name = "variaveis_cortes"
    df = cache.get(name)
    MAP_COEF_TYPE_SHORT_NAME = {
        RHS_COEF_CODE: "RHS",
        VARM_COEF_CODE: "VARM",
        GTER_COEF_CODE: "GTER",
        QDEF_COEF_CODE: "QDEF",
    }
    MAP_COEF_TYPE_LONG_NAME = {
        RHS_COEF_CODE: "Right Hand Side",
        VARM_COEF_CODE: "Volume armazenado",
        GTER_COEF_CODE: "Geração térmica antecipada",
        QDEF_COEF_CODE: "Vazão defluente por tempo de viagem",
    }
    MAP_COEF_TYPE_UNIT = {
        RHS_COEF_CODE: Unit.kRS.value,
        VARM_COEF_CODE: Unit.kRS_hm3.value,
        GTER_COEF_CODE: Unit.kRS_MWh.value,
        QDEF_COEF_CODE: Unit.kRS_hm3.value,
    }
    MAP_COEF_TYPE_STATE_UNIT = {
        RHS_COEF_CODE: Unit.kRS.value,
        VARM_COEF_CODE: Unit.hm3.value,
        GTER_COEF_CODE: Unit.MWh.value,
        QDEF_COEF_CODE: Unit.hm3.value,
    }
    if df is None:
        cuts_df = dec_fcf_cortes(cache, uow)
        df = cuts_df[[COEF_TYPE_COL]].drop_duplicates()
        df["nome_curto_coeficiente"] = df[COEF_TYPE_COL].replace(
            MAP_COEF_TYPE_SHORT_NAME
        )
        df["nome_longo_coeficiente"] = df[COEF_TYPE_COL].replace(
            MAP_COEF_TYPE_LONG_NAME
        )
        df["unidade_coeficiente"] = df[COEF_TYPE_COL].replace(
            MAP_COEF_TYPE_UNIT
        )
        df["unidade_estado"] = df[COEF_TYPE_COL].replace(
            MAP_COEF_TYPE_STATE_UNIT
        )
        df = df.reset_index(drop=True)
        cache[name] = df
    return df.copy()
