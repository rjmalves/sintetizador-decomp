"""
Deck facade for DECOMP synthesis.

All public methods delegate to focused submodules:
  - accessors      : raw file reading (no caching)
  - infrastructure : cached dadger/relato/relato2 + execution metadata
  - temporal       : stage/block durations, dates, counts
  - entities       : hydro/EER/submarket/thermal entity maps
  - operations     : dec_oper_* data processing
  - reports        : relato-based reports and afluent energy
  - bounds_data    : stored-energy, stored-volume, hydro/thermal/exchange bounds
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Type, TypeVar

import pandas as pd
from idecomp.decomp import (  # type: ignore[attr-defined]
    Dadger,
    Decomptim,
    Hidr,
    InviabUnic,
    Relato,
    Vazoes,
)
from idecomp.decomp.avl_turb_max import AvlTurbMax
from idecomp.decomp.dec_eco_discr import DecEcoDiscr
from idecomp.decomp.dec_fcf_cortes import DecFcfCortes
from idecomp.decomp.dec_oper_gnl import DecOperGnl
from idecomp.decomp.dec_oper_interc import DecOperInterc
from idecomp.decomp.dec_oper_ree import DecOperRee
from idecomp.decomp.dec_oper_sist import DecOperSist
from idecomp.decomp.dec_oper_usih import DecOperUsih
from idecomp.decomp.dec_oper_usit import DecOperUsit

from app.services.deck import (
    accessors,
    entities,
    operations,
    processing,
    reports,
)
from app.services.deck import (
    bounds_data as _bounds_data,
)
from app.services.deck import (
    infrastructure as _infra,
)
from app.services.deck import (
    temporal as _temporal,
)
from app.services.unitofwork import AbstractUnitOfWork


# fmt: off
class Deck:
    T = TypeVar("T")
    logger: Optional[logging.Logger] = None
    DECK_DATA_CACHING: Dict[str, Any] = {}

    @classmethod
    def _c(cls) -> Dict[str, Any]:
        return cls.DECK_DATA_CACHING

    @classmethod
    def _log(cls, msg: str, level: int = logging.INFO) -> None:
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _validate_data(cls, data: Any, type: Type[T], msg: str = "dados") -> T:
        if not isinstance(data, type):
            if cls.logger is not None:
                cls.logger.error(f"Erro na leitura de {msg}")
            raise RuntimeError()
        return data

    # --- Raw file accessors (uncached, thin wrappers around accessors module) ---

    @classmethod
    def _get_dadger(cls, uow: AbstractUnitOfWork) -> Dadger:
        return accessors.get_dadger(uow)

    @classmethod
    def _get_relato(cls, uow: AbstractUnitOfWork) -> Relato:
        return accessors.get_relato(uow)

    @classmethod
    def _get_relato2(cls, uow: AbstractUnitOfWork) -> Relato:
        return accessors.get_relato2(uow)

    @classmethod
    def _get_inviabunic(cls, uow: AbstractUnitOfWork) -> InviabUnic:
        return accessors.get_inviabunic(uow)

    @classmethod
    def _get_decomptim(cls, uow: AbstractUnitOfWork) -> Decomptim:
        return accessors.get_decomptim(uow)

    @classmethod
    def _get_vazoes(cls, uow: AbstractUnitOfWork) -> Vazoes:
        return accessors.get_vazoes(uow)

    @classmethod
    def _get_hidr(cls, uow: AbstractUnitOfWork) -> Hidr:
        return accessors.get_hidr(uow)

    @classmethod
    def _get_dec_eco_discr(cls, uow: AbstractUnitOfWork) -> DecEcoDiscr:
        return accessors.get_dec_eco_discr(uow)

    @classmethod
    def _get_dec_oper_sist(cls, uow: AbstractUnitOfWork) -> DecOperSist:
        return accessors.get_dec_oper_sist(uow)

    @classmethod
    def _get_dec_oper_ree(cls, uow: AbstractUnitOfWork) -> DecOperRee:
        return accessors.get_dec_oper_ree(uow)

    @classmethod
    def _get_dec_oper_usih(cls, uow: AbstractUnitOfWork) -> DecOperUsih:
        return accessors.get_dec_oper_usih(uow)

    @classmethod
    def _get_dec_oper_usit(cls, uow: AbstractUnitOfWork) -> DecOperUsit:
        return accessors.get_dec_oper_usit(uow)

    @classmethod
    def _get_dec_oper_gnl(cls, uow: AbstractUnitOfWork) -> DecOperGnl:
        return accessors.get_dec_oper_gnl(uow)

    @classmethod
    def _get_dec_oper_interc(cls, uow: AbstractUnitOfWork) -> DecOperInterc:
        return accessors.get_dec_oper_interc(uow)

    @classmethod
    def _get_avl_turb_max(cls, uow: AbstractUnitOfWork) -> AvlTurbMax:
        return accessors.get_avl_turb_max(uow)

    @classmethod
    def _get_dec_fcf_cortes(cls, stage: int, uow: AbstractUnitOfWork) -> Optional[DecFcfCortes]:
        return accessors.get_dec_fcf_cortes(stage, uow)

    # --- Cached primary file objects (infrastructure) ---

    @classmethod
    def dadger(cls, uow: AbstractUnitOfWork) -> Dadger:
        return _infra.dadger(cls._c(), uow)

    @classmethod
    def relato(cls, uow: AbstractUnitOfWork) -> Relato:
        return _infra.relato(cls._c(), uow)

    @classmethod
    def relato2(cls, uow: AbstractUnitOfWork) -> Relato:
        return _infra.relato2(cls._c(), uow)

    # --- Infrastructure / execution metadata ---

    @classmethod
    def costs(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _infra.costs(cls._c(), uow)

    @classmethod
    def convergence(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _infra.convergence(cls._c(), uow)

    @classmethod
    def infeasibilities_iterations(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _infra.infeasibilities_iterations(cls._c(), uow)

    @classmethod
    def infeasibilities_final_simulation(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _infra.infeasibilities_final_simulation(cls._c(), uow)

    @classmethod
    def infeasibilities(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _infra.infeasibilities(cls._c(), uow)

    @classmethod
    def runtimes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _infra.runtimes(cls._c(), uow)

    @classmethod
    def probabilities(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _infra.probabilities(cls._c(), uow)

    @classmethod
    def expanded_probabilities(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _infra.expanded_probabilities(cls._c(), uow)

    # --- Temporal ---

    @classmethod
    def study_starting_date(cls, uow: AbstractUnitOfWork) -> datetime:
        return _temporal.study_starting_date(cls._c(), uow)

    @classmethod
    def dec_eco_discr(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _temporal.dec_eco_discr(cls._c(), uow)

    @classmethod
    def blocks(cls, uow: AbstractUnitOfWork) -> List[int]:
        return _temporal.blocks(cls._c(), uow)

    @classmethod
    def stages_durations(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _temporal.stages_durations(cls._c(), uow)

    @classmethod
    def blocks_durations(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _temporal.blocks_durations(cls._c(), uow)

    @classmethod
    def stages_start_date(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        return _temporal.stages_start_date(cls._c(), uow)

    @classmethod
    def stages_end_date(cls, uow: AbstractUnitOfWork) -> List[datetime]:
        return _temporal.stages_end_date(cls._c(), uow)

    @classmethod
    def num_stages(cls, uow: AbstractUnitOfWork) -> int:
        return _temporal.num_stages(cls._c(), uow)

    @classmethod
    def version(cls, uow: AbstractUnitOfWork) -> str:
        return _temporal.version(cls._c(), uow)

    @classmethod
    def title(cls, uow: AbstractUnitOfWork) -> str:
        return _temporal.title(cls._c(), uow)

    # --- Entities ---

    @classmethod
    def hydro_eer_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.hydro_eer_submarket_map(cls._c(), uow)

    @classmethod
    def eers(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.eers(cls._c(), uow)

    @classmethod
    def submarkets(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.submarkets(cls._c(), uow)

    @classmethod
    def thermals(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return entities.thermals(cls._c(), uow)

    # --- Processing helpers (private facade shims used by submodules) ---

    @classmethod
    def _add_dates_to_df(cls, df: pd.DataFrame, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return processing.add_dates_to_df(df, uow)

    @classmethod
    def _add_dates_to_df_merge(cls, df: pd.DataFrame, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return processing.add_dates_to_df_merge(df, uow)

    @classmethod
    def _add_stages_durations_to_df(cls, df: pd.DataFrame, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return processing.add_stages_durations_to_df(df, uow)

    @classmethod
    def _add_block_durations_to_df(cls, df: pd.DataFrame, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return processing.add_block_durations_to_df(df, uow)

    @classmethod
    def _fill_average_block_in_df(cls, df: pd.DataFrame, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return processing.fill_average_block_in_df(df, uow)

    @classmethod
    def _expand_scenarios_in_df_single_stochastic_stage(cls, df: pd.DataFrame, stage: int, num_scenarios: int) -> pd.DataFrame:
        return processing.expand_scenarios_in_df_single_stochastic_stage(df, stage, num_scenarios)

    @classmethod
    def _expand_scenarios_in_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        return processing.expand_scenarios_in_df(df)

    # --- Operations helpers (private facade shims used by submodules) ---

    @staticmethod
    def _stub_nodes_scenarios_v31_0_2(df: pd.DataFrame) -> pd.DataFrame:
        return operations._stub_nodes_scenarios_v31_0_2(df)

    @classmethod
    def _add_eer_sbm_to_df(cls, df: pd.DataFrame, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations._add_eer_sbm_to_df(df, uow)

    @classmethod
    def _add_iv_submarket_code(cls, df: pd.DataFrame) -> pd.DataFrame:
        return operations._add_iv_submarket_code(df)

    @classmethod
    def _eval_net_exchange(cls, df: pd.DataFrame, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations._eval_net_exchange(df, uow)

    @classmethod
    def _get_hydro_flow_operative_constraints(cls, uow: AbstractUnitOfWork, type: str) -> pd.DataFrame:
        return _bounds_data.get_hydro_flow_operative_constraints(cls._c(), uow, type)

    # --- Operations data (dec_oper_*) ---

    @classmethod
    def dec_oper_sist(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_sist(cls._c(), uow)

    @classmethod
    def dec_oper_ree(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_ree(cls._c(), uow)

    @classmethod
    def dec_oper_usih(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_usih(cls._c(), uow)

    @classmethod
    def dec_oper_usit(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_usit(cls._c(), uow)

    @classmethod
    def dec_oper_gnl(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_gnl(cls._c(), uow)

    @classmethod
    def dec_oper_interc(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_interc(cls._c(), uow)

    @classmethod
    def dec_oper_interc_net(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_oper_interc_net(cls._c(), uow)

    @classmethod
    def avl_turb_max(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.avl_turb_max(cls._c(), uow)

    @classmethod
    def _dec_fcf_cortes_per_stage(cls, stage: int, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations._dec_fcf_cortes_per_stage(cls._c(), stage, uow)

    @classmethod
    def dec_fcf_cortes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.dec_fcf_cortes(cls._c(), uow)

    @classmethod
    def cortes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.cortes(cls._c(), uow)

    @classmethod
    def variaveis_cortes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return operations.variaveis_cortes(cls._c(), uow)

    # --- Reports ---

    @classmethod
    def _merge_relato_relato2_df_data(cls, relato_df: pd.DataFrame, relato2_df: pd.DataFrame, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return reports._merge_relato_relato2_df_data(relato_df, relato2_df, col, uow)

    @classmethod
    def _merge_relato_relato2_energy_balance_df_data(cls, relato_df: pd.DataFrame, relato2_df: pd.DataFrame, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return reports._merge_relato_relato2_energy_balance_df_data(relato_df, relato2_df, col, uow)

    @classmethod
    def operation_report_data(cls, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return reports.operation_report_data(col, uow)

    @classmethod
    def energy_balance_report_data(cls, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return reports.energy_balance_report_data(col, uow)

    @classmethod
    def _afluent_energy_for_coupling(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return reports._afluent_energy_for_coupling(cls._c(), uow)

    @classmethod
    def eer_afluent_energy(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return reports.eer_afluent_energy(cls._c(), uow)

    @classmethod
    def sbm_afluent_energy(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return reports.sbm_afluent_energy(cls._c(), uow)

    @classmethod
    def _add_eer_sbm_to_expanded_df(cls, df: pd.DataFrame, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return reports._add_eer_sbm_to_expanded_df(df, uow)

    @classmethod
    def hydro_operation_report_data(cls, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return reports.hydro_operation_report_data(col, uow)

    @classmethod
    def hydro_generation_report_data(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return reports.hydro_generation_report_data(uow)

    # --- Bounds data ---

    @classmethod
    def stored_energy_upper_bounds_eer(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _bounds_data.stored_energy_upper_bounds_eer(cls._c(), uow)

    @classmethod
    def stored_energy_lower_bounds_eer(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _bounds_data.stored_energy_lower_bounds_eer(cls._c(), uow)

    @classmethod
    def stored_energy_upper_bounds_sbm(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _bounds_data.stored_energy_upper_bounds_sbm(cls._c(), uow)

    @classmethod
    def stored_volume_upper_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _bounds_data.stored_volume_upper_bounds(cls._c(), uow)

    @classmethod
    def stored_volume_lower_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _bounds_data.stored_volume_lower_bounds(cls._c(), uow)

    @classmethod
    def hydro_spilled_flow_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _bounds_data.hydro_spilled_flow_bounds(cls._c(), uow)

    @classmethod
    def hydro_outflow_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _bounds_data.hydro_outflow_bounds(cls._c(), uow)

    @classmethod
    def hydro_turbined_flow_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _bounds_data.hydro_turbined_flow_bounds(cls._c(), uow)

    @classmethod
    def thermal_generation_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _bounds_data.thermal_generation_bounds(cls._c(), uow)

    @classmethod
    def exchange_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return _bounds_data.exchange_bounds(cls._c(), uow)
# fmt: on
