from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, List

import pandas as pd

if TYPE_CHECKING:
    from app.services.unitofwork import AbstractUnitOfWork


@dataclass
class DeckContext:
    num_stages: int
    num_blocks: int
    stages_start_dates: List[datetime]
    stages_end_dates: List[datetime]
    expanded_probabilities: pd.DataFrame
    hydro_eer_submarket_map: pd.DataFrame

    def __post_init__(self) -> None:
        for field_name, value in self.__dict__.items():
            if value is None:
                raise ValueError(
                    f"DeckContext field '{field_name}' must not be None"
                )

    @classmethod
    def from_deck(cls, uow: "AbstractUnitOfWork") -> "DeckContext":
        from app.services.deck.deck import Deck

        return cls(
            num_stages=Deck.num_stages(uow),
            num_blocks=len(Deck.blocks(uow)),
            stages_start_dates=Deck.stages_start_date(uow),
            stages_end_dates=Deck.stages_end_date(uow),
            expanded_probabilities=Deck.expanded_probabilities(uow),
            hydro_eer_submarket_map=Deck.hydro_eer_submarket_map(uow),
        )
