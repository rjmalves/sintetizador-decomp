import pandas as pd  # type: ignore

from app.internal.constants import (
    EER_CODE_COL,
    HYDRO_CODE_COL,
    IDENTIFICATION_COLUMNS,
    LOWER_BOUND_COL,
    SUBMARKET_CODE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.utils.operations import fast_group_df


def group_hydro_df(
    df: pd.DataFrame, grouping_column: str | None = None
) -> pd.DataFrame:
    valid_grouping_columns = [
        HYDRO_CODE_COL,
        EER_CODE_COL,
        SUBMARKET_CODE_COL,
    ]

    grouping_column_map: dict[str, list[str]] = {
        HYDRO_CODE_COL: [
            HYDRO_CODE_COL,
            EER_CODE_COL,
            SUBMARKET_CODE_COL,
        ],
        EER_CODE_COL: [
            EER_CODE_COL,
            SUBMARKET_CODE_COL,
        ],
        SUBMARKET_CODE_COL: [SUBMARKET_CODE_COL],
    }

    mapped_columns = (
        grouping_column_map[grouping_column] if grouping_column else []
    )
    grouping_columns = mapped_columns + [
        c
        for c in df.columns
        if c in IDENTIFICATION_COLUMNS and c not in valid_grouping_columns
    ]

    grouped_df = fast_group_df(
        df,
        grouping_columns,
        [VALUE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL],
        operation="sum",
    )

    return grouped_df


def group_submarket_df(
    df: pd.DataFrame, grouping_column: str | None = None
) -> pd.DataFrame:
    valid_grouping_columns = [
        SUBMARKET_CODE_COL,
    ]

    grouping_column_map: dict[str, list[str]] = {
        SUBMARKET_CODE_COL: [SUBMARKET_CODE_COL],
    }

    mapped_columns = (
        grouping_column_map[grouping_column] if grouping_column else []
    )
    grouping_columns = mapped_columns + [
        c
        for c in df.columns
        if c in IDENTIFICATION_COLUMNS and c not in valid_grouping_columns
    ]

    grouped_df = fast_group_df(
        df,
        grouping_columns,
        [VALUE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL],
        operation="sum",
    )

    return grouped_df
