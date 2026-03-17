"""Unit tests for app/utils/operations.py — Polars-native statistics functions."""

import math

import numpy as np
import polars as pl
import pytest

from app.internal.constants import (
    BLOCK_COL,
    PROBABILITY_COL,
    SCENARIO_COL,
    STAGE_COL,
    VALUE_COL,
)
from app.utils.operations import (
    _calc_mean_std_pl,
    _calc_quantiles_pl,
    calc_statistics,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

QUANTILES_SUBSET = [0.0, 0.25, 0.5, 0.75, 1.0]


def _make_simple_df(
    stages: list[int],
    scenarios: list[int],
    blocks: list[int],
    values: list[float],
) -> pl.DataFrame:
    """Build a minimal operation synthesis DataFrame for testing."""
    return pl.DataFrame(
        {
            STAGE_COL: stages,
            SCENARIO_COL: scenarios,
            BLOCK_COL: blocks,
            VALUE_COL: values,
        }
    )


def _make_probs_df(
    stages: list[int],
    scenarios: list[int],
    probs: list[float],
) -> pl.DataFrame:
    """Build a minimal probabilities DataFrame."""
    return pl.DataFrame(
        {
            STAGE_COL: stages,
            SCENARIO_COL: scenarios,
            VALUE_COL: probs,
        }
    )


# ---------------------------------------------------------------------------
# Tests for _calc_quantiles_pl
# ---------------------------------------------------------------------------


class TestCalcQuantilesPl:
    def test_quantile_labels_are_correct(self) -> None:
        df = _make_simple_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            blocks=[1, 1, 1, 1],
            values=[10.0, 20.0, 30.0, 40.0],
        )
        result = _calc_quantiles_pl(df, QUANTILES_SUBSET)
        labels = set(result[SCENARIO_COL].to_list())
        assert labels == {"min", "p25", "median", "p75", "max"}

    def test_quantile_min_value(self) -> None:
        values = [10.0, 20.0, 30.0, 40.0]
        df = _make_simple_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            blocks=[1, 1, 1, 1],
            values=values,
        )
        result = _calc_quantiles_pl(df, [0.0])
        min_row = result.filter(pl.col(SCENARIO_COL) == "min")
        assert abs(min_row[VALUE_COL][0] - min(values)) < 1e-9

    def test_quantile_max_value(self) -> None:
        values = [10.0, 20.0, 30.0, 40.0]
        df = _make_simple_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            blocks=[1, 1, 1, 1],
            values=values,
        )
        result = _calc_quantiles_pl(df, [1.0])
        max_row = result.filter(pl.col(SCENARIO_COL) == "max")
        assert abs(max_row[VALUE_COL][0] - max(values)) < 1e-9

    def test_median_matches_numpy(self) -> None:
        values = [10.0, 20.0, 30.0, 40.0]
        df = _make_simple_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            blocks=[1, 1, 1, 1],
            values=values,
        )
        result = _calc_quantiles_pl(df, [0.5])
        median_row = result.filter(pl.col(SCENARIO_COL) == "median")
        expected = float(np.quantile(values, 0.5))
        assert abs(median_row[VALUE_COL][0] - expected) < 1e-6

    def test_empty_dataframe_returns_empty(self) -> None:
        df = pl.DataFrame(
            {
                STAGE_COL: pl.Series([], dtype=pl.Int64),
                SCENARIO_COL: pl.Series([], dtype=pl.Int64),
                BLOCK_COL: pl.Series([], dtype=pl.Int64),
                VALUE_COL: pl.Series([], dtype=pl.Float64),
            }
        )
        result = _calc_quantiles_pl(df, [0.0, 0.5, 1.0])
        assert result.is_empty()

    def test_multiple_stages_grouped_independently(self) -> None:
        # Stage 1 values: 1, 2; Stage 2 values: 10, 20
        df = _make_simple_df(
            stages=[1, 1, 2, 2],
            scenarios=[1, 2, 1, 2],
            blocks=[1, 1, 1, 1],
            values=[1.0, 2.0, 10.0, 20.0],
        )
        result = _calc_quantiles_pl(df, [0.0, 1.0])
        stage1_min = result.filter(
            (pl.col(STAGE_COL) == 1) & (pl.col(SCENARIO_COL) == "min")
        )[VALUE_COL][0]
        stage2_max = result.filter(
            (pl.col(STAGE_COL) == 2) & (pl.col(SCENARIO_COL) == "max")
        )[VALUE_COL][0]
        assert abs(stage1_min - 1.0) < 1e-9
        assert abs(stage2_max - 20.0) < 1e-9


# ---------------------------------------------------------------------------
# Tests for _calc_mean_std_pl
# ---------------------------------------------------------------------------


class TestCalcMeanStdPl:
    def test_weighted_mean_matches_numpy_average(self) -> None:
        values = [10.0, 20.0, 30.0, 40.0]
        weights = [0.1, 0.4, 0.3, 0.2]
        df = _make_simple_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            blocks=[1, 1, 1, 1],
            values=values,
        )
        probs = _make_probs_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            probs=weights,
        )
        result = _calc_mean_std_pl(df, probs)
        mean_row = result.filter(pl.col(SCENARIO_COL) == "mean")
        expected_mean = float(np.average(values, weights=weights))
        assert abs(mean_row[VALUE_COL][0] - expected_mean) < 1e-6

    def test_weighted_std_matches_manual_calculation(self) -> None:
        values = np.array([10.0, 20.0, 30.0, 40.0])
        weights = np.array([0.1, 0.4, 0.3, 0.2])
        df = _make_simple_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            blocks=[1, 1, 1, 1],
            values=values.tolist(),
        )
        probs = _make_probs_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            probs=weights.tolist(),
        )
        result = _calc_mean_std_pl(df, probs)
        std_row = result.filter(pl.col(SCENARIO_COL) == "std")

        # Manual weighted std matching the original pandas implementation
        n = len(values)
        w_mean = np.average(values, weights=weights)
        numerator = np.sum(weights * (values - w_mean) ** 2)
        denominator = ((n - 1) / n) * np.sum(weights)
        expected_std = float(np.sqrt(numerator / denominator))
        assert abs(std_row[VALUE_COL][0] - expected_std) < 1e-6

    def test_uniform_weights_mean_equals_arithmetic_mean(self) -> None:
        values = [5.0, 15.0, 25.0, 35.0]
        weights = [0.25, 0.25, 0.25, 0.25]
        df = _make_simple_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            blocks=[1, 1, 1, 1],
            values=values,
        )
        probs = _make_probs_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            probs=weights,
        )
        result = _calc_mean_std_pl(df, probs)
        mean_row = result.filter(pl.col(SCENARIO_COL) == "mean")
        expected_mean = float(np.mean(values))
        assert abs(mean_row[VALUE_COL][0] - expected_mean) < 1e-6

    def test_empty_dataframe_returns_empty(self) -> None:
        df = pl.DataFrame(
            {
                STAGE_COL: pl.Series([], dtype=pl.Int64),
                SCENARIO_COL: pl.Series([], dtype=pl.Int64),
                BLOCK_COL: pl.Series([], dtype=pl.Int64),
                VALUE_COL: pl.Series([], dtype=pl.Float64),
            }
        )
        probs = _make_probs_df(stages=[], scenarios=[], probs=[])
        result = _calc_mean_std_pl(df, probs)
        assert result.is_empty()

    def test_zero_sum_weights_std_is_nan(self) -> None:
        # All-zero probabilities should produce NaN std, not crash
        values = [10.0, 20.0]
        weights = [0.0, 0.0]
        df = _make_simple_df(
            stages=[1, 1],
            scenarios=[1, 2],
            blocks=[1, 1],
            values=values,
        )
        probs = _make_probs_df(
            stages=[1, 1],
            scenarios=[1, 2],
            probs=weights,
        )
        result = _calc_mean_std_pl(df, probs)
        std_row = result.filter(pl.col(SCENARIO_COL) == "std")
        std_val = std_row[VALUE_COL][0]
        assert std_val is None or math.isnan(float(std_val))

    def test_returns_mean_and_std_rows(self) -> None:
        df = _make_simple_df(
            stages=[1, 1, 1],
            scenarios=[1, 2, 3],
            blocks=[1, 1, 1],
            values=[1.0, 2.0, 3.0],
        )
        probs = _make_probs_df(
            stages=[1, 1, 1],
            scenarios=[1, 2, 3],
            probs=[1 / 3, 1 / 3, 1 / 3],
        )
        result = _calc_mean_std_pl(df, probs)
        labels = set(result[SCENARIO_COL].to_list())
        assert "mean" in labels
        assert "std" in labels


# ---------------------------------------------------------------------------
# Tests for calc_statistics (integration of quantiles + mean/std)
# ---------------------------------------------------------------------------


class TestCalcStatistics:
    def test_returns_polars_dataframe(self) -> None:
        df = _make_simple_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            blocks=[1, 1, 1, 1],
            values=[10.0, 20.0, 30.0, 40.0],
        )
        probs = _make_probs_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            probs=[0.25, 0.25, 0.25, 0.25],
        )
        result = calc_statistics(df, probs)
        assert isinstance(result, pl.DataFrame)

    def test_scenario_column_contains_expected_labels(self) -> None:
        df = _make_simple_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            blocks=[1, 1, 1, 1],
            values=[10.0, 20.0, 30.0, 40.0],
        )
        probs = _make_probs_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            probs=[0.25, 0.25, 0.25, 0.25],
        )
        result = calc_statistics(df, probs)
        labels = set(result[SCENARIO_COL].to_list())
        # Must contain quantile labels and mean/std
        assert "min" in labels
        assert "max" in labels
        assert "median" in labels
        assert "mean" in labels
        assert "std" in labels

    def test_weighted_mean_matches_numpy_average(self) -> None:
        values = [10.0, 20.0, 30.0, 40.0]
        weights = [0.1, 0.4, 0.3, 0.2]
        df = _make_simple_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            blocks=[1, 1, 1, 1],
            values=values,
        )
        probs = _make_probs_df(
            stages=[1, 1, 1, 1],
            scenarios=[1, 2, 3, 4],
            probs=weights,
        )
        result = calc_statistics(df, probs)
        mean_row = result.filter(pl.col(SCENARIO_COL) == "mean")
        expected_mean = float(np.average(values, weights=weights))
        assert abs(mean_row[VALUE_COL][0] - expected_mean) < 1e-6

    def test_empty_dataframe_returns_empty(self) -> None:
        df = pl.DataFrame(
            {
                STAGE_COL: pl.Series([], dtype=pl.Int64),
                SCENARIO_COL: pl.Series([], dtype=pl.Int64),
                BLOCK_COL: pl.Series([], dtype=pl.Int64),
                VALUE_COL: pl.Series([], dtype=pl.Float64),
            }
        )
        probs = pl.DataFrame(
            {
                STAGE_COL: pl.Series([], dtype=pl.Int64),
                SCENARIO_COL: pl.Series([], dtype=pl.Int64),
                VALUE_COL: pl.Series([], dtype=pl.Float64),
            }
        )
        result = calc_statistics(df, probs)
        assert isinstance(result, pl.DataFrame)
        assert result.is_empty()

    def test_value_col_present_in_result(self) -> None:
        df = _make_simple_df(
            stages=[1, 1],
            scenarios=[1, 2],
            blocks=[1, 1],
            values=[5.0, 10.0],
        )
        probs = _make_probs_df(
            stages=[1, 1],
            scenarios=[1, 2],
            probs=[0.5, 0.5],
        )
        result = calc_statistics(df, probs)
        assert VALUE_COL in result.columns
        assert SCENARIO_COL in result.columns
