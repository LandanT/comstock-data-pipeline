"""Phase 4: Per-building EUI conversion and population-level summary statistics."""
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from config import PipelineConfig
from pipeline.discover import DatasetManifest
from pipeline.filter import FilteredData

log = logging.getLogger(__name__)

KWH_TO_KBTU = 3.412141633   # 1 kWh = 3.412141633 kBtu


@dataclass
class SummaryResults:
    summary_table: pd.DataFrame           # wide: one row per upgrade (stock-level, all buildings)
    summary_long: Optional[pd.DataFrame]  # long: one row per upgrade × end-use × statistic
    building_detail: Optional[pd.DataFrame]
    summary_compact: Optional[pd.DataFrame]    # focused end-use EUI table (Key Metrics)
    summary_applicable: Optional[pd.DataFrame] # same as summary_table but applicable buildings only
    not_applicable_upgrades: list[int]
    filter_funnel: dict
    config_used: PipelineConfig
    manifest: DatasetManifest
    n_baseline_buildings: int
    represented_area_ft2: float


# ---------------------------------------------------------------------------
# Weighted statistics helpers
# ---------------------------------------------------------------------------

def _weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    """Compute the weighted quantile of values."""
    mask = ~(np.isnan(values) | np.isnan(weights))
    values = values[mask]
    weights = weights[mask]
    if len(values) == 0:
        return np.nan
    sorter = np.argsort(values)
    values = values[sorter]
    weights = weights[sorter]
    cum_weights = np.cumsum(weights)
    total = cum_weights[-1]
    return float(np.interp(q * total, cum_weights, values))


def _compute_stats(
    series: pd.Series,
    weights: Optional[pd.Series],
    statistics: list[str],
    use_weights: bool,
) -> dict[str, float]:
    vals = pd.to_numeric(series, errors="coerce").values
    if use_weights and weights is not None:
        w = pd.to_numeric(weights, errors="coerce").values
    else:
        w = None

    result = {}
    for stat in statistics:
        if stat == "mean":
            if w is not None:
                mask = ~(np.isnan(vals) | np.isnan(w))
                result["mean"] = float(np.average(vals[mask], weights=w[mask])) if mask.sum() > 0 else np.nan
            else:
                result["mean"] = float(np.nanmean(vals))
        elif stat == "median":
            if w is not None:
                result["median"] = _weighted_quantile(vals, w, 0.5)
            else:
                result["median"] = float(np.nanmedian(vals))
        elif stat == "p25":
            if w is not None:
                result["p25"] = _weighted_quantile(vals, w, 0.25)
            else:
                result["p25"] = float(np.nanpercentile(vals, 25))
        elif stat == "p75":
            if w is not None:
                result["p75"] = _weighted_quantile(vals, w, 0.75)
            else:
                result["p75"] = float(np.nanpercentile(vals, 75))
    return result


# ---------------------------------------------------------------------------
# Column resolution
# ---------------------------------------------------------------------------

def _find_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """Find column by exact match first, then by prefix match (handles ..unit suffixes)."""
    for c in candidates:
        if c in df.columns:
            return c
    # Prefix match: handles columns like "in.sqft..ft2" when searching for "in.sqft"
    col_prefixes = {col.split("..")[0]: col for col in df.columns}
    for c in candidates:
        if c in col_prefixes:
            return col_prefixes[c]
    return None


def _resolve_sqft_col(df: pd.DataFrame) -> Optional[str]:
    return _find_col(df, ["in.sqft", "in.floor_area_ft2"])


def _resolve_weight_col(df: pd.DataFrame) -> Optional[str]:
    return _find_col(df, ["weight", "sample_weight"])


def _resolve_bldg_id_col(df: pd.DataFrame) -> Optional[str]:
    return _find_col(df, ["bldg_id", "building_id"])


# ---------------------------------------------------------------------------
# EUI conversion
# ---------------------------------------------------------------------------

def _detect_col_unit(col: str) -> Optional[str]:
    """Extract unit from double-dot suffix, e.g. 'out.elec.cooling.energy_consumption..kwh' -> 'kwh'."""
    m = re.search(r"\.\.(.+)$", col)
    return m.group(1) if m else None


def _unit_to_kbtu_factor(unit: Optional[str], manifest_default: str) -> float:
    """Return factor to multiply to convert to kBtu."""
    u = (unit or manifest_default or "kwh").lower()
    if "kwh" in u:
        return KWH_TO_KBTU
    if "kbtu" in u:
        return 1.0
    if "mwh" in u:
        return KWH_TO_KBTU * 1000
    if "tbtu" in u:
        return 1e9  # trillion BTU to kBtu
    log.warning("Unknown energy unit '%s', assuming kWh", u)
    return KWH_TO_KBTU


def _compute_eui_columns(
    df: pd.DataFrame,
    manifest: DatasetManifest,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Return a copy of df with EUI columns added, plus the list of EUI column names.
    Prefer pre-computed intensity columns (kwh_per_ft2); fall back to energy / sqft.
    """
    eui_df = df.copy()
    eui_cols = []

    sqft_col = _resolve_sqft_col(df)

    if manifest.has_intensity_columns:
        # Use pre-computed _intensity columns, just convert units
        for col in manifest.intensity_columns:
            if col not in df.columns:
                continue
            unit = _detect_col_unit(col)
            # intensity columns are kwh_per_ft2 -> convert to kbtu_per_ft2
            factor = _unit_to_kbtu_factor(unit, "kwh_per_ft2")
            eui_col = re.sub(r"energy_consumption_intensity(\.\..+)?$", "eui_kbtu_ft2", col)
            eui_col = re.sub(r"\.\.", ".", eui_col)  # clean up double-dots
            eui_df[eui_col] = pd.to_numeric(df[col], errors="coerce") * factor
            eui_cols.append(eui_col)
        log.info("Using %d pre-computed intensity columns for EUI", len(eui_cols))
    else:
        # Manual calculation: energy / sqft * unit_factor
        if sqft_col is None:
            log.warning("No sqft column found; EUI values will be raw energy (not intensity)")
        for col, meta in manifest.energy_column_map.items():
            if col not in df.columns:
                continue
            factor = _unit_to_kbtu_factor(meta.get("unit"), manifest.detected_energy_unit)
            base = f"out.{meta['fuel']}.{meta['end_use']}.eui_kbtu_ft2"
            if sqft_col and sqft_col in df.columns:
                sqft = pd.to_numeric(df[sqft_col], errors="coerce").replace(0, np.nan)
                eui_df[base] = pd.to_numeric(df[col], errors="coerce") * factor / sqft
            else:
                eui_df[base] = pd.to_numeric(df[col], errors="coerce") * factor
            eui_cols.append(base)
        log.info("Computed %d EUI columns manually (energy / sqft)", len(eui_cols))

    return eui_df, eui_cols


def _compute_bill_columns(
    df: pd.DataFrame,
    manifest: DatasetManifest,
) -> list[str]:
    """Return the bill column names available in df (already numeric in 2025 R3)."""
    return [c for c in manifest.bill_columns if c in df.columns]


# ---------------------------------------------------------------------------
# Applicability
# ---------------------------------------------------------------------------

def _get_applicability_mask(df: pd.DataFrame, uid: int) -> Optional[pd.Series]:
    """Return a boolean Series indicating which rows had the upgrade applied."""
    if uid == 0:
        return pd.Series([True] * len(df), index=df.index)
    # Single applicability column
    if "applicability" in df.columns:
        return df["applicability"].astype(bool)
    # Per-measure applicability columns (aggregate files) - not typical in per-building files
    return None


# ---------------------------------------------------------------------------
# Main summarize function
# ---------------------------------------------------------------------------

def summarize(
    config: PipelineConfig,
    manifest: DatasetManifest,
    filtered: FilteredData,
) -> SummaryResults:
    """Run Phase 4: compute per-building EUIs and population-level statistics."""
    df = filtered.data
    weight_col = _resolve_weight_col(df)

    if weight_col is None and config.use_weights:
        log.warning(
            "use_weights=True but no weight column found (tried 'weight', 'sample_weight'). "
            "Falling back to unweighted statistics."
        )

    # Compute EUI columns
    eui_df, eui_cols = _compute_eui_columns(df, manifest)

    # Identify bill columns
    bill_cols = _compute_bill_columns(df, manifest)
    log.info("Bill columns available: %d", len(bill_cols))

    upgrade_col = "upgrade"
    upgrade_ids = sorted(eui_df[upgrade_col].unique().tolist()) if upgrade_col in eui_df.columns else [0]

    # Columns to summarize
    all_summary_cols = eui_cols + bill_cols

    # Build summary rows
    summary_rows = []
    not_applicable_upgrades = []

    # Get baseline medians for savings computation
    baseline_rows = eui_df[eui_df[upgrade_col] == 0] if upgrade_col in eui_df.columns else eui_df
    baseline_weights = baseline_rows[weight_col] if (weight_col and weight_col in baseline_rows.columns) else None
    use_w = config.use_weights and weight_col is not None

    baseline_stats = {}
    for col in all_summary_cols:
        if col not in baseline_rows.columns:
            continue
        stats = _compute_stats(baseline_rows[col], baseline_weights, ["median"], use_w)
        baseline_stats[col] = stats.get("median", np.nan)

    for uid in upgrade_ids:
        uid_rows = eui_df[eui_df[upgrade_col] == uid] if upgrade_col in eui_df.columns else eui_df
        upgrade_name = manifest.upgrades.get(uid, f"Upgrade {uid}")
        n_buildings = len(uid_rows)

        weights = uid_rows[weight_col] if (weight_col and weight_col in uid_rows.columns) else None

        # Applicability
        app_mask = _get_applicability_mask(uid_rows, uid)
        n_applicable = int(app_mask.sum()) if app_mask is not None else n_buildings
        if uid != 0 and n_applicable == 0:
            not_applicable_upgrades.append(uid)

        row = {
            "upgrade_id": uid,
            "upgrade_name": upgrade_name,
            "n_buildings": n_buildings,
            "n_applicable": n_applicable,
            "represented_area_ft2": filtered.represented_area_ft2,
        }

        # Stats for each column
        for col in all_summary_cols:
            if col not in uid_rows.columns:
                continue
            stats = _compute_stats(uid_rows[col], weights, config.statistics, use_w)
            for stat_name, val in stats.items():
                row[f"{col}.{stat_name}"] = val

        # Savings vs baseline for total site energy and bills
        total_eui_col = next(
            (c for c in eui_cols if "site_energy.total" in c or "site_energy" in c and "total" in c),
            eui_cols[0] if eui_cols else None,
        )
        if total_eui_col and f"{total_eui_col}.median" in row:
            baseline_med = baseline_stats.get(total_eui_col, np.nan)
            upgrade_med = row[f"{total_eui_col}.median"]
            row["site_eui_savings_kbtu_ft2"] = baseline_med - upgrade_med if not np.isnan(baseline_med) and not np.isnan(upgrade_med) else np.nan
            row["site_eui_savings_pct"] = (row["site_eui_savings_kbtu_ft2"] / baseline_med * 100) if baseline_med and baseline_med != 0 else np.nan

        # Total bill savings (use mean_intensity or median if available)
        total_bill_col = next(
            (c for c in bill_cols if "total_bill" in c and "intensity" in c),
            next((c for c in bill_cols if "total_bill" in c), None),
        )
        if total_bill_col and f"{total_bill_col}.median" in row:
            baseline_bill = baseline_stats.get(total_bill_col, np.nan)
            upgrade_bill = row[f"{total_bill_col}.median"]
            row["total_bill_savings"] = baseline_bill - upgrade_bill if not np.isnan(baseline_bill) and not np.isnan(upgrade_bill) else np.nan
            row["total_bill_savings_pct"] = (row["total_bill_savings"] / baseline_bill * 100) if baseline_bill and baseline_bill != 0 else np.nan

        summary_rows.append(row)

    summary_table = pd.DataFrame(summary_rows)

    # Long format (optional)
    summary_long = None
    if config.include_long_format:
        summary_long = _build_long_format(summary_table, manifest)

    # Building detail (optional)
    building_detail = None
    if config.include_building_detail:
        detail_cols = [c for c in eui_df.columns if c in eui_cols or c in bill_cols or
                       c in ("bldg_id", "building_id", "upgrade", "weight", "in.sqft", "in.floor_area_ft2")]
        building_detail = eui_df[[c for c in detail_cols if c in eui_df.columns]].copy()

    # Compact summary (Key Metrics)
    summary_compact = None
    if config.include_compact_summary:
        summary_compact = _build_compact_summary(summary_table, eui_cols, bill_cols)
        log.info("Compact summary: %d rows × %d columns", len(summary_compact), len(summary_compact.columns))

    # Applicable-only summary (Option B: matched baseline per upgrade)
    summary_applicable = None
    if config.include_applicable_summary:
        summary_applicable = _build_applicable_summary(
            eui_df, eui_cols, bill_cols, manifest, config, filtered
        )
        if summary_applicable is not None:
            log.info("Applicable summary: %d rows", len(summary_applicable))
        else:
            log.info("Applicable summary: skipped (no applicability column)")

    _print_summary(summary_table, manifest, not_applicable_upgrades)

    return SummaryResults(
        summary_table=summary_table,
        summary_long=summary_long,
        building_detail=building_detail,
        summary_compact=summary_compact,
        summary_applicable=summary_applicable,
        not_applicable_upgrades=not_applicable_upgrades,
        filter_funnel=filtered.funnel,
        config_used=config,
        manifest=manifest,
        n_baseline_buildings=filtered.n_baseline_buildings,
        represented_area_ft2=filtered.represented_area_ft2,
    )


def _build_compact_summary(
    summary_table: pd.DataFrame,
    eui_cols: list[str],
    bill_cols: list[str],
) -> pd.DataFrame:
    """
    Build a focused summary: median EUI per fuel×end_use, total site EUI, EUI savings,
    total bill mean intensity median, and total bill savings.
    """
    id_cols = ["upgrade_id", "upgrade_name", "n_buildings", "n_applicable"]
    keep = [c for c in id_cols if c in summary_table.columns]

    # End-use median EUI columns (exclude total — that's added separately)
    end_use_median_cols = [
        c for c in summary_table.columns
        if c.endswith(".eui_kbtu_ft2.median") and "site_energy" not in c
    ]
    keep += end_use_median_cols

    # Total site EUI median
    total_eui_median_cols = [
        c for c in summary_table.columns
        if "site_energy" in c and "total" in c and c.endswith(".eui_kbtu_ft2.median")
    ]
    keep += total_eui_median_cols

    # EUI savings
    for col in ("site_eui_savings_kbtu_ft2", "site_eui_savings_pct"):
        if col in summary_table.columns:
            keep.append(col)

    # Total bill mean intensity median
    bill_intensity_median_cols = [
        c for c in summary_table.columns
        if "total_bill" in c and "intensity" in c and c.endswith(".median")
    ]
    keep += bill_intensity_median_cols

    # Total bill savings
    for col in ("total_bill_savings", "total_bill_savings_pct"):
        if col in summary_table.columns:
            keep.append(col)

    keep = [c for c in keep if c in summary_table.columns]
    return summary_table[keep].copy()


def _build_applicable_summary(
    eui_df: pd.DataFrame,
    eui_cols: list[str],
    bill_cols: list[str],
    manifest: DatasetManifest,
    config: PipelineConfig,
    filtered: FilteredData,
) -> Optional[pd.DataFrame]:
    """
    Build a summary table using only applicable buildings per upgrade (Option B).
    For each upgrade: baseline row uses buildings where applicability=True for THAT upgrade,
    and the upgrade row uses the same set. Savings = matched_baseline_median - upgrade_median.
    """
    upgrade_col = "upgrade"
    if upgrade_col not in eui_df.columns:
        log.warning("No 'upgrade' column — skipping applicable summary")
        return None
    if "applicability" not in eui_df.columns:
        log.warning("No 'applicability' column — skipping applicable summary")
        return None

    all_cols = eui_cols + bill_cols
    weight_col = _resolve_weight_col(eui_df)
    bldg_id_col = _resolve_bldg_id_col(eui_df)
    use_w = config.use_weights and weight_col is not None

    upgrade_ids = sorted(eui_df[upgrade_col].unique().tolist())
    non_baseline_ids = [u for u in upgrade_ids if u != 0]

    rows = []
    for uid in non_baseline_ids:
        upgrade_name = manifest.upgrades.get(uid, f"Upgrade {uid}")

        # Applicable building IDs for this upgrade
        upgrade_mask = (eui_df[upgrade_col] == uid) & (eui_df["applicability"].astype(bool))
        upgrade_rows = eui_df[upgrade_mask]
        n_applicable = len(upgrade_rows)

        if n_applicable == 0:
            # Still emit a row with NaN stats so the upgrade appears in output
            row_baseline = {"upgrade_id": 0, "upgrade_name": "Baseline (matched)", "n_buildings": 0, "n_applicable": 0}
            row_upgrade = {"upgrade_id": uid, "upgrade_name": upgrade_name, "n_buildings": 0, "n_applicable": 0}
            for col in all_cols:
                for stat in config.statistics:
                    row_baseline[f"{col}.{stat}"] = np.nan
                    row_upgrade[f"{col}.{stat}"] = np.nan
            row_upgrade["site_eui_savings_kbtu_ft2"] = np.nan
            row_upgrade["site_eui_savings_pct"] = np.nan
            row_upgrade["total_bill_savings"] = np.nan
            row_upgrade["total_bill_savings_pct"] = np.nan
            rows.extend([row_baseline, row_upgrade])
            continue

        # Matched baseline: baseline rows for the same applicable buildings
        if bldg_id_col:
            applicable_ids = set(upgrade_rows[bldg_id_col].tolist())
            baseline_rows = eui_df[
                (eui_df[upgrade_col] == 0) & (eui_df[bldg_id_col].isin(applicable_ids))
            ]
        else:
            # No building ID column — fall back to all baseline buildings
            baseline_rows = eui_df[eui_df[upgrade_col] == 0]
            log.warning("No bldg_id column for applicable summary; using full baseline for upgrade %d", uid)

        baseline_weights = baseline_rows[weight_col] if (weight_col and weight_col in baseline_rows.columns) else None
        upgrade_weights = upgrade_rows[weight_col] if (weight_col and weight_col in upgrade_rows.columns) else None

        row_baseline = {
            "upgrade_id": 0,
            "upgrade_name": "Baseline (matched)",
            "n_buildings": len(baseline_rows),
            "n_applicable": len(baseline_rows),
        }
        row_upgrade = {
            "upgrade_id": uid,
            "upgrade_name": upgrade_name,
            "n_buildings": n_applicable,
            "n_applicable": n_applicable,
        }

        baseline_medians: dict[str, float] = {}
        for col in all_cols:
            if col not in eui_df.columns:
                continue
            b_stats = _compute_stats(baseline_rows[col], baseline_weights, config.statistics, use_w)
            u_stats = _compute_stats(upgrade_rows[col], upgrade_weights, config.statistics, use_w)
            for stat, val in b_stats.items():
                row_baseline[f"{col}.{stat}"] = val
            for stat, val in u_stats.items():
                row_upgrade[f"{col}.{stat}"] = val
            baseline_medians[col] = b_stats.get("median", np.nan)

        # Savings vs matched baseline
        total_eui_col = next(
            (c for c in eui_cols if "site_energy.total" in c or ("site_energy" in c and "total" in c)),
            eui_cols[0] if eui_cols else None,
        )
        if total_eui_col:
            b_med = baseline_medians.get(total_eui_col, np.nan)
            u_med = row_upgrade.get(f"{total_eui_col}.median", np.nan)
            row_upgrade["site_eui_savings_kbtu_ft2"] = (b_med - u_med) if not (np.isnan(b_med) or np.isnan(u_med)) else np.nan
            row_upgrade["site_eui_savings_pct"] = (row_upgrade["site_eui_savings_kbtu_ft2"] / b_med * 100) if b_med and b_med != 0 else np.nan

        total_bill_col = next(
            (c for c in bill_cols if "total_bill" in c and "intensity" in c),
            next((c for c in bill_cols if "total_bill" in c), None),
        )
        if total_bill_col:
            b_bill = baseline_medians.get(total_bill_col, np.nan)
            u_bill = row_upgrade.get(f"{total_bill_col}.median", np.nan)
            row_upgrade["total_bill_savings"] = (b_bill - u_bill) if not (np.isnan(b_bill) or np.isnan(u_bill)) else np.nan
            row_upgrade["total_bill_savings_pct"] = (row_upgrade["total_bill_savings"] / b_bill * 100) if b_bill and b_bill != 0 else np.nan

        rows.extend([row_baseline, row_upgrade])

    if not rows:
        return None

    return pd.DataFrame(rows)


def _build_long_format(summary_table: pd.DataFrame, manifest: DatasetManifest) -> pd.DataFrame:
    """Unpivot the wide summary table into long format for Tableau."""
    id_vars = [c for c in summary_table.columns if not any(
        c.endswith(f".{s}") for s in ("median", "mean", "p25", "p75")
    )]
    value_vars = [c for c in summary_table.columns if any(
        c.endswith(f".{s}") for s in ("median", "mean", "p25", "p75")
    )]
    if not value_vars:
        return pd.DataFrame()

    long = summary_table[id_vars + value_vars].melt(
        id_vars=id_vars, var_name="metric_stat", value_name="value"
    )

    # Split metric_stat into metric + statistic
    stat_suffixes = ["median", "mean", "p25", "p75"]
    def split_metric(ms: str):
        for s in stat_suffixes:
            if ms.endswith(f".{s}"):
                return ms[: -(len(s) + 1)], s
        return ms, "unknown"

    long[["metric", "statistic"]] = pd.DataFrame(
        long["metric_stat"].apply(split_metric).tolist(), index=long.index
    )
    long = long.drop(columns=["metric_stat"])
    return long


def _print_summary(
    summary_table: pd.DataFrame,
    manifest: DatasetManifest,
    not_applicable: list[int],
) -> None:
    print(f"\n=== Summary Statistics ===")
    print(f"  Upgrades summarized: {len(summary_table)}")
    if not_applicable:
        print(f"  Not-applicable upgrades (n=0 buildings affected): {not_applicable}")

    # Print a sample: baseline + first few upgrades, total site EUI median
    total_col_candidates = [c for c in summary_table.columns if "site_energy" in c and "total" in c and "median" in c]
    if total_col_candidates:
        col = total_col_candidates[0]
        print(f"\n  Total site EUI (kBtu/ft²) — median by upgrade:")
        sample = summary_table[["upgrade_id", "upgrade_name", "n_buildings", col]].head(10)
        for _, r in sample.iterrows():
            print(f"    [{int(r['upgrade_id']):>3}] {str(r['upgrade_name']):<50} N={int(r['n_buildings']):>6}  EUI={r[col]:.1f}" if pd.notna(r[col]) else f"    [{int(r['upgrade_id']):>3}] {str(r['upgrade_name']):<50} N={int(r['n_buildings']):>6}  EUI=N/A")
    print()
