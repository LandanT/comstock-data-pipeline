"""Phase 3: Apply building stock filters and report the filter funnel."""
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from config import PipelineConfig, STATE_NAME_TO_ABBR
from pipeline.pull import PulledData

log = logging.getLogger(__name__)


@dataclass
class FilteredData:
    data: pd.DataFrame
    baseline_bldg_ids: list
    n_baseline_buildings: int
    represented_area_ft2: float
    available_upgrade_ids: list[int]
    funnel: dict[str, int]
    matched_vintages: list[str]
    warnings: list[str]
    building_type_col: Optional[str] = None
    vintage_col: Optional[str] = None
    climate_zone_col: Optional[str] = None
    state_col: Optional[str] = None
    sqft_col: Optional[str] = None
    heating_fuel_col: Optional[str] = None
    hvac_system_col: Optional[str] = None


# --- Column resolution helpers ---

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


def _resolve_columns(df: pd.DataFrame) -> dict[str, Optional[str]]:
    return {
        "building_type": _find_col(df, ["in.comstock_building_type", "in.building_type"]),
        "vintage": _find_col(df, ["in.vintage"]),
        "year_built": _find_col(df, ["in.year_built"]),
        "climate_zone": _find_col(df, [
            "in.ashrae_iecc_climate_zone_2006",
            "in.as_simulated_ashrae_iecc_climate_zone_2006",
        ]),
        "state": _find_col(df, [
            "in.as_simulated_state_name", "in.state_name", "in.state",
        ]),
        "sqft": _find_col(df, ["in.sqft", "in.floor_area_ft2"]),
        "weight": _find_col(df, ["weight", "sample_weight"]),
        "bldg_id": _find_col(df, ["bldg_id", "building_id"]),
        "heating_fuel": _find_col(df, ["in.heating_fuel"]),
        "hvac_system": _find_col(df, ["in.hvac_system"]),
    }


# --- Vintage parsing ---

def _parse_vintage_upper(vintage_str: str) -> Optional[int]:
    """Extract the upper year from a vintage category string."""
    s = str(vintage_str).strip()
    # "Before 1946" -> 1945
    m = re.match(r"(?:Before|Pre)[- ](\d{4})", s, re.IGNORECASE)
    if m:
        return int(m.group(1)) - 1

    # "1946 to 1959" or "1946-1959"
    m = re.match(r"\d{4}\s*(?:to|-)\s*(\d{4})", s, re.IGNORECASE)
    if m:
        return int(m.group(1))

    # Single year "1975"
    m = re.match(r"^(\d{4})$", s)
    if m:
        return int(m.group(1))

    return None


def _parse_vintage_lower(vintage_str: str) -> Optional[int]:
    """Extract the lower year from a vintage category string."""
    s = str(vintage_str).strip()
    m = re.match(r"(?:Before|Pre)[- ](\d{4})", s, re.IGNORECASE)
    if m:
        return 0

    m = re.match(r"(\d{4})\s*(?:to|-)\s*\d{4}", s, re.IGNORECASE)
    if m:
        return int(m.group(1))

    m = re.match(r"^(\d{4})$", s)
    if m:
        return int(m.group(1))

    return None


def _match_vintages(
    vintage_values: list[str],
    vintage_min: Optional[int],
    vintage_max: Optional[int],
) -> list[str]:
    """Return the vintage category strings that fall within [vintage_min, vintage_max]."""
    matched = []
    for v in vintage_values:
        upper = _parse_vintage_upper(v)
        lower = _parse_vintage_lower(v)
        if upper is None or lower is None:
            continue
        # Include if the category's upper bound <= vintage_max AND lower bound >= vintage_min
        include = True
        if vintage_max is not None and upper > vintage_max:
            include = False
        if vintage_min is not None and lower < vintage_min:
            include = False
        if include:
            matched.append(v)
    return matched


# --- State matching ---

def _normalize_state_filter(state_col_values: pd.Series, config_states: list[str]) -> list[str]:
    """
    Return the values in state_col_values that match the requested states.
    Handles full names, abbreviations, and case differences.
    """
    # Build set of target full names
    target_names = set()
    for s in config_states:
        if s in STATE_NAME_TO_ABBR:
            target_names.add(s.lower())
        else:
            # Maybe it's an abbreviation
            from config import STATE_ABBR_TO_NAME
            if s.upper() in STATE_ABBR_TO_NAME:
                target_names.add(STATE_ABBR_TO_NAME[s.upper()].lower())
            else:
                target_names.add(s.lower())

    # Also match abbreviations
    from config import STATE_ABBR_TO_NAME
    target_abbrs = set()
    for s in config_states:
        if s.upper() in STATE_ABBR_TO_NAME:
            target_abbrs.add(s.upper())
        elif s in STATE_NAME_TO_ABBR:
            target_abbrs.add(STATE_NAME_TO_ABBR[s])

    unique_vals = state_col_values.dropna().unique()
    matched = []
    for v in unique_vals:
        v_str = str(v)
        if v_str.lower() in target_names:
            matched.append(v_str)
        elif v_str.upper() in target_abbrs:
            matched.append(v_str)
    return matched


# --- Main filter function ---

def apply_filters(config: PipelineConfig, pulled: PulledData) -> FilteredData:
    """Apply building stock filters and report the filter funnel."""
    df = pulled.df.copy()
    cols = _resolve_columns(df)
    warnings = []
    funnel = {}
    matched_vintages = []

    # Work on baseline only for funnel reporting
    upgrade_col = "upgrade"
    baseline_df = df[df[upgrade_col] == 0].copy() if upgrade_col in df.columns else df.copy()
    funnel["Total baseline models loaded"] = len(baseline_df)

    # --- Building type filter ---
    if config.building_types and cols["building_type"]:
        bt_col = cols["building_type"]
        mask = baseline_df[bt_col].isin(config.building_types)
        baseline_df = baseline_df[mask]
        funnel[f"After building type filter ({', '.join(config.building_types)})"] = len(baseline_df)
        log.info("After building type filter: %d", len(baseline_df))
    elif config.building_types and not cols["building_type"]:
        warnings.append("Building type filter requested but no building type column found.")

    # --- Vintage filter ---
    if (config.vintage_min is not None or config.vintage_max is not None):
        if cols["vintage"]:
            vintage_col = cols["vintage"]
            unique_vintages = baseline_df[vintage_col].dropna().unique().tolist()
            matched_vintages = _match_vintages(unique_vintages, config.vintage_min, config.vintage_max)
            if matched_vintages:
                mask = baseline_df[vintage_col].isin(matched_vintages)
                baseline_df = baseline_df[mask]
                funnel[f"After vintage filter (min={config.vintage_min}, max={config.vintage_max})"] = len(baseline_df)
                log.info("Matched vintage categories: %s", matched_vintages)
            else:
                warnings.append(
                    f"No vintage categories matched for min={config.vintage_min}, "
                    f"max={config.vintage_max}. Available: {unique_vintages}"
                )
        elif cols["year_built"]:
            # Numeric year_built fallback
            yb_col = cols["year_built"]
            mask = pd.Series([True] * len(baseline_df), index=baseline_df.index)
            if config.vintage_max is not None:
                mask &= pd.to_numeric(baseline_df[yb_col], errors="coerce") <= config.vintage_max
            if config.vintage_min is not None:
                mask &= pd.to_numeric(baseline_df[yb_col], errors="coerce") >= config.vintage_min
            baseline_df = baseline_df[mask]
            funnel[f"After vintage filter (year_built, min={config.vintage_min}, max={config.vintage_max})"] = len(baseline_df)
        else:
            warnings.append("Vintage filter requested but no vintage/year_built column found.")

    # --- Climate zone filter ---
    if config.climate_zones and cols["climate_zone"]:
        cz_col = cols["climate_zone"]
        mask = baseline_df[cz_col].isin(config.climate_zones)
        baseline_df = baseline_df[mask]
        funnel[f"After climate zone filter ({', '.join(config.climate_zones)})"] = len(baseline_df)
        log.info("After climate zone filter: %d", len(baseline_df))
    elif config.climate_zones and not cols["climate_zone"]:
        warnings.append("Climate zone filter requested but no climate zone column found.")

    # --- State filter ---
    if config.states and cols["state"]:
        state_col = cols["state"]
        matched_state_vals = _normalize_state_filter(baseline_df[state_col], config.states)
        if matched_state_vals:
            mask = baseline_df[state_col].isin(matched_state_vals)
            baseline_df = baseline_df[mask]
            funnel[f"After state filter ({', '.join(config.states)})"] = len(baseline_df)
            log.info("After state filter: %d", len(baseline_df))
        else:
            warnings.append(
                f"No state values matched for {config.states}. "
                f"Sample values: {df[state_col].dropna().unique()[:5].tolist()}"
            )
    elif config.states and not cols["state"]:
        warnings.append("State filter requested but no state column found.")

    # --- Heating fuel filter ---
    if config.heating_fuels and cols["heating_fuel"]:
        hf_col = cols["heating_fuel"]
        mask = baseline_df[hf_col].isin(config.heating_fuels)
        baseline_df = baseline_df[mask]
        funnel[f"After heating fuel filter ({', '.join(config.heating_fuels)})"] = len(baseline_df)
        log.info("After heating fuel filter: %d", len(baseline_df))
    elif config.heating_fuels and not cols["heating_fuel"]:
        warnings.append("Heating fuel filter requested but no heating fuel column found.")

    # --- HVAC system filter ---
    if config.hvac_systems and cols["hvac_system"]:
        hvac_col = cols["hvac_system"]
        mask = baseline_df[hvac_col].isin(config.hvac_systems)
        baseline_df = baseline_df[mask]
        funnel[f"After HVAC system filter ({', '.join(config.hvac_systems)})"] = len(baseline_df)
        log.info("After HVAC system filter: %d", len(baseline_df))
    elif config.hvac_systems and not cols["hvac_system"]:
        warnings.append("HVAC system filter requested but no HVAC system column found.")

    #--- Final baseline count and represented area ---
    n_baseline = len(baseline_df)
    funnel["Final baseline building models"] = n_baseline

    # Represented floor area
    represented_area = 0.0
    if not cols["sqft"]:
        log.warning("Could not find sqft column (tried 'in.sqft', 'in.floor_area_ft2' and ..unit variants). represented_area_ft2 will be 0.")
    if cols["sqft"] and n_baseline > 0:
        if cols["weight"]:
            represented_area = float((baseline_df[cols["sqft"]] * baseline_df[cols["weight"]]).sum())
            log.info("represented_area_ft2 computed using weight column '%s'", cols["weight"])
        else:
            represented_area = float(baseline_df[cols["sqft"]].sum())
            log.warning("No weight column found — represented_area_ft2 is raw sqft sum (unweighted).")

    # Sample size warning
    if n_baseline < config.min_sample_warning:
        warnings.append(
            f"N={n_baseline} is below the recommended minimum of {config.min_sample_warning} models. "
            "Aggregate results may be unreliable. Consider expanding geographic scope."
        )

    # Get baseline building IDs
    bldg_id_col = cols["bldg_id"]
    if bldg_id_col and bldg_id_col in baseline_df.columns:
        baseline_ids = baseline_df[bldg_id_col].tolist()
    else:
        baseline_ids = baseline_df.index.tolist()

    # Now filter the FULL dataset (all upgrades) to only the matching building IDs
    if bldg_id_col and bldg_id_col in df.columns and baseline_ids:
        filtered_df = df[df[bldg_id_col].isin(baseline_ids)].copy()
    else:
        # Can't filter other upgrades without bldg_id; warn and use all
        filtered_df = df.copy()
        warnings.append("No bldg_id column found; could not apply baseline filter to upgrade rows.")

    available_upgrade_ids = sorted(filtered_df["upgrade"].unique().tolist()) if "upgrade" in filtered_df.columns else []

    _print_filter_funnel(funnel, matched_vintages, represented_area, available_upgrade_ids, warnings)

    return FilteredData(
        data=filtered_df,
        baseline_bldg_ids=baseline_ids,
        n_baseline_buildings=n_baseline,
        represented_area_ft2=represented_area,
        available_upgrade_ids=available_upgrade_ids,
        funnel=funnel,
        matched_vintages=matched_vintages,
        warnings=warnings,
        building_type_col=cols["building_type"],
        vintage_col=cols["vintage"],
        climate_zone_col=cols["climate_zone"],
        state_col=cols["state"],
        sqft_col=cols["sqft"],
        heating_fuel_col=cols["heating_fuel"],
        hvac_system_col=cols["hvac_system"],
    )


def _print_filter_funnel(
    funnel: dict,
    matched_vintages: list,
    represented_area: float,
    upgrade_ids: list,
    warnings: list,
) -> None:
    print("\n=== Filter Funnel ===")
    for step, count in funnel.items():
        print(f"  {step:<60} {count:>10,}")
    if matched_vintages:
        print(f"  Matched vintage categories: {', '.join(matched_vintages)}")
    print(f"  Total weight (represented floor area):                    {represented_area:>10,.0f} ft²")
    print(f"  Upgrade scenarios available:                              {len(upgrade_ids):>10}")
    print("=" * 72)
    for w in warnings:
        print(f"  ⚠️  WARNING: {w}")
    if warnings:
        print()
