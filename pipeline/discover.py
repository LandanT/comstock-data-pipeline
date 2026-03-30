"""Phase 1: Discover dataset paths, schema, and data dictionary from S3."""
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq
import s3fs

from config import PipelineConfig

log = logging.getLogger(__name__)


@dataclass
class DatasetManifest:
    base_path: str
    release_id: str
    upgrades: dict[int, str]                 # {0: "Baseline", 1: "...", ...}

    # File structure discovered
    has_by_state_and_county: bool = False
    has_by_state: bool = False
    has_national: bool = False
    has_metadata_dir: bool = False
    available_states: list[str] = field(default_factory=list)   # abbreviations
    available_upgrades_on_disk: list[int] = field(default_factory=list)

    # Column schema (discovered from one sample file)
    energy_columns: list[str] = field(default_factory=list)
    intensity_columns: list[str] = field(default_factory=list)  # _intensity..kwh_per_ft2
    savings_columns: list[str] = field(default_factory=list)
    bill_columns: list[str] = field(default_factory=list)
    input_columns: list[str] = field(default_factory=list)
    calc_columns: list[str] = field(default_factory=list)
    id_columns: list[str] = field(default_factory=list)
    applicability_columns: list[str] = field(default_factory=list)

    # Parsed energy column structure: {col_name: {fuel, end_use, unit, has_intensity}}
    energy_column_map: dict[str, dict] = field(default_factory=dict)

    # Units
    detected_energy_unit: str = "unknown"   # "kwh", "kbtu", or "unknown"
    has_intensity_columns: bool = False      # pre-computed EUI available

    # Data dictionary
    data_dictionary: Optional[pd.DataFrame] = None
    data_dictionary_source: Optional[str] = None

    # Partition style used for reads
    preferred_partition: str = "national"   # "by_state_and_county", "by_state", "national", "metadata"


def _get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(anon=True)


def _path_exists(fs: s3fs.S3FileSystem, path: str) -> bool:
    try:
        return fs.exists(path)
    except Exception:
        return False


def _list_dir(fs: s3fs.S3FileSystem, path: str) -> list[str]:
    try:
        return fs.ls(path, detail=False)
    except Exception:
        return []


def _fetch_upgrades_lookup(fs: s3fs.S3FileSystem, base_path: str) -> dict[int, str]:
    path = f"{base_path}/upgrades_lookup.json"
    log.info("Fetching upgrades_lookup.json from s3://%s", path)
    try:
        with fs.open(path, "r") as f:
            raw = json.load(f)
        return {int(k): v for k, v in raw.items()}
    except Exception as e:
        raise RuntimeError(f"Could not fetch upgrades_lookup.json from s3://{path}: {e}") from e


def _fetch_data_dictionary(
    fs: s3fs.S3FileSystem,
    base_path: str,
    release_year: str,
    release_name: str,
) -> tuple[Optional[pd.DataFrame], Optional[str]]:
    """Try to fetch data_dictionary.tsv from target or nearby releases."""
    # Attempt current release first
    candidates = [
        (base_path, release_name),
    ]
    # Walk backwards: try release_N-1, release_N-2
    m = re.search(r"release_(\d+)$", release_name)
    if m:
        n = int(m.group(1))
        for i in range(n - 1, 0, -1):
            alt_name = re.sub(r"release_\d+$", f"release_{i}", release_name)
            alt_base = (
                "oedi-data-lake/nrel-pds-building-stock/"
                "end-use-load-profiles-for-us-building-stock/"
                f"{release_year}/{alt_name}"
            )
            candidates.append((alt_base, alt_name))

    for path, name in candidates:
        dict_path = f"{path}/data_dictionary.tsv"
        if _path_exists(fs, dict_path):
            log.info("Using data_dictionary.tsv from %s", name)
            try:
                with fs.open(dict_path, "r") as f:
                    df = pd.read_csv(f, sep="\t")
                return df, name
            except Exception as e:
                log.warning("Failed to parse data_dictionary.tsv from %s: %s", name, e)

    log.warning("No data_dictionary.tsv found for %s or nearby releases", release_name)
    return None, None


def _probe_structure(
    fs: s3fs.S3FileSystem, base_path: str
) -> dict:
    """Probe S3 to determine which file structures are available."""
    result = {
        "has_by_state_and_county": False,
        "has_by_state": False,
        "has_national": False,
        "has_metadata_dir": False,
        "available_states": [],
    }

    county_full = f"{base_path}/metadata_and_annual_results/by_state_and_county/full/parquet"
    county_basic = f"{base_path}/metadata_and_annual_results/by_state_and_county/basic/parquet"
    if _path_exists(fs, county_full) or _path_exists(fs, county_basic):
        result["has_by_state_and_county"] = True
        log.info("Found by_state_and_county partition")

    state_path = f"{base_path}/metadata_and_annual_results/by_state"
    if _path_exists(fs, state_path):
        result["has_by_state"] = True
        log.info("Found by_state partition")
        state_dirs = _list_dir(fs, state_path)
        for d in state_dirs:
            # d looks like ".../by_state/state=CO"
            if "state=" in d:
                abbr = d.split("state=")[-1]
                result["available_states"].append(abbr)
        log.info("Available states: %s", sorted(result["available_states"]))

    nat_path = f"{base_path}/metadata_and_annual_results/national/parquet"
    if _path_exists(fs, nat_path):
        result["has_national"] = True
        log.info("Found national partition")

    meta_path = f"{base_path}/metadata"
    if _path_exists(fs, meta_path):
        result["has_metadata_dir"] = True
        log.info("Found metadata/ directory")

    return result


def _find_sample_file(
    fs: s3fs.S3FileSystem,
    base_path: str,
    structure: dict,
    preferred_states: Optional[list[str]],
) -> Optional[str]:
    """Find a single baseline parquet file to read schema from."""
    # Prefer by_state_and_county > by_state > national > metadata
    if structure["has_by_state_and_county"]:
        county_base = f"{base_path}/metadata_and_annual_results/by_state_and_county/full/parquet"
        states_to_try = preferred_states or structure.get("available_states", [])
        if not states_to_try:
            # List top-level state= dirs
            dirs = _list_dir(fs, county_base)
            states_to_try = [d.split("state=")[-1] for d in dirs if "state=" in d]
        for state in states_to_try[:3]:
            county_dirs = _list_dir(fs, f"{county_base}/state={state}")
            for cdir in county_dirs[:2]:
                files = [f for f in _list_dir(fs, cdir) if f.endswith(".parquet") and "upgrade0" in f]
                if files:
                    return files[0]

    if structure["has_by_state"]:
        states_to_try = preferred_states or structure.get("available_states", [])
        for state in states_to_try[:3]:
            state_path = f"{base_path}/metadata_and_annual_results/by_state/state={state}/parquet"
            files = [f for f in _list_dir(fs, state_path) if f.endswith(".parquet") and "baseline" in f.lower()]
            if files:
                return files[0]

    if structure["has_national"]:
        nat_path = f"{base_path}/metadata_and_annual_results/national/parquet"
        files = [f for f in _list_dir(fs, nat_path) if "baseline" in f.lower() and f.endswith(".parquet")]
        if files:
            return files[0]

    if structure["has_metadata_dir"]:
        meta_path = f"{base_path}/metadata"
        files = [f for f in _list_dir(fs, meta_path) if "baseline" in f.lower() and f.endswith(".parquet")]
        if files:
            return files[0]

    return None


def _discover_available_upgrades(
    fs: s3fs.S3FileSystem,
    base_path: str,
    structure: dict,
    upgrades_lookup: dict[int, str],
    preferred_states: Optional[list[str]],
) -> list[int]:
    """List upgrade IDs that actually have files on disk."""
    found = set()

    if structure["has_by_state_and_county"]:
        county_base = f"{base_path}/metadata_and_annual_results/by_state_and_county/full/parquet"
        states = preferred_states or structure.get("available_states", [])
        if not states:
            dirs = _list_dir(fs, county_base)
            states = [d.split("state=")[-1] for d in dirs if "state=" in d]
        for state in states[:2]:
            county_dirs = _list_dir(fs, f"{county_base}/state={state}")
            for cdir in county_dirs[:1]:
                for f_path in _list_dir(fs, cdir):
                    m = re.search(r"upgrade(\d+)\.parquet", f_path)
                    if m:
                        found.add(int(m.group(1)))
    elif structure["has_national"]:
        nat_path = f"{base_path}/metadata_and_annual_results/national/parquet"
        for f_path in _list_dir(fs, nat_path):
            m = re.search(r"upgrade(\d+)", f_path)
            if m:
                found.add(int(m.group(1)))
            if "baseline" in f_path.lower():
                found.add(0)
    elif structure["has_metadata_dir"]:
        meta_path = f"{base_path}/metadata"
        for f_path in _list_dir(fs, meta_path):
            m = re.search(r"upgrade(\d+)", f_path)
            if m:
                found.add(int(m.group(1)))
            if "baseline" in f_path.lower():
                found.add(0)

    # Fall back to all keys in upgrades_lookup if nothing found
    if not found:
        found = set(upgrades_lookup.keys())

    return sorted(found)


def _classify_columns(all_columns: list[str]) -> dict:
    """Classify columns by type and parse energy column structure."""
    energy_cols = []
    intensity_cols = []
    savings_cols = []
    savings_intensity_cols = []
    bill_cols = []
    input_cols = []
    calc_cols = []
    id_cols = []
    applicability_cols = []

    energy_col_map = {}

    for col in all_columns:
        if col.startswith("in."):
            input_cols.append(col)
        elif col.startswith("calc."):
            calc_cols.append(col)
        elif col.startswith("out.utility_bills"):
            bill_cols.append(col)
        elif col.startswith("out.") and "energy_consumption_intensity" in col:
            intensity_cols.append(col)
        elif col.startswith("out.") and "energy_savings_intensity" in col:
            savings_intensity_cols.append(col)
        elif col.startswith("out.") and "energy_savings" in col:
            savings_cols.append(col)
        elif col.startswith("out.") and "energy_consumption" in col:
            energy_cols.append(col)
        elif col in ("bldg_id", "building_id", "upgrade", "weight", "sample_weight"):
            id_cols.append(col)
        elif col == "applicability" or col.startswith("applicability."):
            applicability_cols.append(col)

    # Parse energy column structure: out.{fuel}.{end_use}.energy_consumption[..{unit}]
    # Pattern: out.electricity.cooling.energy_consumption..kwh
    energy_pattern = re.compile(
        r"^out\.(?P<fuel>[^.]+)\.(?P<end_use>[^.]+)\.energy_consumption(?:\.\.(?P<unit>.+))?$"
    )
    for col in energy_cols:
        m = energy_pattern.match(col)
        if m:
            fuel = m.group("fuel")
            end_use = m.group("end_use")
            unit = m.group("unit") or "unknown"
            # Check if a corresponding intensity column exists
            intensity_col = col.replace("energy_consumption", "energy_consumption_intensity")
            has_intensity = intensity_col in intensity_cols
            energy_col_map[col] = {
                "fuel": fuel,
                "end_use": end_use,
                "unit": unit,
                "intensity_col": intensity_col if has_intensity else None,
            }

    # Detect primary energy unit
    detected_unit = "unknown"
    for info in energy_col_map.values():
        if info["unit"] in ("kwh", "kbtu"):
            detected_unit = info["unit"]
            break
    if detected_unit == "unknown" and energy_cols:
        # Default assumption: ComStock uses kWh
        detected_unit = "kwh"

    return {
        "energy_columns": energy_cols,
        "intensity_columns": intensity_cols,
        "savings_columns": savings_cols,
        "bill_columns": bill_cols,
        "input_columns": input_cols,
        "calc_columns": calc_cols,
        "id_columns": id_cols,
        "applicability_columns": applicability_cols,
        "energy_column_map": energy_col_map,
        "detected_energy_unit": detected_unit,
        "has_intensity_columns": len(intensity_cols) > 0,
    }


def _determine_preferred_partition(structure: dict) -> str:
    if structure["has_by_state_and_county"]:
        return "by_state_and_county"
    if structure["has_by_state"]:
        return "by_state"
    if structure["has_national"]:
        return "national"
    if structure["has_metadata_dir"]:
        return "metadata"
    raise RuntimeError("No recognized file structure found in S3 release path.")


def discover(config: PipelineConfig) -> "DatasetManifest":
    """Run Phase 1: discover dataset structure, schema, and available data."""
    fs = _get_fs()
    base_path = config.s3_base_path
    log.info("Discovering dataset at s3://%s", base_path)

    # 1. Fetch upgrades lookup
    upgrades = _fetch_upgrades_lookup(fs, base_path)
    log.info("Found %d upgrades: %s", len(upgrades), list(upgrades.values())[:5])

    # 2. Fetch data dictionary (with fallback)
    dd, dd_source = _fetch_data_dictionary(fs, base_path, config.release_year, config.release_name)

    # 3. Probe file structure
    structure = _probe_structure(fs, base_path)

    # 4. Find a sample parquet file to read schema
    state_abbrs = config.state_abbreviations()
    sample_file = _find_sample_file(fs, base_path, structure, state_abbrs)
    if sample_file is None:
        raise RuntimeError(
            f"Could not find any parquet file under s3://{base_path}. "
            "Check release_year and release_name in config."
        )
    log.info("Reading schema from s3://%s", sample_file)

    schema = pq.read_schema(f"s3://{sample_file}", filesystem=fs)
    all_columns = schema.names
    log.info("Schema has %d columns", len(all_columns))

    # 5. Classify columns
    col_info = _classify_columns(all_columns)

    # 6. Discover available upgrades on disk
    avail_upgrades = _discover_available_upgrades(fs, base_path, structure, upgrades, state_abbrs)

    # 7. Determine preferred partition
    preferred = _determine_preferred_partition(structure)
    log.info("Preferred partition strategy: %s", preferred)

    manifest = DatasetManifest(
        base_path=base_path,
        release_id=config.release_name,
        upgrades=upgrades,
        has_by_state_and_county=structure["has_by_state_and_county"],
        has_by_state=structure["has_by_state"],
        has_national=structure["has_national"],
        has_metadata_dir=structure["has_metadata_dir"],
        available_states=structure.get("available_states", []),
        available_upgrades_on_disk=avail_upgrades,
        energy_columns=col_info["energy_columns"],
        intensity_columns=col_info["intensity_columns"],
        savings_columns=col_info["savings_columns"],
        bill_columns=col_info["bill_columns"],
        input_columns=col_info["input_columns"],
        calc_columns=col_info["calc_columns"],
        id_columns=col_info["id_columns"],
        applicability_columns=col_info["applicability_columns"],
        energy_column_map=col_info["energy_column_map"],
        detected_energy_unit=col_info["detected_energy_unit"],
        has_intensity_columns=col_info["has_intensity_columns"],
        data_dictionary=dd,
        data_dictionary_source=dd_source,
        preferred_partition=preferred,
    )

    _print_manifest_summary(manifest)
    return manifest


def _print_manifest_summary(m: DatasetManifest) -> None:
    print(f"\n=== Dataset Manifest: {m.release_id} ===")
    print(f"  Upgrades found:          {len(m.upgrades)}")
    print(f"  Upgrades on disk:        {len(m.available_upgrades_on_disk)}")
    print(f"  Energy columns:          {len(m.energy_columns)}")
    print(f"  Intensity columns:       {len(m.intensity_columns)}")
    print(f"  Bill columns:            {len(m.bill_columns)}")
    print(f"  Input (filter) columns:  {len(m.input_columns)}")
    print(f"  Detected energy unit:    {m.detected_energy_unit}")
    print(f"  Has pre-computed EUIs:   {m.has_intensity_columns}")
    print(f"  Partition strategy:      {m.preferred_partition}")
    print(f"  Data dictionary:         {m.data_dictionary_source or 'not found'}")
    if m.available_states:
        print(f"  Available states:        {len(m.available_states)} states")
    print()
