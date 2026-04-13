"""Phase 2: Pull per-building annual results from S3."""
import logging
import os
import re
from dataclasses import dataclass

import pandas as pd
import pyarrow.parquet as pq
import s3fs

from config import PipelineConfig
from pipeline.discover import DatasetManifest

log = logging.getLogger(__name__)

# Columns always needed regardless of config
_ALWAYS_INCLUDE_PATTERNS = [
    "bldg_id", "building_id", "upgrade", "weight", "sample_weight",
    "applicability",
]

# Input columns needed for filtering
_FILTER_INPUT_COLS = [
    "in.comstock_building_type",
    "in.building_type",                          # older releases
    "in.vintage",
    "in.year_built",
    "in.ashrae_iecc_climate_zone_2006",
    "in.as_simulated_ashrae_iecc_climate_zone_2006",
    "in.state",
    "in.state_name",
    "in.as_simulated_state_name",
    "in.sqft",
    "in.floor_area_ft2",
    "in.heating_fuel",
    "in.hvac_system",
]


@dataclass
class PulledData:
    df: pd.DataFrame
    upgrades_loaded: list[int]
    columns_loaded: list[str]
    partition_used: str
    n_rows: int


def _get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(anon=True)


def _select_columns(manifest: DatasetManifest, all_file_columns: list[str]) -> list[str]:
    """Select the minimal set of columns needed from the parquet files."""
    wanted = set()

    # Always-include patterns
    for col in all_file_columns:
        base = col.split("..")[0]
        if base in _ALWAYS_INCLUDE_PATTERNS:
            wanted.add(col)
        if col == "applicability" or col.startswith("applicability."):
            wanted.add(col)

    # Filter input columns
    for col in all_file_columns:
        base = col.split("..")[0]
        if base in _FILTER_INPUT_COLS:
            wanted.add(col)

    # All energy columns (both raw and intensity)
    for col in manifest.energy_columns:
        if col in all_file_columns:
            wanted.add(col)
    for col in manifest.intensity_columns:
        if col in all_file_columns:
            wanted.add(col)

    # All bill columns
    for col in manifest.bill_columns:
        if col in all_file_columns:
            wanted.add(col)

    # Total site energy savings for applicability check fallback
    for col in all_file_columns:
        if "site_energy.total" in col and "savings" in col:
            wanted.add(col)
        if "percent_savings.site_energy.total" in col:
            wanted.add(col)

    return [c for c in all_file_columns if c in wanted]


def _local_cache_path(s3_path: str, config: PipelineConfig) -> str:
    """Map an S3 path to a local cache path under config.cache_dir/{release_name}/..."""
    # s3_path looks like: oedi-data-lake/.../release_name/metadata_and_annual_results/...
    # Strip the S3 bucket prefix and keep only from release_name onward
    release = config.release_name
    idx = s3_path.find(release)
    if idx == -1:
        # Fallback: use the full path as a flat filename
        safe = s3_path.replace("/", "_").replace(":", "_")
        return os.path.join(config.cache_dir, safe)
    relative = s3_path[idx:]  # e.g. comstock_amy2018_release_3/metadata_and.../file.parquet
    return os.path.join(config.cache_dir, relative)


def _read_parquet_cached(
    fs: s3fs.S3FileSystem,
    s3_path: str,
    columns: list[str],
    config: PipelineConfig,
) -> pd.DataFrame:
    """Read a parquet file, using local cache if configured."""
    if not config.use_cache:
        return _read_parquet_s3(fs, s3_path, columns)

    local_path = _local_cache_path(s3_path, config)

    if not config.refresh_cache and os.path.exists(local_path):
        log.debug("Cache hit: %s", local_path)
        try:
            return pd.read_parquet(local_path, columns=columns)
        except Exception as e:
            log.warning("Cache read failed (%s), fetching from S3: %s", local_path, e)

    log.debug("Cache miss, fetching s3://%s", s3_path)
    df = _read_parquet_s3(fs, s3_path, columns=None)  # fetch all columns, cache full file

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    df.to_parquet(local_path, index=False)
    log.debug("Cached to %s", local_path)

    # Now return only the requested columns
    available = [c for c in columns if c in df.columns]
    return df[available]


def _read_parquet_s3(fs: s3fs.S3FileSystem, path: str, columns: list[str] | None) -> pd.DataFrame:
    log.debug("Reading s3://%s", path)
    try:
        table = pq.read_table(f"s3://{path}", columns=columns, filesystem=fs)
        return table.to_pandas()
    except Exception as e:
        log.error("Failed to read s3://%s: %s", path, e)
        raise


def _read_parquet(fs: s3fs.S3FileSystem, path: str, columns: list[str]) -> pd.DataFrame:
    """Legacy wrapper — reads directly from S3 without caching."""
    return _read_parquet_s3(fs, path, columns)


def _get_file_columns(fs: s3fs.S3FileSystem, path: str, config: PipelineConfig | None = None) -> list[str]:
    """Read column names from parquet schema. Uses cached file if available."""
    if config and config.use_cache and not config.refresh_cache:
        local_path = _local_cache_path(path, config)
        if os.path.exists(local_path):
            try:
                schema = pq.read_schema(local_path)
                return schema.names
            except Exception:
                pass
    schema = pq.read_schema(f"s3://{path}", filesystem=fs)
    return schema.names


def _upgrade_id_to_filename_part(upgrade_id: int) -> str:
    """Convert upgrade ID to the zero-padded string used in file names."""
    if upgrade_id == 0:
        return "baseline"
    return f"upgrade{upgrade_id:02d}"


def _find_files_by_state_and_county(
    fs: s3fs.S3FileSystem,
    base_path: str,
    state_abbrs: list[str],
    upgrade_ids: list[int],
) -> list[tuple[int, str]]:
    """Return list of (upgrade_id, file_path) tuples from county-partitioned structure."""
    county_base = f"{base_path}/metadata_and_annual_results/by_state_and_county/full/parquet"
    results = []

    for state in state_abbrs:
        county_dirs = []
        try:
            county_dirs = fs.ls(f"{county_base}/state={state}", detail=False)
        except Exception:
            log.warning("No county dirs found for state=%s", state)
            continue

        for cdir in county_dirs:
            try:
                files = fs.ls(cdir, detail=False)
            except Exception:
                continue
            for uid in upgrade_ids:
                if uid == 0:
                    pattern = re.compile(r"upgrade0\.parquet$")
                else:
                    pattern = re.compile(rf"upgrade{uid:02d}\.parquet$", re.IGNORECASE)
                    # Also match "upgrade{uid}.parquet" without zero-padding
                    pattern2 = re.compile(rf"upgrade{uid}\.parquet$", re.IGNORECASE)

                for fpath in files:
                    fname = fpath.split("/")[-1]
                    if uid == 0 and (pattern.search(fname) or "baseline" in fname.lower()):
                        results.append((uid, fpath))
                        break
                    elif uid != 0 and (pattern.search(fname) or pattern2.search(fname)):
                        results.append((uid, fpath))
                        break

    return results


def _find_files_by_state(
    fs: s3fs.S3FileSystem,
    base_path: str,
    state_abbrs: list[str],
    upgrade_ids: list[int],
) -> list[tuple[int, str]]:
    """Return list of (upgrade_id, file_path) tuples from state-partitioned structure."""
    results = []
    for state in state_abbrs:
        state_path = f"{base_path}/metadata_and_annual_results/by_state/state={state}/parquet"
        try:
            files = fs.ls(state_path, detail=False)
        except Exception:
            log.warning("No files found at %s", state_path)
            continue

        for uid in upgrade_ids:
            for fpath in files:
                fname = fpath.split("/")[-1].lower()
                if uid == 0 and ("baseline" in fname) and fname.endswith(".parquet"):
                    results.append((uid, fpath))
                    break
                elif uid != 0 and f"upgrade{uid:02d}" in fname and fname.endswith(".parquet"):
                    results.append((uid, fpath))
                    break
                elif uid != 0 and f"upgrade{uid}_" in fname and fname.endswith(".parquet"):
                    results.append((uid, fpath))
                    break

    return results


def _find_files_national(
    fs: s3fs.S3FileSystem,
    base_path: str,
    upgrade_ids: list[int],
) -> list[tuple[int, str]]:
    nat_path = f"{base_path}/metadata_and_annual_results/national/parquet"
    results = []
    try:
        files = fs.ls(nat_path, detail=False)
    except Exception:
        log.warning("No files found at %s", nat_path)
        return results

    for uid in upgrade_ids:
        for fpath in files:
            fname = fpath.split("/")[-1].lower()
            if uid == 0 and "baseline" in fname and fname.endswith(".parquet"):
                results.append((uid, fpath))
                break
            elif uid != 0 and f"upgrade{uid:02d}" in fname and fname.endswith(".parquet"):
                results.append((uid, fpath))
                break
            elif uid != 0 and f"upgrade{uid}_" in fname and fname.endswith(".parquet"):
                results.append((uid, fpath))
                break

    return results


def _find_files_metadata(
    fs: s3fs.S3FileSystem,
    base_path: str,
    upgrade_ids: list[int],
) -> list[tuple[int, str]]:
    meta_path = f"{base_path}/metadata"
    results = []
    try:
        files = fs.ls(meta_path, detail=False)
    except Exception:
        return results

    for uid in upgrade_ids:
        for fpath in files:
            fname = fpath.split("/")[-1].lower()
            if uid == 0 and "baseline" in fname and fname.endswith(".parquet"):
                results.append((uid, fpath))
                break
            elif uid != 0 and f"upgrade{uid}" in fname and fname.endswith(".parquet"):
                results.append((uid, fpath))
                break

    return results


def _find_files_aggregate_by_state(
    fs: s3fs.S3FileSystem,
    base_path: str,
    state_abbrs: list[str],
    upgrade_ids: list[int],
) -> list[tuple[int, str]]:
    """Return (upgrade_id, file_path) tuples from aggregate by_state path."""
    results = []
    agg_base = f"{base_path}/metadata_and_annual_results_aggregates/by_state/full/parquet"
    for state in state_abbrs:
        state_path = f"{agg_base}/state={state}"
        try:
            files = fs.ls(state_path, detail=False)
        except Exception:
            log.warning("No aggregate files found for state=%s at %s", state, state_path)
            continue
        for uid in upgrade_ids:
            for fpath in files:
                fname = fpath.split("/")[-1].lower()
                if uid == 0 and (f"upgrade{uid}_" in fname or "baseline" in fname) and fname.endswith(".parquet"):
                    results.append((uid, fpath))
                    break
                elif uid != 0 and f"upgrade{uid:02d}" in fname and fname.endswith(".parquet"):
                    results.append((uid, fpath))
                    break
                elif uid != 0 and f"upgrade{uid}_" in fname and fname.endswith(".parquet"):
                    results.append((uid, fpath))
                    break
    return results


def _find_files_aggregate_national(
    fs: s3fs.S3FileSystem,
    base_path: str,
    upgrade_ids: list[int],
) -> list[tuple[int, str]]:
    """Return (upgrade_id, file_path) tuples from aggregate national path."""
    results = []
    nat_path = f"{base_path}/metadata_and_annual_results_aggregates/national/full/parquet"
    try:
        files = fs.ls(nat_path, detail=False)
    except Exception:
        log.warning("No aggregate national files found at %s", nat_path)
        return results
    for uid in upgrade_ids:
        for fpath in files:
            fname = fpath.split("/")[-1].lower()
            if uid == 0 and (f"upgrade{uid}_" in fname or "baseline" in fname) and fname.endswith(".parquet"):
                results.append((uid, fpath))
                break
            elif uid != 0 and f"upgrade{uid:02d}" in fname and fname.endswith(".parquet"):
                results.append((uid, fpath))
                break
            elif uid != 0 and f"upgrade{uid}_" in fname and fname.endswith(".parquet"):
                results.append((uid, fpath))
                break
    return results


def pull(config: PipelineConfig, manifest: DatasetManifest) -> PulledData:
    """Run Phase 2: pull per-building data for all requested upgrades."""
    fs = _get_fs()
    base_path = manifest.base_path

    if config.use_cache:
        if config.refresh_cache:
            log.info("Cache mode: refresh (re-downloading all files to %s/)", config.cache_dir)
        else:
            log.info("Cache mode: enabled (reading from %s/ when available)", config.cache_dir)
    else:
        log.info("Cache mode: disabled (fetching directly from S3)")

    # Determine which upgrades to load
    requested_upgrades = config.upgrade_ids
    if requested_upgrades is None:
        requested_upgrades = manifest.available_upgrades_on_disk
        if not requested_upgrades:
            requested_upgrades = sorted(manifest.upgrades.keys())
    # Always include baseline (0)
    if 0 not in requested_upgrades:
        requested_upgrades = [0] + list(requested_upgrades)
    requested_upgrades = sorted(requested_upgrades)

    state_abbrs = config.state_abbreviations()

    # Aggregate mode: read pre-collapsed files (one row per bldg_id per upgrade)
    if config.use_aggregate:
        log.warning(
            "use_aggregate=True (scope=%s): reading aggregate files. "
            "Energy/EUI statistics are correct. Bill statistics are approximate "
            "(bills summed across samples per archetype, not per-building values).",
            config.aggregate_scope,
        )
        if config.aggregate_scope == "national" and manifest.has_aggregate_national:
            file_list = _find_files_aggregate_national(fs, base_path, requested_upgrades)
            partition = "aggregate_national"
        elif manifest.has_aggregate_by_state:
            if not state_abbrs:
                # No state filter — list all available aggregate state dirs
                agg_base = f"{base_path}/metadata_and_annual_results_aggregates/by_state/full/parquet"
                dirs = []
                try:
                    dirs = fs.ls(agg_base, detail=False)
                except Exception:
                    pass
                state_abbrs = [d.split("state=")[-1] for d in dirs if "state=" in d]
            file_list = _find_files_aggregate_by_state(fs, base_path, state_abbrs, requested_upgrades)
            partition = "aggregate_by_state"
        else:
            log.warning("use_aggregate=True but no aggregate paths found — falling back to non-aggregate.")
            file_list = []
            partition = manifest.preferred_partition
    else:
        file_list = []
        partition = manifest.preferred_partition

    # Non-aggregate (or fallback)
    if not config.use_aggregate or not file_list:
        if state_abbrs and partition == "by_state_and_county":
            file_list = _find_files_by_state_and_county(fs, base_path, state_abbrs, requested_upgrades)
        elif state_abbrs and partition == "by_state":
            file_list = _find_files_by_state(fs, base_path, state_abbrs, requested_upgrades)
        elif partition == "national":
            file_list = _find_files_national(fs, base_path, requested_upgrades)
        else:
            file_list = _find_files_metadata(fs, base_path, requested_upgrades)

    if not file_list:
        raise RuntimeError(
            f"No parquet files found for partition={partition}, states={state_abbrs}, "
            f"upgrades={requested_upgrades}"
        )

    log.info("Found %d files to read", len(file_list))

    # Read one file to determine actual available columns (may differ from manifest schema)
    first_file = file_list[0][1]
    actual_columns = _get_file_columns(fs, first_file, config)
    columns_to_read = _select_columns(manifest, actual_columns)
    log.info("Selecting %d of %d columns", len(columns_to_read), len(actual_columns))

    # Read all files, concat
    frames = []
    loaded_upgrades = set()

    for uid, fpath in file_list:
        log.info("Reading upgrade=%d from %s", uid, fpath)
        df = _read_parquet_cached(fs, fpath, columns_to_read, config)
        # Tag with upgrade ID if column missing
        if "upgrade" not in df.columns:
            df["upgrade"] = uid
        frames.append(df)
        loaded_upgrades.add(uid)

    combined = pd.concat(frames, ignore_index=True)
    log.info("Total rows loaded: %d", len(combined))

    return PulledData(
        df=combined,
        upgrades_loaded=sorted(loaded_upgrades),
        columns_loaded=columns_to_read,
        partition_used=partition,
        n_rows=len(combined),
    )
