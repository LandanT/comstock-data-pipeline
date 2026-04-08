# CLAUDE.md — ComStock Data Pipeline

> This file doubles as both the **project spec** and the **CLAUDE.md context document** loaded automatically by Claude Code. Keep it updated as the implementation evolves.

---

## README maintenance

**Keep README.md up to date whenever you:**
- Add, rename, or remove a module or CLI flag
- Change output formats or file structure
- Add a new pipeline phase or configuration option
- Fix a bug that changes observable behavior (filter logic, unit conversion, etc.)

The README is the user-facing contract. If the code changes, the README changes in the same commit.

---

# ComStock EUSS Data Pipeline

## 1. Project Overview

**Goal:** Build a reusable, standalone Python pipeline that pulls ComStock (or ResStock) End-Use Savings Shape (EUSS) data from the OEDI S3 data lake, filters by user-specified building stock characteristics, and produces structured summary statistics of energy end-use breakdowns and utility bill impacts across baseline and all upgrade scenarios.

**This is NOT a game-specific tool.** It is a general-purpose ComStock data extraction and summarization pipeline. Downstream consumers (like the Better Buildings Workshop game) reference its output, but the pipeline itself has no knowledge of game cards or game logic.

**Core output:** For every upgrade scenario, the pipeline produces per-building end-use energy breakdowns (by fuel and end-use category), then summarizes those into population-level statistics (median, mean, percentiles) with sample counts and utility bill summaries.

**Design Principles:**
- **Top-down configuration** — user specifies release, building type, location, vintage; pipeline handles everything
- **Release-agnostic** — point it at any EUSS release and it discovers available data, upgrade names, and column schemas automatically
- **End-use granularity preserved** — every `out.{fuel}.{end_use}.energy_consumption` column is carried through per-building, then summarized
- **Uses ComStock's native data** — upgrade names from `upgrades_lookup.json`; utility bills from ComStock's `out.utility_bills.*` columns (URDB-based)
- **Annual results only** (v1) — uses `metadata_and_annual_results` files, no timeseries
- **Reports sample sizes** — always surface how many buildings/models are represented

---

## 2. OEDI S3 File Structure

### 2.1 Path Convention

```
s3://oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/
  {year}/
    {dataset}_{weather}_{release}/
```

**Naming formula:** `{dataset}_{weather}_{year}_release_{number}`
- `dataset`: `comstock` or `resstock`
- `weather`: `amy2018`, `amy2012`, `tmy3`
- `year`: publication year (2021, 2022, 2023, 2024, 2025)
- `release`: `release_1`, `release_2`, `release_3`

**Example:** `comstock_amy2018_release_3` in the `2025/` directory

### 2.2 Key Files per Release

```
{release_name}/
├── upgrades_lookup.json                           # upgrade ID → name (ALWAYS present)
├── data_dictionary.tsv                            # column definitions & units (NOT always present)
├── enumeration_dictionary.tsv                     # enum value descriptions
├── metadata/                                      # COMPACT: building chars + annual results (absent in 2025 R3)
│   ├── baseline.parquet                           # (2022+ structure)
│   └── upgrade{N}.parquet
├── metadata_and_annual_results/                   # NON-AGGREGATE: per-sample rows (what we want)
│   ├── by_state/                                  # state-level partition (legacy, pre-2025 R3)
│   ├── by_state_and_county/                       # county-level partition (2025 R3+, preferred)
│   │   ├── basic/parquet/state={XX}/county={FIPS}/
│   │   └── full/parquet/state={XX}/county={FIPS}/
│   │       └── {STATE}_{FIPS}_upgrade{N}.parquet
│   └── national/parquet/
└── metadata_and_annual_results_aggregates/        # AGGREGATE: one row per bldg_id (not for distribution stats)
    ├── by_state_and_county/                       # county-level partition (2025 R3+, preferred)
    │   ├── basic/parquet/state={XX}/county={FIPS}/
    │   └── full/parquet/state={XX}/county={FIPS}/
    │       └── {STATE}_{FIPS}_upgrade{N}.parquet
    ├── by_state_and_puma/
    ├── by_state/
    │   ├── baic/parquet/state={XX}
    |   ├── full/parquet/state={XX}                # county-level partition (2025 R3+, preferred)
    │       └── {STATE}_upgrade{N}_agg.parquet
    └── national/
```

### 2.3 CRITICAL: Non-Aggregate vs Aggregate Data

In ComStock, each building archetype (`bldg_id`) is **sampled multiple times** to account for geographic variability and statistical weighting. This is the fundamental design of the dataset.

**Non-aggregate files (`metadata_and_annual_results/`)** — what the pipeline uses:
- Multiple rows per `bldg_id` (~7–8 on average for 2025 R3)
- Each row is one sampled instance of a building archetype for a particular upgrade
- Each row carries a **`weight`** column indicating how many real buildings that sample represents
- Contains full per-building columns: `out.{fuel}.{end_use}.energy_consumption..kwh`, `_intensity`, percent savings, applicability flags, detailed geography, EJ flags, utility IDs
- **Required for computing medians, percentiles, and any distribution statistics**

**Aggregate files (`metadata_and_annual_results_aggregates/`)** — NOT for this pipeline's core use:
- One row per `bldg_id` per upgrade (sampling dimension collapsed)
- Additive fields (`weight`, `calc.weighted.*`, `out.utility_bills.*`) are **summed** across sampled instances
- Invariant metadata (`in.*`) is carried forward; sample-specific columns (PUMA, tract, EJ flags, utility IDs) are dropped
- These are **NOT** grouped across different buildings — they aggregate over the sampling dimension only
- Faster to load, but **cannot compute medians, percentiles, or sub-county filters**
- Use only as an optional fast-path for coarse total-energy summaries

**Pipeline default:** Always use non-aggregate data (`metadata_and_annual_results/`). The preferred partition for 2025 R3+ is `by_state_and_county/full/`. When a state filter is provided, restrict to matching `state={XX}` directories; otherwise iterate all available states.

**Confirmed S3 path structure (2025 R3):**
```
{release}/
├── metadata_and_annual_results/              ← NON-AGGREGATE (use this)
│   └── by_state_and_county/
│       ├── basic/parquet/state={XX}/county={FIPS}/
│       └── full/parquet/state={XX}/county={FIPS}/
│           └── {STATE}_{FIPS}_upgrade{N}.parquet
└── metadata_and_annual_results_aggregates/   ← AGGREGATE (one row per bldg_id, not for distributions)
    ├── by_state_and_county/
    ├── by_state_and_puma/
    ├── by_state/
    └── national/
```

### 2.4 `upgrades_lookup.json`

Simple `{id_string: name_string}` mapping. Present in every release.

```json
{"0": "Baseline", "1": "Variable Speed HP RTU, Electric Backup", ...}
```

The pipeline fetches this automatically — no hardcoded upgrade names.

### 2.5 `data_dictionary.tsv`

**NOT available in every release.** Contains column names, data types, units, descriptions, and allowable enumerations.

**Fallback strategy:**
1. Try to fetch `data_dictionary.tsv` from the target release
2. If not found, walk backwards through recent releases to find the nearest one
3. Use the dictionary for unit detection and column descriptions
4. If no dictionary found, rely on column name pattern matching for unit inference
5. Log which dictionary was used and whether it's from the target release or a fallback

---

## 3. Data Model

### 3.1 What We Pull Per Building Sample

The core data unit is a **single sampled instance of a building archetype for a single upgrade**. Multiple rows per `bldg_id` are expected and correct — each represents a different real-world placement of that archetype.

**Identification:**
- `bldg_id` — building archetype ID (repeats across samples)
- `upgrade` — upgrade number (0 = baseline)
- `weight` — number of real buildings this sample represents (**confirmed present** in non-aggregate files)

**Building characteristics (for filtering):**
- `in.comstock_building_type` — e.g., `"LargeOffice"`, `"MediumOffice"`, `"SmallOffice"` (not `in.building_type`)
- `in.vintage` — categorical: `"Before 1946"`, `"1946 to 1959"`, ..., `"2013 to 2018"`
- `in.ashrae_iecc_climate_zone_2006` — assigned climate zone, e.g., `"4A"` (**use this**, not `as_simulated_*`)
- `in.as_simulated_state_name` — full state name, e.g., `"District of Columbia"` (not abbreviations)
- `in.sqft` (or `in.sqft..ft2` with unit suffix) — building floor area in ft²

**Annual end-use energy (the core output):**

Every `out.{fuel}.{end_use}.energy_consumption..kwh` column. Fuels in 2025 R3:
- `electricity`, `natural_gas`, `fuel_oil`, `propane`, `district_cooling`, `district_heating`, `site_energy`

Plus pre-computed intensity columns: `out.{fuel}.{end_use}.energy_consumption_intensity..kwh_per_ft2`

**Utility bills (2025 R3 — numeric columns, not strings):**
- `out.utility_bills.electricity_bill_mean..usd`, `..electricity_bill_max..usd`, etc.
- `out.utility_bills.total_bill_mean_intensity..usd_per_ft2`
- Gas/propane/fuel oil: `out.utility_bills.natural_gas_bill_state_average_intensity..usd_per_ft2`, etc.

### 3.2 Column Discovery Strategy

The pipeline MUST NOT hardcode column names. Discover from Parquet schema at runtime:

```python
schema = pq.read_schema(sample_file, filesystem=fs)
# All output columns use "..unit" suffix (e.g., "..kwh", "..kwh_per_ft2", "..usd")
energy_cols = [c for c in schema.names if "energy_consumption" in c and c.startswith("out.")]
intensity_cols = [c for c in schema.names if "energy_consumption_intensity" in c and c.startswith("out.")]
bill_cols = [c for c in schema.names if c.startswith("out.utility_bills")]
```

**Column name resolution** must handle `..unit` suffixes — searching for `"in.sqft"` must also match `"in.sqft..ft2"`. Use prefix matching: `col.split("..")[0] == candidate`.

### 3.3 Unit Detection & Normalization

**Priority:** column name suffix (`..kwh`) → data dictionary → default (kWh for ComStock)

**Target output unit:** kBtu/ft² (site EUI)
- kWh → kBtu: × 3.412
- kBtu → kBtu/ft²: ÷ `in.sqft`
- Prefer `_intensity..kwh_per_ft2` columns (pre-computed) over manual calculation when available

---

## 4. Pipeline Architecture

### 4.1 Module Structure

```
comstock_pipeline/
├── CLAUDE.md                        # This file
├── config.py                        # PipelineConfig dataclass
├── pipeline/
│   ├── __init__.py
│   ├── discover.py                  # Phase 1: Discover dataset paths, schema, dictionary
│   ├── pull.py                      # Phase 2: Pull per-building data from S3
│   ├── filter.py                    # Phase 3: Apply building stock filters
│   ├── summarize.py                 # Phase 4: Per-building EUIs → population statistics
│   └── export.py                    # Phase 5: Format and write outputs
├── outputs/                         # Generated files
├── run.py                           # CLI entry point
└── requirements.txt
```

### 4.2 Phase Details

**Phase 1 — Discover (`discover.py`):** Fetch `upgrades_lookup.json`, attempt `data_dictionary.tsv` with fallback, probe S3 structure (`by_state_and_county` → `by_state` → `national` → `metadata`), read Parquet schema from one file to classify all columns, build `DatasetManifest`.

**Phase 2 — Pull (`pull.py`):** Read only needed columns. For 2025 R3+, iterate `by_state_and_county/full/parquet/state={XX}/county={FIPS}/` files. For legacy releases, fall back to `by_state/` or `national/`. Always read non-aggregate path. Return DataFrame with all energy, intensity, bill, and filter columns preserved.

**Phase 3 — Filter (`filter.py`):** Apply building type → vintage → climate zone → state filters in order. Report filter funnel with sample counts at each step. Match vintage categories by parsing year ranges from category strings (e.g., `"1946 to 1959"` → upper=1959). Compute `represented_area_ft2 = sum(in.sqft × weight)` for filtered baseline buildings.

**Phase 4 — Summarize (`summarize.py`):** Convert per-sample energy to EUI (prefer intensity columns). Compute weighted median, mean, p25, p75 across samples using `weight` column. Compute savings vs baseline per end-use. Detect applicability from `applicability` boolean column.

**Phase 5 — Export (`export.py`):** Write CSV (wide + long), Parquet, Excel (multi-sheet), Markdown. Include provenance header in all outputs.

**Filter funnel output (always printed):**
```
=== Filter Funnel (baseline, upgrade=0) ===
Total baseline models loaded:        350,000
After building type filter:           42,000  (LargeOffice, MediumOffice)
After vintage filter (max=1980):      18,500
  Matched vintages: ['Before 1946', '1946 to 1959', '1960 to 1969', '1970 to 1979']
After climate zone filter:             1,200  (4A)
After state filter:                      180  (DC)
Final baseline building models:          180
Total weight (represented floor area): 12,400,000 ft²
Upgrade scenarios available:              65
```

---

## 5. Configuration

```python
@dataclass
class PipelineConfig:
    # ── Dataset Selection ──
    release_year: str = "2025"
    release_name: str = "comstock_amy2018_release_3"

    # ── Building Stock Filters ──
    # Maps to: in.comstock_building_type
    building_types: list[str] = field(default_factory=lambda: [
        "LargeOffice", "MediumOffice", "SmallOffice"
    ])
    vintage_max: Optional[int] = 1980
    vintage_min: Optional[int] = None
    # Maps to: in.ashrae_iecc_climate_zone_2006
    climate_zones: Optional[list[str]] = field(default_factory=lambda: ["4A"])
    # Maps to: in.as_simulated_state_name (FULL names, not abbreviations)
    # The by_state_and_county S3 partition uses abbreviations — pipeline maps between them
    states: Optional[list[str]] = field(default_factory=lambda: ["District of Columbia"])
    upgrade_ids: Optional[list[int]] = None  # None = all upgrades

    # ── Output ──
    output_dir: str = "outputs"
    output_formats: list[str] = field(default_factory=lambda: ["csv", "xlsx", "md"])
    include_building_detail: bool = False
    include_long_format: bool = True

    # ── Statistical Options ──
    use_weights: bool = True
    statistics: list[str] = field(default_factory=lambda: ["median", "mean", "p25", "p75"])

    # ── Aggregate Mode ──
    use_aggregate: bool = False
    # When True, read from metadata_and_annual_results_aggregates/ (one row per bldg_id).
    # Energy/EUI medians are correct. Bill statistics are approximate (bills summed across samples).
    aggregate_scope: str = "state"  # "state" or "national"

    # ── Advanced ──
    min_sample_warning: int = 100
    energy_unit: str = "kbtu_ft2"
```

---

## 6. Column Name Corrections (Confirmed from 2025 R2 Data Dictionary)

| What we assumed | Actual column name | Notes |
|----------------|-------------------|-------|
| `in.building_type` | **`in.comstock_building_type`** | Enums: `LargeOffice`, `MediumOffice`, `SmallOffice`, etc. |
| `in.state` | **`in.as_simulated_state_name`** | Full state names: `"District of Columbia"`, `"Colorado"` |
| `in.ashrae_iecc_climate_zone_2006` | **`in.as_simulated_ashrae_iecc_climate_zone_2006`** | Use `in.ashrae_iecc_climate_zone_2006` for filtering (assigned, not simulated) |
| `in.floor_area_ft2` | **`in.sqft`** (or `in.sqft..ft2` with unit suffix) | Units: ft² |
| `weight` | **`weight`** ✓ | **Confirmed present** in non-aggregate per-building files. Each sample row carries weight = number of real buildings represented. NOT in data dictionary but confirmed in actual Parquet. |
| `in.vintage` | **`in.vintage`** ✓ | Enums: `"Before 1946"`, `"1946 to 1959"`, `"1960 to 1969"`, `"1970 to 1979"`, `"1980 to 1989"`, `"1990 to 1999"`, `"2000 to 2012"`, `"2013 to 2018"` |
| `upgrade` | **`upgrade`** ✓ | ID including `0` for Baseline |

### Vintage Matching Logic

For `vintage_max=1980`, match: `"Before 1946"`, `"1946 to 1959"`, `"1960 to 1969"`, `"1970 to 1979"` (upper bound ≤ 1980).

Parse upper bound year from category string. Handle `"Before XXXX"` as always included if XXXX ≤ vintage_max.

### Applicability Field

`applicability` boolean column per building per upgrade. Check `applicability == True` — no tolerance-based comparison needed.

### Pre-Computed Fields (2025 R3)

- **EUI intensity**: `out.{fuel}.{end_use}.energy_consumption_intensity..kwh_per_ft2` — prefer these over manual energy/sqft
- **Energy savings**: `out.{fuel}.{end_use}.energy_savings..kwh`
- **Percent savings**: `calc.percent_savings.{fuel}.{end_use}.energy_consumption..percent`
- **Utility bills**: Numeric columns (not pipe-delimited strings) — `out.utility_bills.electricity_bill_mean..usd`, `out.utility_bills.total_bill_mean_intensity..usd_per_ft2`, etc.

### Basic vs Full Metadata

The pipeline always targets full metadata (`by_state_and_county/full/`). Basic metadata (43 fields) only has total site EUI, not end-use breakdowns. Warn if only basic is available.

---

## 7. Known Issues & Resolved Questions

| Question | Status | Answer |
|----------|--------|--------|
| Weight column in 2025 R3? | ✅ Resolved | `weight` IS present in non-aggregate per-building files |
| `in.sqft` with unit suffix? | ✅ Resolved | Column is `in.sqft..ft2` — use prefix matching in `_find_col` |
| Aggregate vs per-building distinction? | ✅ Resolved | See §2.3. Aggregates collapse sampling rows, not buildings. Use non-aggregate. |
| Utility bill format? | ✅ Resolved | Numeric columns in 2025 R3. Pipe-delimited strings in older releases (not yet supported). |
| State name vs abbreviation? | ✅ Resolved | `in.as_simulated_state_name` uses full names; S3 partition dirs use abbreviations. `config.py` maps between them. |
| `in.year_built` format? | ⚠️ Unknown | May be numeric or string. Vintage category matching (`in.vintage`) is the primary filter. |
| Memory with full metadata? | ⚠️ Ongoing | Column selection before read is critical. 1,287 cols × ~10K rows per county file is manageable. |
| `use_aggregate` / `aggregate_scope` | ✅ Implemented | `use_aggregate=True` reads from `metadata_and_annual_results_aggregates/`. `aggregate_scope="state"` (default) or `"national"`. Energy/EUI medians are correct; bill statistics are approximate (bills summed across samples per archetype, not per-building values). |
