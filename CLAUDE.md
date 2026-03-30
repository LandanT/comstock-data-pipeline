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

## Project Plan & CLAUDE.md (v3)

---

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

From the official README at `s3://oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/README.md`:

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
├── metadata/                                      # COMPACT: building chars + annual results
│   ├── baseline.parquet                           # (2022+ structure)
│   ├── upgrade{N}.parquet                         # (2022+ structure)
│   └── metadata.parquet                           # (2021 structure — single file, all upgrades)
├── metadata_and_annual_results/                   # FULL: same data, human-readable format
│   ├── by_state/
│   │   └── state={XX}/
│   │       ├── csv/
│   │       │   ├── {XX}_baseline_metadata_and_annual_results.csv
│   │       │   └── {XX}_upgrade{N}_metadata_and_annual_results.csv
│   │       └── parquet/
│   │           ├── {XX}_baseline_metadata_and_annual_results.parquet
│   │           └── {XX}_upgrade{N}_metadata_and_annual_results.parquet
│   └── national/
│       ├── csv/
│       │   ├── baseline_metadata_and_annual_results.csv
│       │   └── upgrade{N}_metadata_and_annual_results.csv
│       └── parquet/
│           ├── baseline_metadata_and_annual_results.parquet
│           └── upgrade{N}_metadata_and_annual_results.parquet
└── timeseries_individual_buildings/               # (NOT used in v1 — timeseries)
    └── by_state/
        └── upgrade={N}/
            └── state={XX}/
                └── {building_id}-{N}.parquet
```

### 2.3 Two Metadata Path Options

The pipeline should support both paths:

| Path | Contents | Use Case |
|------|----------|----------|
| `metadata/` | Compact parquet, split by upgrade | Faster reads when you need all buildings |
| `metadata_and_annual_results/` | Full parquet/csv, split by state AND upgrade | Preferred when filtering by state (partition pruning) |

**Decision logic:**
- If `states` filter is specified → use `metadata_and_annual_results/by_state/state={XX}/parquet/`
- If no state filter → use `metadata_and_annual_results/national/parquet/` or `metadata/`
- Discover which structure exists (varies by release year)

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
4. If no dictionary found at all, rely on column name pattern matching for unit inference (see §3.2)
5. Log which dictionary was used and whether it's from the target release or a fallback

---

## 3. Data Model

### 3.1 What We Pull Per Building

The core data unit is a **single building model for a single upgrade scenario**. For each building, we pull:

**Identification:**
- `bldg_id` (or `building_id`)
- `upgrade` — upgrade number (0 = baseline)

**Building characteristics (for filtering):**
- `in.building_type` — e.g., "LargeOffice", "MediumOffice", "SmallOffice"
- `in.vintage` — categorical vintage string (e.g., "1946-1979", "Pre-1946")
- `in.ashrae_iecc_climate_zone_2006` — e.g., "4A"
- `in.state` — 2-letter abbreviation
- `in.sqft` (or `in.floor_area_ft2` — verify per release) — building floor area
- `weight` — floor area weight for representativeness

**Annual end-use energy consumption (the core output):**

Every column matching `out.{fuel}.{end_use}.energy_consumption` — these represent the **annual total energy for that fuel+end-use combination for that building**. Typical columns include:

| Fuel | End Uses |
|------|----------|
| `electricity` | `cooling`, `heating`, `interior_lighting`, `exterior_lighting`, `interior_equipment`, `fans`, `pumps`, `heat_rejection`, `heat_recovery`, `water_systems`, `refrigeration` |
| `natural_gas` | `heating`, `water_systems`, `interior_equipment` |
| `other_fuel` | `heating`, `cooling`, `water_systems` (propane, fuel oil, district) |
| `district_cooling` | `cooling` |
| `district_heating` | `heating`, `cooling`, `water_systems` |

Plus totals:
- `out.site_energy.total.energy_consumption`
- `out.electricity.total.energy_consumption`
- `out.natural_gas.total.energy_consumption`

**Units:** Column names in newer releases include unit suffixes (e.g., `..kwh`, `..kbtu`). Older releases may not. The pipeline must detect and normalize units.

**Utility bills (from ComStock's URDB-based calculations, available since ~2024 R1):**
- `out.utility_bills.electricity_bill_median..usd`
- `out.utility_bills.electricity_bill_mean..usd`
- `out.utility_bills.electricity_bill_min..usd`
- `out.utility_bills.electricity_bill_max..usd`
- `out.utility_bills.natural_gas_bill..usd`
- `out.utility_bills.propane_bill..usd`
- `out.utility_bills.fuel_oil_bill..usd`
- `out.utility_bills.electricity_bill_num_bills`

If utility bill columns don't exist for a release, the pipeline reports that bill data is unavailable (no fallback calculation).

### 3.2 Column Discovery Strategy

The pipeline MUST NOT hardcode column names. Instead:

```python
# Read Parquet schema (zero rows) to discover columns
schema = pq.read_schema(sample_file, filesystem=fs)

# Categorize columns by pattern
energy_cols = [c for c in schema.names if "energy_consumption" in c and c.startswith("out.")]
bill_cols = [c for c in schema.names if c.startswith("out.utility_bills")]
input_cols = [c for c in schema.names if c.startswith("in.")]
calc_cols = [c for c in schema.names if c.startswith("calc.")]

# Parse fuel and end-use from column names
# Pattern: out.{fuel}.{end_use}.energy_consumption[..{unit}]
for col in energy_cols:
    parts = col.replace("energy_consumption", "").strip(".").split(".")
    fuel = parts[1]      # electricity, natural_gas, etc.
    end_use = parts[2]   # cooling, heating, interior_lighting, etc.
    unit_suffix = parts[-1] if ".." in col else None  # kwh, kbtu, etc.
```

### 3.3 Unit Detection & Normalization

**Priority order:**
1. Column name suffix (e.g., `..kwh`) — most reliable in newer releases
2. Data dictionary `units` field — if dictionary is available
3. Known defaults — ComStock metadata annual results are typically in kWh
4. The 2022-03-07 changelog notes that commercial data corrected to kWh, matching data dictionary

**Target output unit:** kBtu/ft² (site EUI) — the standard for building energy analysis.

**Conversion factors:**
- kWh → kBtu: multiply by 3.412
- kBtu → kBtu/ft²: divide by building `in.sqft`

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

---

#### Phase 1: Discover (`discover.py`)

**Purpose:** Given a release, discover what's available on S3 and build a manifest.

**Steps:**
1. Construct S3 base path from config
2. Fetch `upgrades_lookup.json` → `{upgrade_id: upgrade_name}`
3. Attempt to fetch `data_dictionary.tsv` from target release
   - If not found → walk backwards through releases to find nearest available
   - Log which dictionary version is being used
4. Probe S3 to determine file structure:
   - Does `metadata_and_annual_results/by_state/` exist? List available states.
   - Does `metadata_and_annual_results/national/parquet/` exist?
   - Does `metadata/` with per-upgrade parquets exist?
   - What upgrade files exist on disk? (Match against `upgrades_lookup.json`)
5. Read Parquet **schema only** (zero rows) from one file:
   - Discover all `out.*.energy_consumption*` columns → `energy_columns`
   - Discover all `out.utility_bills.*` columns → `bill_columns`
   - Discover all `in.*` columns → `input_columns`
   - Detect unit suffixes in column names
6. If data dictionary available, cross-reference for unit confirmation

**Output:**
```python
@dataclass
class DatasetManifest:
    base_path: str
    release_id: str
    upgrades: dict[int, str]               # {0: "Baseline", ...}

    # File structure discovered
    has_by_state: bool
    has_national: bool
    has_metadata_dir: bool
    available_states: list[str]            # if by_state exists
    available_upgrades_on_disk: list[int]  # upgrade IDs with files present

    # Column schema
    energy_columns: list[str]              # all out.*.energy_consumption columns
    bill_columns: list[str]                # all out.utility_bills.* columns
    input_columns: list[str]              # all in.* columns
    id_columns: list[str]                 # bldg_id, upgrade, weight, etc.

    # Parsed energy column structure
    energy_column_map: dict[str, dict]    # {col_name: {fuel, end_use, unit}}

    # Units
    detected_energy_unit: str             # "kwh", "kbtu", or "unknown"

    # Data dictionary
    data_dictionary: pd.DataFrame | None
    data_dictionary_source: str | None    # which release it came from
```

---

#### Phase 2: Pull (`pull.py`)

**Purpose:** Retrieve per-building annual results from S3 for baseline + all requested upgrades.

**Strategy — read as little as possible:**

1. **Determine read path** based on config and manifest:
   - State filter + `has_by_state` → read `by_state/state={XX}/parquet/{XX}_baseline*.parquet` and `{XX}_upgrade{N}*.parquet`
   - No state filter + `has_national` → read `national/parquet/baseline*.parquet` and `upgrade{N}*.parquet`
   - Fallback to `metadata/` if neither exists

2. **Column selection:** Only read columns needed:
   - `id_columns` (bldg_id, upgrade, weight)
   - `input_columns` needed for filtering (building_type, vintage, climate_zone, state, sqft)
   - ALL `energy_columns` (every fuel × end-use combination)
   - ALL `bill_columns` (utility bills)

3. **Upgrade selection:**
   - Always read upgrade=0 (baseline)
   - Read all other upgrades by default (or subset if user specifies)
   - For `by_state` structure: loop over `{state}_upgrade{N}_metadata_and_annual_results.parquet` files
   - For `metadata/` structure: read `baseline.parquet` + `upgrade{N}.parquet` files

4. **Return per-building DataFrame** with ALL end-use columns preserved.

**This is the key — we preserve every end-use column per building.** The summarization step then aggregates.

---

#### Phase 3: Filter (`filter.py`)

**Purpose:** Apply user-specified filters and report the filter funnel.

**Filters (applied in order):**
1. Building type (e.g., `["LargeOffice", "MediumOffice"]`)
2. Vintage (categorical matching — see note below)
3. Climate zone (e.g., `["4A"]`)
4. State (may already be applied via partition in Phase 2)
5. Custom filters from config (optional key-value pairs)

**Vintage handling:**
- `in.vintage` is categorical, not numeric
- User specifies `vintage_max=1980` → pipeline discovers unique vintage values from data, then selects categories representing buildings built before 1980
- Example: `vintage_max=1980` → match `"Pre-1946"`, `"1946-1979"` (exclude `"1980-1989"`, `"1990-1999"`, etc.)
- The discovery of vintage category labels happens at filter time from the actual data
- Log which vintage categories were matched

**Filter funnel (always printed):**
```
=== Filter Funnel (baseline, upgrade=0) ===
Total baseline models loaded:        350,000
After building type filter:           42,000  (LargeOffice, MediumOffice)
  Matched types: ['LargeOffice', 'MediumOffice']
After vintage filter (max=1980):      18,500
  Matched vintages: ['Pre-1946', '1946-1979']
After climate zone filter:             1,200  (4A)
After state filter:                      180  (DC)
───────────────────────────────────────────
Final baseline building models:          180
Total weight (represented floor area): 12,400,000 ft²
Upgrade scenarios available:              65

⚠️  WARNING: N=180 is below the recommended minimum of 1,000 models.
   Aggregate results may be unreliable for small sub-populations.
   Consider expanding geographic scope (e.g., all CZ 4A states: DC, MD, VA, DE, NJ).
```

---

#### Phase 4: Summarize (`summarize.py`)

**Purpose:** Take per-building end-use data and produce population-level statistics per upgrade.

##### 4a. Per-Building EUI Calculation

For each building, convert raw energy consumption to EUI:

```python
# For each energy column:
building_eui[col] = building_energy[col] * unit_conversion_to_kbtu / building_sqft

# For each bill column:
building_bill_per_sqft[col] = building_bill[col] / building_sqft
```

##### 4b. Population Summary Statistics

Group by `upgrade` and compute weighted statistics across the building population:

For **every end-use EUI column** and **every bill column**:
- Weighted median
- Weighted mean
- Weighted p25, p75
- Unweighted count (N)
- Count of buildings where upgrade was applicable (value differs from baseline)

**Savings vs. baseline:**
For each upgrade, compute:
- `savings_eui = baseline_median - upgrade_median` (for each end-use and total)
- `savings_pct = savings_eui / baseline_median * 100`
- Same for bills

**Applicability detection:**
- Use `applicability` boolean column per building per upgrade (§9.3)
- If column absent, compare total site energy to baseline within 0.1% tolerance as fallback
- Report `n_applicable` alongside `n_buildings` for each upgrade

---

#### Phase 5: Export (`export.py`)

**Purpose:** Write outputs in requested formats.

**Formats:**
- **CSV** — wide summary table + long summary table
- **Parquet** — same, efficient for reuse
- **Excel** — formatted workbook:
  - "Summary (Wide)" sheet — upgrade × end-use matrix
  - "Summary (Long)" sheet — unpivoted for Tableau
  - "Building Detail" sheet (optional) — per-building EUIs
  - "Upgrade Lookup" sheet — full upgrade name list
  - "Metadata" sheet — filter config, sample sizes, warnings, data dictionary source
- **Markdown** — human-readable report with key findings and tables

**All exports include provenance header:**
```
Dataset: comstock_amy2018_release_3 (2025)
Data dictionary: comstock_amy2018_release_3 (native)  # or "fallback from release_2"
Filters: LargeOffice + MediumOffice | vintage ≤ 1980 | CZ 4A | DC
Matched vintages: Pre-1946, 1946-1979
Baseline buildings: 180
Represented floor area: 12,400,000 ft²
Upgrades analyzed: 66
Energy unit: kBtu/ft²
Generated: 2026-03-30
```

---

## 5. Configuration

```python
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class PipelineConfig:
    # ── Dataset Selection ──
    release_year: str = "2025"
    release_name: str = "comstock_amy2018_release_3"

    # ── Building Stock Filters ──
    building_types: list[str] = field(default_factory=lambda: [
        "LargeOffice", "MediumOffice", "SmallOffice"
    ])
    vintage_max: Optional[int] = 1980       # include vintages up to this year
    vintage_min: Optional[int] = None       # include vintages from this year
    climate_zones: Optional[list[str]] = field(default_factory=lambda: ["4A"])
    states: Optional[list[str]] = field(default_factory=lambda: ["DC"])
    upgrade_ids: Optional[list[int]] = None  # None = all upgrades

    # ── Output ──
    output_dir: str = "outputs"
    output_formats: list[str] = field(default_factory=lambda: ["csv", "xlsx", "md"])
    include_building_detail: bool = False
    include_long_format: bool = True        # unpivoted summary for Tableau

    # ── Statistical Options ──
    use_weights: bool = True
    statistics: list[str] = field(default_factory=lambda: [
        "median", "mean", "p25", "p75"
    ])

    # ── Advanced ──
    min_sample_warning: int = 100
    energy_unit: str = "kbtu_ft2"           # output EUI unit

    @property
    def s3_base_path(self) -> str:
        return (
            "oedi-data-lake/nrel-pds-building-stock/"
            "end-use-load-profiles-for-us-building-stock/"
            f"{self.release_year}/{self.release_name}"
        )
```

---

## 6. Execution Plan for Claude Code

### Step 1: Project Skeleton
- Directory structure, `config.py`, `requirements.txt`
- `run.py` CLI entry point with argparse

### Step 2: Discovery (`discover.py`)
- Fetch `upgrades_lookup.json` from S3
- Attempt `data_dictionary.tsv` with fallback logic
- Probe file structure (by_state vs national vs metadata)
- Read Parquet schema, classify columns
- Build `DatasetManifest`

### Step 3: Data Pull (`pull.py`)
- Implement path-aware S3 reads (by_state vs national vs metadata)
- Column selection
- Loop over baseline + upgrade files

### Step 4: Filtering (`filter.py`)
- Apply building type, vintage (categorical matching), CZ, state filters
- Print filter funnel with sample sizes
- Vintage category auto-matching

### Step 5: Summarization (`summarize.py`)
- Per-building EUI conversion (energy / sqft, with unit normalization)
- Weighted population statistics per upgrade per end-use column
- Utility bill summarization
- Savings vs. baseline
- Applicability detection
- Long-format pivot

### Step 6: Export (`export.py`)
- CSV (wide + long), Parquet, Excel (multi-sheet), Markdown
- Provenance headers

### Step 7: Polish
- Error handling, retries for S3 network issues
- Logging (`logging` module, configurable verbosity)
- Type hints, docstrings
- README with usage examples

---

## 7. Dependencies

```
# requirements.txt
boto3>=1.28
s3fs>=2024.1
pyarrow>=14.0
pandas>=2.0
numpy>=1.24
openpyxl>=3.1       # Excel export
```

---

## 8. Usage Examples

### Python API
```python
from config import PipelineConfig
from run import run_pipeline

config = PipelineConfig(
    release_year="2025",
    release_name="comstock_amy2018_release_3",
    building_types=["LargeOffice", "MediumOffice"],
    vintage_max=1980,
    climate_zones=["4A"],
    states=["DC"],
    include_building_detail=True,
    include_long_format=True,
)

results = run_pipeline(config)

print(results.building_detail.head())
print(results.summary_table[["upgrade_name", "out.site_energy.total.eui_kbtu_ft2.median"]])
print(f"Baseline N: {results.n_baseline_buildings}")
print(f"Not-applicable upgrades: {results.not_applicable_upgrades}")
```

### CLI
```bash
# DC pre-1980 offices from 2025 R3
python run.py \
  --release-year 2025 \
  --release-name comstock_amy2018_release_3 \
  --building-types LargeOffice MediumOffice \
  --vintage-max 1980 \
  --climate-zones 4A \
  --states "District of Columbia" \
  --include-building-detail \
  --output-formats csv xlsx md

# All offices nationwide, no geographic filter
python run.py \
  --release-year 2025 \
  --release-name comstock_amy2018_release_3 \
  --building-types LargeOffice MediumOffice SmallOffice \
  --vintage-max 1980

# Retail buildings in Colorado from 2024 R2
python run.py \
  --release-year 2024 \
  --release-name comstock_amy2018_release_2 \
  --building-types RetailStripmall RetailStandalone \
  --states CO
```

---

## 9. Corrections from Actual Data Dictionary (2025 R2)

The 2025 R2 `data_dictionary.tsv` (1,381 fields) reveals several important differences from our assumptions. These MUST be accounted for in the pipeline:

### 9.1 Column Name Corrections

| What we assumed | Actual column name (2025 R2) | Notes |
|----------------|------------------------------|-------|
| `in.building_type` | **`in.comstock_building_type`** | Enums: `LargeOffice`, `MediumOffice`, `SmallOffice`, `Hospital`, `RetailStripmall`, etc. |
| `in.state` | **`in.as_simulated_state_name`** | Full state names, not abbreviations! e.g., `"District of Columbia"`, `"Colorado"` |
| `in.ashrae_iecc_climate_zone_2006` | **`in.as_simulated_ashrae_iecc_climate_zone_2006`** | Same enums: `"4A"`, `"5A"`, etc. |
| `in.floor_area_ft2` | **`in.sqft`** | Units: ft² |
| `weight` (floor area weight) | **Not in dictionary** | There is NO floor area weight column in 2025 R2 dictionary. The `weight` column may only exist in older releases or the `metadata/` path. Need to check actual Parquet schema. |
| `in.vintage` | **`in.vintage`** ✓ | Enums: `"Before 1946"`, `"1946 to 1959"`, `"1960 to 1969"`, `"1970 to 1979"`, `"1980 to 1989"`, `"1990 to 1999"`, `"2000 to 2012"`, `"2013 to 2018"` |
| `in.year_built` | **`in.year_built`** ✓ | Also present (no enums listed — likely numeric). Both vintage and year_built exist. |
| `upgrade` (id) | **`upgrade`** ✓ | ID including "00" for Baseline |

### 9.2 Vintage Categories (Confirmed)

```
"Before 1946"
"1946 to 1959"
"1960 to 1969"
"1970 to 1979"
"1980 to 1989"
"1990 to 1999"
"2000 to 2012"
"2013 to 2018"
```

For `vintage_max=1980`, match: `"Before 1946"`, `"1946 to 1959"`, `"1960 to 1969"`, `"1970 to 1979"`.

The matching logic should:
1. Parse the upper bound year from each category string
2. Compare to `vintage_max`
3. Handle `"Before XXXX"` as always included if XXXX ≤ vintage_max

### 9.3 Applicability Field

There is a dedicated **`applicability`** boolean field per building per upgrade. This eliminates the need for our hacky "compare to baseline within 0.1% tolerance" approach. Simply check `applicability == True` for each building in each upgrade.

### 9.4 Pre-Computed EUI Fields Exist

ComStock already provides **34 `energy_consumption_intensity` columns** in `kwh_per_ft2` units. These are pre-computed EUI values for every fuel × end-use combination:

```
out.electricity.cooling.energy_consumption_intensity        (kwh_per_ft2)
out.electricity.heating.energy_consumption_intensity        (kwh_per_ft2)
out.electricity.interior_lighting.energy_consumption_intensity  (kwh_per_ft2)
out.natural_gas.heating.energy_consumption_intensity        (kwh_per_ft2)
out.site_energy.total.energy_consumption_intensity          (kwh_per_ft2)
... etc (34 total)
```

**This means we DON'T need to manually divide energy by sqft.** We can pull the `_intensity` columns directly and just convert units (kwh_per_ft2 → kbtu_per_ft2 via × 3.412). This is simpler and avoids potential mismatches.

**However:** The `_intensity` columns are in the full metadata file only (`in_full_metadata_file=true`), NOT in basic metadata. The pipeline should prefer these when available but fall back to manual calculation (energy / sqft) if only basic metadata columns are accessible.

### 9.5 Utility Bill Structure is Complex (older releases)

In releases prior to 2025 R3, the utility bill columns were **pipe-delimited strings** that embed structured results:

```
out.utility_bills.electricity_utility_bill_results
  Schema: |utility_id:min_bill_dollars:max_bill_dollars:mean_bill_dollars:median_bill_dollars:n_bills|

out.utility_bills.state_average_electricity_cost_results
  Schema: |state_abbreviation:total_dollars|
```

**2025 R3 uses numeric columns instead** (see §13.2). No parsing needed for the current target release.

### 9.6 Calc Fields — Pre-Computed Savings & End-Use Groups

ComStock provides pre-computed fields that could simplify or validate our calculations:

**End-use groups** (`calc.enduse_group.*`):
```
calc.enduse_group.electricity.hvac.energy_consumption
calc.enduse_group.electricity.lighting.energy_consumption
calc.enduse_group.site_energy.heating.energy_consumption
calc.enduse_group.site_energy.cooling.energy_consumption
```

**Percent savings** (`calc.percent_savings.*`):
```
calc.percent_savings.site_energy.total.energy_consumption
calc.percent_savings.electricity.cooling.energy_consumption
```

### 9.7 Basic vs Full Metadata

The dictionary distinguishes `in_basic_metadata_file` (43 fields) from `in_full_metadata_file` (1,381 fields). Basic includes:
- All `in.*` building characteristics needed for filtering
- `applicability` boolean
- `out.site_energy.total.energy_consumption_intensity` (total EUI only)
- ALL utility bill string fields
- `calc.percent_savings.site_energy.total.energy_consumption`

**For our use case (end-use breakdowns per building), we NEED the full metadata file.** The basic metadata only gives us total site EUI, not the end-use breakdown. The pipeline should always target full metadata and warn if only basic is available.

### 9.8 Data Dictionary TSV Format (Confirmed)

```
Columns: field_name | field_location | data_type | units | field_description | allowable_enumeration | in_full_metadata_file | in_basic_metadata_file
Delimiter: tab
Enumerations: pipe-delimited (|)
```

---

## 10. Revised Config Defaults

Based on actual column names:

```python
# Maps to: in.comstock_building_type
building_types: list[str] = field(default_factory=lambda: [
    "LargeOffice", "MediumOffice", "SmallOffice"
])

# Maps to: in.as_simulated_state_name (FULL names, not abbreviations)
# The by_state partition key may still use abbreviations — discover from S3
states: Optional[list[str]] = field(default_factory=lambda: ["District of Columbia"])

# Maps to: in.ashrae_iecc_climate_zone_2006
climate_zones: Optional[list[str]] = field(default_factory=lambda: ["4A"])
```

---

## 11. Open Questions

1. **Weight column** — The 2025 R2 dictionary does not include a floor area weight column. Need to check the actual Parquet schema to see if `weight` or `sample_weight` exists. If not, the pipeline may need to use unweighted statistics or compute weights from `in.sqft`.

2. **Utility bill parsing** — The packed string format (`|utility_id:min:max:mean:median:n_bills|`) needs robust parsing in older releases. In 2025 R3, bills are numeric (§13.2).

3. **Basic vs full metadata file paths** — Need to verify whether `metadata_and_annual_results/by_state/` contains full or basic metadata. If basic only, use `metadata/` or `national/` for end-use breakdowns.

4. **State name vs abbreviation** — `in.as_simulated_state_name` uses full names ("District of Columbia"), but the `by_state` S3 partition uses abbreviations ("DC"). Pipeline maps between them via lookup dict in `config.py`.

5. **`in.year_built` format** — Could be numeric year (1975) or a string. If numeric, can be used directly for vintage filtering instead of parsing category strings.

6. **Memory** — Full metadata with 1,381 columns × 350K buildings × 66 upgrades is potentially very large. Column selection before read is critical.

---

## 12. CRITICAL: Aggregate vs Per-Building Files (from actual Parquet inspection)

Files from `metadata_and_annual_results_AGGREGATES/by_state/full/parquet/state=CO/` are **aggregate** files — NOT the per-building files we need.

### Aggregate files (`metadata_and_annual_results_aggregates/`)
- Energy in `calc.weighted.*.energy_consumption..tbtu` (trillion BTU — stock-level)
- Bills in `calc.weighted.utility_bills.*..billion_usd` (stock-level dollars)
- Does NOT have `out.*.energy_consumption` (per-building kWh)
- Does NOT have `out.*.energy_consumption_intensity` (per-building kWh/ft2 EUI)
- Has `weight` column confirmed
- Has `in.state` and `in.state_name` (in addition to `in.as_simulated_state_name`)
- Has per-measure `applicability.{measure_name}` booleans (one per upgrade measure, 40+)

### Per-building files (`metadata_and_annual_results/by_state/`)
- Each row is one building with its individual simulation results
- Energy in `out.{fuel}.{end_use}.energy_consumption` (kWh per building)
- EUI in `out.{fuel}.{end_use}.energy_consumption_intensity` (kWh/ft2 per building)
- **This is what the pipeline needs for end-use breakdown statistics**

### Confirmed S3 path structure (2025 R3)

```
{release}/
├── metadata_and_annual_results/              ← PER-BUILDING (what we want)
│   ├── by_state/
│   │   └── state={XX}/
│   │       ├── csv/
│   │       └── parquet/
│   └── national/
│       ├── csv/
│       └── parquet/
└── metadata_and_annual_results_aggregates/   ← STOCK-LEVEL (pre-weighted)
    ├── by_state/
    │   ├── basic/
    │   │   └── parquet/state={XX}/
    │   └── full/
    │       └── parquet/state={XX}/
    └── national/
        ├── basic/
        └── full/
```

---

## 13. CONFIRMED: Per-Building File Schema (2025 R3, from actual Parquet)

Inspected: `CO_G0800010_upgrade0.parquet` from `metadata_and_annual_results/by_state_and_county/full/parquet/state=CO/county=G0800010/`

File: 12.7 MB, ~1,287 columns (full metadata).

### 13.1 Column Name Format Uses `..unit` Suffix

ALL output columns include unit suffixes with double-dot notation:
```
out.electricity.cooling.energy_consumption..kwh
out.electricity.cooling.energy_consumption_intensity..kwh_per_ft2
out.utility_bills.electricity_bill_mean..usd
out.utility_bills.total_bill_mean_intensity..usd_per_ft2
calc.weighted.electricity.cooling.energy_consumption..tbtu
```

The pipeline's column discovery must account for the `..unit` suffix when pattern-matching.

### 13.2 Utility Bills Are Numeric Columns (NOT pipe-delimited strings)

In 2025 R3, utility bill data is broken out into individual numeric columns.

**Electric bills (per building, in USD):**
- `out.utility_bills.electricity_bill_max..usd`
- `out.utility_bills.electricity_bill_mean..usd`
- `out.utility_bills.electricity_bill_median_high..usd`
- `out.utility_bills.electricity_bill_median_low..usd`
- `out.utility_bills.electricity_bill_min..usd`
- `out.utility_bills.electricity_bill_num_bills`

**Electric bill intensity (per building, in USD/ft2):**
- `out.utility_bills.electricity_bill_mean_intensity..usd_per_ft2`

**Demand charge breakdown:**
- `out.utility_bills.electricity_demandcharge_flat_bill_mean..usd`
- `out.utility_bills.electricity_demandcharge_tou_bill_mean..usd`

**Gas/propane/fuel oil (state average, per building):**
- `out.utility_bills.natural_gas_bill_state_average_intensity..usd_per_ft2`
- `out.utility_bills.propane_bill_state_average_intensity..usd_per_ft2`
- `out.utility_bills.fuel_oil_bill_state_average_intensity..usd_per_ft2`

**Total bill:**
- `out.utility_bills.total_bill_mean_intensity..usd_per_ft2`

### 13.3 Pre-Computed Fields Available

**EUI intensity** (per building, kwh_per_ft2): Every fuel × end-use has a `_intensity` column. No manual division needed.

**Energy savings** (per building, kwh): `out.{fuel}.{end_use}.energy_savings..kwh`

**Savings intensity** (per building, kwh_per_ft2): `out.{fuel}.{end_use}.energy_savings_intensity..kwh_per_ft2`

**Percent savings** (per building, percent): `calc.percent_savings.{fuel}.{end_use}.energy_consumption..percent`

### 13.4 Fuel Types (expanded from older releases)

2025 R3 separates fuels more granularly:
- `electricity` (including PV, net, purchased)
- `natural_gas`
- `fuel_oil` (separate from propane — was lumped as "other_fuel" previously)
- `propane` (separate)
- `district_cooling`
- `district_heating`
- `site_energy` (total across all fuels)

### 13.5 Partitioning Structure

2025 R3 goes to **county level** (finer than state):
```
metadata_and_annual_results/
├── by_state/                    (state-level partitioning)
├── by_state_and_county/         (county-level partitioning — NEW in 2025 R3)
│   ├── basic/
│   │   └── parquet/state=CO/county=G0800010/
│   └── full/
│       └── parquet/state=CO/county=G0800010/
│           └── CO_G0800010_upgrade0.parquet
└── national/
```

### 13.6 Both Climate Zone Columns Exist

- `in.ashrae_iecc_climate_zone_2006` — assigned climate zone ← **use this for filtering**
- `in.as_simulated_ashrae_iecc_climate_zone_2006` — where model was actually simulated

### 13.7 Impact on Pipeline Design

1. **No utility bill string parsing needed** — bills are direct numeric columns in 2025 R3
2. **No manual EUI calculation needed** — `_intensity..kwh_per_ft2` columns are pre-computed
3. **No manual savings calculation needed** — `energy_savings..kwh` columns exist
4. **Column discovery** must handle the `..unit` suffix pattern
5. **Pipeline should still support older formats** where these pre-computed columns may not exist
