# ComStock Data Pipeline

A standalone Python pipeline for extracting, filtering, and summarizing End-Use Savings Shape (EUSS) data from the [OEDI data lake](https://data.openei.org/submissions/4520). Supports ComStock and ResStock datasets with configurable building stock filters, automated upgrade discovery, and per-building end-use energy breakdowns with utility bill summaries.

## Overview

The pipeline pulls per-building annual simulation results from S3, filters by building stock characteristics (type, vintage, climate zone, state), and produces population-level summary statistics for every energy end-use and every upgrade scenario. Output is ready for Tableau, Excel, or downstream analysis.

## Installation

```bash
pip install -r requirements.txt
```

**Dependencies:** boto3, s3fs, pyarrow, pandas, numpy, openpyxl

No AWS credentials needed — OEDI is a public S3 bucket.

## Quick Start

```bash
# DC pre-1980 offices from 2025 Release 3
python run.py \
  --release-year 2025 \
  --release-name comstock_amy2018_release_3 \
  --building-types LargeOffice MediumOffice \
  --vintage-max 1980 \
  --climate-zones 4A \
  --states "District of Columbia" \
  --output-formats csv xlsx md
```

```bash
# All office building types, nationwide, no vintage filter
python run.py \
  --release-year 2025 \
  --release-name comstock_amy2018_release_3 \
  --building-types LargeOffice MediumOffice SmallOffice

# Retail in Colorado from 2024 Release 2
python run.py \
  --release-year 2024 \
  --release-name comstock_amy2018_release_2 \
  --building-types RetailStripmall RetailStandalone \
  --states Colorado
```

## Python API

```python
from config import PipelineConfig
from run import run_pipeline

config = PipelineConfig(
    release_year="2025",
    release_name="comstock_amy2018_release_3",
    building_types=["LargeOffice", "MediumOffice"],
    vintage_max=1980,
    climate_zones=["4A"],
    states=["District of Columbia"],
    include_building_detail=True,
    include_long_format=True,
)

results = run_pipeline(config)

# Population summary — one row per upgrade
print(results.summary_table[["upgrade_name", "n_buildings"]].head())

# Per-building EUI detail
print(results.building_detail.head())
```

## Configuration

| Parameter | CLI flag | Default | Description |
|-----------|----------|---------|-------------|
| `release_year` | `--release-year` | `2025` | OEDI release year |
| `release_name` | `--release-name` | `comstock_amy2018_release_3` | Full release identifier |
| `building_types` | `--building-types` | `LargeOffice MediumOffice SmallOffice` | Building types to include |
| `vintage_max` | `--vintage-max` | None | Include vintages with upper year ≤ this |
| `vintage_min` | `--vintage-min` | None | Include vintages with lower year ≥ this |
| `climate_zones` | `--climate-zones` | None | ASHRAE climate zones (e.g. `4A 5A`) |
| `states` | `--states` | None | Full state names or 2-letter abbreviations |
| `upgrade_ids` | `--upgrade-ids` | None (all) | Specific upgrade IDs to include |
| `output_dir` | `--output-dir` | `outputs/` | Output directory |
| `output_formats` | `--output-formats` | `csv xlsx md` | Output formats |
| `include_building_detail` | `--include-building-detail` | False | Export per-building EUI rows |
| `use_weights` | `--no-weights` (to disable) | True | Weighted population statistics |

## Output Files

All files are written to `outputs/` (or `--output-dir`).

| File | Description |
|------|-------------|
| `summary_wide.csv` / `.parquet` | One row per upgrade; columns for every end-use × statistic |
| `summary_long.csv` / `.parquet` | Unpivoted (upgrade × metric × statistic) — Tableau-ready |
| `building_detail.csv` / `.parquet` | Per-building EUIs in kBtu/ft² (optional) |
| `comstock_summary.xlsx` | Multi-sheet workbook: Wide, Long, Building Detail, Upgrade Lookup, Metadata |
| `summary_report.md` | Human-readable report with provenance header and key tables |

All outputs include a provenance header:
```
Dataset: comstock_amy2018_release_3 (2025)
Filters: LargeOffice + MediumOffice | vintage –1980 | CZ 4A | District of Columbia
Baseline buildings: 180
Energy unit: kBtu/ft²
Generated: 2026-03-30
```

## Pipeline Phases

| Phase | Module | Description |
|-------|--------|-------------|
| 1. Discover | `pipeline/discover.py` | Probe S3 structure, fetch upgrade names, read Parquet schema |
| 2. Pull | `pipeline/pull.py` | Column-selective S3 reads for baseline + all upgrades |
| 3. Filter | `pipeline/filter.py` | Apply building stock filters; print filter funnel with sample counts |
| 4. Summarize | `pipeline/summarize.py` | Per-building EUI conversion → weighted population statistics |
| 5. Export | `pipeline/export.py` | Write CSV, Parquet, Excel, and Markdown outputs |

## Data Source

Data comes from NREL's [End-Use Load Profiles for the U.S. Building Stock](https://www.nrel.gov/buildings/end-use-load-profiles.html), hosted on the OEDI S3 data lake at:

```
s3://oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/
```

The pipeline targets `metadata_and_annual_results/` (per-building full-metadata files), not the aggregate files.

## Supported Releases

The pipeline is release-agnostic — it discovers available upgrades, column schemas, and file structure automatically. Tested against:

- `2025/comstock_amy2018_release_3` (primary target)
- `2024/comstock_amy2018_release_2` (fallback tested)

Older releases may use different column naming conventions; the pipeline handles this via priority-ordered column resolution.

## Vintage Categories (2025 R3)

| Category string | Matched by `vintage_max=1980` |
|----------------|-------------------------------|
| Before 1946 | yes |
| 1946 to 1959 | yes |
| 1960 to 1969 | yes |
| 1970 to 1979 | yes |
| 1980 to 1989 | no |
| 1990 to 1999 | no |
| 2000 to 2012 | no |
| 2013 to 2018 | no |
