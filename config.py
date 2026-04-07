from dataclasses import dataclass, field
from typing import Optional

# Mapping from full state name to 2-letter abbreviation (used for S3 partition paths)
STATE_NAME_TO_ABBR: dict[str, str] = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
}

STATE_ABBR_TO_NAME: dict[str, str] = {v: k for k, v in STATE_NAME_TO_ABBR.items()}


@dataclass
class PipelineConfig:
    # -- Dataset Selection --
    release_year: str = "2025"
    release_name: str = "comstock_amy2018_release_3"

    # -- Building Stock Filters --
    # Maps to: in.comstock_building_type
    building_types: list[str] = field(default_factory=lambda: [
        "LargeOffice", "MediumOffice", "SmallOffice"
    ])
    # Vintage upper bound: include vintages where upper year <= vintage_max
    # Maps to: in.vintage (categorical, e.g. "1946 to 1959")
    vintage_max: Optional[int] = None
    vintage_min: Optional[int] = None

    # Maps to: in.ashrae_iecc_climate_zone_2006
    climate_zones: Optional[list[str]] = None

    # Full state names (e.g. "District of Columbia"), NOT abbreviations
    # Maps to: in.as_simulated_state_name (or in.state_name if available)
    states: Optional[list[str]] = None

    # None = all upgrades; otherwise list of upgrade IDs (0 = baseline)
    upgrade_ids: Optional[list[int]] = None

    # -- Output --
    output_dir: str = "outputs"
    output_formats: list[str] = field(default_factory=lambda: ["csv", "xlsx", "md"])
    include_building_detail: bool = False
    include_long_format: bool = True

    # -- Statistical Options --
    use_weights: bool = True
    statistics: list[str] = field(default_factory=lambda: [
        "median", "mean", "p25", "p75"
    ])

    # -- Summary Options --
    include_compact_summary: bool = True    # focused end-use EUI breakdown table (Key Metrics)
    include_applicable_summary: bool = True  # repeat stats filtered to applicable buildings only

    # -- Cache --
    use_cache: bool = True        # use local parquet cache under cache_dir/
    refresh_cache: bool = False   # re-download even if cached files exist
    cache_dir: str = "downloads"  # root directory for cached parquet files

    # -- Aggregate Mode --
    use_aggregate: bool = False
    # When True, read from metadata_and_annual_results_aggregates/ instead of the
    # per-sample county files. Much faster (one file per state or one file per upgrade
    # nationally). Energy/EUI statistics are correct. Bill statistics are approximate
    # (bills are summed across samples per archetype, not per-building values).
    aggregate_scope: str = "state"
    # "state"    → aggregates/by_state/ — one file per state per upgrade
    # "national" → aggregates/national/ — one file per upgrade, entire US stock
    #              (states filter still applied post-read in filter phase)
    # Only used when use_aggregate=True.

    # -- Advanced --
    min_sample_warning: int = 100
    energy_unit: str = "kbtu_ft2"  # output EUI unit

    @property
    def s3_base_path(self) -> str:
        return (
            "oedi-data-lake/nrel-pds-building-stock/"
            "end-use-load-profiles-for-us-building-stock/"
            f"{self.release_year}/{self.release_name}"
        )

    def state_abbreviations(self) -> Optional[list[str]]:
        """Return 2-letter abbreviations for configured states (for S3 partition paths)."""
        if self.states is None:
            return None
        abbrs = []
        for name in self.states:
            abbr = STATE_NAME_TO_ABBR.get(name)
            if abbr is None:
                # Try interpreting as abbreviation directly
                if name.upper() in STATE_ABBR_TO_NAME:
                    abbrs.append(name.upper())
                else:
                    raise ValueError(
                        f"Unknown state '{name}'. Use full name (e.g. 'District of Columbia') "
                        f"or 2-letter abbreviation."
                    )
            else:
                abbrs.append(abbr)
        return abbrs
