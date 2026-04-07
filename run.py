"""CLI entry point for the ComStock data pipeline."""
import argparse
import logging
import sys

from config import PipelineConfig
from pipeline.discover import discover
from pipeline.pull import pull
from pipeline.filter import apply_filters
from pipeline.summarize import summarize
from pipeline.export import export


def run_pipeline(config: PipelineConfig):
    """Run all pipeline phases and return SummaryResults."""
    log = logging.getLogger(__name__)
    log.info("Starting ComStock pipeline for %s/%s", config.release_year, config.release_name)

    # Phase 1: Discover
    manifest = discover(config)

    # Phase 2: Pull
    pulled = pull(config, manifest)

    # Phase 3: Filter
    filtered = apply_filters(config, pulled)

    if filtered.n_baseline_buildings == 0:
        log.error("No buildings remain after filtering. Check your filter configuration.")
        sys.exit(1)

    # Phase 4: Summarize
    results = summarize(config, manifest, filtered)

    # Phase 5: Export
    export(results, config)

    return results


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="ComStock EUSS data pipeline — extract, filter, and summarize building energy data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Dataset selection
    p.add_argument("--release-year", default="2025", help="OEDI release year (e.g. 2025)")
    p.add_argument(
        "--release-name",
        default="comstock_amy2018_release_3",
        help="Full release name (e.g. comstock_amy2018_release_3)",
    )

    # Building stock filters
    p.add_argument(
        "--building-types",
        nargs="+",
        default=["LargeOffice", "MediumOffice", "SmallOffice"],
        help="Building types to include (e.g. LargeOffice MediumOffice)",
    )
    p.add_argument("--vintage-max", type=int, default=None, help="Include vintages up to this year (e.g. 1980)")
    p.add_argument("--vintage-min", type=int, default=None, help="Include vintages from this year")
    p.add_argument("--climate-zones", nargs="+", default=None, help="ASHRAE climate zones (e.g. 4A 5A)")
    p.add_argument(
        "--states",
        nargs="+",
        default=None,
        help="Full state names (e.g. 'District of Columbia' Colorado) or 2-letter abbreviations",
    )
    p.add_argument(
        "--upgrade-ids",
        nargs="+",
        type=int,
        default=None,
        help="Specific upgrade IDs to include (0=baseline). Default: all.",
    )

    # Output
    p.add_argument("--output-dir", default="outputs", help="Output directory")
    p.add_argument(
        "--output-formats",
        nargs="+",
        default=["csv", "xlsx", "md"],
        choices=["csv", "parquet", "xlsx", "excel", "md", "markdown"],
        help="Output formats",
    )
    p.add_argument("--include-building-detail", action="store_true", help="Export per-building EUI detail")
    p.add_argument("--no-long-format", action="store_true", help="Skip Tableau-ready long-format output")

    # Statistical options
    p.add_argument("--no-weights", action="store_true", help="Use unweighted statistics")
    p.add_argument(
        "--statistics",
        nargs="+",
        default=["median", "mean", "p25", "p75"],
        choices=["median", "mean", "p25", "p75"],
        help="Statistics to compute",
    )

    # Summary options
    p.add_argument(
        "--no-compact-summary",
        action="store_true",
        help="Skip the compact Key Metrics end-use EUI breakdown",
    )
    p.add_argument(
        "--no-applicable-summary",
        action="store_true",
        help="Skip the applicable-only (matched baseline) summary",
    )

    # Cache options
    p.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass local cache; always fetch from S3",
    )
    p.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Re-download all files even if cached, overwriting cache",
    )
    p.add_argument(
        "--cache-dir",
        default="downloads",
        help="Root directory for cached parquet files (default: downloads/)",
    )

    # Aggregate mode
    p.add_argument(
        "--use-aggregate",
        action="store_true",
        help=(
            "Read aggregate files (one row per building archetype per upgrade) instead of "
            "per-sample county files. Much faster for state or national runs. "
            "Energy/EUI statistics are correct; bill statistics are approximate."
        ),
    )
    p.add_argument(
        "--aggregate-scope",
        choices=["state", "national"],
        default="state",
        help=(
            "Scope for aggregate reads: 'state' (one file per state per upgrade) or "
            "'national' (one file per upgrade, entire US stock). Only used with --use-aggregate."
        ),
    )

    # Misc
    p.add_argument("--min-sample-warning", type=int, default=100, help="Warn if N below this threshold")
    p.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    config = PipelineConfig(
        release_year=args.release_year,
        release_name=args.release_name,
        building_types=args.building_types,
        vintage_max=args.vintage_max,
        vintage_min=args.vintage_min,
        climate_zones=args.climate_zones,
        states=args.states,
        upgrade_ids=args.upgrade_ids,
        output_dir=args.output_dir,
        output_formats=args.output_formats,
        include_building_detail=args.include_building_detail,
        include_long_format=not args.no_long_format,
        include_compact_summary=not args.no_compact_summary,
        include_applicable_summary=not args.no_applicable_summary,
        use_weights=not args.no_weights,
        statistics=args.statistics,
        use_cache=not args.no_cache,
        refresh_cache=args.refresh_cache,
        cache_dir=args.cache_dir,
        use_aggregate=args.use_aggregate,
        aggregate_scope=args.aggregate_scope,
        min_sample_warning=args.min_sample_warning,
    )

    cache_status = "disabled (--no-cache)" if not config.use_cache else \
                   f"refresh ({config.cache_dir}/)" if config.refresh_cache else \
                   f"enabled ({config.cache_dir}/)"

    print(f"\nComStock Pipeline")
    print(f"  Release:        {config.release_year}/{config.release_name}")
    print(f"  Building types: {config.building_types}")
    print(f"  Vintage:        min={config.vintage_min}  max={config.vintage_max}")
    print(f"  Climate zones:  {config.climate_zones}")
    print(f"  States:         {config.states}")
    print(f"  Output dir:     {config.output_dir}")
    print(f"  Formats:        {config.output_formats}")
    print(f"  Cache:          {cache_status}")
    print()

    run_pipeline(config)


if __name__ == "__main__":
    main()
