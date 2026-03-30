"""Phase 5: Write outputs in requested formats (CSV, Parquet, Excel, Markdown)."""
import logging
import os
from datetime import date
from typing import Optional

import pandas as pd

from config import PipelineConfig
from pipeline.summarize import SummaryResults

log = logging.getLogger(__name__)


def _provenance_lines(results: SummaryResults) -> list[str]:
    cfg = results.config_used
    m = results.manifest
    dd_src = m.data_dictionary_source or "not found"
    dd_note = "(native)" if dd_src == cfg.release_name else f"(fallback from {dd_src})"
    filters_parts = []
    if cfg.building_types:
        filters_parts.append(" + ".join(cfg.building_types))
    if cfg.vintage_max or cfg.vintage_min:
        filters_parts.append(f"vintage {cfg.vintage_min or ''}–{cfg.vintage_max or ''}")
    if cfg.climate_zones:
        filters_parts.append(f"CZ {', '.join(cfg.climate_zones)}")
    if cfg.states:
        filters_parts.append(", ".join(cfg.states))

    return [
        f"Dataset: {cfg.release_name} ({cfg.release_year})",
        f"Data dictionary: {dd_src} {dd_note}",
        f"Filters: {' | '.join(filters_parts) if filters_parts else 'none'}",
        f"Baseline buildings: {results.n_baseline_buildings:,}",
        f"Represented floor area: {results.represented_area_ft2:,.0f} ft²",
        f"Upgrades analyzed: {len(results.summary_table)}",
        f"Energy unit: kBtu/ft²",
        f"Generated: {date.today().isoformat()}",
    ]


def _write_csv(results: SummaryResults, output_dir: str) -> list[str]:
    paths = []
    wide_path = os.path.join(output_dir, "summary_wide.csv")
    results.summary_table.to_csv(wide_path, index=False)
    paths.append(wide_path)
    log.info("Wrote %s", wide_path)

    if results.summary_long is not None and not results.summary_long.empty:
        long_path = os.path.join(output_dir, "summary_long.csv")
        results.summary_long.to_csv(long_path, index=False)
        paths.append(long_path)
        log.info("Wrote %s", long_path)

    if results.building_detail is not None:
        detail_path = os.path.join(output_dir, "building_detail.csv")
        results.building_detail.to_csv(detail_path, index=False)
        paths.append(detail_path)
        log.info("Wrote %s", detail_path)

    return paths


def _write_parquet(results: SummaryResults, output_dir: str) -> list[str]:
    paths = []
    wide_path = os.path.join(output_dir, "summary_wide.parquet")
    results.summary_table.to_parquet(wide_path, index=False)
    paths.append(wide_path)

    if results.summary_long is not None and not results.summary_long.empty:
        long_path = os.path.join(output_dir, "summary_long.parquet")
        results.summary_long.to_parquet(long_path, index=False)
        paths.append(long_path)

    if results.building_detail is not None:
        detail_path = os.path.join(output_dir, "building_detail.parquet")
        results.building_detail.to_parquet(detail_path, index=False)
        paths.append(detail_path)

    return paths


def _write_excel(results: SummaryResults, output_dir: str) -> str:
    xl_path = os.path.join(output_dir, "comstock_summary.xlsx")
    with pd.ExcelWriter(xl_path, engine="openpyxl") as writer:
        # Sheet 1: Wide summary
        results.summary_table.to_excel(writer, sheet_name="Summary (Wide)", index=False)

        # Sheet 2: Long summary
        if results.summary_long is not None and not results.summary_long.empty:
            results.summary_long.to_excel(writer, sheet_name="Summary (Long)", index=False)

        # Sheet 3: Building detail (optional)
        if results.building_detail is not None:
            results.building_detail.to_excel(writer, sheet_name="Building Detail", index=False)

        # Sheet 4: Upgrade lookup
        upgrades_df = pd.DataFrame(
            [(k, v) for k, v in results.manifest.upgrades.items()],
            columns=["upgrade_id", "upgrade_name"],
        )
        upgrades_df.to_excel(writer, sheet_name="Upgrade Lookup", index=False)

        # Sheet 5: Metadata / provenance
        prov = _provenance_lines(results)
        funnel_rows = [(k, v) for k, v in results.filter_funnel.items()]
        meta_rows = (
            [("--- Provenance ---", "")] +
            [(line, "") for line in prov] +
            [("", ""), ("--- Filter Funnel ---", "")] +
            funnel_rows
        )
        meta_df = pd.DataFrame(meta_rows, columns=["key", "value"])
        meta_df.to_excel(writer, sheet_name="Metadata", index=False)

    log.info("Wrote %s", xl_path)
    return xl_path


def _write_markdown(results: SummaryResults, output_dir: str) -> str:
    md_path = os.path.join(output_dir, "summary_report.md")
    prov = _provenance_lines(results)

    lines = ["# ComStock Pipeline Summary Report", ""]
    lines += ["## Provenance", ""]
    lines += [f"- {line}" for line in prov]
    lines += [""]

    # Filter funnel
    lines += ["## Filter Funnel", ""]
    lines += ["| Step | Count |", "|------|-------|"]
    for k, v in results.filter_funnel.items():
        lines.append(f"| {k} | {v:,} |")
    lines += [""]

    # Warnings
    # (not stored in results — embed from manifest warnings if needed)

    # Summary table: key columns
    lines += ["## Upgrade Summary", ""]
    summary = results.summary_table

    # Pick a small set of key columns for the markdown table
    display_cols = ["upgrade_id", "upgrade_name", "n_buildings", "n_applicable"]
    eui_med_cols = [c for c in summary.columns if "site_energy" in c and "total" in c and c.endswith(".median")]
    bill_med_cols = [c for c in summary.columns if "total_bill" in c and c.endswith(".median")]
    savings_cols = [c for c in summary.columns if c in ("site_eui_savings_kbtu_ft2", "site_eui_savings_pct")]
    display_cols += eui_med_cols[:2] + bill_med_cols[:1] + savings_cols

    display_cols = [c for c in display_cols if c in summary.columns]

    if display_cols:
        header = "| " + " | ".join(display_cols) + " |"
        sep = "| " + " | ".join(["---"] * len(display_cols)) + " |"
        lines += [header, sep]
        for _, row in summary[display_cols].iterrows():
            cells = []
            for c in display_cols:
                v = row[c]
                if pd.isna(v):
                    cells.append("—")
                elif isinstance(v, float):
                    cells.append(f"{v:.2f}")
                else:
                    cells.append(str(v))
            lines.append("| " + " | ".join(cells) + " |")
        lines += [""]

    lines += [f"*Not-applicable upgrades: {results.not_applicable_upgrades}*", ""]

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    log.info("Wrote %s", md_path)
    return md_path


def export(results: SummaryResults, config: PipelineConfig) -> dict[str, list[str]]:
    """Write all requested output formats. Returns dict of format -> list of file paths."""
    os.makedirs(config.output_dir, exist_ok=True)
    written = {}

    for fmt in config.output_formats:
        fmt = fmt.lower().strip()
        if fmt == "csv":
            written["csv"] = _write_csv(results, config.output_dir)
        elif fmt in ("parquet", "pq"):
            written["parquet"] = _write_parquet(results, config.output_dir)
        elif fmt in ("xlsx", "excel"):
            written["xlsx"] = [_write_excel(results, config.output_dir)]
        elif fmt in ("md", "markdown"):
            written["md"] = [_write_markdown(results, config.output_dir)]
        else:
            log.warning("Unknown output format '%s'; skipping", fmt)

    print("\n=== Output Files ===")
    for fmt, paths in written.items():
        for p in paths:
            print(f"  {p}")
    print()

    return written
