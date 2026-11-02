#!/usr/bin/env python3
"""
Compare the hardware resource use of two or more Dimster run or
drain-backlog results.

Usage:
    python3 generate_compare_resource_charts.py \
        --entry LABEL1=FILE1 --entry LABEL2=FILE2 [--entry ...] --output <dir>

The companion of generate_compare_charts.py, which `dimster compare` runs
first with the same entries. Each result's resources report data
(resource-metrics.json beside it, from the end-of-run metrics export) is
analysed as the resources report does (generate_resource_charts.py), and the
entries are set side by side in three tables: the per-service summary, page
cache vs disk and bottlenecks. Writes, in --output:

    compare-<labels>-resources.html
    compare-<labels>-resources.csv     the resources CSV rows of every entry,
                                       with a leading compare_label column

Run and drain-backlog results have resource data. Fewer than two entries
with it, or a comparison of another mode, write nothing.
"""

import argparse
import html
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate_compare_charts import parse_entries, validate_compare  # noqa: E402
from generate_resource_charts import (  # noqa: E402
    PAGE,
    RESOURCE_METRICS_FILE,
    analyze,
    bottleneck_table,
    cache_table,
    find_run_artifact,
    load_resource_metrics,
    measured_span_note,
    service_rows,
    summary_table,
    write_csv,
)
from generate_run_charts import dict_or_empty, text_or_empty  # noqa: E402


def entry_resources(entries):
    """([(label, result, resources, test points)] for entries with resource
    data, [labels without]), in the entries' order. An entry's resources are
    the resource-metrics.json in its result file's directory."""
    measured, missing = [], []
    for label, path, data in entries:
        directory = os.path.dirname(os.path.abspath(path))
        metrics_path = find_run_artifact(path, directory, RESOURCE_METRICS_FILE)
        resources = load_resource_metrics(metrics_path) if metrics_path else None
        points = analyze(data, resources) if resources else []
        if points:
            measured.append((label, data, resources, points))
        else:
            missing.append(label)
    return measured, missing


def test_point_groups(measured):
    """[[(entry label, test point), ...], ...]: each test point (by scenario
    and index, which compare has checked match) with every entry's
    measurement of it, in the first entry's order."""
    order = []
    by_key = {}
    for label, _data, _res, points in measured:
        for p in points:
            key = (p["scenario"], p["testIndex"])
            if key not in by_key:
                by_key[key] = []
                order.append(key)
            by_key[key].append((label, p))
    return [by_key[key] for key in order]


def compare_rows(measured):
    """The resources CSV rows of every entry, labelled."""
    header = None
    rows = []
    for label, _data, _res, points in measured:
        entry_rows = service_rows(points)
        header = header or ["compare_label"] + entry_rows[0]
        rows += [[label] + row for row in entry_rows[1:]]
    return [header] + rows


def build_html(measured, missing):
    """The comparison page: the three tables, entries side by side."""
    groups = test_point_groups(measured)
    labels = [label for label, *_ in measured]
    no_cgroup = [label for label, _d, res, _p in measured if not res.get("cgroupMetrics")]
    # Name each short window with its entry, as the entries' windows differ.
    labelled_points = [dict(p, display=f"{label}: {p['display']}")
                       for label, _d, _r, points in measured for p in points]

    notes = []
    if missing:
        notes.append("Not compared, as they have no resource data (resource-metrics.json "
                     "beside the result): " + html.escape(", ".join(missing)) + ".")
    if no_cgroup:
        notes.append("Collected without per-pod metrics, so only node totals and the page "
                     "cache estimate are available: " + html.escape(", ".join(no_cgroup)) + ".")
    notes_html = "".join(f'<div class="note">{n}</div>' for n in notes)

    sections = [
        f"""<section id="summary"><h2>Summary</h2>{notes_html}
<p class="note">Averages over each test point's measured window, summed over each service's pods,
with each entry's row side by side. See each run's resources report for its charts.</p>
{measured_span_note(labelled_points)}
{summary_table(groups)}</section>""",
        f"""<section id="cache"><h2>Page cache vs disk</h2>
<p class="note">Totals over each test point's window. Estimates: broker logical reads are what
brokers served to consumers and followers; what they did not read from disk is assumed to come from
the page cache.</p>
{cache_table(groups) or '<div class="note">No Kafka broker metrics (external Kafka).</div>'}
</section>""",
        f'<section id="bottlenecks"><h2>Bottlenecks</h2>{bottleneck_table(groups)}</section>',
    ]
    nav = "".join(f'<a class="nav-link" href="#{sid}">{name}</a>' for sid, name in
                  (("summary", "Summary"), ("cache", "Page cache vs disk"),
                   ("bottlenecks", "Bottlenecks")))
    meta = "".join(
        f'<div><span class="label">{html.escape(label)}:</span> '
        f'{html.escape(text_or_empty(dict_or_empty(data.get("metadata")).get("environment")))} '
        f'{html.escape(text_or_empty(dict_or_empty(data.get("metadata")).get("beginTime")))}</div>'
        for label, data, _r, _p in measured)
    name = text_or_empty(dict_or_empty(measured[0][1].get("metadata")).get("name")) or "Unknown Workload"
    title = " vs ".join(labels)
    return PAGE.format(
        title=html.escape(f"Dimster Resources - {title}"),
        name=html.escape(f"{name}: {title}"),
        meta=meta,
        nav=nav,
        sections="".join(sections),
        configs="[]",
    )


def main():
    parser = argparse.ArgumentParser(
        description="Compare the resource use of two or more Dimster run results.")
    parser.add_argument("--entry", action="append", required=True,
                        help="LABEL=FILE entry (specify at least twice)")
    parser.add_argument("--output", required=True, help="Output directory")
    args = parser.parse_args()

    entries = parse_entries(args.entry)
    if len(entries) < 2 or entries[0][2].get("mode") not in ("run", "drain-backlog"):
        return
    validate_compare(entries)
    measured, missing = entry_resources(entries)
    if len(measured) < 2:
        if measured:
            print(f"Resource comparison skipped: only {measured[0][0]} has resource data")
        return

    labels = "-vs-".join(e[0] for e in entries)
    os.makedirs(args.output, exist_ok=True)
    out_path = os.path.join(args.output, f"compare-{labels}-resources.html")
    with open(out_path, "w") as fh:
        fh.write(build_html(measured, missing))
    print(f"Generated: {out_path}")
    write_csv(os.path.join(args.output, f"compare-{labels}-resources.csv"), compare_rows(measured))


if __name__ == "__main__":
    main()
