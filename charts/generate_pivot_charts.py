#!/usr/bin/env python3
"""
Generate an HTML "pivot" view from a single Dimster run result JSON file.

Where generate_compare_charts.py overlays two or more separate result files,
this script slices ONE file along the scenario axis. The result file is
expected to contain N scenarios that share the same array shape (same
number of "tests", same per-index test labels) but differ on scalar
fields (e.g. consumersPerGroup, messageDistributor). For each test index
we emit one HTML section whose comparison series are the scenarios.

Usage:
    python3 generate_pivot_charts.py --input <result.json> --output <dir>

Validates that all scenarios have equal-length tests arrays with matching
labels at each index, then generates a single HTML page with one section
per array index. Output filename: pivot-<workload-name>.html.
"""

import argparse
import json
import os
import sys

# Allow importing sibling helper modules.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from generate_run_charts import (
    PERCENTILE_TICK_VALUES,
    PERCENTILE_TICK_LABELS,
    avg,
    avg_mbs,
    bar_options,
    line_options,
    message_size_cell,
    metadata_annotation_html,
    percentile_options,
    percentile_points,
    strip_heavy_data,
    time_labels,
)
from generate_compare_charts import (
    compare_line_dataset,
    compare_percentile_dataset,
)


# Tab20-derived palette: 16 visually distinct hues, used round-robin per
# scenario. The scenario axis is one-dimensional (no file overlay), so we
# need only one color per scenario (no dash variation).
PIVOT_PALETTE = [
    "#1f77b4", "#aec7e8", "#ff7f0e", "#ffbb78",
    "#2ca02c", "#98df8a", "#d62728", "#ff9896",
    "#9467bd", "#c5b0d5", "#8c564b", "#c49c94",
    "#e377c2", "#f7b6d2", "#7f7f7f", "#c7c7c7",
]

PIVOT_PALETTE_ALPHA = [
    "rgba(31,119,180,0.15)",  "rgba(174,199,232,0.15)",
    "rgba(255,127,14,0.15)",  "rgba(255,187,120,0.15)",
    "rgba(44,160,44,0.15)",   "rgba(152,223,138,0.15)",
    "rgba(214,39,40,0.15)",   "rgba(255,152,150,0.15)",
    "rgba(148,103,189,0.15)", "rgba(197,176,213,0.15)",
    "rgba(140,86,75,0.15)",   "rgba(196,156,148,0.15)",
    "rgba(227,119,194,0.15)", "rgba(247,182,210,0.15)",
    "rgba(127,127,127,0.15)", "rgba(199,199,199,0.15)",
]


def scenario_color(si):
    """Pick a stable color for scenario index si, wrapping past 16."""
    return PIVOT_PALETTE[si % len(PIVOT_PALETTE)]


def scenario_color_alpha(si):
    return PIVOT_PALETTE_ALPHA[si % len(PIVOT_PALETTE_ALPHA)]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_pivot(data):
    """Ensure the input file has the shape required for a pivot view.

    Bails (exit 1) when:
      - file is not a run result
      - fewer than 2 scenarios (pivot is meaningless with one)
      - scenarios have differing test counts
      - per-index test labels disagree across scenarios

    A non-fatal warning (per-cell missing quantiles) is handled later
    inside chart construction.
    """
    if data.get("mode") != "run":
        print(
            f"Error: file is not a run result (mode={data.get('mode')})",
            file=sys.stderr,
        )
        sys.exit(1)

    scenarios = data.get("scenarioResults", [])
    if len(scenarios) < 2:
        print(
            f"Error: pivot requires at least 2 scenarios; found {len(scenarios)}",
            file=sys.stderr,
        )
        sys.exit(1)

    lengths = [(s.get("name", f"scenario-{i}"), len(s.get("tests", [])))
               for i, s in enumerate(scenarios)]
    distinct_lengths = {n for _, n in lengths}
    if len(distinct_lengths) != 1:
        print(
            "Error: scenarios must have equal-length tests arrays for pivot. "
            "Found:",
            file=sys.stderr,
        )
        for name, n in lengths:
            print(f"    {name}: {n} tests", file=sys.stderr)
        sys.exit(1)

    n_tests = lengths[0][1]
    if n_tests == 0:
        print("Error: scenarios contain no tests", file=sys.stderr)
        sys.exit(1)

    # Per-index labels must match — we use them as section headings.
    for t_idx in range(n_tests):
        labels_at_idx = {
            s.get("name", f"scenario-{i}"): s["tests"][t_idx].get(
                "label", f"test-{t_idx}"
            )
            for i, s in enumerate(scenarios)
        }
        distinct = set(labels_at_idx.values())
        if len(distinct) != 1:
            print(
                f"Error: test label mismatch at index {t_idx}:",
                file=sys.stderr,
            )
            for name, lbl in labels_at_idx.items():
                print(f"    {name}: {lbl!r}", file=sys.stderr)
            sys.exit(1)


# ---------------------------------------------------------------------------
# Chart construction
# ---------------------------------------------------------------------------

# Percentile keys reused across publish/E2E latency charts.
_PCTILE_NAMES = ["p50", "p95", "p99", "p99.9", "max"]
_PUB_PCTILE_KEYS = [
    "aggregatedPublishLatency50pct",
    "aggregatedPublishLatency95pct",
    "aggregatedPublishLatency99pct",
    "aggregatedPublishLatency999pct",
    "aggregatedPublishLatencyMax",
]
_E2E_PCTILE_KEYS = [
    "aggregatedEndToEndLatency50pct",
    "aggregatedEndToEndLatency95pct",
    "aggregatedEndToEndLatency99pct",
    "aggregatedEndToEndLatency999pct",
    "aggregatedEndToEndLatencyMax",
]


def build_pivot_chart_configs(data):
    """Build chart configs and section ranges, iterating test-major.

    Returns (chart_configs, chart_sections) where chart_sections maps
    each test index to:
        {"aggregate": (start, end), "extras": {...optional notes...}}

    Chart IDs follow the pattern f"chart-{t_idx}-{c_idx}". Zoom groups
    are scoped per test index so range-zoom inside one section does not
    affect the others.
    """
    scenarios = data.get("scenarioResults", [])
    sample_rate_ms = data.get("runConfig", {}).get("sampleRateMillis", 10000)
    n_scenarios = len(scenarios)
    scenario_names = [s.get("name", f"scenario-{si}")
                      for si, s in enumerate(scenarios)]
    n_tests = len(scenarios[0].get("tests", []))

    chart_configs = []
    chart_sections = {}

    # Track per-section warnings about scenarios omitted from percentile
    # scatters — surfaced to the user both as inline HTML notes and as
    # stderr lines once at the end.
    quantile_warnings = []

    for t_idx in range(n_tests):
        chart_idx = 0
        agg_start = chart_idx

        # Vertical bar charts on n_scenarios. The plot itself doesn't grow
        # with the number of bars (Chart.js packs them horizontally); we
        # only need room for the legend, the rotated X-axis labels, and a
        # readable plot area. Latency bars carry a 5-item legend that
        # tends to wrap to two rows, so they get extra headroom.
        tp_height = min(440, max(340, 260 + n_scenarios * 10))
        lat_height = min(520, max(400, 320 + n_scenarios * 10))
        # Time-series and percentile-scatter charts render full-width too;
        # with N scenarios the legend can wrap to 3-4 rows, so add ~10px
        # per scenario to keep the plot area meaningful without exploding.
        series_height = min(560, max(400, 320 + n_scenarios * 10))

        # ------- Aggregate bar: Throughput (msg/s) -------
        avg_pub = [
            round(avg(scenarios[si]["tests"][t_idx]["results"].get("publishRate", [])), 1)
            for si in range(n_scenarios)
        ]
        avg_con = [
            round(avg(scenarios[si]["tests"][t_idx]["results"].get("consumeRate", [])), 1)
            for si in range(n_scenarios)
        ]
        opts_tp = bar_options("Throughput (msg/s)", "msg/s")
        opts_tp["maintainAspectRatio"] = False
        chart_configs.append({
            "id": f"chart-{t_idx}-{chart_idx}",
            "fullWidth": True,
            "height": tp_height,
            "config": {
                "type": "bar",
                "data": {
                    "labels": scenario_names,
                    "datasets": [
                        {"label": "Publish", "data": avg_pub,
                         "backgroundColor": PIVOT_PALETTE[0]},
                        {"label": "Consume", "data": avg_con,
                         "backgroundColor": PIVOT_PALETTE[2]},
                    ],
                },
                "options": opts_tp,
            },
        })
        chart_idx += 1

        # ------- Aggregate bar: Throughput (MB/s) -------
        avg_pub_mb = [avg_mbs(scenarios[si]["tests"][t_idx], "publish") for si in range(n_scenarios)]
        avg_con_mb = [avg_mbs(scenarios[si]["tests"][t_idx], "consume") for si in range(n_scenarios)]
        opts_mbs = bar_options("Throughput (MB/s)", "MB/s")
        opts_mbs["maintainAspectRatio"] = False
        chart_configs.append({
            "id": f"chart-{t_idx}-{chart_idx}",
            "fullWidth": True,
            "height": tp_height,
            "config": {
                "type": "bar",
                "data": {
                    "labels": scenario_names,
                    "datasets": [
                        {"label": "Publish", "data": avg_pub_mb,
                         "backgroundColor": PIVOT_PALETTE[0]},
                        {"label": "Consume", "data": avg_con_mb,
                         "backgroundColor": PIVOT_PALETTE[2]},
                    ],
                },
                "options": opts_mbs,
            },
        })
        chart_idx += 1

        # ------- Aggregate bar: Publish Latency (5 percentile datasets) -------
        # X-axis = scenarios; one dataset group per percentile. Colors
        # picked from the palette so the percentile bars don't bleed
        # into the per-scenario palette used in the time-series charts.
        pub_lat_datasets = []
        for pi, (pname, pkey) in enumerate(zip(_PCTILE_NAMES, _PUB_PCTILE_KEYS)):
            pub_lat_datasets.append({
                "label": pname,
                "data": [
                    scenarios[si]["tests"][t_idx]["results"].get(pkey, 0)
                    for si in range(n_scenarios)
                ],
                "backgroundColor": PIVOT_PALETTE[(pi * 2) % len(PIVOT_PALETTE)],
            })
        opts_pub_lat = bar_options("Aggregated Latency - Publish", "ms")
        opts_pub_lat["maintainAspectRatio"] = False
        chart_configs.append({
            "id": f"chart-{t_idx}-{chart_idx}",
            "fullWidth": True,
            "height": lat_height,
            "config": {
                "type": "bar",
                "data": {"labels": scenario_names, "datasets": pub_lat_datasets},
                "options": opts_pub_lat,
            },
        })
        chart_idx += 1

        # ------- Aggregate bar: E2E Latency -------
        e2e_lat_datasets = []
        for pi, (pname, ekey) in enumerate(zip(_PCTILE_NAMES, _E2E_PCTILE_KEYS)):
            e2e_lat_datasets.append({
                "label": pname,
                "data": [
                    scenarios[si]["tests"][t_idx]["results"].get(ekey, 0)
                    for si in range(n_scenarios)
                ],
                "backgroundColor": PIVOT_PALETTE[(pi * 2) % len(PIVOT_PALETTE)],
            })
        opts_e2e_lat = bar_options("Aggregated Latency - E2E", "ms")
        opts_e2e_lat["maintainAspectRatio"] = False
        chart_configs.append({
            "id": f"chart-{t_idx}-{chart_idx}",
            "fullWidth": True,
            "height": lat_height,
            "config": {
                "type": "bar",
                "data": {"labels": scenario_names, "datasets": e2e_lat_datasets},
                "options": opts_e2e_lat,
            },
        })
        chart_idx += 1

        agg_end = chart_idx

        # ------- Time-series (one chart per metric, scenarios overlaid) -------
        # Closure captures t_idx so every nested helper renders the same
        # array index across all scenarios.
        def make_time_chart(title, y_label, data_key, _t_idx=t_idx):
            nonlocal chart_idx
            max_len = 0
            for si in range(n_scenarios):
                series = scenarios[si]["tests"][_t_idx]["results"].get(data_key, [])
                if len(series) > max_len:
                    max_len = len(series)
            x_labels = time_labels(max_len, sample_rate_ms)
            datasets = []
            for si in range(n_scenarios):
                datasets.append(compare_line_dataset(
                    scenario_names[si],
                    scenarios[si]["tests"][_t_idx]["results"].get(data_key, []),
                    scenario_color(si),
                    scenario_color_alpha(si),
                    [],  # solid line; the comparison axis is one-dimensional
                ))
            opts = line_options(title, y_label, "Time (s)")
            # Render full-width with explicit pixel height so the legend
            # (one entry per scenario, often 12-16 entries) doesn't squeeze
            # the plot area.
            opts["maintainAspectRatio"] = False
            chart_configs.append({
                "id": f"chart-{_t_idx}-{chart_idx}",
                "zoomGroup": f"time-{_t_idx}",
                "fullWidth": True,
                "height": series_height,
                "config": {
                    "type": "line",
                    "data": {"labels": x_labels, "datasets": datasets},
                    "options": opts,
                },
            })
            chart_idx += 1

        make_time_chart("Publish Rate over Time", "msg/s", "publishRate")
        make_time_chart("Consume Rate over Time", "msg/s", "consumeRate")
        make_time_chart("Publish Latency p99 over Time", "ms", "publishLatency99pct")
        make_time_chart("E2E Latency p99 over Time", "ms", "endToEndLatency99pct")
        make_time_chart("Publish Delay p99 over Time", "ms", "publishDelayLatency99pct")

        # ------- Percentile distribution scatters -------
        # Skip a scenario silently if the quantile map is missing for
        # this (scenario, t_idx) cell — record the omission so we can
        # both warn the user and surface an inline note in the HTML.
        def make_percentile_chart(title, quantile_key, _t_idx=t_idx):
            nonlocal chart_idx
            datasets = []
            omitted = []
            for si in range(n_scenarios):
                q = scenarios[si]["tests"][_t_idx]["results"].get(quantile_key, {})
                if not q:
                    omitted.append(scenario_names[si])
                    continue
                datasets.append(compare_percentile_dataset(
                    scenario_names[si],
                    percentile_points(q),
                    scenario_color(si),
                    [],
                ))
            if not datasets:
                return None
            chart_id = f"chart-{_t_idx}-{chart_idx}"
            opts = percentile_options(title)
            opts["maintainAspectRatio"] = False
            chart_configs.append({
                "id": chart_id,
                "zoomGroup": f"pct-{_t_idx}",
                "fullWidth": True,
                "height": series_height,
                "config": {
                    "type": "scatter",
                    "data": {"datasets": datasets},
                    "options": opts,
                },
            })
            chart_idx += 1
            if omitted:
                quantile_warnings.append((t_idx, title, omitted))
            return chart_id

        pub_pct_id = make_percentile_chart(
            "Publish Latency Percentile Distribution",
            "aggregatedPublishLatencyQuantiles",
        )
        e2e_pct_id = make_percentile_chart(
            "E2E Latency Percentile Distribution",
            "aggregatedEndToEndLatencyQuantiles",
        )

        chart_sections[t_idx] = {
            "aggregate": (agg_start, agg_end),
            "rest": (agg_end, chart_idx),
            "pub_pct_id": pub_pct_id,
            "e2e_pct_id": e2e_pct_id,
        }

    # Emit deduplicated stderr warnings — one line per (section, chart)
    # combination. Inline HTML notes are rendered separately from the
    # quantile_warnings list captured on the data dict below.
    for t_idx, title, omitted in quantile_warnings:
        print(
            f"Warning: section {t_idx} ({title}) omits scenarios without "
            f"quantiles: {', '.join(omitted)}",
            file=sys.stderr,
        )

    # Stash warnings on the section map so build_pivot_test_sections can
    # render inline notes without re-walking the data.
    chart_sections["__quantile_warnings__"] = quantile_warnings

    return chart_configs, chart_sections


# ---------------------------------------------------------------------------
# HTML construction
# ---------------------------------------------------------------------------

def build_pivot_header(data):
    """Single-column metadata block — one file in, one column out.

    Implemented locally rather than reusing the N-column compare header
    because the layout is genuinely simpler (no dividers, no grid).
    """
    m = data.get("metadata", {})
    rc = data.get("runConfig", {})
    kc = data.get("baseKafkaConfig", {})
    annotation_meta = metadata_annotation_html(m)
    return f"""
    <div class="pivot-header">
        <div class="meta-item"><span class="label">System:</span> <span class="value">{m.get("system", "")}</span></div>
        <div class="meta-item"><span class="label">Environment:</span> <span class="value">{m.get("environment", "")}</span></div>
        <div class="meta-item"><span class="label">Hardware:</span> <span class="value">{m.get("hardware", "")}</span></div>
        <div class="meta-item"><span class="label">Kafka Config:</span> <span class="value">{m.get("kafkaConfigName", "")}</span></div>
        <div class="meta-item"><span class="label">Replication Factor:</span> <span class="value">{kc.get("replicationFactor", "")}</span></div>
        <div class="meta-item"><span class="label">Test Duration:</span> <span class="value">{rc.get("testDurationMinutes", "")} min</span></div>
        <div class="meta-item"><span class="label">Warmup:</span> <span class="value">{rc.get("warmupDurationMinutes", "")} min</span></div>
        {annotation_meta}
        <div class="meta-item"><span class="label">Start:</span> <span class="value">{m.get("beginTime", "")}</span></div>
        <div class="meta-item"><span class="label">End:</span> <span class="value">{m.get("endTime", "")}</span></div>
    </div>
    """


def build_pivot_config_section(data):
    """Render the input file's structural config (without heavy time-series).

    Reuses strip_heavy_data() so the user can audit scenario+test
    configuration alongside the charts.
    """
    stripped = strip_heavy_data(data)
    config_json = json.dumps(stripped, indent=2)
    return f"""
    <section id="config-section" class="scenario-section">
        <h2>Configuration</h2>
        <div class="config-json-wrap"><pre class="config-json">{config_json}</pre></div>
    </section>
    """


def _safe_id(label):
    """Make a label safe for use as an HTML id fragment."""
    return label.replace(" ", "-").replace(".", "_").replace("/", "_")


def build_pivot_sidebar(data, chart_sections):
    """Sidebar: Overview → Configuration, then one nav-link per test index."""
    scenarios = data.get("scenarioResults", [])
    n_tests = len(scenarios[0].get("tests", []))

    items = []
    items.append('<div class="sidebar-label">Overview</div>')
    items.append('<a class="nav-link" href="#config-section">Configuration</a>')
    items.append('<div class="sidebar-label">Sections (per array index)</div>')
    for t_idx in range(n_tests):
        test_label = scenarios[0]["tests"][t_idx].get("label", f"test-{t_idx}")
        safe = _safe_id(test_label)
        items.append(
            f'<a class="nav-link" href="#section-{t_idx}-{safe}">{test_label}</a>'
        )
    return "\n".join(items)


def _scenario_distinguishing_table(scenarios, t_idx):
    """Table summarising scenario-distinguishing fields at this t_idx.

    The pivot view's central value is letting the user see, at one
    array index, exactly how the scenarios differ. We surface the
    workloadConfig fields that tend to vary across scenarios in a
    Dimster sweep (consumer counts, key distribution, producer rate,
    message size). All values come from per-test workloadConfig — this
    is the resolved view after scenario+test overrides are merged.
    """
    rows = []
    for s in scenarios:
        name = s.get("name", "")
        wc = s["tests"][t_idx].get("workloadConfig", {}) or {}
        rows.append(
            f'<tr>'
            f'<td>{name}</td>'
            f'<td>{wc.get("topics", "")}</td>'
            f'<td>{wc.get("partitionsPerTopic", "")}</td>'
            f'<td>{message_size_cell(wc)}</td>'
            f'<td>{wc.get("producersPerTopic", "")}</td>'
            f'<td>{wc.get("producerRate", "")}</td>'
            f'<td>{wc.get("consumerGroupsPerTopic", "")}</td>'
            f'<td>{wc.get("consumersPerGroup", "")}</td>'
            f'<td>{wc.get("messageDistributor", "")}</td>'
            f'<td>{wc.get("messageDistributorSkew", "")}</td>'
            f'<td>{wc.get("consumerType", "CONSUMER_GROUP")}</td>'
            f'</tr>'
        )
    return f"""
    <div class="config-table-wrap">
        <table class="config-table">
            <thead><tr>
                <th>Scenario</th><th>Topics</th><th>Partitions</th>
                <th>Msg Size</th><th>Producers</th><th>Rate</th>
                <th>Consumer Groups</th><th>Consumers/Group</th>
                <th>Key Dist</th><th>Skew</th><th>Consumer Type</th>
            </tr></thead>
            <tbody>{"".join(rows)}</tbody>
        </table>
    </div>
    """


def build_pivot_test_sections(data, chart_configs, chart_sections):
    """One <section> per test index; each contains config table + charts.

    Aggregate bars render full-width to fit ≤16 scenarios on the X-axis;
    time-series and percentile charts tile in a 2-column grid.
    """
    scenarios = data.get("scenarioResults", [])
    n_tests = len(scenarios[0].get("tests", []))
    config_by_id = {c["id"]: c for c in chart_configs}

    quantile_warnings = chart_sections.get("__quantile_warnings__", [])
    warnings_by_chart_id = {}
    for t_idx, title, omitted in quantile_warnings:
        # Map back to the chart id we recorded in chart_sections.
        ranges = chart_sections[t_idx]
        target = (
            ranges["pub_pct_id"]
            if "Publish" in title
            else ranges["e2e_pct_id"]
        )
        if target:
            warnings_by_chart_id[target] = omitted

    sections = []
    for t_idx in range(n_tests):
        ranges = chart_sections[t_idx]
        test_label = scenarios[0]["tests"][t_idx].get("label", f"test-{t_idx}")
        safe = _safe_id(test_label)

        # Aggregate cards (full-width).
        agg_start, agg_end = ranges["aggregate"]
        agg_cards = []
        for c_idx in range(agg_start, agg_end):
            chart_id = f"chart-{t_idx}-{c_idx}"
            cfg = config_by_id.get(chart_id, {})
            height = cfg.get("height", 300)
            agg_cards.append(
                f'<div class="chart-card chart-card-full">'
                f'<canvas id="{chart_id}" style="height:{height}px;"></canvas>'
                f'</div>'
            )
        agg_html = "\n".join(agg_cards)

        # All charts after the aggregate range — time-series + percentile
        # scatters — also render full-width. With 12-16 scenarios the
        # legend is large; a side-by-side layout left the plot area too
        # small to read, so each chart gets the full container width.
        rest_start, rest_end = ranges["rest"]
        rest_cards = []
        for c_idx in range(rest_start, rest_end):
            cid = f"chart-{t_idx}-{c_idx}"
            cfg = config_by_id.get(cid, {})
            height = cfg.get("height", 400)
            note = ""
            if cid in warnings_by_chart_id:
                omitted = warnings_by_chart_id[cid]
                note = (
                    f'<div class="legend-hint">Omitted (no quantiles): '
                    f'{", ".join(omitted)}</div>'
                )
            rest_cards.append(
                f'<div class="chart-card chart-card-full">{note}'
                f'<canvas id="{cid}" style="height:{height}px;"></canvas>'
                f'</div>'
            )
        rest_html = (
            f'<div class="chart-grid">{"".join(rest_cards)}</div>'
            if rest_cards else ""
        )

        sections.append(f"""
        <section id="section-{t_idx}-{safe}" class="scenario-section">
            <h2>Section: {test_label}</h2>
            {_scenario_distinguishing_table(scenarios, t_idx)}
            <h3 class="subsection-heading">Aggregate</h3>
            {agg_html}
            <h3 class="subsection-heading">Per-scenario series</h3>
            {rest_html}
        </section>
        """)

    return "\n".join(sections)


def build_pivot_html(data, chart_configs, chart_sections):
    """Assemble the full HTML page.

    Mirrors build_compare_html's shell (same Chart.js + zoom plugins,
    same CSS) so the look matches the rest of dimster's chart output.
    The inline JS (zoom sync, percentile callbacks, chart instantiation)
    is duplicated rather than factored out — it depends on template-string
    interpolation of `chartConfigs` and PTICK_MAP, and extracting it would
    force a separate module just to share ~80 lines.
    """
    workload_name = data.get("metadata", {}).get("name", "Unknown Workload")

    sidebar_html = build_pivot_sidebar(data, chart_sections)
    header_html = build_pivot_header(data)
    config_html = build_pivot_config_section(data)
    sections_html = build_pivot_test_sections(data, chart_configs, chart_sections)

    # One reset bar per zoom group present in the page.
    zoom_groups_seen = set()
    for c in chart_configs:
        zg = c.get("zoomGroup")
        if zg:
            zoom_groups_seen.add(zg)
    reset_bars = []
    for zg in sorted(zoom_groups_seen):
        kind = "time" if zg.startswith("time") else "percentile"
        reset_bars.append(
            f'<div id="resetBar-{zg}" class="reset-bar">'
            f'<span>Zoomed &mdash; drag on any {kind} chart to adjust</span>'
            f'<button onclick="resetZoom(\'{zg}\')">Reset Zoom</button>'
            f'</div>'
        )
    reset_bars_html = "\n    ".join(reset_bars)

    configs_json = json.dumps(chart_configs)
    ptick_json = json.dumps({
        "values": PERCENTILE_TICK_VALUES,
        "labels": PERCENTILE_TICK_LABELS,
    })

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dimster Pivot - {workload_name}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdn.jsdelivr.net/npm/hammerjs@2.0.8"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom"></script>
<style>
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    background: #f8f9fa;
    color: #1a1a2e;
    display: flex;
    min-height: 100vh;
}}
.sidebar {{
    position: fixed;
    top: 0;
    left: 0;
    width: 240px;
    height: 100vh;
    background: #1a1a2e;
    color: #c8c8d4;
    padding: 24px 0;
    overflow-y: auto;
    z-index: 100;
}}
.sidebar-title {{
    font-size: 15px;
    font-weight: 700;
    color: #fff;
    padding: 0 20px 16px;
    border-bottom: 1px solid #2a2a4e;
    margin-bottom: 12px;
    letter-spacing: 0.5px;
}}
.sidebar-label {{
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #6c6c8a;
    padding: 12px 20px 6px;
}}
.nav-link {{
    display: block;
    padding: 8px 20px;
    color: #a0a0b8;
    text-decoration: none;
    font-size: 13px;
    transition: background 0.15s, color 0.15s;
    border-left: 3px solid transparent;
}}
.nav-link:hover {{
    background: #2a2a4e;
    color: #fff;
    border-left-color: #4e79a7;
}}
.main {{
    margin-left: 240px;
    flex: 1;
    padding: 32px 40px;
    max-width: 1500px;
}}
.header {{
    background: #fff;
    border-radius: 10px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    padding: 28px 32px;
    margin-bottom: 32px;
}}
.header h1 {{
    font-size: 22px;
    font-weight: 700;
    margin-bottom: 16px;
    color: #1a1a2e;
}}
.pivot-header {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 4px 24px;
}}
.meta-item {{
    font-size: 13px;
    margin-bottom: 4px;
}}
.meta-item .label {{
    color: #6c6c8a;
    font-weight: 500;
}}
.meta-item .value {{
    color: #1a1a2e;
    font-weight: 600;
}}
.scenario-section {{
    margin-bottom: 48px;
}}
.scenario-section h2 {{
    font-size: 18px;
    font-weight: 700;
    margin-bottom: 16px;
    padding-bottom: 8px;
    border-bottom: 2px solid #e0e0e0;
}}
.config-table-wrap {{
    overflow-x: auto;
    margin-bottom: 20px;
}}
.config-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
    background: #fff;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}}
.config-table th {{
    background: #f0f1f3;
    padding: 8px 12px;
    text-align: left;
    font-weight: 600;
    color: #4a4a6a;
    white-space: nowrap;
}}
.config-table td {{
    padding: 7px 12px;
    border-top: 1px solid #eee;
    white-space: nowrap;
}}
.config-table tr:hover td {{
    background: #f8f9ff;
}}
.chart-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
}}
.chart-card {{
    background: #fff;
    border-radius: 8px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    padding: 20px;
}}
.chart-card canvas {{
    width: 100% !important;
}}
.chart-card-full {{
    grid-column: 1 / -1;
}}
.subsection-heading {{
    font-size: 15px;
    font-weight: 700;
    margin-top: 28px;
    margin-bottom: 12px;
    padding-bottom: 6px;
    border-bottom: 1px solid #e0e0e0;
    color: #4e79a7;
}}
.config-json-wrap {{
    overflow-x: auto;
    margin-bottom: 20px;
    max-height: 500px;
    overflow-y: auto;
}}
.config-json {{
    background: #fff;
    border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    padding: 20px 24px;
    font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
    font-size: 12px;
    line-height: 1.6;
    color: #1a1a2e;
    white-space: pre-wrap;
    word-break: break-word;
}}
.reset-bar {{
    display: none;
    position: sticky;
    top: 8px;
    z-index: 50;
    background: #fff;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    padding: 8px 16px;
    margin-bottom: 12px;
    align-items: center;
    gap: 12px;
    font-size: 13px;
    color: #4a4a6a;
}}
.reset-bar.visible {{
    display: flex;
}}
.reset-bar button {{
    background: #4e79a7;
    color: #fff;
    border: none;
    border-radius: 5px;
    padding: 5px 14px;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
    transition: background 0.15s;
}}
.reset-bar button:hover {{
    background: #3b6291;
}}
.legend-hint {{
    font-size: 12px;
    color: #8a8a9a;
    margin-bottom: 12px;
    padding: 6px 10px;
    background: #f0f1f3;
    border-radius: 6px;
}}
@media (max-width: 900px) {{
    .chart-grid {{ grid-template-columns: 1fr; }}
    .sidebar {{ display: none; }}
    .main {{ margin-left: 0; }}
    .pivot-header {{ grid-template-columns: 1fr; }}
}}
</style>
</head>
<body>

<nav class="sidebar">
    <div class="sidebar-title">Dimster Pivot</div>
    {sidebar_html}
</nav>

<div class="main">
    <div class="header">
        <h1>{workload_name} &mdash; pivot</h1>
        {header_html}
    </div>

    {reset_bars_html}

    {config_html}
    {sections_html}
</div>

<script>
const PTICK_MAP = {ptick_json};

function xToPercentile(x) {{
    return 100.0 - 100.0 / Math.pow(10, x);
}}

function applyPercentileCallbacks(opts) {{
    if (opts.scales && opts.scales.x && opts.scales.x.ticks && opts.scales.x.ticks.__percentile_ticks) {{
        const tickVals = PTICK_MAP.values;
        const tickLabels = PTICK_MAP.labels;
        opts.scales.x.afterBuildTicks = function(axis) {{
            axis.ticks = tickVals
                .filter(v => v >= axis.min && v <= axis.max)
                .map(v => ({{ value: v }}));
        }};
        opts.scales.x.ticks = {{
            callback: function(value) {{
                const idx = tickVals.indexOf(value);
                return idx >= 0 ? tickLabels[idx] : '';
            }}
        }};
    }}
    if (opts.plugins && opts.plugins.tooltip && opts.plugins.tooltip.callbacks
        && opts.plugins.tooltip.callbacks.__percentile_tooltip) {{
        opts.plugins.tooltip.callbacks = {{
            title: function(items) {{
                if (!items.length) return '';
                const p = xToPercentile(items[0].parsed.x);
                if (p >= 99.99) return 'p99.99';
                if (p >= 99.9) return 'p' + p.toFixed(1);
                if (p >= 99) return 'p' + p.toFixed(0);
                return 'p' + p.toFixed(0);
            }},
            label: function(ctx) {{
                return ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(2) + ' ms';
            }}
        }};
    }}
}}

var zoomGroups = {{}};

function syncZoom(sourceChart, group) {{
    var xScale = sourceChart.scales.x;
    var min = xScale.min, max = xScale.max;
    if (!zoomGroups[group]) return;
    Object.values(zoomGroups[group]).forEach(function(c) {{
        if (c !== sourceChart) c.zoomScale('x', {{min: min, max: max}}, 'none');
    }});
    var bar = document.getElementById('resetBar-' + group);
    if (bar) bar.classList.add('visible');
}}

function resetZoom(group) {{
    if (!zoomGroups[group]) return;
    Object.values(zoomGroups[group]).forEach(function(c) {{ c.resetZoom('none'); }});
    var bar = document.getElementById('resetBar-' + group);
    if (bar) bar.classList.remove('visible');
}}

function applyZoomCallbacks(config, group) {{
    if (!config.options.plugins) config.options.plugins = {{}};
    config.options.plugins.zoom = {{
        zoom: {{
            drag: {{
                enabled: true,
                backgroundColor: 'rgba(78,121,167,0.15)',
                borderColor: '#4e79a7',
                borderWidth: 1
            }},
            mode: 'x',
            onZoomComplete: function(ctx) {{ syncZoom(ctx.chart, group); }}
        }}
    }};
}}

const chartConfigs = {configs_json};
chartConfigs.forEach(c => {{
    applyPercentileCallbacks(c.config.options);
    if (c.zoomGroup) {{
        if (!zoomGroups[c.zoomGroup]) zoomGroups[c.zoomGroup] = {{}};
        applyZoomCallbacks(c.config, c.zoomGroup);
    }}
    const ctx = document.getElementById(c.id);
    if (ctx) {{
        var chart = new Chart(ctx, c.config);
        if (c.zoomGroup) zoomGroups[c.zoomGroup][c.id] = chart;
    }}
}});
</script>

</body>
</html>"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_scenarios_arg(arg, n_scenarios, scenario_names):
    """Parse a CSV of 0-indexed scenario positions into a list.

    Order is preserved (e.g. "5,2,0" yields [5, 2, 0]) so the caller
    can also reorder the legend / color assignment. Duplicates and
    out-of-range positions are rejected with a message that lists
    every scenario alongside its index, so the user can fix the input.
    """
    raw = [s.strip() for s in arg.split(",") if s.strip()]
    if not raw:
        print("Error: --scenarios was empty", file=sys.stderr)
        sys.exit(1)

    indices = []
    for tok in raw:
        try:
            i = int(tok)
        except ValueError:
            print(
                f"Error: --scenarios entry {tok!r} is not an integer",
                file=sys.stderr,
            )
            sys.exit(1)
        if i < 0 or i >= n_scenarios:
            print(
                f"Error: --scenarios index {i} out of range "
                f"[0, {n_scenarios - 1}]. Available scenarios:",
                file=sys.stderr,
            )
            for idx, name in enumerate(scenario_names):
                print(f"    {idx}: {name}", file=sys.stderr)
            sys.exit(1)
        if i in indices:
            print(
                f"Error: --scenarios contains duplicate index {i}",
                file=sys.stderr,
            )
            sys.exit(1)
        indices.append(i)
    return indices


def main():
    parser = argparse.ArgumentParser(
        description="Generate pivot HTML from a single Dimster run result."
    )
    parser.add_argument("--input", required=True,
                        help="Path to a single run result JSON file")
    parser.add_argument("--output", required=True,
                        help="Output directory for the HTML file")
    parser.add_argument(
        "--scenarios",
        help=(
            "CSV of 0-indexed scenario positions to include "
            "(e.g. '0,3,5'). Order is preserved, so this also reorders "
            "the legend and color assignment. Default: include all."
        ),
    )
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(f"Error: file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(args.input) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: invalid JSON in {args.input}: {e}", file=sys.stderr)
        sys.exit(1)

    # Apply --scenarios filter BEFORE validate_pivot so the validator
    # only sees the scenarios that will actually be charted (otherwise
    # an unequal-length scenario the user is excluding would still
    # cause validation to bail).
    filter_suffix = ""
    if args.scenarios:
        scenarios = data.get("scenarioResults", [])
        names = [s.get("name", f"scenario-{i}")
                 for i, s in enumerate(scenarios)]
        keep = _parse_scenarios_arg(args.scenarios, len(scenarios), names)
        data["scenarioResults"] = [scenarios[i] for i in keep]
        kept_names = [scenarios[i].get("name", f"scenario-{i}") for i in keep]
        print(
            f"Filtered to {len(keep)} scenario(s): "
            f"{', '.join(kept_names)}"
        )
        # Flush so any subsequent validator error on stderr stays in order.
        sys.stdout.flush()
        # Encode the filter into the output filename so successive runs
        # with different filters don't overwrite each other and the user
        # can tell which file came from which slice at a glance.
        filter_suffix = "-scenarios-" + ",".join(str(i) for i in keep)

    validate_pivot(data)

    chart_configs, chart_sections = build_pivot_chart_configs(data)
    html = build_pivot_html(data, chart_configs, chart_sections)

    workload_name = data.get("metadata", {}).get("name", "pivot")
    os.makedirs(args.output, exist_ok=True)
    out_path = os.path.join(
        args.output, f"pivot-{workload_name}{filter_suffix}.html"
    )
    with open(out_path, "w") as f:
        f.write(html)

    print(f"Generated: {out_path}")


if __name__ == "__main__":
    main()
