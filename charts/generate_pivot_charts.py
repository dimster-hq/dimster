#!/usr/bin/env python3
"""
Generate an HTML "pivot" view from one or more Dimster result JSON files.

A workload's scenarios often share one array shape (the same number of
"tests", the same label at each index) and differ on scalar fields such as
consumersPerGroup or messageDistributor. The run and drain charts lay a
result out scenario-first; the pivot lays it out test-point-first: one HTML
section per test index, whose comparison series are the scenarios.

With one file (`dimster pivot`), the series are that file's scenarios. With
several (`dimster compare --pivot`), every scenario has one series per
entry: bars put the scenarios on the x-axis with one dataset per entry, and
lines take their colour from the scenario and their dash from the entry.
A single file is simply a pivot of one entry, so both share one builder.

Run and drain-backlog results are supported.

Usage:
    python3 generate_pivot_charts.py --input <result.json> --output <dir> [--scenarios 0,3]
    python3 generate_pivot_charts.py --entry L1=F1 --entry L2=F2 --output <dir> [--name NAME]

Validates that every file's scenarios have equal-length tests arrays with
matching labels at each index (and, for several files, that the files match
each other as `dimster compare` requires). Writes pivot-<workload-name>.html
for --input and compare-<name>-pivot.html for --entry.
"""

import argparse
import colorsys
import json
import os
import sys

# Allow importing sibling helper modules.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from generate_drain_backlog_charts import drain_rows_table
from generate_run_charts import (
    PERCENTILE_TICK_VALUES,
    PERCENTILE_TICK_LABELS,
    avg,
    avg_mbs,
    bar_options,
    line_options,
    message_size_cell,
    percentile_options,
    percentile_points,
    time_labels,
)
from generate_compare_charts import (
    FILE_DASHES,
    FILE_DASH_NAMES,
    build_compare_config_section,
    build_compare_header,
    compare_line_dataset,
    compare_percentile_dataset,
    dash_swatch_css,
    drain_consume_mbs,
    entry_disk_reads,
    file_palette,
    parse_entries,
    validate_compare,
)


# One colour per scenario, so scenarios never share a colour family (the
# tab20 palette this replaced paired each hue with its own light shade).
# Tab10, then the six colours from Kelly's and tab20b that stay as far apart
# (CIELAB distance >= 27) as tab10's closest pair (28); further additions
# crowd existing colours, so past 16 scenarios the colours repeat. Entries
# are told apart within a scenario by dash, then by shade.
DISTINCT_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#222222", "#637939", "#393b79", "#d6616b", "#008856", "#882d17",
]

# Lightness shifts applied to a scenario's hue once every dash style is in
# use: entries 0-3 take the base hue with each dash, entries 4-7 a darker
# shade, 8-11 a lighter one, and so on.
SHADE_OFFSETS = [0.0, -0.18, 0.18, -0.3, 0.3]
SHADE_NAMES = ["", "darker", "lighter", "darkest", "lightest"]

# The result modes a pivot can lay out.
PIVOT_MODES = ("run", "drain-backlog")

# Neutral base colour for the entry legend: colour means scenario here.
LEGEND_SWATCH_COLOR = "#6c6c8a"

# Chart.js legend labels that draw each item as a short line in its
# dataset's colour and dash, so the legend shows which dash is which entry
# (the default point-style circle hides the dash).
LINE_LEGEND_LABELS = {"usePointStyle": True, "pointStyle": "line", "pointStyleWidth": 36}


def _hex_to_rgb(color):
    return tuple(int(color[i:i + 2], 16) / 255 for i in (1, 3, 5))


def _shade(color, step):
    """`color` with its lightness shifted for shade step `step` (0 = unchanged)."""
    offset = SHADE_OFFSETS[step % len(SHADE_OFFSETS)]
    if not offset:
        return color
    hue, lightness, saturation = colorsys.rgb_to_hls(*_hex_to_rgb(color))
    lightness = min(0.88, max(0.12, lightness + offset))
    red, green, blue = colorsys.hls_to_rgb(hue, lightness, saturation)
    return "#" + "".join(f"{round(c * 255):02x}" for c in (red, green, blue))


def _alpha(color, alpha=0.15):
    red, green, blue = (round(c * 255) for c in _hex_to_rgb(color))
    return f"rgba({red},{green},{blue},{alpha})"


def scenario_color(si):
    """Scenario si's colour, distinct from every other scenario's up to 16."""
    return DISTINCT_COLORS[si % len(DISTINCT_COLORS)]


def entry_dash(entry_idx):
    """Entry entry_idx's line dash: each style in turn, then again per shade."""
    return FILE_DASHES[entry_idx % len(FILE_DASHES)]


def entry_shade_step(entry_idx):
    """How many times the dash styles have been used up before this entry."""
    return entry_idx // len(FILE_DASHES)


def line_color(si, entry_idx):
    """Scenario si's hue, in entry entry_idx's shade."""
    return _shade(scenario_color(si), entry_shade_step(entry_idx))


def entry_style_name(entry_idx):
    """How an entry's lines look, for the legend hint: 'dashed', 'solid, darker'."""
    shade = SHADE_NAMES[entry_shade_step(entry_idx) % len(SHADE_NAMES)]
    dash = FILE_DASH_NAMES[entry_idx % len(FILE_DASH_NAMES)]
    return f"{dash}, {shade}" if shade else dash


def bar_color(entry_idx, series_idx, n_entries):
    """Colour of an aggregate bar dataset. A single entry keeps the pivot's
    distinct hues per series (publish/consume, percentile); several entries
    take compare's colour family per entry, a shade per series, so bars of
    one entry read as a group."""
    if n_entries == 1:
        return DISTINCT_COLORS[series_idx % len(DISTINCT_COLORS)]
    palette = file_palette(entry_idx)
    return palette[series_idx % len(palette)]


def bar_label(entry_label, series_name, n_entries):
    """Legend label of an aggregate bar dataset: the series alone for one
    entry, prefixed by the entry otherwise (as compare labels them)."""
    return series_name if n_entries == 1 else f"{entry_label} {series_name}"


def line_label(scenario_name, entry_label, n_entries):
    """Legend label of a time-series or percentile line: one per scenario,
    or per scenario and entry."""
    return scenario_name if n_entries == 1 else f"{scenario_name} / {entry_label}"


def single_entry_label(path, data):
    """Label of a lone `dimster pivot` input: its metadata label, else its
    file name. It heads the page's metadata column."""
    label = (data.get("metadata") or {}).get("label")
    return label or os.path.splitext(os.path.basename(path))[0]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _fail(message, details=()):
    """Print an error (and indented details) to stderr and exit 1."""
    print(f"Error: {message}", file=sys.stderr)
    for line in details:
        print(f"    {line}", file=sys.stderr)
    sys.exit(1)


def validate_pivot_shape(label, data):
    """Ensure one result has the shape a pivot needs.

    Bails (exit 1) when:
      - the result is not a run or drain-backlog result
      - fewer than 2 scenarios (pivot is meaningless with one)
      - scenarios have differing test counts
      - per-index test labels disagree across scenarios
    """
    mode = data.get("mode")
    if mode not in PIVOT_MODES:
        _fail(f"{label} is not a run or drain-backlog result (mode={mode})")

    scenarios = data.get("scenarioResults", [])
    if len(scenarios) < 2:
        _fail(f"pivot requires at least 2 scenarios; {label} has {len(scenarios)}")

    lengths = [(s.get("name", f"scenario-{i}"), len(s.get("tests", [])))
               for i, s in enumerate(scenarios)]
    if len({n for _, n in lengths}) != 1:
        _fail(f"{label}: scenarios must have equal-length tests arrays for pivot. Found:",
              [f"{name}: {n} tests" for name, n in lengths])

    n_tests = lengths[0][1]
    if n_tests == 0:
        _fail(f"{label}: scenarios contain no tests")

    # Per-index labels must match — they head the sections.
    for t_idx in range(n_tests):
        labels_at_idx = {
            s.get("name", f"scenario-{i}"): s["tests"][t_idx].get("label", f"test-{t_idx}")
            for i, s in enumerate(scenarios)
        }
        if len(set(labels_at_idx.values())) != 1:
            _fail(f"{label}: test label mismatch at index {t_idx}:",
                  [f"{name}: {lbl!r}" for name, lbl in labels_at_idx.items()])


def validate_pivot(entries):
    """Validate each entry's pivot shape, then, with several entries, that
    they match as a comparison (same mode, scenario names and test labels)."""
    for label, _path, data in entries:
        validate_pivot_shape(label, data)
    if len(entries) > 1:
        validate_compare(entries)


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


def build_pivot_chart_configs(entries):
    """Dispatch to the chart model of the validated result mode.

    Returns (chart_configs, chart_sections) where chart_sections maps each
    test index to {"aggregate": (start, end), "rest": (start, end),
    "notes": {chart_id: note}}. Chart IDs follow f"chart-{t_idx}-{c_idx}";
    zoom groups are scoped per test index so zooming one section leaves the
    others alone.
    """
    if entries[0][2].get("mode") == "drain-backlog":
        return build_drain_pivot_chart_configs(entries)
    return build_run_pivot_chart_configs(entries)


class _SectionCharts:
    """Appends one section's charts with sequential ids. Exists so the run
    and drain builders share chart registration, not chart content."""

    def __init__(self, chart_configs, t_idx):
        self.chart_configs = chart_configs
        self.t_idx = t_idx
        self.count = 0

    def add(self, config, height, zoom_group=None):
        chart_id = f"chart-{self.t_idx}-{self.count}"
        chart = {"id": chart_id, "fullWidth": True, "height": height, "config": config}
        if zoom_group:
            chart["zoomGroup"] = f"{zoom_group}-{self.t_idx}"
        self.chart_configs.append(chart)
        self.count += 1
        return chart_id

    def bar(self, title, y_label, x_labels, datasets, height):
        opts = bar_options(title, y_label)
        opts["maintainAspectRatio"] = False
        return self.add({"type": "bar", "data": {"labels": x_labels, "datasets": datasets},
                         "options": opts}, height)

    def time_series(self, title, y_label, datasets, sample_rate_ms, height):
        """A line chart over sample time; the x-axis spans the longest series."""
        max_len = max((len(ds["data"]) for ds in datasets), default=0)
        opts = line_options(title, y_label, "Time (s)")
        # Full width with a fixed height, so a legend of many scenarios
        # doesn't squeeze the plot area.
        opts["maintainAspectRatio"] = False
        opts["plugins"]["legend"]["labels"].update(LINE_LEGEND_LABELS)
        return self.add({"type": "line",
                         "data": {"labels": time_labels(max_len, sample_rate_ms),
                                  "datasets": datasets},
                         "options": opts}, height, zoom_group="time")


def _section_heights(n_series):
    """(throughput bar, latency bar, line chart) heights for a section of
    n_series scenario series. Chart.js packs bars horizontally, so heights
    only make room for the legend (which wraps as series grow) and the
    rotated x-axis labels. Latency bars carry a 5-item legend per entry."""
    return (min(440, max(340, 260 + n_series * 10)),
            min(520, max(400, 320 + n_series * 10)),
            min(560, max(400, 320 + n_series * 10)))


def _cells(entries, t_idx):
    """cells[f][si]: entry f's test at index t_idx of scenario si."""
    return [[s["tests"][t_idx] for s in data.get("scenarioResults", [])]
            for _l, _p, data in entries]


def _lines(entries, scenario_names, cells, series_of):
    """One line per scenario and entry, scenario-major so each scenario's
    entries sit together in the legend."""
    n_entries = len(entries)
    return [compare_line_dataset(line_label(name, entries[f][0], n_entries),
                                 series_of(cells[f][si]),
                                 line_color(si, f), _alpha(line_color(si, f)), entry_dash(f))
            for si, name in enumerate(scenario_names)
            for f in range(n_entries)]


def build_run_pivot_chart_configs(entries):
    """Per test index: throughput and latency percentile bars across the
    scenarios (one dataset per entry), then rate and latency series and
    percentile distributions with one line per scenario and entry."""
    n_entries = len(entries)
    labels = [e[0] for e in entries]
    scenarios = entries[0][2].get("scenarioResults", [])
    scenario_names = [s.get("name", f"scenario-{si}") for si, s in enumerate(scenarios)]
    n_tests = len(scenarios[0].get("tests", []))
    sample_rate_ms = max(e[2].get("runConfig", {}).get("sampleRateMillis", 10000)
                         for e in entries)
    tp_height, lat_height, series_height = _section_heights(len(scenarios) * n_entries)

    chart_configs = []
    chart_sections = {}
    for t_idx in range(n_tests):
        cells = _cells(entries, t_idx)
        charts = _SectionCharts(chart_configs, t_idx)
        notes = {}

        def results(f, si):
            return cells[f][si]["results"]

        def pub_con_bars(title, y_label, pub_of, con_of):
            """Publish and consume bars per entry, across the scenarios."""
            datasets = []
            for f in range(n_entries):
                for series_idx, (name, value_of) in enumerate((("Publish", pub_of),
                                                               ("Consume", con_of))):
                    datasets.append({
                        "label": bar_label(labels[f], name, n_entries),
                        "data": [value_of(f, si) for si in range(len(scenarios))],
                        "backgroundColor": bar_color(f, series_idx, n_entries),
                    })
            charts.bar(title, y_label, scenario_names, datasets, tp_height)

        def percentile_bars(title, keys):
            """A bar per percentile per entry, across the scenarios."""
            datasets = [{
                "label": bar_label(labels[f], pname, n_entries),
                "data": [results(f, si).get(key, 0) for si in range(len(scenarios))],
                "backgroundColor": bar_color(f, pi, n_entries),
            } for f in range(n_entries) for pi, (pname, key) in enumerate(zip(_PCTILE_NAMES, keys))]
            charts.bar(title, "ms", scenario_names, datasets, lat_height)

        pub_con_bars("Throughput (msg/s)", "msg/s",
                     lambda f, si: round(avg(results(f, si).get("publishRate", [])), 1),
                     lambda f, si: round(avg(results(f, si).get("consumeRate", [])), 1))
        pub_con_bars("Throughput (MB/s)", "MB/s",
                     lambda f, si: avg_mbs(cells[f][si], "publish"),
                     lambda f, si: avg_mbs(cells[f][si], "consume"))
        percentile_bars("Aggregated Latency - Publish", _PUB_PCTILE_KEYS)
        percentile_bars("Aggregated Latency - E2E", _E2E_PCTILE_KEYS)
        agg_end = charts.count

        for title, y_label, key in (
                ("Publish Rate over Time", "msg/s", "publishRate"),
                ("Consume Rate over Time", "msg/s", "consumeRate"),
                ("Publish Latency p99 over Time", "ms", "publishLatency99pct"),
                ("E2E Latency p99 over Time", "ms", "endToEndLatency99pct"),
                ("Publish Delay p99 over Time", "ms", "publishDelayLatency99pct")):
            charts.time_series(title, y_label,
                               _lines(entries, scenario_names, cells,
                                      lambda t, _k=key: t["results"].get(_k, [])),
                               sample_rate_ms, series_height)

        # Percentile distributions: a series without its quantile map is
        # left out, and the omission is noted on the chart and on stderr.
        for title, key in (("Publish Latency Percentile Distribution",
                            "aggregatedPublishLatencyQuantiles"),
                           ("E2E Latency Percentile Distribution",
                            "aggregatedEndToEndLatencyQuantiles")):
            datasets, omitted = [], []
            for si, name in enumerate(scenario_names):
                for f in range(n_entries):
                    series = line_label(name, labels[f], n_entries)
                    q = results(f, si).get(key, {})
                    if not q:
                        omitted.append(series)
                        continue
                    datasets.append(compare_percentile_dataset(
                        series, percentile_points(q), line_color(si, f), entry_dash(f)))
            if not datasets:
                continue
            opts = percentile_options(title)
            opts["maintainAspectRatio"] = False
            opts["plugins"]["legend"]["labels"].update(LINE_LEGEND_LABELS)
            chart_id = charts.add({"type": "scatter", "data": {"datasets": datasets},
                                   "options": opts}, series_height, zoom_group="pct")
            if omitted:
                notes[chart_id] = f"Omitted (no quantiles): {', '.join(omitted)}"
                print(f"Warning: section {t_idx} ({title}) omits series without "
                      f"quantiles: {', '.join(omitted)}", file=sys.stderr)

        chart_sections[t_idx] = {"aggregate": (0, agg_end), "rest": (agg_end, charts.count),
                                 "notes": notes}
    return chart_configs, chart_sections


def build_drain_pivot_chart_configs(entries):
    """Per test index: drain duration, consumer throughput and (where any
    entry measured it) disk read fraction bars across the scenarios, one
    dataset per entry, then backlog and consume rate over time with one
    line per scenario and entry."""
    n_entries = len(entries)
    labels = [e[0] for e in entries]
    scenarios = entries[0][2].get("scenarioResults", [])
    scenario_names = [s.get("name", f"scenario-{si}") for si, s in enumerate(scenarios)]
    n_tests = len(scenarios[0].get("tests", []))
    sample_rate_ms = max(
        e[2].get("config", {}).get("drainBacklogConfig", {}).get("sampleRateMillis", 10000)
        for e in entries)
    disk_reads = [entry_disk_reads(path, data) for _l, path, data in entries]
    bar_height, _lat, series_height = _section_heights(len(scenarios) * n_entries)

    chart_configs = []
    chart_sections = {}
    for t_idx in range(n_tests):
        cells = _cells(entries, t_idx)
        charts = _SectionCharts(chart_configs, t_idx)

        def entry_bars(title, y_label, value_of):
            """One bar per entry for each scenario."""
            datasets = [{"label": labels[f],
                         "data": [value_of(f, si) for si in range(len(scenarios))],
                         "backgroundColor": bar_color(f, 0, n_entries)}
                        for f in range(n_entries)]
            charts.bar(title, y_label, scenario_names, datasets, bar_height)

        entry_bars("Drain Duration", "seconds", lambda f, si: round(
            cells[f][si].get("drain", {}).get("drainDurationMs", 0) / 1000, 1))
        entry_bars("Consume Throughput during the Drain", "MB/s",
                   lambda f, si: drain_consume_mbs(cells[f][si]))
        fractions = [[disk_reads[f].get((si, t_idx), (None, None))[1]
                      for si in range(len(scenarios))] for f in range(n_entries)]
        if any(v is not None for row in fractions for v in row):
            entry_bars("Disk Read Fraction (disk reads / bytes drained)", "fraction",
                       lambda f, si: None if fractions[f][si] is None
                       else round(fractions[f][si], 3))
        agg_end = charts.count

        charts.time_series("Backlog Messages over Time", "messages",
                           _lines(entries, scenario_names, cells,
                                  lambda t: t.get("drain", {}).get("backlogMessagesOverTime", [])),
                           sample_rate_ms, series_height)
        charts.time_series("Consume Rate over Time", "msg/s",
                           _lines(entries, scenario_names, cells,
                                  lambda t: t.get("performance", {}).get("consumeRate", [])),
                           sample_rate_ms, series_height)

        chart_sections[t_idx] = {"aggregate": (0, agg_end), "rest": (agg_end, charts.count),
                                 "notes": {}}
    return chart_configs, chart_sections


# ---------------------------------------------------------------------------
# HTML construction
# ---------------------------------------------------------------------------

def _safe_id(label):
    """Make a label safe for use as an HTML id fragment."""
    return label.replace(" ", "-").replace(".", "_").replace("/", "_")


def build_pivot_sidebar(entries):
    """Sidebar: Overview → Configuration, then one nav-link per test index."""
    tests = entries[0][2].get("scenarioResults", [])[0].get("tests", [])

    items = []
    items.append('<div class="sidebar-label">Overview</div>')
    items.append('<a class="nav-link" href="#config-section">Configuration</a>')
    items.append('<div class="sidebar-label">Test points</div>')
    for t_idx, test in enumerate(tests):
        test_label = test.get("label", f"test-{t_idx}")
        items.append(
            f'<a class="nav-link" href="#section-{t_idx}-{_safe_id(test_label)}">{test_label}</a>'
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


def _section_tables(entries, t_idx):
    """How the scenarios differ at test index t_idx: one table per entry,
    headed by the entry's label when there are several."""
    tables = []
    for label, path, data in entries:
        scenarios = data.get("scenarioResults", [])
        if data.get("mode") == "drain-backlog":
            reads = entry_disk_reads(path, data)
            table = drain_rows_table("Scenario", [
                (s.get("name", ""), s["tests"][t_idx], reads.get((si, t_idx)))
                for si, s in enumerate(scenarios)])
        else:
            table = _scenario_distinguishing_table(scenarios, t_idx)
        heading = f'<h4 class="table-heading">{label}</h4>' if len(entries) > 1 else ""
        tables.append(heading + table)
    return "".join(tables)


def _chart_cards(t_idx, start, end, config_by_id, notes):
    """Full-width chart cards: with many scenarios the legend is large, and
    a side-by-side layout left the plot area too small to read."""
    cards = []
    for c_idx in range(start, end):
        chart_id = f"chart-{t_idx}-{c_idx}"
        height = config_by_id.get(chart_id, {}).get("height", 400)
        note = (f'<div class="legend-hint">{notes[chart_id]}</div>'
                if chart_id in notes else "")
        cards.append(
            f'<div class="chart-card chart-card-full">{note}'
            f'<canvas id="{chart_id}" style="height:{height}px;"></canvas>'
            f'</div>'
        )
    return "\n".join(cards)


def build_pivot_test_sections(entries, chart_configs, chart_sections):
    """One <section> per test index: how the scenarios differ, then the
    aggregate bars and the per-series charts."""
    tests = entries[0][2].get("scenarioResults", [])[0].get("tests", [])
    config_by_id = {c["id"]: c for c in chart_configs}

    sections = []
    for t_idx, test in enumerate(tests):
        ranges = chart_sections[t_idx]
        test_label = test.get("label", f"test-{t_idx}")
        agg_html = _chart_cards(t_idx, *ranges["aggregate"], config_by_id, ranges["notes"])
        rest_html = _chart_cards(t_idx, *ranges["rest"], config_by_id, ranges["notes"])
        sections.append(f"""
        <section id="section-{t_idx}-{_safe_id(test_label)}" class="scenario-section">
            <h2>Test point: {test_label}</h2>
            {_section_tables(entries, t_idx)}
            <h3 class="subsection-heading">Aggregate</h3>
            {agg_html}
            <h3 class="subsection-heading">Per-scenario series</h3>
            <div class="chart-grid">{rest_html}</div>
        </section>
        """)

    return "\n".join(sections)


def build_entry_legend_hint(entries):
    """Above the charts: line colour is the scenario, and each entry's dash
    (and past four entries, shade) is drawn beside its label."""
    items = [
        f'<span class="swatch" style="{dash_swatch_css(_shade(LEGEND_SWATCH_COLOR, entry_shade_step(i)), i)}"></span> '
        f'{label} ({entry_style_name(i)})'
        for i, (label, _path, _data) in enumerate(entries)]
    return ('<div class="legend-hint">Line colour is the scenario; line style is the entry: '
            f'{" &nbsp;&nbsp; ".join(items)}</div>')


def build_pivot_html(entries, chart_configs, chart_sections):
    """Assemble the full HTML page.

    Mirrors build_compare_html's shell (same Chart.js + zoom plugins,
    same CSS) so the look matches the rest of dimster's chart output.
    The inline JS (zoom sync, percentile callbacks, chart instantiation)
    is duplicated rather than factored out — it depends on template-string
    interpolation of `chartConfigs` and PTICK_MAP, and extracting it would
    force a separate module just to share ~80 lines.
    """
    labels = [e[0] for e in entries]
    workload_name = entries[0][2].get("metadata", {}).get("name", "Unknown Workload")
    if len(entries) == 1:
        page_title = f"Dimster Pivot - {workload_name}"
        heading = workload_name
        legend_html = ""
    else:
        page_title = f"Dimster Compare (pivot) - {' vs '.join(labels)} - {workload_name}"
        heading = " vs ".join(labels)
        legend_html = build_entry_legend_hint(entries)
    # Header columns separated by dividers, as on the compare page.
    grid_cols = " auto ".join(["1fr"] * len(entries))

    sidebar_html = build_pivot_sidebar(entries)
    header_html = build_compare_header(entries)
    config_html = build_compare_config_section(entries)
    sections_html = build_pivot_test_sections(entries, chart_configs, chart_sections)

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
<title>{page_title}</title>
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
.compare-header {{
    display: grid;
    grid-template-columns: {grid_cols};
    gap: 0 24px;
}}
.compare-col h3 {{
    font-size: 15px;
    font-weight: 700;
    margin-bottom: 10px;
    color: #4e79a7;
}}
.compare-divider {{
    width: 1px;
    background: #e0e0e0;
}}
.table-heading {{
    font-size: 13px;
    font-weight: 600;
    color: #4e79a7;
    margin-top: 12px;
    margin-bottom: 6px;
}}
.config-details {{
    margin-bottom: 12px;
}}
.config-details summary {{
    font-size: 14px;
    font-weight: 600;
    color: #4a4a6a;
    cursor: pointer;
    padding: 8px 0;
}}
.config-details summary:hover {{
    color: #4e79a7;
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
.legend-hint .swatch {{
    display: inline-block;
    width: 24px;
    height: 3px;
    vertical-align: middle;
    margin: 0 4px;
    border-radius: 1px;
}}
@media (max-width: 900px) {{
    .chart-grid {{ grid-template-columns: 1fr; }}
    .sidebar {{ display: none; }}
    .main {{ margin-left: 0; }}
    .compare-header {{ grid-template-columns: 1fr; }}
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
        <h1>{heading} &mdash; pivot</h1>
        {header_html}
    </div>

    {legend_html}

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
        _fail("--scenarios was empty")

    indices = []
    for tok in raw:
        try:
            i = int(tok)
        except ValueError:
            _fail(f"--scenarios entry {tok!r} is not an integer")
        if i < 0 or i >= n_scenarios:
            _fail(f"--scenarios index {i} out of range [0, {n_scenarios - 1}]. "
                  "Available scenarios:",
                  [f"{idx}: {name}" for idx, name in enumerate(scenario_names)])
        if i in indices:
            _fail(f"--scenarios contains duplicate index {i}")
        indices.append(i)
    return indices


def _load_single(path):
    """The lone --input entry."""
    if not os.path.isfile(path):
        _fail(f"file not found: {path}")
    try:
        with open(path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        _fail(f"invalid JSON in {path}: {e}")
    return single_entry_label(path, data), path, data


def filter_scenarios(entries, scenarios_arg):
    """Keep the --scenarios positions, in their given order, in every entry.
    Positions are those of the first entry; entries must list the same
    scenarios, which validation checks after filtering. Returns the suffix
    naming the filter in a single pivot's file name.

    Filtering happens BEFORE validation so the validator only sees the
    scenarios that will be charted: a scenario with other test points that
    the user is excluding must not fail validation.
    """
    scenarios = entries[0][2].get("scenarioResults", [])
    names = [s.get("name", f"scenario-{i}") for i, s in enumerate(scenarios)]
    keep = _parse_scenarios_arg(scenarios_arg, len(scenarios), names)
    for label, _path, data in entries:
        entry_scenarios = data.get("scenarioResults", [])
        if len(entry_scenarios) != len(scenarios):
            _fail(f"--scenarios needs every entry to have the same scenarios; "
                  f"{entries[0][0]} has {len(scenarios)}, {label} has {len(entry_scenarios)}")
        data["scenarioResults"] = [entry_scenarios[i] for i in keep]
    print(f"Filtered to {len(keep)} scenario(s): {', '.join(names[i] for i in keep)}")
    # Flush so any subsequent validator error on stderr stays in order.
    sys.stdout.flush()
    return "-scenarios-" + ",".join(str(i) for i in keep)


def main():
    parser = argparse.ArgumentParser(
        description="Generate pivot HTML from one Dimster run or drain-backlog "
                    "result, or from several compared side by side."
    )
    parser.add_argument("--input",
                        help="A single result JSON file to pivot")
    parser.add_argument("--entry", action="append",
                        help="LABEL=FILE result to compare (specify at least twice)")
    parser.add_argument("--name",
                        help="With --entry, writes compare-NAME-pivot.html "
                             "(default: the labels joined by -vs-)")
    parser.add_argument("--output", required=True,
                        help="Output directory for the HTML file")
    parser.add_argument(
        "--scenarios",
        help=(
            "CSV of 0-indexed scenario positions to include (e.g. '0,3,5'). "
            "Order is preserved, so this also reorders the legend and color "
            "assignment. Default: include all."
        ),
    )
    args = parser.parse_args()

    if bool(args.input) == bool(args.entry):
        _fail("use either --input FILE or --entry LABEL=FILE arguments")

    if args.input:
        entries = [_load_single(args.input)]
    else:
        if len(args.entry) < 2:
            _fail("at least 2 --entry arguments required")
        entries = parse_entries(args.entry)
    filter_suffix = filter_scenarios(entries, args.scenarios) if args.scenarios else ""

    if args.input:
        # Encode the filter into the file name so runs with different
        # filters don't overwrite each other.
        workload_name = entries[0][2].get("metadata", {}).get("name", "pivot")
        file_name = f"pivot-{workload_name}{filter_suffix}.html"
    else:
        # `dimster compare --pivot` always passes --name, so every output
        # file of the comparison shares one name; a filtered comparison is
        # kept apart by its --name.
        name = args.name or "-vs-".join(e[0] for e in entries)
        file_name = f"compare-{name}-pivot.html"

    validate_pivot(entries)

    chart_configs, chart_sections = build_pivot_chart_configs(entries)
    html = build_pivot_html(entries, chart_configs, chart_sections)

    os.makedirs(args.output, exist_ok=True)
    out_path = os.path.join(args.output, file_name)
    with open(out_path, "w") as f:
        f.write(html)

    print(f"Generated: {out_path}")


if __name__ == "__main__":
    main()
