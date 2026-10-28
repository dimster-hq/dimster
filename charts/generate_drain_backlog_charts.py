#!/usr/bin/env python3
"""
Generate HTML charts from Dimster drain-backlog result JSON files.

Usage:
    python3 generate_drain_backlog_charts.py --results <dir> --output <dir>

Scans --results dir recursively for JSON files where mode == "drain-backlog",
generates one HTML file per result using Chart.js for visualization.

Each drain's disk reads, which show whether it was served from disk or from
the page cache, come from drain-disk-reads.json beside the result. The
end-of-run metrics export measures them in Prometheus.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate_run_charts import is_result_of_mode, metadata_annotation_html


COLORS = [
    "#4e79a7",  # steel blue
    "#e15759",  # coral
    "#59a14f",  # green
    "#f28e2b",  # amber
    "#9c755f",  # taupe
    "#76b7b2",  # teal
]

COLORS_ALPHA = [
    "rgba(78, 121, 167, 0.15)",
    "rgba(225, 87, 89, 0.15)",
    "rgba(89, 161, 79, 0.15)",
    "rgba(242, 142, 43, 0.15)",
    "rgba(156, 117, 95, 0.15)",
    "rgba(118, 183, 178, 0.15)",
]


def find_drain_backlog_json_files(results_dir):
    """Recursively find drain-backlog results, excluding typed helper artifacts."""
    found = []
    for root, _dirs, files in os.walk(results_dir):
        for f in files:
            if not f.endswith(".json"):
                continue
            path = os.path.join(root, f)
            try:
                with open(path, "r") as fh:
                    data = json.load(fh)
                if is_result_of_mode(data, "drain-backlog"):
                    found.append((path, data))
            except json.JSONDecodeError as e:
                print(f"Warning: skipping {path}: invalid JSON: {e}", file=sys.stderr)
            except IOError:
                continue
    return found


# Written beside the result JSON by the end-of-run metrics export (export.py).
DISK_READS_FILE = "drain-disk-reads.json"


def find_disk_reads_file(result_path, results_root):
    """The disk reads measured for the run a result belongs to, or None: the
    nearest file in the result's directory or a parent, up to results_root."""
    root = os.path.abspath(results_root)
    d = os.path.dirname(os.path.abspath(result_path))
    while True:
        candidate = os.path.join(d, DISK_READS_FILE)
        if os.path.isfile(candidate):
            return candidate
        if d == root or os.path.dirname(d) == d:
            return None
        d = os.path.dirname(d)


def load_disk_reads(path):
    """{(drainStartTime, drainEndTime): bytes read from disk} from a disk reads file."""
    try:
        with open(path) as fh:
            drains = json.load(fh).get("drains", [])
    except (IOError, json.JSONDecodeError, AttributeError) as e:
        print(f"Warning: ignoring {path}: {e}", file=sys.stderr)
        return {}
    return {(d["drainStartTime"], d["drainEndTime"]): d["diskReadBytes"] for d in drains}


def measure_disk_reads(data, reads_by_window):
    """{(scenario index, test index): (bytes read, fraction of bytes drained)}
    for each drain whose window has a measurement."""
    reads = {}
    if not reads_by_window:
        return reads
    for s_idx, scenario in enumerate(data.get("scenarioResults", [])):
        for t_idx, test in enumerate(scenario.get("tests", [])):
            drain = test.get("drain", {})
            read = reads_by_window.get((drain.get("drainStartTime"), drain.get("drainEndTime")))
            if read is None:
                continue
            drained = drain.get("backlogSizeBytes", 0)
            reads[(s_idx, t_idx)] = (read, read / drained if drained > 0 else None)
    return reads


def avg(lst):
    if not lst:
        return 0
    return sum(lst) / len(lst)


def time_labels(arr_len, sample_rate_ms):
    step = sample_rate_ms / 1000
    return [round(i * step) for i in range(arr_len)]


def line_dataset(label, data, color_idx):
    c = COLORS[color_idx % len(COLORS)]
    ca = COLORS_ALPHA[color_idx % len(COLORS_ALPHA)]
    return {
        "label": label,
        "data": data,
        "borderColor": c,
        "backgroundColor": ca,
        "borderWidth": 2,
        "pointRadius": 0,
        "pointHoverRadius": 4,
        "tension": 0.2,
        "fill": False,
    }


def bar_options(title, y_label):
    return {
        "responsive": True,
        "maintainAspectRatio": True,
        "plugins": {
            "title": {"display": True, "text": title, "font": {"size": 14, "weight": "600"}},
            "legend": {"position": "top", "labels": {"usePointStyle": True, "padding": 16}},
            "tooltip": {"enabled": True},
        },
        "scales": {
            "y": {
                "beginAtZero": True,
                "title": {"display": True, "text": y_label},
                "grid": {"color": "#e0e0e0"},
            },
            "x": {"grid": {"display": False}},
        },
    }


def line_options(title, y_label, x_label):
    return {
        "responsive": True,
        "maintainAspectRatio": True,
        "plugins": {
            "title": {"display": True, "text": title, "font": {"size": 14, "weight": "600"}},
            "legend": {"position": "top", "labels": {"usePointStyle": True, "padding": 16}},
            "tooltip": {"enabled": True, "mode": "index", "intersect": False},
        },
        "scales": {
            "y": {
                "beginAtZero": True,
                "title": {"display": True, "text": y_label},
                "grid": {"color": "#e0e0e0"},
            },
            "x": {
                "title": {"display": True, "text": x_label},
                "grid": {"color": "#f0f0f0"},
            },
        },
        "interaction": {"mode": "nearest", "axis": "x", "intersect": False},
    }


def build_chart_configs(data, disk_reads=None):
    """Build all chart configs for a drain-backlog result. disk_reads is
    measure_disk_reads() output."""
    disk_reads = disk_reads or {}
    config = data.get("config", {})
    drain_config = config.get("drainBacklogConfig", {})
    sample_rate_ms = drain_config.get("sampleRateMillis", 10000)
    scenarios = data.get("scenarioResults", [])

    chart_configs = []
    chart_counts = {}

    for s_idx, scenario in enumerate(scenarios):
        tests = scenario.get("tests", [])
        if not tests:
            continue

        labels = [t.get("label", f"test-{i}") for i, t in enumerate(tests)]
        chart_idx = 0

        # --- 1. Drain Duration (bar) ---
        drain_durations = [round(t.get("drain", {}).get("drainDurationMs", 0) / 1000, 1) for t in tests]
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "bar",
                "data": {
                    "labels": labels,
                    "datasets": [{
                        "label": "Drain Duration",
                        "data": drain_durations,
                        "backgroundColor": COLORS[0],
                    }],
                },
                "options": bar_options("Drain Duration", "seconds"),
            },
        })
        chart_idx += 1

        # --- 2. Build Duration (bar) ---
        build_durations = [round(t.get("drain", {}).get("buildDurationMs", 0) / 1000, 1) for t in tests]
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "bar",
                "data": {
                    "labels": labels,
                    "datasets": [{
                        "label": "Build Duration",
                        "data": build_durations,
                        "backgroundColor": COLORS[2],
                    }],
                },
                "options": bar_options("Build Duration", "seconds"),
            },
        })
        chart_idx += 1

        # --- 2b. Disk Read Fraction (bar), when the drains' disk reads were measured ---
        # Disk reads during the drain / bytes drained: ~1 means served from
        # disk (cold cache), ~0 from the page cache. Shows whether test points
        # that drain a reused backlog started from the same cache state.
        fractions = [disk_reads.get((s_idx, t_idx), (None, None))[1] for t_idx in range(len(tests))]
        if any(f is not None for f in fractions):
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "config": {
                    "type": "bar",
                    "data": {
                        "labels": labels,
                        "datasets": [{
                            "label": "Disk Reads / Bytes Drained",
                            "data": [round(f, 3) if f is not None else None for f in fractions],
                            "backgroundColor": COLORS[3],
                        }],
                    },
                    "options": bar_options("Disk Read Fraction", "fraction of bytes drained"),
                },
            })
            chart_idx += 1

        # --- 3. Backlog Messages over Time ---
        max_len = max((len(t.get("drain", {}).get("backlogMessagesOverTime", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_msgs = []
        for ti, t in enumerate(tests):
            datasets_msgs.append(line_dataset(
                labels[ti], t.get("drain", {}).get("backlogMessagesOverTime", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_msgs},
                "options": line_options("Backlog Messages over Time", "messages", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 4. Backlog Bytes over Time ---
        datasets_bytes = []
        for ti, t in enumerate(tests):
            raw = t.get("drain", {}).get("backlogBytesOverTime", [])
            mb = [round(b / 1_000_000, 2) for b in raw]
            datasets_bytes.append(line_dataset(labels[ti], mb, ti))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_bytes},
                "options": line_options("Backlog Size over Time", "MB", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 5. Publish Rate over Time ---
        max_len = max((len(t.get("performance", {}).get("publishRate", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_pub = []
        for ti, t in enumerate(tests):
            datasets_pub.append(line_dataset(
                labels[ti], t.get("performance", {}).get("publishRate", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_pub},
                "options": line_options("Publish Rate over Time", "msg/s", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 6. Consume Rate over Time ---
        max_len = max((len(t.get("performance", {}).get("consumeRate", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_con = []
        for ti, t in enumerate(tests):
            datasets_con.append(line_dataset(
                labels[ti], t.get("performance", {}).get("consumeRate", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_con},
                "options": line_options("Consume Rate over Time", "msg/s", "Time (s)"),
            },
        })
        chart_idx += 1

        chart_counts[s_idx] = chart_idx

    return chart_configs, chart_counts


def build_sidebar_html(scenarios):
    items = []
    items.append('<div class="sidebar-label">Overview</div>')
    items.append('<a class="nav-link" href="#config-section">Configuration</a>')
    items.append('<div class="sidebar-label">Scenarios</div>')
    for i, s in enumerate(scenarios):
        name = s.get("name", f"Scenario {i + 1}")
        items.append(f'<a class="nav-link" href="#scenario-{i}">{name}</a>')
    return "\n".join(items)


def strip_heavy_data(data):
    """Return a copy with performance time series removed."""
    stripped = {}
    for key, value in data.items():
        if key == "scenarioResults":
            stripped_scenarios = []
            for scenario in value:
                stripped_scenario = {"name": scenario.get("name", "")}
                stripped_tests = []
                for test in scenario.get("tests", []):
                    stripped_test = {}
                    for tk, tv in test.items():
                        if tk == "performance":
                            continue
                        if tk == "drain":
                            stripped_drain = {k: v for k, v in tv.items()
                                              if k not in ("backlogMessagesOverTime", "backlogBytesOverTime")}
                            stripped_test[tk] = stripped_drain
                        else:
                            stripped_test[tk] = tv
                    stripped_tests.append(stripped_test)
                stripped_scenario["tests"] = stripped_tests
                stripped_scenarios.append(stripped_scenario)
            stripped[key] = stripped_scenarios
        else:
            stripped[key] = value
    return stripped


def build_config_section(data):
    stripped = strip_heavy_data(data)
    config_json = json.dumps(stripped, indent=2)
    return f"""
    <section id="config-section" class="scenario-section">
        <h2>Configuration</h2>
        <div class="config-json-wrap">
            <pre class="config-json">{config_json}</pre>
        </div>
    </section>
    """


def build_cell(drain):
    """Build time, or which test point's backlog a reused test point drained."""
    reused_from = drain.get("reusedBacklogFrom")
    if reused_from is None:
        return f'{round(drain.get("buildDurationMs", 0) / 1000, 1)}s'
    return f"reused (test point {reused_from + 1})"


def disk_cell(reading):
    """Bytes read from disk during the drain and their share of the bytes drained."""
    if reading is None:
        return "-"
    read, fraction = reading
    share = f" ({fraction * 100:.0f}%)" if fraction is not None else ""
    return f"{round(read / 1_000_000):,} MB{share}"


def build_scenario_sections(scenarios, chart_counts, disk_reads=None):
    disk_reads = disk_reads or {}
    sections = []
    for s_idx, scenario in enumerate(scenarios):
        name = scenario.get("name", f"Scenario {s_idx + 1}")
        tests = scenario.get("tests", [])
        num_charts = chart_counts.get(s_idx, 0)

        # Summary table
        table_rows = []
        for t_idx, t in enumerate(tests):
            wc = t.get("workloadConfig", {})
            d = t.get("drain", {})
            table_rows.append(
                f'<tr>'
                f'<td>{t.get("label", "")}</td>'
                f'<td>{d.get("backlogMessages", 0):,}</td>'
                f'<td>{round(d.get("backlogSizeBytes", 0) / 1_000_000):,} MB</td>'
                f'<td>{build_cell(d)}</td>'
                f'<td>{round(d.get("drainDurationMs", 0) / 1000, 1)}s</td>'
                f'<td>{disk_cell(disk_reads.get((s_idx, t_idx)))}</td>'
                f'<td>{"paused" if d.get("backlogProducerRate", 0) <= 0 else str(d.get("backlogProducerRate")) + " msg/s"}</td>'
                f'<td>{wc.get("partitionsPerTopic", "")}</td>'
                f'<td>{wc.get("consumersPerGroup", "")}</td>'
                f'</tr>'
            )

        table_html = ""
        if table_rows:
            table_html = f"""
            <div class="config-table-wrap">
                <table class="config-table">
                    <thead><tr>
                        <th>Label</th><th>Backlog Msgs</th><th>Backlog Size</th>
                        <th>Build Time</th><th>Drain Time</th><th>Disk Reads</th><th>Producer Rate (drain)</th>
                        <th>Partitions</th><th>Consumers/Group</th>
                    </tr></thead>
                    <tbody>{"".join(table_rows)}</tbody>
                </table>
            </div>
            """

        chart_cards = []
        for c_idx in range(num_charts):
            chart_id = f"chart-{s_idx}-{c_idx}"
            chart_cards.append(
                f'<div class="chart-card"><canvas id="{chart_id}"></canvas></div>'
            )

        sections.append(f"""
        <section id="scenario-{s_idx}" class="scenario-section">
            <h2>{name}</h2>
            {table_html}
            <div class="chart-grid">
                {"".join(chart_cards)}
            </div>
        </section>
        """)

    return "\n".join(sections)


def build_html(data, chart_configs, chart_counts, disk_reads=None):
    metadata = data.get("metadata", {})
    config = data.get("config", {})
    drain_config = config.get("drainBacklogConfig", {})
    scenarios = data.get("scenarioResults", [])

    workload_name = metadata.get("name", "Unknown Workload")
    kafka_config_name = metadata.get("kafkaConfigName", "")
    environment = metadata.get("environment", "")
    system = metadata.get("system", "")
    begin_time = metadata.get("beginTime", "")
    end_time = metadata.get("endTime", "")
    backlog_size_gb = drain_config.get("backlogSizeGB", "")
    backlog_producer_rate = drain_config.get("backlogProducerRate", -1)
    producer_rate_display = "paused" if backlog_producer_rate == -1 else f"{backlog_producer_rate} msg/s"
    annotation_meta = metadata_annotation_html(metadata)

    sidebar_html = build_sidebar_html(scenarios)
    config_section_html = build_config_section(data)
    sections_html = build_scenario_sections(scenarios, chart_counts, disk_reads)
    configs_json = json.dumps(chart_configs)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dimster Drain-Backlog - {workload_name}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
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
    width: 220px;
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
    margin-left: 220px;
    flex: 1;
    padding: 32px 40px;
    max-width: 1400px;
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
.meta-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: 10px 24px;
}}
.meta-item {{
    font-size: 13px;
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
@media (max-width: 900px) {{
    .chart-grid {{ grid-template-columns: 1fr; }}
    .sidebar {{ display: none; }}
    .main {{ margin-left: 0; }}
}}
</style>
</head>
<body>

<nav class="sidebar">
    <div class="sidebar-title">Drain-Backlog</div>
    {sidebar_html}
</nav>

<div class="main">
    <div class="header">
        <h1>{workload_name}</h1>
        <div class="meta-grid">
            <div class="meta-item"><span class="label">System:</span> <span class="value">{system}</span></div>
            <div class="meta-item"><span class="label">Environment:</span> <span class="value">{environment}</span></div>
            <div class="meta-item"><span class="label">Kafka Config:</span> <span class="value">{kafka_config_name}</span></div>
            <div class="meta-item"><span class="label">Backlog Size:</span> <span class="value">{backlog_size_gb} GB</span></div>
            <div class="meta-item"><span class="label">Producer Rate (drain):</span> <span class="value">{producer_rate_display}</span></div>
            {annotation_meta}
            <div class="meta-item"><span class="label">Start:</span> <span class="value">{begin_time}</span></div>
            <div class="meta-item"><span class="label">End:</span> <span class="value">{end_time}</span></div>
        </div>
    </div>

    {config_section_html}
    {sections_html}
</div>

<script>
const chartConfigs = {configs_json};
chartConfigs.forEach(c => {{
    const ctx = document.getElementById(c.id);
    if (ctx) {{
        new Chart(ctx, c.config);
    }}
}});
</script>

</body>
</html>"""


def main():
    parser = argparse.ArgumentParser(
        description="Generate HTML charts from Dimster drain-backlog result JSON files."
    )
    parser.add_argument(
        "--results", required=True, help="Directory to scan for result JSON files"
    )
    parser.add_argument(
        "--output", required=True, help="Directory to write HTML files"
    )
    args = parser.parse_args()

    if not os.path.isdir(args.results):
        print(f"Error: results directory does not exist: {args.results}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.output, exist_ok=True)

    files = find_drain_backlog_json_files(args.results)
    if not files:
        sys.exit(0)

    reads_by_file = {}
    for path, data in files:
        rel = os.path.relpath(path, args.results)
        out_name = os.path.splitext(rel)[0] + ".html"
        out_path = os.path.join(args.output, out_name)

        os.makedirs(os.path.dirname(out_path), exist_ok=True)

        reads_file = find_disk_reads_file(path, args.results)
        if reads_file and reads_file not in reads_by_file:
            reads_by_file[reads_file] = load_disk_reads(reads_file)
        disk_reads = measure_disk_reads(data, reads_by_file.get(reads_file))
        chart_configs, chart_counts = build_chart_configs(data, disk_reads)
        html = build_html(data, chart_configs, chart_counts, disk_reads)

        with open(out_path, "w") as fh:
            fh.write(html)

        print(f"Generated: {out_path}")

    print(f"Done. Processed {len(files)} drain-backlog file(s).")


if __name__ == "__main__":
    main()
