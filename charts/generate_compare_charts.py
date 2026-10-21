#!/usr/bin/env python3
"""
Generate HTML comparison charts from two or more Dimster result JSON files.

Usage:
    python3 generate_compare_charts.py \
        --entry LABEL1=FILE1 --entry LABEL2=FILE2 [--entry LABEL3=FILE3 ...] \
        --output <dir>

Validates that all files have matching scenario names and test labels,
then generates a single HTML page with comparison charts. Run results use
overlaid performance series; correctness results use aggregate violation bars
and recorded-event timelines.
"""

import argparse
import colorsys
from datetime import datetime, timezone
import json
import math
import os
import sys

# Allow importing from the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from generate_run_charts import (
    COLORS,
    COLORS_ALPHA,
    PERCENTILE_TICK_VALUES,
    PERCENTILE_TICK_LABELS,
    avg,
    avg_mbs,
    bar_options,
    line_options,
    message_size_cell,
    metadata_annotation_html,
    percentile_dataset,
    percentile_options,
    percentile_points,
    strip_heavy_data,
    time_labels,
)

# Color palettes for up to 4 files.
# Each file gets a distinct color family (6 shades for test-point cycling).
FILE_PALETTES = [
    # File 0: Blues
    ["#1f77b4", "#4a90c4", "#6fa8d6", "#2b5c8a", "#3d6e9e", "#87b6e0"],
    # File 1: Reds/Oranges
    ["#e15759", "#c44e52", "#f28e2b", "#d4453a", "#e8734a", "#f5a05c"],
    # File 2: Greens
    ["#2ca02c", "#59a14f", "#76b76e", "#1e7a1e", "#449e3d", "#8fcb87"],
    # File 3: Purples
    ["#9467bd", "#7b4faa", "#b07cd3", "#6a3d94", "#a35cc0", "#c9a0e0"],
]

FILE_PALETTES_ALPHA = [
    # Blues
    ["rgba(31,119,180,0.15)", "rgba(74,144,196,0.15)", "rgba(111,168,214,0.15)",
     "rgba(43,92,138,0.15)", "rgba(61,110,158,0.15)", "rgba(135,182,224,0.15)"],
    # Reds/Oranges
    ["rgba(225,87,89,0.15)", "rgba(196,78,82,0.15)", "rgba(242,142,43,0.15)",
     "rgba(212,69,58,0.15)", "rgba(232,115,74,0.15)", "rgba(245,160,92,0.15)"],
    # Greens
    ["rgba(44,160,44,0.15)", "rgba(89,161,79,0.15)", "rgba(118,183,110,0.15)",
     "rgba(30,122,30,0.15)", "rgba(68,158,61,0.15)", "rgba(143,203,135,0.15)"],
    # Purples
    ["rgba(148,103,189,0.15)", "rgba(123,79,170,0.15)", "rgba(176,124,211,0.15)",
     "rgba(106,61,148,0.15)", "rgba(163,92,192,0.15)", "rgba(201,160,224,0.15)"],
]

# Line dash patterns per file index
FILE_DASHES = [
    [],          # solid
    [6, 3],      # dashed
    [2, 3],      # dotted
    [6, 3, 2, 3],  # dash-dot
]

FILE_DASH_NAMES = ["solid", "dashed", "dotted", "dash-dot"]


def _hls_to_rgb_ints(hue, lightness, saturation):
    """Convert HLS color components into 0-255 RGB integers."""
    red, green, blue = colorsys.hls_to_rgb(hue, lightness, saturation)
    return tuple(round(channel * 255) for channel in (red, green, blue))


def _hls_to_hex(hue, lightness, saturation):
    """Return a CSS hex color from HLS components."""
    red, green, blue = _hls_to_rgb_ints(hue, lightness, saturation)
    return f"#{red:02x}{green:02x}{blue:02x}"


def _hls_to_rgba(hue, lightness, saturation, alpha):
    """Return a CSS rgba color from HLS components."""
    red, green, blue = _hls_to_rgb_ints(hue, lightness, saturation)
    return f"rgba({red},{green},{blue},{alpha})"


def _generated_file_palette(file_index):
    """Generate a deterministic six-shade palette for result indexes beyond
    the original four hand-picked families.
    """
    hue = (file_index * 0.618033988749895) % 1.0
    saturation = 0.62
    lightness_values = [0.43, 0.54, 0.65, 0.32, 0.47, 0.73]
    return [_hls_to_hex(hue, lightness, saturation) for lightness in lightness_values]


def _generated_file_palette_alpha(file_index):
    """Generate translucent companions for generated file palettes."""
    hue = (file_index * 0.618033988749895) % 1.0
    saturation = 0.62
    lightness_values = [0.43, 0.54, 0.65, 0.32, 0.47, 0.73]
    return [_hls_to_rgba(hue, lightness, saturation, 0.15) for lightness in lightness_values]


def file_palette(file_index):
    """Return a color family for an arbitrary compare input index."""
    if file_index < len(FILE_PALETTES):
        return FILE_PALETTES[file_index]
    return _generated_file_palette(file_index)


def file_palette_alpha(file_index):
    """Return a translucent color family for an arbitrary compare input index."""
    if file_index < len(FILE_PALETTES_ALPHA):
        return FILE_PALETTES_ALPHA[file_index]
    return _generated_file_palette_alpha(file_index)


def file_dash(file_index):
    """Return a line dash pattern, cycling once the finite set is exhausted."""
    return FILE_DASHES[file_index % len(FILE_DASHES)]


def file_dash_name(file_index):
    """Return the user-facing dash pattern name for a compare input."""
    return FILE_DASH_NAMES[file_index % len(FILE_DASH_NAMES)]


def parse_entries(entry_args):
    """Parse --entry LABEL=FILE arguments into [(label, filepath, data), ...]."""
    entries = []
    for arg in entry_args:
        if "=" not in arg:
            print(f"Error: invalid entry format '{arg}': expected LABEL=FILE", file=sys.stderr)
            sys.exit(1)
        label, filepath = arg.split("=", 1)
        if not label:
            print(f"Error: empty label in '{arg}'", file=sys.stderr)
            sys.exit(1)
        if not os.path.isfile(filepath):
            print(f"Error: file not found: {filepath}", file=sys.stderr)
            sys.exit(1)
        try:
            with open(filepath) as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error: invalid JSON in {filepath}: {e}", file=sys.stderr)
            sys.exit(1)
        entries.append((label, filepath, data))
    return entries


def validate_compare(entries):
    """Validate that all result files have compatible structure for comparison."""
    ref_label, _, ref_data = entries[0]
    ref_mode = ref_data.get("mode")
    if ref_mode not in ("run", "correctness"):
        print(
            f"Error: {ref_label} file is not a run or correctness result "
            f"(mode={ref_mode})",
            file=sys.stderr,
        )
        sys.exit(1)

    ref_scenarios = ref_data.get("scenarioResults", [])

    for label, _, data in entries[1:]:
        if data.get("mode") != ref_mode:
            print(
                f"Error: result mode mismatch — {ref_label} has mode={ref_mode}, "
                f"{label} has mode={data.get('mode')}",
                file=sys.stderr,
            )
            sys.exit(1)

        scenarios = data.get("scenarioResults", [])
        if len(scenarios) != len(ref_scenarios):
            print(
                f"Error: scenario count mismatch — {ref_label} has {len(ref_scenarios)} "
                f"scenario(s), {label} has {len(scenarios)}",
                file=sys.stderr,
            )
            sys.exit(1)

        for i, (s_ref, s_other) in enumerate(zip(ref_scenarios, scenarios)):
            name_ref = s_ref.get("name", "")
            name_other = s_other.get("name", "")
            if name_ref != name_other:
                print(
                    f"Error: scenario {i + 1} name mismatch — "
                    f"{ref_label} has '{name_ref}', {label} has '{name_other}'",
                    file=sys.stderr,
                )
                sys.exit(1)

            tests_ref = s_ref.get("tests", [])
            tests_other = s_other.get("tests", [])
            if len(tests_ref) != len(tests_other):
                print(
                    f"Error: scenario '{name_ref}' test count mismatch — "
                    f"{ref_label} has {len(tests_ref)} test(s), {label} has {len(tests_other)}",
                    file=sys.stderr,
                )
                sys.exit(1)

            for j, (t_ref, t_other) in enumerate(zip(tests_ref, tests_other)):
                lbl_ref = t_ref.get("label", f"test-{j}")
                lbl_other = t_other.get("label", f"test-{j}")
                if lbl_ref != lbl_other:
                    print(
                        f"Error: scenario '{name_ref}' test {j + 1} label mismatch — "
                        f"{ref_label} has '{lbl_ref}', {label} has '{lbl_other}'",
                        file=sys.stderr,
                    )
                    sys.exit(1)


def compare_line_dataset(label, data, color, color_alpha, dash_pattern):
    """Build a line dataset with explicit color and dash pattern."""
    ds = {
        "label": label,
        "data": data,
        "borderColor": color,
        "backgroundColor": color_alpha,
        "borderWidth": 2,
        "pointRadius": 0,
        "pointHoverRadius": 4,
        "tension": 0.2,
        "fill": False,
    }
    if dash_pattern:
        ds["borderDash"] = dash_pattern
    return ds


def compare_percentile_dataset(label, points, color, dash_pattern):
    """Build a percentile scatter dataset with explicit color and dash pattern."""
    ds = {
        "label": label,
        "data": points,
        "borderColor": color,
        "backgroundColor": color,
        "borderWidth": 1.5,
        "pointRadius": 0,
        "pointHoverRadius": 5,
        "showLine": True,
        "tension": 0.3,
        "fill": False,
    }
    if dash_pattern:
        ds["borderDash"] = dash_pattern
    return ds


def build_compare_chart_configs(entries):
    """Dispatch to the chart model matching the validated result mode."""
    if entries[0][2].get("mode") == "correctness":
        return build_correctness_compare_chart_configs(entries)
    return build_run_compare_chart_configs(entries)


def _parse_timestamp_ms(value):
    """Parse Dimster's compact or ISO-8601 timestamps as UTC epoch millis."""
    if not value:
        return None
    try:
        if "T" in value:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            parsed = datetime.strptime(value, "%Y-%m-%d-%H-%M-%S").replace(
                tzinfo=timezone.utc
            )
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return round(parsed.timestamp() * 1000)
    except (TypeError, ValueError):
        return None


def _recorded_violation_count(correctness, violation_type):
    """Count represented events, respecting collapsed ViolationEntry ranges."""
    return sum(
        max(0, violation.get("count", 1))
        for violation in correctness.get("violations", [])
        if violation.get("type") == violation_type
    )


def _violation_buckets(test, violation_type, start_ms, bucket_ms, bucket_count):
    """Bin recorded violations by elapsed time from the scenario start."""
    buckets = [0] * bucket_count
    if start_ms is None:
        return buckets
    for violation in test.get("correctness", {}).get("violations", []):
        if violation.get("type") != violation_type:
            continue
        timestamp_ms = violation.get("timestampMs")
        if not isinstance(timestamp_ms, (int, float)):
            continue
        elapsed_ms = max(0, timestamp_ms - start_ms)
        index = min(bucket_count - 1, int(elapsed_ms // bucket_ms))
        buckets[index] += max(0, violation.get("count", 1))
    return buckets


def _cumulative(values):
    """Convert interval counts from legacy violation details into snapshots."""
    total = 0
    cumulative = []
    for value in values:
        total += value
        cumulative.append(total)
    return cumulative


def _correctness_time_series(
    test, series_key, legacy_type, start_ms, bucket_ms, bucket_count
):
    """Return a complete sampled series or reconstruct a legacy fallback.

    New results contain uncapped cumulative counter snapshots. Older results
    only have capped event details, so their fallback is necessarily partial.
    """
    correctness = test.get("correctness", {})
    sampled = correctness.get(series_key)
    if isinstance(sampled, list) and sampled:
        return sampled
    return _cumulative(
        _violation_buckets(
            test, legacy_type, start_ms, bucket_ms, bucket_count
        )
    )


def build_correctness_compare_chart_configs(entries):
    """Build correctness count bars and cumulative counter timelines.

    New results provide complete sampled counter arrays. Capped ViolationEntry
    details are used only as a compatibility fallback for older results.
    """
    labels = [entry[0] for entry in entries]
    all_data = [entry[2] for entry in entries]
    all_scenarios = [data.get("scenarioResults", []) for data in all_data]
    chart_configs = []
    chart_sections = {}

    metrics = [
        ("Message Loss", "lostMessages", "Lost messages", "#e15759"),
        ("Duplicates", "duplicateCount", "Duplicate messages", "#f28e2b"),
        ("Out of Order", "outOfOrderCount", "Out-of-order messages", "#59a14f"),
        ("CRC Failures", "crcFailureCount", "CRC failures", "#4e79a7"),
    ]
    timeline_metrics = [
        ("duplicateCountOverTime", "DUPLICATE", "Duplicates over Time"),
        ("outOfOrderCountOverTime", "OUT_OF_ORDER", "Out-of-Order Violations over Time"),
        ("crcFailureCountOverTime", "CRC_FAILURE", "CRC Failures over Time"),
    ]

    for scenario_index in range(len(all_scenarios[0])):
        scenario_tests = [
            scenarios[scenario_index].get("tests", []) for scenarios in all_scenarios
        ]
        if not scenario_tests[0]:
            continue

        test_labels = [
            test.get("label", f"test-{index}")
            for index, test in enumerate(scenario_tests[0])
        ]
        chart_index = 0
        aggregate_start = chart_index

        # One chart per authoritative correctness counter. The x-axis is the
        # compared protocol/run and each dataset is a test point.
        for title, key, y_label, color in metrics:
            datasets = []
            for test_index, test_label in enumerate(test_labels):
                datasets.append({
                    "label": test_label,
                    "data": [
                        scenario_tests[file_index][test_index]
                        .get("correctness", {})
                        .get(key, 0)
                        for file_index in range(len(entries))
                    ],
                    "backgroundColor": color,
                })
            options = bar_options(title, y_label)
            options["maintainAspectRatio"] = False
            chart_configs.append({
                "id": f"chart-{scenario_index}-{chart_index}",
                "fullWidth": True,
                "height": 320,
                "config": {
                    "type": "bar",
                    "data": {"labels": labels, "datasets": datasets},
                    "options": options,
                },
            })
            chart_index += 1

        aggregate_end = chart_index
        per_dimension = []

        for test_index, test_label in enumerate(test_labels):
            dimension_start = chart_index
            sample_rates = [
                data.get("config", {})
                .get("correctnessConfig", {})
                .get("sampleRateMillis", 10000)
                for data in all_data
            ]
            bucket_ms = max(1000, max(sample_rates, default=10000))

            starts = []
            ends = []
            for file_index, data in enumerate(all_data):
                scenario = all_scenarios[file_index][scenario_index]
                starts.append(_parse_timestamp_ms(scenario.get("beginTime")))
                ends.append(_parse_timestamp_ms(scenario.get("endTime")))
            sampled_lengths = [
                len(value)
                for file_index in range(len(entries))
                for series_key, _, _ in timeline_metrics
                for value in [
                    scenario_tests[file_index][test_index]
                    .get("correctness", {})
                    .get(series_key)
                ]
                if isinstance(value, list) and value
            ]
            if sampled_lengths:
                bucket_count = max(sampled_lengths)
            else:
                durations = [
                    end - start
                    for start, end in zip(starts, ends)
                    if start is not None and end is not None and end >= start
                ]
                max_duration_ms = max(durations, default=bucket_ms)
                bucket_count = max(1, math.ceil(max_duration_ms / bucket_ms) + 1)
            time_axis = [round(index * bucket_ms / 1000) for index in range(bucket_count)]

            for series_key, legacy_type, title in timeline_metrics:
                datasets = []
                for file_index in range(len(entries)):
                    palette = file_palette(file_index)
                    alpha_palette = file_palette_alpha(file_index)
                    datasets.append(compare_line_dataset(
                        labels[file_index],
                        _correctness_time_series(
                            scenario_tests[file_index][test_index],
                            series_key,
                            legacy_type,
                            starts[file_index],
                            bucket_ms,
                            bucket_count,
                        ),
                        palette[0],
                        alpha_palette[0],
                        file_dash(file_index),
                    ))
                chart_configs.append({
                    "id": f"chart-{scenario_index}-{chart_index}",
                    "zoomGroup": f"time-{scenario_index}-{test_index}",
                    "config": {
                        "type": "line",
                        "data": {"labels": time_axis, "datasets": datasets},
                        "options": line_options(
                            title, "Cumulative count", "Elapsed test time (s)"
                        ),
                    },
                })
                chart_index += 1

            per_dimension.append((test_label, dimension_start, chart_index))

        chart_sections[scenario_index] = {
            "aggregate": (aggregate_start, aggregate_end),
            "per_dimension": per_dimension,
        }

    return chart_configs, chart_sections


def build_run_compare_chart_configs(entries):
    """Build chart configs comparing N run results.

    Returns (chart_configs, chart_sections).
    Chart sections maps scenario index to:
        {"aggregate": (start, end), "per_dimension": [(label, start, end), ...]}
    """
    n_files = len(entries)
    labels = [e[0] for e in entries]
    all_data = [e[2] for e in entries]

    sample_rates = [d.get("runConfig", {}).get("sampleRateMillis", 10000) for d in all_data]
    all_scenarios = [d.get("scenarioResults", []) for d in all_data]

    chart_configs = []
    chart_sections = {}

    n_scenarios = len(all_scenarios[0])
    for s_idx in range(n_scenarios):
        scenario_tests = [all_scenarios[f_idx][s_idx].get("tests", []) for f_idx in range(n_files)]
        if not scenario_tests[0]:
            continue

        dim_labels = [t.get("label", f"test-{i}") for i, t in enumerate(scenario_tests[0])]
        n_dims = len(dim_labels)
        chart_idx = 0
        agg_start = chart_idx

        # Dynamic height for aggregate bar charts
        n_datasets_tp = n_files * 2  # pub + con per file
        n_datasets_lat = n_dims * n_files
        tp_height = min(600, max(300, n_datasets_tp * 30 + n_dims * 20))
        lat_height = min(600, max(300, n_datasets_lat * 30))

        # --- 1. Throughput (msg/s) ---
        datasets_tp = []
        for f_idx in range(n_files):
            palette = file_palette(f_idx)
            avg_pub = [round(avg(t["results"].get("publishRate", [])), 1) for t in scenario_tests[f_idx]]
            avg_con = [round(avg(t["results"].get("consumeRate", [])), 1) for t in scenario_tests[f_idx]]
            datasets_tp.append({"label": f"{labels[f_idx]} Publish", "data": avg_pub,
                                "backgroundColor": palette[0]})
            datasets_tp.append({"label": f"{labels[f_idx]} Consume", "data": avg_con,
                                "backgroundColor": palette[1]})
        opts_tp = bar_options("Throughput (msg/s)", "msg/s")
        opts_tp["maintainAspectRatio"] = False
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "fullWidth": True,
            "height": tp_height,
            "config": {
                "type": "bar",
                "data": {"labels": dim_labels, "datasets": datasets_tp},
                "options": opts_tp,
            },
        })
        chart_idx += 1

        # --- 2. Throughput MB/s ---
        datasets_mbs = []
        for f_idx in range(n_files):
            palette = file_palette(f_idx)
            avg_pub_mb = [avg_mbs(t, "publish") for t in scenario_tests[f_idx]]
            avg_con_mb = [avg_mbs(t, "consume") for t in scenario_tests[f_idx]]
            datasets_mbs.append({"label": f"{labels[f_idx]} Publish", "data": avg_pub_mb,
                                 "backgroundColor": palette[0]})
            datasets_mbs.append({"label": f"{labels[f_idx]} Consume", "data": avg_con_mb,
                                 "backgroundColor": palette[1]})
        opts_mbs = bar_options("Throughput (MB/s)", "MB/s")
        opts_mbs["maintainAspectRatio"] = False
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "fullWidth": True,
            "height": tp_height,
            "config": {
                "type": "bar",
                "data": {"labels": dim_labels, "datasets": datasets_mbs},
                "options": opts_mbs,
            },
        })
        chart_idx += 1

        # --- 3. Aggregated Latency - Publish ---
        # X-axis = dimension labels, bars = percentiles per file (toggleable via legend)
        pctile_names = ["p50", "p95", "p99", "p99.9", "max"]
        pctile_keys = [
            "aggregatedPublishLatency50pct",
            "aggregatedPublishLatency95pct",
            "aggregatedPublishLatency99pct",
            "aggregatedPublishLatency999pct",
            "aggregatedPublishLatencyMax",
        ]
        datasets_pub_lat = []
        for f_idx in range(n_files):
            palette = file_palette(f_idx)
            for pi, (pname, pkey) in enumerate(zip(pctile_names, pctile_keys)):
                data = [scenario_tests[f_idx][ti]["results"].get(pkey, 0) for ti in range(n_dims)]
                datasets_pub_lat.append({
                    "label": f"{labels[f_idx]} {pname}",
                    "data": data,
                    "backgroundColor": palette[pi % len(palette)],
                })
        opts_pub_lat = bar_options("Aggregated Latency - Publish", "ms")
        opts_pub_lat["maintainAspectRatio"] = False
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "fullWidth": True,
            "height": lat_height,
            "config": {
                "type": "bar",
                "data": {"labels": dim_labels, "datasets": datasets_pub_lat},
                "options": opts_pub_lat,
            },
        })
        chart_idx += 1

        # --- 4. Aggregated Latency - E2E ---
        e2e_keys = [
            "aggregatedEndToEndLatency50pct",
            "aggregatedEndToEndLatency95pct",
            "aggregatedEndToEndLatency99pct",
            "aggregatedEndToEndLatency999pct",
            "aggregatedEndToEndLatencyMax",
        ]
        datasets_e2e_lat = []
        for f_idx in range(n_files):
            palette = file_palette(f_idx)
            for pi, (pname, ekey) in enumerate(zip(pctile_names, e2e_keys)):
                data = [scenario_tests[f_idx][ti]["results"].get(ekey, 0) for ti in range(n_dims)]
                datasets_e2e_lat.append({
                    "label": f"{labels[f_idx]} {pname}",
                    "data": data,
                    "backgroundColor": palette[pi % len(palette)],
                })
        opts_e2e_lat = bar_options("Aggregated Latency - E2E", "ms")
        opts_e2e_lat["maintainAspectRatio"] = False
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "fullWidth": True,
            "height": lat_height,
            "config": {
                "type": "bar",
                "data": {"labels": dim_labels, "datasets": datasets_e2e_lat},
                "options": opts_e2e_lat,
            },
        })
        chart_idx += 1

        agg_end = chart_idx

        # --- Per-dimension charts ---
        sample_rate_ms = max(sample_rates)
        per_dimension = []

        for ti in range(n_dims):
            dim_start = chart_idx
            ci = ti % len(COLORS)

            # Time-series helper for a single dimension
            def make_dim_time_chart(title, y_label, data_key, _ti=ti, _ci=ci):
                nonlocal chart_idx
                max_len = 0
                for f_idx in range(n_files):
                    max_len = max(max_len, len(scenario_tests[f_idx][_ti]["results"].get(data_key, [])))
                x_labels = time_labels(max_len, sample_rate_ms)
                datasets = []
                for f_idx in range(n_files):
                    palette = file_palette(f_idx)
                    alpha_palette = file_palette_alpha(f_idx)
                    datasets.append(compare_line_dataset(
                        labels[f_idx],
                        scenario_tests[f_idx][_ti]["results"].get(data_key, []),
                        palette[0],
                        alpha_palette[0],
                        file_dash(f_idx)))
                chart_configs.append({
                    "id": f"chart-{s_idx}-{chart_idx}",
                    "zoomGroup": f"time-{s_idx}-{_ti}",
                    "config": {
                        "type": "line",
                        "data": {"labels": x_labels, "datasets": datasets},
                        "options": line_options(title, y_label, "Time (s)"),
                    },
                })
                chart_idx += 1

            make_dim_time_chart("Publish Rate over Time", "msg/s", "publishRate")
            make_dim_time_chart("Consume Rate over Time", "msg/s", "consumeRate")
            make_dim_time_chart("Publish Latency p99 over Time", "ms", "publishLatency99pct")
            make_dim_time_chart("E2E Latency p99 over Time", "ms", "endToEndLatency99pct")
            make_dim_time_chart("Publish Delay p99 over Time", "ms", "publishDelayLatency99pct")

            # Publish Latency Percentile Distribution
            pub_q_datasets = []
            for f_idx in range(n_files):
                palette = file_palette(f_idx)
                q = scenario_tests[f_idx][ti]["results"].get("aggregatedPublishLatencyQuantiles", {})
                if q:
                    pub_q_datasets.append(compare_percentile_dataset(
                        labels[f_idx],
                        percentile_points(q),
                        palette[0],
                        file_dash(f_idx)))
            if pub_q_datasets:
                chart_configs.append({
                    "id": f"chart-{s_idx}-{chart_idx}",
                    "zoomGroup": f"pct-{s_idx}-{ti}",
                    "config": {
                        "type": "scatter",
                        "data": {"datasets": pub_q_datasets},
                        "options": percentile_options("Publish Latency Percentile Distribution"),
                    },
                })
                chart_idx += 1

            # E2E Latency Percentile Distribution
            e2e_q_datasets = []
            for f_idx in range(n_files):
                palette = file_palette(f_idx)
                q = scenario_tests[f_idx][ti]["results"].get("aggregatedEndToEndLatencyQuantiles", {})
                if q:
                    e2e_q_datasets.append(compare_percentile_dataset(
                        labels[f_idx],
                        percentile_points(q),
                        palette[0],
                        file_dash(f_idx)))
            if e2e_q_datasets:
                chart_configs.append({
                    "id": f"chart-{s_idx}-{chart_idx}",
                    "zoomGroup": f"pct-{s_idx}-{ti}",
                    "config": {
                        "type": "scatter",
                        "data": {"datasets": e2e_q_datasets},
                        "options": percentile_options("E2E Latency Percentile Distribution"),
                    },
                })
                chart_idx += 1

            per_dimension.append((dim_labels[ti], dim_start, chart_idx))

        chart_sections[s_idx] = {
            "aggregate": (agg_start, agg_end),
            "per_dimension": per_dimension,
        }

    return chart_configs, chart_sections


def build_compare_header(entries):
    """Build an N-column metadata comparison header."""
    def meta_col(data, label):
        m = data.get("metadata", {})
        if data.get("mode") == "correctness":
            rc = data.get("config", {}).get("correctnessConfig", {})
            kc = data.get("config", {}).get("baseKafkaConfig", {})
            secondary_timing = (
                f'<div class="meta-item"><span class="label">Grace Period:</span> '
                f'<span class="value">{rc.get("gracePeriodSeconds", "")} s</span></div>'
            )
        else:
            rc = data.get("runConfig", {})
            kc = data.get("baseKafkaConfig", {})
            secondary_timing = (
                f'<div class="meta-item"><span class="label">Warmup:</span> '
                f'<span class="value">{rc.get("warmupDurationMinutes", "")} min</span></div>'
            )
        annotation_meta = metadata_annotation_html(m)
        return f"""
        <div class="compare-col">
            <h3>{label}</h3>
            <div class="meta-item"><span class="label">System:</span> <span class="value">{m.get("system", "")}</span></div>
            <div class="meta-item"><span class="label">Environment:</span> <span class="value">{m.get("environment", "")}</span></div>
            <div class="meta-item"><span class="label">Kafka Config:</span> <span class="value">{m.get("kafkaConfigName", "")}</span></div>
            <div class="meta-item"><span class="label">Replication Factor:</span> <span class="value">{kc.get("replicationFactor", "")}</span></div>
            <div class="meta-item"><span class="label">Test Duration:</span> <span class="value">{rc.get("testDurationMinutes", "")} min</span></div>
            {secondary_timing}
            {annotation_meta}
            <div class="meta-item"><span class="label">Start:</span> <span class="value">{m.get("beginTime", "")}</span></div>
            <div class="meta-item"><span class="label">End:</span> <span class="value">{m.get("endTime", "")}</span></div>
        </div>
        """

    cols = []
    for i, (label, _, data) in enumerate(entries):
        if i > 0:
            cols.append('<div class="compare-divider"></div>')
        cols.append(meta_col(data, label))

    return f'<div class="compare-header">{"".join(cols)}</div>'


def build_compare_config_section(entries):
    """Build configuration section with N collapsible JSON blocks."""
    blocks = []
    for label, _, data in entries:
        s = json.dumps(strip_heavy_data(data), indent=2)
        blocks.append(f"""
        <details class="config-details">
            <summary>{label}</summary>
            <div class="config-json-wrap"><pre class="config-json">{s}</pre></div>
        </details>
        """)
    return f"""
    <section id="config-section" class="scenario-section">
        <h2>Configuration</h2>
        {"".join(blocks)}
    </section>
    """


def build_compare_sidebar(scenarios, chart_sections):
    """Build sidebar navigation for comparison page."""
    items = []
    items.append('<div class="sidebar-label">Overview</div>')
    items.append('<a class="nav-link" href="#config-section">Configuration</a>')
    items.append('<div class="sidebar-label">Scenarios</div>')
    for i, s in enumerate(scenarios):
        name = s.get("name", f"Scenario {i + 1}")
        items.append(f'<a class="nav-link" href="#scenario-{i}">{name}</a>')
        items.append(f'<a class="nav-sublink" href="#scenario-{i}-aggregate">Aggregate</a>')
        per_dim = chart_sections.get(i, {}).get("per_dimension", [])
        for dim_label, _, _ in per_dim:
            safe_id = dim_label.replace(" ", "-")
            items.append(f'<a class="nav-sublink" href="#scenario-{i}-dim-{safe_id}">{dim_label}</a>')
    return "\n".join(items)


def _build_workload_table(tests, heading):
    """Build a workload config table with an optional heading."""
    rows = []
    for t in tests:
        wc = t.get("workloadConfig", {})
        rows.append(
            f'<tr>'
            f'<td>{t.get("label", "")}</td>'
            f'<td>{wc.get("topics", "")}</td>'
            f'<td>{wc.get("partitionsPerTopic", "")}</td>'
            f'<td>{message_size_cell(wc)}</td>'
            f'<td>{wc.get("producersPerTopic", "")}</td>'
            f'<td>{wc.get("producerRate", "")}</td>'
            f'<td>{wc.get("consumerGroupsPerTopic", "")}</td>'
            f'<td>{wc.get("consumersPerGroup", "")}</td>'
            f'<td>{wc.get("messageDistributor", "")}</td>'
            f'<td>{wc.get("consumerType", "CONSUMER_GROUP")}</td>'
            f'</tr>'
        )
    if not rows:
        return ""
    heading_html = f'<h4 class="table-heading">{heading}</h4>' if heading else ""
    return f"""
    {heading_html}
    <div class="config-table-wrap">
        <table class="config-table">
            <thead><tr>
                <th>Label</th><th>Topics</th><th>Partitions</th>
                <th>Msg Size</th><th>Producers</th><th>Rate</th>
                <th>Consumer Groups</th><th>Consumers/Group</th><th>Key Dist</th><th>Consumer Type</th>
            </tr></thead>
            <tbody>{"".join(rows)}</tbody>
        </table>
    </div>
    """


def build_compare_scenario_sections(entries, chart_configs, chart_sections):
    """Build scenario sections with aggregate + per-dimension chart grids."""
    all_scenarios = [e[2].get("scenarioResults", []) for e in entries]
    labels = [e[0] for e in entries]

    # Build a lookup from chart id to config for height info
    config_by_id = {c["id"]: c for c in chart_configs}

    sections = []
    for s_idx in range(len(all_scenarios[0])):
        name = all_scenarios[0][s_idx].get("name", f"Scenario {s_idx + 1}")
        ranges = chart_sections.get(s_idx, {})

        table_html = ""
        for f_idx in range(len(entries)):
            tests = all_scenarios[f_idx][s_idx].get("tests", [])
            table_html += _build_workload_table(tests, labels[f_idx])

        # Aggregate charts — rendered full-width
        agg_start, agg_end = ranges.get("aggregate", (0, 0))
        agg_cards = []
        for c_idx in range(agg_start, agg_end):
            chart_id = f"chart-{s_idx}-{c_idx}"
            cfg = config_by_id.get(chart_id, {})
            height = cfg.get("height", 300)
            agg_cards.append(
                f'<div class="chart-card chart-card-full">'
                f'<canvas id="{chart_id}" style="height:{height}px;"></canvas>'
                f'</div>'
            )
        agg_html = "\n".join(agg_cards)

        # Per-dimension charts
        per_dim = ranges.get("per_dimension", [])
        dim_sections = []
        for dim_label, dim_start, dim_end in per_dim:
            safe_id = dim_label.replace(" ", "-")
            cards = []
            for c_idx in range(dim_start, dim_end):
                chart_id = f"chart-{s_idx}-{c_idx}"
                cards.append(f'<div class="chart-card"><canvas id="{chart_id}"></canvas></div>')
            dim_grid = f'<div class="chart-grid">{"".join(cards)}</div>'
            dim_sections.append(
                f'<h3 id="scenario-{s_idx}-dim-{safe_id}" class="subsection-heading">{dim_label}</h3>\n'
                f'{dim_grid}'
            )
        dim_html = "\n".join(dim_sections)

        sections.append(f"""
        <section id="scenario-{s_idx}" class="scenario-section">
            <h2>Scenario: {name}</h2>
            {table_html}
            <h3 id="scenario-{s_idx}-aggregate" class="subsection-heading">Aggregate</h3>
            {agg_html}
            {dim_html}
        </section>
        """)

    return "\n".join(sections)


def build_legend_hint(entries):
    """Build a legend showing line style per file."""
    n = len(entries)
    labels = [e[0] for e in entries]

    def dash_css(file_index):
        color = file_palette(file_index)[0]
        style_index = file_index % len(FILE_DASHES)
        if style_index == 0:
            return f"background: {color};"
        if style_index == 1:
            return (
                f"background: repeating-linear-gradient(90deg, {color} 0, "
                f"{color} 6px, transparent 6px, transparent 9px);"
            )
        if style_index == 2:
            return (
                f"background: repeating-linear-gradient(90deg, {color} 0, "
                f"{color} 2px, transparent 2px, transparent 5px);"
            )
        return (
            f"background: repeating-linear-gradient(90deg, {color} 0, "
            f"{color} 6px, transparent 6px, transparent 9px, {color} 9px, "
            f"{color} 11px, transparent 11px, transparent 14px);"
        )

    items = []
    for i in range(n):
        items.append(
            f'<span class="swatch" style="{dash_css(i)}"></span> {labels[i]} ({file_dash_name(i)})'
        )
    return f'<div class="legend-hint">{" &nbsp;&nbsp; ".join(items)}</div>'


def build_correctness_quality_note(entries):
    """Explain sampled-series semantics and any legacy fallback limitations."""
    if entries[0][2].get("mode") != "correctness":
        return ""

    count_keys = {
        "DUPLICATE": ("duplicateCount", "duplicateCountOverTime"),
        "OUT_OF_ORDER": ("outOfOrderCount", "outOfOrderCountOverTime"),
        "CRC_FAILURE": ("crcFailureCount", "crcFailureCountOverTime"),
    }
    warnings = []
    uses_legacy_fallback = False
    for label, _, data in entries:
        for scenario in data.get("scenarioResults", []):
            for test in scenario.get("tests", []):
                correctness = test.get("correctness", {})
                test_label = test.get("label", "default")
                for violation_type, (total_key, series_key) in count_keys.items():
                    sampled = correctness.get(series_key)
                    if isinstance(sampled, list) and sampled:
                        continue
                    uses_legacy_fallback = True
                    total = correctness.get(total_key, 0)
                    recorded = _recorded_violation_count(correctness, violation_type)
                    if recorded != total:
                        warnings.append(
                            f"{label} / {test_label}: {violation_type} timeline represents "
                            f"{recorded} of {total} total violations."
                        )
                    timestamps = {
                        violation.get("timestampMs")
                        for violation in correctness.get("violations", [])
                        if violation.get("type") == violation_type
                    }
                    if recorded > 1 and len(timestamps) == 1:
                        warnings.append(
                            f"{label} / {test_label}: all recorded {violation_type} "
                            "events share one timestamp; the source result has no finer timing."
                        )

    details = "".join(f"<li>{warning}</li>" for warning in warnings)
    warning_block = f"<ul>{details}</ul>" if details else ""
    fallback_text = (
        " Older results without sampled arrays fall back to the recorded violation "
        "list, which may be capped."
        if uses_legacy_fallback
        else ""
    )
    return f"""
    <div class="data-quality-note">
        <strong>Correctness timeline semantics</strong>
        <p>Bar charts use final aggregate counters. Timelines prefer complete
        cumulative counter snapshots recorded at the configured sample interval.
        Message loss is intentionally shown only as a final aggregate because
        intermediate missing-message counts primarily represent lag.{fallback_text}</p>
        {warning_block}
    </div>
    """


def build_compare_html(entries, chart_configs, chart_sections):
    """Generate the full comparison HTML page."""
    labels = [e[0] for e in entries]
    workload_name = entries[0][2].get("metadata", {}).get("name", "Unknown Workload")
    scenarios = entries[0][2].get("scenarioResults", [])

    title_text = " vs ".join(labels)

    # Dynamic grid columns for header
    n = len(entries)
    grid_cols = " auto ".join(["1fr"] * n)

    sidebar_html = build_compare_sidebar(scenarios, chart_sections)
    header_html = build_compare_header(entries)
    config_html = build_compare_config_section(entries)
    sections_html = build_compare_scenario_sections(entries, chart_configs, chart_sections)
    legend_html = build_legend_hint(entries)
    quality_note_html = build_correctness_quality_note(entries)

    # Build reset bars for all zoom groups
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
<title>Dimster Compare - {title_text} - {workload_name}</title>
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
.nav-sublink {{
    display: block;
    padding: 5px 20px 5px 32px;
    color: #8080a0;
    text-decoration: none;
    font-size: 11px;
    transition: background 0.15s, color 0.15s;
    border-left: 3px solid transparent;
}}
.nav-sublink:hover {{
    background: #2a2a4e;
    color: #c8c8d4;
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
.data-quality-note {{
    background: #fff8e6;
    border: 1px solid #f1cf7a;
    border-radius: 6px;
    color: #4f3b0b;
    font-size: 13px;
    line-height: 1.45;
    margin: 0 0 24px;
    padding: 14px 18px;
}}
.data-quality-note p {{ margin-top: 5px; }}
.data-quality-note ul {{ margin: 8px 0 0 20px; }}
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
.table-heading {{
    font-size: 13px;
    font-weight: 600;
    color: #4e79a7;
    margin-top: 12px;
    margin-bottom: 6px;
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
    margin-bottom: 16px;
    padding: 8px 12px;
    background: #f0f1f3;
    border-radius: 6px;
    display: inline-block;
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
    .compare-divider {{ display: none; }}
}}
</style>
</head>
<body>

<nav class="sidebar">
    <div class="sidebar-title">Dimster Compare</div>
    {sidebar_html}
</nav>

<div class="main">
    <div class="header">
        <h1>{workload_name} &mdash; {title_text}</h1>
        {header_html}
    </div>

    {legend_html}

    {quality_note_html}

    {reset_bars_html}

    {config_html}
    {sections_html}
</div>

<script>
const PTICK_MAP = {ptick_json};

function xToPercentile(x) {{
    return 100.0 - 100.0 / Math.pow(10, x);
}}

function pctLabel(x) {{
    const p = xToPercentile(x);
    if (p >= 99.99) return 'p99.99';
    if (p >= 99.9) return 'p99.9';
    if (p >= 99) return 'p' + p.toFixed(0);
    return 'p' + p.toFixed(0);
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


def main():
    parser = argparse.ArgumentParser(
        description="Generate comparison charts from two or more Dimster run result JSON files."
    )
    parser.add_argument("--entry", action="append", required=True,
                        help="LABEL=FILE entry (specify at least twice)")
    parser.add_argument("--output", required=True, help="Output directory for HTML file")

    # Support legacy --t1-label/--t1-file/--t2-label/--t2-file args
    parser.add_argument("--t1-label", help=argparse.SUPPRESS)
    parser.add_argument("--t1-file", help=argparse.SUPPRESS)
    parser.add_argument("--t2-label", help=argparse.SUPPRESS)
    parser.add_argument("--t2-file", help=argparse.SUPPRESS)

    args = parser.parse_args()

    # Handle legacy args
    if args.t1_label and args.t1_file:
        if args.entry is None:
            args.entry = []
        args.entry.insert(0, f"{args.t1_label}={args.t1_file}")
    if args.t2_label and args.t2_file:
        if args.entry is None:
            args.entry = []
        args.entry.append(f"{args.t2_label}={args.t2_file}")

    if not args.entry or len(args.entry) < 2:
        print("Error: at least 2 --entry arguments required", file=sys.stderr)
        sys.exit(1)

    entries = parse_entries(args.entry)
    validate_compare(entries)

    chart_configs, chart_sections = build_compare_chart_configs(entries)
    html = build_compare_html(entries, chart_configs, chart_sections)

    labels = [e[0] for e in entries]
    os.makedirs(args.output, exist_ok=True)
    out_path = os.path.join(args.output, f"compare-{'-vs-'.join(labels)}.html")
    with open(out_path, "w") as f:
        f.write(html)

    print(f"Generated: {out_path}")


if __name__ == "__main__":
    main()
