#!/usr/bin/env python3
"""
Generate HTML charts from Dimster run result JSON files.

Usage:
    python3 generate_run_charts.py --results <dir> --output <dir>

Scans --results dir recursively for JSON files where mode == "run",
generates one HTML file per result using Chart.js for visualization.
When availability data is present, availability charts are included
in the same HTML file alongside performance charts.
"""

import argparse
import html
import json
import math
import os
import sys
from collections import defaultdict


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


def build_histogram_buckets(values, max_buckets=20):
    """Bucket values into a histogram. Returns (labels, counts).

    Uses 1ms minimum bucket width — if the value range is smaller than
    max_buckets, fewer buckets are produced.
    """
    if not values:
        return [], []
    min_val = math.floor(min(values))
    max_val = math.ceil(max(values))
    if min_val == max_val:
        return [f"{min_val}"], [len(values)]
    span = max_val - min_val
    num_buckets = min(max_buckets, span)
    bucket_width = span / num_buckets
    counts = [0] * num_buckets
    for v in values:
        idx = int((v - min_val) / bucket_width)
        if idx >= num_buckets:
            idx = num_buckets - 1
        counts[idx] += 1
    labels = []
    for i in range(num_buckets):
        lo = min_val + i * bucket_width
        hi = lo + bucket_width
        if bucket_width >= 1:
            labels.append(f"{lo:.0f}-{hi:.0f}")
        else:
            labels.append(f"{lo:.1f}-{hi:.1f}")
    return labels, counts


def is_result_of_mode(data, mode):
    """True if data is a result file of the given mode.

    Run requests also carry ``mode`` and must be excluded, so the typed
    ``fileType`` is checked too. Older result files predate ``fileType``,
    so an absent type remains valid.
    """
    return data.get("mode") == mode and data.get("fileType") in (None, "dimster-result")


def find_run_json_files(results_dir):
    """Recursively find run results, excluding typed helper artifacts."""
    found = []
    for root, _dirs, files in os.walk(results_dir):
        for f in files:
            if not f.endswith(".json"):
                continue
            path = os.path.join(root, f)
            try:
                with open(path, "r") as fh:
                    data = json.load(fh)
                if is_result_of_mode(data, "run"):
                    found.append((path, data))
            except json.JSONDecodeError as e:
                print(f"Warning: skipping {path}: invalid JSON: {e}", file=sys.stderr)
            except IOError:
                continue
    return found


def has_availability_data(data):
    """Check if any test in the result has availability data."""
    for scenario in data.get("scenarioResults", []):
        for test in scenario.get("tests", []):
            if test.get("availability"):
                return True
    return False


def has_per_partition_data(data):
    """Check if any test result includes per-partition metrics.

    Drives the decision to emit a sibling per-partition HTML. The flag that
    governs emission of these results is engineConfig.perPartitionMetrics
    (in the workload YAML), which produces the perPartitionResults list here.
    """
    for scenario in data.get("scenarioResults", []):
        for test in scenario.get("tests", []):
            if test.get("results", {}).get("perPartitionResults"):
                return True
    return False


def filter_for_mode(chart_configs, chart_sections, mode):
    """Return (configs, sections) limited to the chart sections relevant to mode.

    The two output HTML files share the same build_chart_configs() output but
    each renders only a subset:
      - "main": aggregate + availability sections
      - "per_partition": per-partition section only

    Chart IDs (e.g. "chart-0-7") are kept as-is rather than reindexed so the
    canvases in build_chart_grid line up with the entries in the chartConfigs
    JSON without any mapping table. Configs not in the kept ranges are dropped
    so the JSON payload in the HTML stays small.
    """
    if mode == "main":
        keep_keys = ("aggregate", "availability")
    elif mode == "per_partition":
        keep_keys = ("per_partition",)
    else:
        raise ValueError(f"unknown mode: {mode}")

    keep_ids = set()
    filtered_sections = {}
    for s_idx, ranges in chart_sections.items():
        new_ranges = {}
        for key in keep_keys:
            if key in ranges:
                start, end = ranges[key]
                new_ranges[key] = (start, end)
                for c_idx in range(start, end):
                    keep_ids.add(f"chart-{s_idx}-{c_idx}")
        # per_partition_tests is metadata describing the per-test grids inside
        # the per_partition range — only meaningful when the per-partition
        # section is included in this output.
        if mode == "per_partition" and "per_partition_tests" in ranges:
            new_ranges["per_partition_tests"] = ranges["per_partition_tests"]
        filtered_sections[s_idx] = new_ranges

    filtered_configs = [c for c in chart_configs if c["id"] in keep_ids]
    return filtered_configs, filtered_sections


def avg(lst):
    if not lst:
        return 0
    return sum(lst) / len(lst)


def avg_mbs(test, direction):
    """Average throughput of a test point in MB/s, for "publish" or "consume".

    Prefers the measured byte rates (publishBytesRate / consumeBytesRate),
    which are exact when record sizes vary, as with generated datasets, and
    falls back to rate x messageSize for results that predate them.
    """
    results = test.get("results", {})
    measured = results.get(f"{direction}BytesRate")
    if measured:
        return round(avg(measured), 2)
    msg_size = test.get("workloadConfig", {}).get("messageSize", 1000)
    return round(avg(results.get(f"{direction}Rate", [])) * msg_size / 1_000_000, 2)


def message_size_cell(workload_config):
    """messageSize for config tables; with a dataset it is an estimated average.

    The default payload dataset (format RAW) has exact sizes, so it shows as
    a plain size.
    """
    size = workload_config.get("messageSize", "")
    fmt = workload_config.get("datasetFormat")
    if fmt and fmt != "RAW":
        return f'~{size} ({workload_config.get("datasetPreset", "custom")}/{fmt})'
    return size


def dict_or_empty(value):
    """Return dict values as-is and coerce JSON null/missing values to {}.

    Interrupted runs and runs without a separate kafka config can serialize
    top-level config blocks such as baseKafkaConfig as null. The chart renderer
    only reads optional fields from these blocks, so an empty dict is the
    correct tolerant representation.
    """
    return value if isinstance(value, dict) else {}


def text_or_empty(value):
    """Return a display-safe metadata value.

    JSON null should render as a blank cell in chart metadata, not as the
    Python string "None".
    """
    return "" if value is None else value


# Result metadata fields Dimster itself writes, so overview panels list only
# user metadata as "Metadata <key>". "hooks" (hook executions) is long and
# its configuration already appears in the config section.
BUILTIN_METADATA_KEYS = {
    "dimsterVersion",
    "name",
    "kafkaConfigName",
    "environment",
    "system",
    "hardware",
    "beginTime",
    "endTime",
    "extraDimensions",
    "hooks",
}


def metadata_annotation_html(metadata):
    """Render user metadata and extraDimensions for overview panels."""
    items = []
    for key in sorted(k for k in metadata.keys() if k not in BUILTIN_METADATA_KEYS):
        items.append(meta_item(f"Metadata {key}", metadata.get(key)))
    dimensions = metadata.get("extraDimensions")
    if isinstance(dimensions, dict):
        for key in sorted(dimensions.keys()):
            items.append(meta_item(f"Dimension {key}", dimensions.get(key)))
    return "\n".join(items)


def meta_item(label, value):
    return (
        f'<div class="meta-item"><span class="label">{html.escape(label)}:</span> '
        f'<span class="value">{html.escape(format_meta_value(value))}</span></div>'
    )


def format_meta_value(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def time_labels(arr_len, sample_rate_ms):
    """Generate time labels in seconds."""
    step = sample_rate_ms / 1000
    return [round(i * step) for i in range(arr_len)]


# End-to-end latency split at the Flink job, present only in results of test
# points with a Flink stage: (result field name, chart title).
FLINK_LATENCY_SEGMENTS = [
    ("ProducerToFlinkLatency", "Producer → Flink Latency"),
    ("FlinkToConsumerLatency", "Flink → Consumer Latency"),
]


def lower_first(name):
    return name[0].lower() + name[1:]


def build_chart_configs(data):
    """Build all chart configs for a single run result.

    Returns (chart_configs, chart_sections) where chart_sections maps
    scenario index to a dict with subsection chart index ranges:
      {"aggregate": (start, end), "per_partition": (start, end), "availability": (start, end)}
    """
    run_config = dict_or_empty(data.get("runConfig"))
    sample_rate_ms = run_config.get("sampleRateMillis", 10000)
    avail_config = dict_or_empty(data.get("availabilityConfig"))
    threshold_ms = avail_config.get("unavailabilityThresholdMs", 5000)
    scenarios = data.get("scenarioResults", [])

    chart_configs = []
    chart_sections = {}

    for s_idx, scenario in enumerate(scenarios):
        tests = scenario.get("tests", [])
        if not tests:
            continue

        labels = [t.get("label", f"test-{i}") for i, t in enumerate(tests)]
        chart_idx = 0

        # ═══════════════════════════════════════════════════════════════
        # Performance charts
        # ═══════════════════════════════════════════════════════════════

        # --- 1. Throughput (grouped bar) ---
        avg_pub = [round(avg(t["results"].get("publishRate", [])), 1) for t in tests]
        avg_con = [round(avg(t["results"].get("consumeRate", [])), 1) for t in tests]
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "bar",
                "data": {
                    "labels": labels,
                    "datasets": [
                        {"label": "Publish", "data": avg_pub, "backgroundColor": COLORS[0]},
                        {"label": "Consume", "data": avg_con, "backgroundColor": COLORS[1]},
                    ],
                },
                "options": bar_options("Throughput (msg/s)", "msg/s"),
            },
        })
        chart_idx += 1

        # --- 2. Throughput MB/s ---
        avg_pub_mb = [avg_mbs(t, "publish") for t in tests]
        avg_con_mb = [avg_mbs(t, "consume") for t in tests]

        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "bar",
                "data": {
                    "labels": labels,
                    "datasets": [
                        {"label": "Publish", "data": avg_pub_mb, "backgroundColor": COLORS[0]},
                        {"label": "Consume", "data": avg_con_mb, "backgroundColor": COLORS[1]},
                    ],
                },
                "options": bar_options("Throughput (MB/s)", "MB/s"),
            },
        })
        chart_idx += 1

        # --- 3. Aggregated Latency - Publish ---
        pctiles = ["p50", "p95", "p99", "p99.9", "max"]
        pctile_keys = [
            "aggregatedPublishLatency50pct",
            "aggregatedPublishLatency95pct",
            "aggregatedPublishLatency99pct",
            "aggregatedPublishLatency999pct",
            "aggregatedPublishLatencyMax",
        ]
        datasets_pub_lat = []
        for pi, (pname, pkey) in enumerate(zip(pctiles, pctile_keys)):
            datasets_pub_lat.append({
                "label": pname,
                "data": [t["results"].get(pkey, 0) for t in tests],
                "backgroundColor": COLORS[pi % len(COLORS)],
            })
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "bar",
                "data": {"labels": labels, "datasets": datasets_pub_lat},
                "options": bar_options("Aggregated Latency - Publish", "ms"),
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
        for pi, (pname, pkey) in enumerate(zip(pctiles, e2e_keys)):
            datasets_e2e_lat.append({
                "label": pname,
                "data": [t["results"].get(pkey, 0) for t in tests],
                "backgroundColor": COLORS[pi % len(COLORS)],
            })
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "bar",
                "data": {"labels": labels, "datasets": datasets_e2e_lat},
                "options": bar_options("Aggregated Latency - E2E", "ms"),
            },
        })
        chart_idx += 1

        # --- 4b. Aggregated Latency - the e2e split at the Flink job ---
        for field, title in FLINK_LATENCY_SEGMENTS:
            keys = [f"aggregated{field}{suffix}" for suffix in ("50pct", "95pct", "99pct", "999pct", "Max")]
            if not any(keys[2] in t["results"] for t in tests):
                continue
            datasets = [{
                "label": pname,
                "data": [t["results"].get(pkey, 0) for t in tests],
                "backgroundColor": COLORS[pi % len(COLORS)],
            } for pi, (pname, pkey) in enumerate(zip(pctiles, keys))]
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "config": {
                    "type": "bar",
                    "data": {"labels": labels, "datasets": datasets},
                    "options": bar_options(f"Aggregated {title}", "ms"),
                },
            })
            chart_idx += 1

        # --- 5. Publish Rate over time ---
        max_len = max((len(t["results"].get("publishRate", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_pub_ts = []
        for ti, t in enumerate(tests):
            datasets_pub_ts.append(line_dataset(
                labels[ti], t["results"].get("publishRate", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "zoomGroup": "time",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_pub_ts},
                "options": line_options("Publish Rate over Time", "msg/s", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 6. Consume Rate over time ---
        max_len = max((len(t["results"].get("consumeRate", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_con_ts = []
        for ti, t in enumerate(tests):
            datasets_con_ts.append(line_dataset(
                labels[ti], t["results"].get("consumeRate", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "zoomGroup": "time",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_con_ts},
                "options": line_options("Consume Rate over Time", "msg/s", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 7. Publish Latency p99 over time ---
        max_len = max((len(t["results"].get("publishLatency99pct", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_plat = []
        for ti, t in enumerate(tests):
            datasets_plat.append(line_dataset(
                labels[ti], t["results"].get("publishLatency99pct", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "zoomGroup": "time",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_plat},
                "options": line_options("Publish Latency p99 over Time", "ms", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 8. E2E Latency p99 over time ---
        max_len = max((len(t["results"].get("endToEndLatency99pct", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_elat = []
        for ti, t in enumerate(tests):
            datasets_elat.append(line_dataset(
                labels[ti], t["results"].get("endToEndLatency99pct", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "zoomGroup": "time",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_elat},
                "options": line_options("E2E Latency p99 over Time", "ms", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 8b. E2E Delivery Latency p99 over time ---
        # Only meaningful when consumers simulate processing time. The Java side gates the
        # recording so when no consumer has processing time the per-period values are all 0
        # — skip the chart in that case rather than render flat-zero lines.
        delivery_series = [t["results"].get("endToEndDeliveryLatency99pct", []) for t in tests]
        if any(any(v > 0 for v in s) for s in delivery_series):
            max_len = max((len(s) for s in delivery_series), default=0)
            x_labels = time_labels(max_len, sample_rate_ms)
            datasets_dlat = []
            for ti, t in enumerate(tests):
                datasets_dlat.append(line_dataset(
                    labels[ti], t["results"].get("endToEndDeliveryLatency99pct", []), ti
                ))
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "zoomGroup": "time",
                "config": {
                    "type": "line",
                    "data": {"labels": x_labels, "datasets": datasets_dlat},
                    "options": line_options("E2E Delivery Latency p99 over Time", "ms", "Time (s)"),
                },
            })
            chart_idx += 1

        # --- 8c. The e2e split at the Flink job, p99 over time ---
        for field, title in FLINK_LATENCY_SEGMENTS:
            key = f"{lower_first(field)}99pct"
            series = [t["results"].get(key, []) for t in tests]
            if not any(series):
                continue
            x_labels = time_labels(max(len(v) for v in series), sample_rate_ms)
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "zoomGroup": "time",
                "config": {
                    "type": "line",
                    "data": {"labels": x_labels,
                             "datasets": [line_dataset(labels[ti], v, ti) for ti, v in enumerate(series)]},
                    "options": line_options(f"{title} p99 over Time", "ms", "Time (s)"),
                },
            })
            chart_idx += 1

        # --- 9. Publish Delay p99 over time ---
        max_len = max((len(t["results"].get("publishDelayLatency99pct", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_pdlat = []
        for ti, t in enumerate(tests):
            datasets_pdlat.append(line_dataset(
                labels[ti], t["results"].get("publishDelayLatency99pct", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "zoomGroup": "time",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_pdlat},
                "options": line_options("Publish Delay p99 over Time", "ms", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 10. Publish Latency Percentile Distribution ---
        pub_quantile_datasets = []
        for ti, t in enumerate(tests):
            quantiles = t["results"].get("aggregatedPublishLatencyQuantiles", {})
            if quantiles:
                points = percentile_points(quantiles)
                pub_quantile_datasets.append(percentile_dataset(labels[ti], points, ti))
        if pub_quantile_datasets:
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "zoomGroup": "percentile",
                "config": {
                    "type": "scatter",
                    "data": {"datasets": pub_quantile_datasets},
                    "options": percentile_options("Publish Latency Percentile Distribution"),
                },
            })
            chart_idx += 1

        # --- 11. E2E Latency Percentile Distribution ---
        e2e_quantile_datasets = []
        for ti, t in enumerate(tests):
            quantiles = t["results"].get("aggregatedEndToEndLatencyQuantiles", {})
            if quantiles:
                points = percentile_points(quantiles)
                e2e_quantile_datasets.append(percentile_dataset(labels[ti], points, ti))
        if e2e_quantile_datasets:
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "zoomGroup": "percentile",
                "config": {
                    "type": "scatter",
                    "data": {"datasets": e2e_quantile_datasets},
                    "options": percentile_options("E2E Latency Percentile Distribution"),
                },
            })
            chart_idx += 1

        # --- 11b. E2E Delivery Latency Percentile Distribution ---
        # Quantile map is only populated when delivery-latency recording is enabled
        # (i.e. when some consumer simulates processing time), so the empty-check
        # alone is enough to suppress the chart on no-processing workloads.
        delivery_quantile_datasets = []
        for ti, t in enumerate(tests):
            quantiles = t["results"].get("aggregatedEndToEndDeliveryLatencyQuantiles", {})
            if quantiles:
                points = percentile_points(quantiles)
                delivery_quantile_datasets.append(percentile_dataset(labels[ti], points, ti))
        if delivery_quantile_datasets:
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "zoomGroup": "percentile",
                "config": {
                    "type": "scatter",
                    "data": {"datasets": delivery_quantile_datasets},
                    "options": percentile_options("E2E Delivery Latency Percentile Distribution"),
                },
            })
            chart_idx += 1

        # --- 11c. The e2e split at the Flink job, percentile distributions ---
        for field, title in FLINK_LATENCY_SEGMENTS:
            datasets = []
            for ti, t in enumerate(tests):
                quantiles = t["results"].get(f"aggregated{field}Quantiles", {})
                if quantiles:
                    datasets.append(percentile_dataset(labels[ti], percentile_points(quantiles), ti))
            if datasets:
                chart_configs.append({
                    "id": f"chart-{s_idx}-{chart_idx}",
                    "zoomGroup": "percentile",
                    "config": {
                        "type": "scatter",
                        "data": {"datasets": datasets},
                        "options": percentile_options(f"{title} Percentile Distribution"),
                    },
                })
                chart_idx += 1

        aggregate_end = chart_idx

        # ═══════════════════════════════════════════════════════════════
        # Per-partition charts (per test, only if perPartitionResults exists)
        # ═══════════════════════════════════════════════════════════════

        pp_test_ranges = []  # [(label, start, end), ...]

        for t_idx, test in enumerate(tests):
            pp = test["results"].get("perPartitionResults")
            if not pp:
                continue

            test_label = labels[t_idx]
            pp_test_start = chart_idx

            # --- Publish Latency: Distribution Histograms (p50, p95, p99, p99.9) ---
            for pctile_name, pctile_key, ci in [
                ("p50", "publishLatency50pct", 0),
                ("p95", "publishLatency95pct", 1),
                ("p99", "publishLatency99pct", 2),
                ("p99.9", "publishLatency999pct", 3),
            ]:
                values = [pr.get(pctile_key, 0) for pr in pp]
                h_labels, h_counts = build_histogram_buckets(values)
                chart_configs.append({
                    "id": f"chart-{s_idx}-{chart_idx}",
                    "config": {
                        "type": "bar",
                        "data": {
                            "labels": h_labels,
                            "datasets": [{
                                "label": "Partitions",
                                "data": h_counts,
                                "backgroundColor": COLORS[ci % len(COLORS)],
                            }],
                        },
                        "options": bar_options(
                            f"Publish Latency {pctile_name} Distribution — {test_label}",
                            "Number of partitions"),
                    },
                })
                chart_idx += 1

            # --- E2E Latency: Distribution Histograms (p50, p95, p99, p99.9) ---
            for pctile_name, pctile_key, ci in [
                ("p50", "endToEndLatency50pct", 0),
                ("p95", "endToEndLatency95pct", 1),
                ("p99", "endToEndLatency99pct", 2),
                ("p99.9", "endToEndLatency999pct", 3),
            ]:
                values = [pr.get(pctile_key, 0) for pr in pp]
                h_labels, h_counts = build_histogram_buckets(values)
                chart_configs.append({
                    "id": f"chart-{s_idx}-{chart_idx}",
                    "config": {
                        "type": "bar",
                        "data": {
                            "labels": h_labels,
                            "datasets": [{
                                "label": "Partitions",
                                "data": h_counts,
                                "backgroundColor": COLORS[ci % len(COLORS)],
                            }],
                        },
                        "options": bar_options(
                            f"E2E Latency {pctile_name} Distribution — {test_label}",
                            "Number of partitions"),
                    },
                })
                chart_idx += 1

            # --- Throughput: Distribution Histogram ---
            sent_values = [pr.get("messagesSent", 0) for pr in pp]
            hist_labels, hist_counts = build_histogram_buckets(sent_values)
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "config": {
                    "type": "bar",
                    "data": {
                        "labels": hist_labels,
                        "datasets": [{
                            "label": "Partitions",
                            "data": hist_counts,
                            "backgroundColor": COLORS[3],
                        }],
                    },
                    "options": bar_options(
                        f"Messages Sent Distribution — {test_label}",
                        "Number of partitions"),
                },
            })
            chart_idx += 1

            # --- Top 5 Worst Publish Latency ---
            top5_pub = sorted(pp, key=lambda pr: pr.get("publishLatency99pct", 0),
                              reverse=True)[:5]
            top5_pub_labels = [pr["key"] for pr in top5_pub]
            top5_pub_datasets = []
            for pctile_name, pctile_key, ci in [
                ("p50", "publishLatency50pct", 0),
                ("p95", "publishLatency95pct", 1),
                ("p99", "publishLatency99pct", 2),
                ("p99.9", "publishLatency999pct", 3),
            ]:
                top5_pub_datasets.append({
                    "label": pctile_name,
                    "data": [pr.get(pctile_key, 0) for pr in top5_pub],
                    "backgroundColor": COLORS[ci % len(COLORS)],
                })
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "config": {
                    "type": "bar",
                    "data": {"labels": top5_pub_labels, "datasets": top5_pub_datasets},
                    "options": bar_options(
                        f"Top 5 Worst Publish Latency — {test_label}", "ms"),
                },
            })
            chart_idx += 1

            # --- Top 5 Worst E2E Latency ---
            top5_e2e = sorted(pp, key=lambda pr: pr.get("endToEndLatency99pct", 0),
                              reverse=True)[:5]
            top5_e2e_labels = [pr["key"] for pr in top5_e2e]
            top5_e2e_datasets = []
            for pctile_name, pctile_key, ci in [
                ("p50", "endToEndLatency50pct", 0),
                ("p95", "endToEndLatency95pct", 1),
                ("p99", "endToEndLatency99pct", 2),
                ("p99.9", "endToEndLatency999pct", 3),
            ]:
                top5_e2e_datasets.append({
                    "label": pctile_name,
                    "data": [pr.get(pctile_key, 0) for pr in top5_e2e],
                    "backgroundColor": COLORS[ci % len(COLORS)],
                })
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "config": {
                    "type": "bar",
                    "data": {"labels": top5_e2e_labels, "datasets": top5_e2e_datasets},
                    "options": bar_options(
                        f"Top 5 Worst E2E Latency — {test_label}", "ms"),
                },
            })
            chart_idx += 1

            # --- Bottom 5 Throughput ---
            top5_low = sorted(pp, key=lambda pr: pr.get("messagesSent", 0))[:5]
            top5_low_labels = [pr["key"] for pr in top5_low]
            top5_low_sent = [pr.get("messagesSent", 0) for pr in top5_low]
            top5_low_recv = [pr.get("messagesReceived", 0) for pr in top5_low]
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "config": {
                    "type": "bar",
                    "data": {
                        "labels": top5_low_labels,
                        "datasets": [
                            {"label": "Sent", "data": top5_low_sent, "backgroundColor": COLORS[0]},
                            {"label": "Received", "data": top5_low_recv, "backgroundColor": COLORS[1]},
                        ],
                    },
                    "options": bar_options(
                        f"Bottom 5 Throughput — {test_label}", "messages"),
                },
            })
            chart_idx += 1

            pp_test_ranges.append((test_label, pp_test_start, chart_idx))

        per_partition_end = chart_idx

        # ═══════════════════════════════════════════════════════════════
        # Availability charts (per test, only if availability data exists)
        # ═══════════════════════════════════════════════════════════════

        for t_idx, test in enumerate(tests):
            avail = test.get("availability")
            if not avail:
                continue

            partition_results = avail.get("partitionResults", [])
            if not partition_results:
                continue

            test_label = test.get("label", f"test-{t_idx}")

            # Collect time series timestamps for relative time alignment
            all_ts = []
            for pr in partition_results:
                for dp in pr.get("timeSeries", []):
                    all_ts.append(dp["timestampMs"])
            if not all_ts:
                continue
            t0 = min(all_ts)

            # --- Probe Produce Duration over Time ---
            datasets_produce = []
            for pi, pr in enumerate(partition_results):
                ts = pr.get("timeSeries", [])
                points = [{"x": round((dp["timestampMs"] - t0) / 1000, 1),
                           "y": dp["produceDurationMs"]}
                          for dp in ts if dp["produceDurationMs"] >= 0]
                datasets_produce.append({
                    "label": f"p{pr['partition']}",
                    "data": points,
                    "borderColor": COLORS[pi % len(COLORS)],
                    "backgroundColor": COLORS_ALPHA[pi % len(COLORS_ALPHA)],
                    "borderWidth": 1.5,
                    "pointRadius": 1.5,
                    "pointHoverRadius": 4,
                    "tension": 0,
                    "fill": False,
                    "showLine": True,
                })
            if points:
                max_x = max(dp["x"] for ds in datasets_produce for dp in ds["data"])
                datasets_produce.append({
                    "label": f"threshold ({threshold_ms}ms)",
                    "data": [{"x": 0, "y": threshold_ms}, {"x": max_x, "y": threshold_ms}],
                    "borderColor": "#ff0000",
                    "borderWidth": 1.5,
                    "borderDash": [6, 4],
                    "pointRadius": 0,
                    "fill": False,
                    "showLine": True,
                })
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "config": {
                    "type": "scatter",
                    "data": {"datasets": datasets_produce},
                    "options": scatter_time_options(
                        f"Produce Duration — {test_label}", "ms", "Time (s)"),
                },
            })
            chart_idx += 1

            # --- E2E Latency over Time ---
            datasets_e2e_avail = []
            for pi, pr in enumerate(partition_results):
                ts = pr.get("timeSeries", [])
                points = [{"x": round((dp["timestampMs"] - t0) / 1000, 1),
                           "y": dp["e2eLatencyMs"]}
                          for dp in ts if dp["e2eLatencyMs"] >= 0]
                datasets_e2e_avail.append({
                    "label": f"p{pr['partition']}",
                    "data": points,
                    "borderColor": COLORS[pi % len(COLORS)],
                    "backgroundColor": COLORS_ALPHA[pi % len(COLORS_ALPHA)],
                    "borderWidth": 1.5,
                    "pointRadius": 1.5,
                    "pointHoverRadius": 4,
                    "tension": 0,
                    "fill": False,
                    "showLine": True,
                })
            if points:
                max_x = max(dp["x"] for ds in datasets_e2e_avail for dp in ds["data"])
                datasets_e2e_avail.append({
                    "label": f"threshold ({threshold_ms}ms)",
                    "data": [{"x": 0, "y": threshold_ms}, {"x": max_x, "y": threshold_ms}],
                    "borderColor": "#ff0000",
                    "borderWidth": 1.5,
                    "borderDash": [6, 4],
                    "pointRadius": 0,
                    "fill": False,
                    "showLine": True,
                })
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "config": {
                    "type": "scatter",
                    "data": {"datasets": datasets_e2e_avail},
                    "options": scatter_time_options(
                        f"E2E Latency — {test_label}", "ms", "Time (s)"),
                },
            })
            chart_idx += 1

            # --- Per-partition Availability (grouped bar) ---
            partitions_labels = [f"p{pr['partition']}" for pr in partition_results]
            producer_avail = [round(pr["producerAvailabilityPct"], 2) for pr in partition_results]
            e2e_avail_vals = [round(pr["e2eAvailabilityPct"], 2) for pr in partition_results]
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "config": {
                    "type": "bar",
                    "data": {
                        "labels": partitions_labels,
                        "datasets": [
                            {"label": "Producer", "data": producer_avail, "backgroundColor": COLORS[0]},
                            {"label": "Consumer (E2E)", "data": e2e_avail_vals, "backgroundColor": COLORS[1]},
                        ],
                    },
                    "options": bar_options(f"Availability by Partition — {test_label}", "%",
                                           y_max=100),
                },
            })
            chart_idx += 1

            # --- Per-partition Unavailability Duration (grouped bar) ---
            producer_unavail = [pr["producerUnavailableMs"] for pr in partition_results]
            e2e_unavail = [pr["e2eUnavailableMs"] for pr in partition_results]
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "config": {
                    "type": "bar",
                    "data": {
                        "labels": partitions_labels,
                        "datasets": [
                            {"label": "Producer", "data": producer_unavail, "backgroundColor": COLORS[0]},
                            {"label": "Consumer (E2E)", "data": e2e_unavail, "backgroundColor": COLORS[1]},
                        ],
                    },
                    "options": bar_options(f"Unavailability Duration by Partition — {test_label}", "ms"),
                },
            })
            chart_idx += 1

            # --- Unavailability Events Timeline (horizontal bar / Gantt-like) ---
            events = []
            for pr in partition_results:
                for ev in pr.get("unavailabilityEvents", []):
                    events.append(ev)
            if events:
                event_t0 = min(e["startTimestampMs"] for e in events)
                partition_ids = sorted(set(e["partition"] for e in events))
                y_labels = [f"p{p}" for p in partition_ids]
                partition_to_idx = {p: i for i, p in enumerate(partition_ids)}
                num_partitions = len(partition_ids)

                events_by_pt = defaultdict(list)
                for ev in events:
                    events_by_pt[(ev["partition"], ev["type"])].append(ev)

                max_producer = max((len(events_by_pt.get((p, "PRODUCER"), []))
                                    for p in partition_ids), default=0)
                max_e2e = max((len(events_by_pt.get((p, "E2E"), []))
                               for p in partition_ids), default=0)

                datasets = []
                for occ in range(max_producer):
                    bar_data = [None] * num_partitions
                    for p in partition_ids:
                        evts = events_by_pt.get((p, "PRODUCER"), [])
                        if occ < len(evts):
                            e = evts[occ]
                            bar_data[partition_to_idx[p]] = [
                                round((e["startTimestampMs"] - event_t0) / 1000, 1),
                                round((e["endTimestampMs"] - event_t0) / 1000, 1)]
                    datasets.append({
                        "label": "Producer",
                        "data": bar_data,
                        "backgroundColor": "rgba(78, 121, 167, 0.7)",
                        "borderColor": COLORS[0],
                        "borderWidth": 1,
                    })

                for occ in range(max_e2e):
                    bar_data = [None] * num_partitions
                    for p in partition_ids:
                        evts = events_by_pt.get((p, "E2E"), [])
                        if occ < len(evts):
                            e = evts[occ]
                            bar_data[partition_to_idx[p]] = [
                                round((e["startTimestampMs"] - event_t0) / 1000, 1),
                                round((e["endTimestampMs"] - event_t0) / 1000, 1)]
                    datasets.append({
                        "label": "Consumer (E2E)",
                        "data": bar_data,
                        "backgroundColor": "rgba(225, 87, 89, 0.7)",
                        "borderColor": COLORS[1],
                        "borderWidth": 1,
                    })

                chart_configs.append({
                    "id": f"chart-{s_idx}-{chart_idx}",
                    "config": {
                        "type": "bar",
                        "data": {
                            "labels": y_labels,
                            "datasets": datasets,
                        },
                        "options": floating_bar_options(
                            f"Unavailability Events — {test_label}", "Time (s)"),
                    },
                })
                chart_idx += 1

        chart_sections[s_idx] = {
            "aggregate": (0, aggregate_end),
            "per_partition": (aggregate_end, per_partition_end),
            "per_partition_tests": pp_test_ranges,
            "availability": (per_partition_end, chart_idx),
        }

    return chart_configs, chart_sections


def bar_options(title, y_label, y_max=None):
    opts = {
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
    if y_max is not None:
        opts["scales"]["y"]["max"] = y_max
    return opts


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


def scatter_time_options(title, y_label, x_label):
    return {
        "responsive": True,
        "maintainAspectRatio": True,
        "plugins": {
            "title": {"display": True, "text": title, "font": {"size": 14, "weight": "600"}},
            "legend": {"position": "top", "labels": {"usePointStyle": True, "padding": 16}},
            "tooltip": {"enabled": True, "mode": "nearest", "intersect": False},
        },
        "scales": {
            "y": {
                "beginAtZero": True,
                "title": {"display": True, "text": y_label},
                "grid": {"color": "#e0e0e0"},
            },
            "x": {
                "type": "linear",
                "title": {"display": True, "text": x_label},
                "grid": {"color": "#f0f0f0"},
            },
        },
        "interaction": {"mode": "nearest", "axis": "x", "intersect": False},
    }


def floating_bar_options(title, x_label):
    return {
        "indexAxis": "y",
        "responsive": True,
        "maintainAspectRatio": True,
        "__dedup_legend": True,
        "plugins": {
            "title": {"display": True, "text": title, "font": {"size": 14, "weight": "600"}},
            "legend": {"position": "top", "labels": {"usePointStyle": True, "padding": 16}},
            "tooltip": {"enabled": True},
        },
        "scales": {
            "x": {
                "title": {"display": True, "text": x_label},
                "grid": {"color": "#e0e0e0"},
            },
            "y": {
                "grid": {"display": False},
            },
        },
    }


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


def percentile_to_x(p):
    """Transform percentile to log-scale x value for percentile charts."""
    if p >= 100.0:
        return 5.0
    return math.log10(100.0 / (100.0 - p))


def percentile_points(quantiles_map):
    """Convert a {percentile: latency} map to [{x, y}] scatter points."""
    points = []
    for pct_str, latency in sorted(quantiles_map.items(), key=lambda kv: float(kv[0])):
        pct = float(pct_str)
        if pct > 100.0:
            continue
        points.append({"x": round(percentile_to_x(pct), 4), "y": latency})
    return points


def percentile_dataset(label, points, color_idx):
    c = COLORS[color_idx % len(COLORS)]
    return {
        "label": label,
        "data": points,
        "borderColor": c,
        "backgroundColor": c,
        "borderWidth": 1.5,
        "pointRadius": 0,
        "pointHoverRadius": 5,
        "showLine": True,
        "tension": 0.3,
        "fill": False,
    }


# Tick positions and labels for the percentile x-axis
PERCENTILE_TICKS = [
    (50, "p50"), (75, "p75"), (90, "p90"), (95, "p95"),
    (99, "p99"), (99.9, "p99.9"), (99.99, "p99.99"), (100, "max"),
]
PERCENTILE_TICK_VALUES = [round(percentile_to_x(p), 4) for p, _ in PERCENTILE_TICKS]
PERCENTILE_TICK_LABELS = [label for _, label in PERCENTILE_TICKS]


def percentile_options(title):
    return {
        "responsive": True,
        "maintainAspectRatio": True,
        "plugins": {
            "title": {"display": True, "text": title, "font": {"size": 14, "weight": "600"}},
            "legend": {"position": "top", "labels": {"usePointStyle": True, "padding": 16}},
            "tooltip": {
                "enabled": True,
                "callbacks": {
                    "__percentile_tooltip": True,
                },
            },
        },
        "scales": {
            "x": {
                "type": "linear",
                "title": {"display": True, "text": "Percentile"},
                "min": PERCENTILE_TICK_VALUES[0],
                "max": PERCENTILE_TICK_VALUES[-1],
                "ticks": {
                    "__percentile_ticks": True,
                },
                "grid": {"color": "#f0f0f0"},
            },
            "y": {
                "beginAtZero": True,
                "title": {"display": True, "text": "Latency (ms)"},
                "grid": {"color": "#e0e0e0"},
            },
        },
        "interaction": {"mode": "nearest", "intersect": False},
    }


def build_sidebar_html(scenarios, has_avail, chart_sections, mode="main", sibling_link=None):
    """Build the sidebar nav. Layout differs by mode:
      - "main": Configuration, Availability Summary, per-scenario Aggregate +
        Availability subsections, plus a top-level link to the per-partition
        sibling HTML when sibling_link is provided.
      - "per_partition": back-link to the main HTML, Configuration, then
        per-scenario Partition-Level Performance with one sublink per test.
    """
    items = []
    if mode == "per_partition" and sibling_link:
        items.append(f'<a class="nav-link nav-back" href="{sibling_link}">← Main results</a>')
    items.append('<div class="sidebar-label">Overview</div>')
    items.append('<a class="nav-link" href="#config-section">Configuration</a>')
    if mode == "main" and has_avail:
        items.append('<a class="nav-link" href="#availability-summary">Availability Summary</a>')
    if mode == "main" and sibling_link:
        items.append(
            f'<a class="nav-link nav-external" href="{sibling_link}">Per-partition charts ↗</a>'
        )
    items.append('<div class="sidebar-label">Scenarios</div>')
    for i, s in enumerate(scenarios):
        name = s.get("name", f"Scenario {i + 1}")
        items.append(
            f'<a class="nav-link" href="#scenario-{i}">{name}</a>'
        )
        if mode == "main":
            items.append(f'<a class="nav-sublink" href="#scenario-{i}-aggregate">Aggregate Performance</a>')
            items.append(f'<a class="nav-sublink" href="#scenario-{i}-availability">Availability</a>')
        else:  # per_partition
            items.append(f'<a class="nav-sublink" href="#scenario-{i}-partition">Partition-Level</a>')
            pp_test_ranges = chart_sections.get(i, {}).get("per_partition_tests", [])
            for t_label, _, _ in pp_test_ranges:
                items.append(f'<a class="nav-sublink2" href="#scenario-{i}-partition-{t_label}">{t_label}</a>')
    return "\n".join(items)


def strip_heavy_data(data):
    """Return a copy with high-volume result and diagnostic arrays removed."""
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
                        if tk in ("results", "performance"):
                            continue
                        if tk == "correctness":
                            stripped_test[tk] = {
                                k: v
                                for k, v in tv.items()
                                if k not in (
                                    "violations",
                                    "lostMessageRanges",
                                    "lostMessagesOverTime",
                                    "duplicateCountOverTime",
                                    "outOfOrderCountOverTime",
                                    "crcFailureCountOverTime",
                                )
                            }
                            continue
                        if tk == "availability":
                            stripped_avail = dict(tv)
                            stripped_prs = []
                            for pr in tv.get("partitionResults", []):
                                stripped_pr = {k: v for k, v in pr.items() if k != "timeSeries"}
                                stripped_prs.append(stripped_pr)
                            stripped_avail["partitionResults"] = stripped_prs
                            stripped_test[tk] = stripped_avail
                        else:
                            stripped_test[tk] = tv
                    stripped_tests.append(stripped_test)
                stripped_scenario["tests"] = stripped_tests
                stripped_scenarios.append(stripped_scenario)
            stripped["scenarioConfigurations"] = stripped_scenarios
        else:
            stripped[key] = value
    return stripped


def build_config_section(data):
    """Build the Configuration section showing the result JSON without heavy data."""
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


def build_availability_summary(data):
    """Build a summary table showing overall availability per test."""
    scenarios = data.get("scenarioResults", [])
    rows = []
    for scenario in scenarios:
        for test in scenario.get("tests", []):
            avail = test.get("availability")
            if not avail:
                continue
            label = test.get("label", "")
            partitions = avail.get("partitions", 0)
            wall_clock_s = round(avail.get("wallClockMs", 0) / 1000, 1)
            rows.append(
                f'<tr>'
                f'<td>{label}</td>'
                f'<td>{partitions}</td>'
                f'<td>{wall_clock_s}s</td>'
                f'<td>{avail.get("producerAvailabilityPct", 100):.2f}%</td>'
                f'<td>{avail.get("producerUnavailableMs", 0):,}ms</td>'
                f'<td>{avail.get("e2eAvailabilityPct", 100):.2f}%</td>'
                f'<td>{avail.get("e2eUnavailableMs", 0):,}ms</td>'
                f'<td>{avail.get("overallAvailabilityPct", 100):.2f}%</td>'
                f'<td>{avail.get("totalUnavailableMs", 0):,}ms</td>'
                f'</tr>'
            )

    if not rows:
        return ""

    return f"""
    <section id="availability-summary" class="scenario-section">
        <h2>Availability Summary</h2>
        <div class="config-table-wrap">
            <table class="config-table">
                <thead><tr>
                    <th>Test</th><th>Partitions</th><th>Wall Clock</th>
                    <th>Producer Avail</th><th>Producer Unavail</th>
                    <th>Consumer Avail</th><th>Consumer Unavail</th>
                    <th>Overall Avail</th><th>Overall Unavail</th>
                </tr></thead>
                <tbody>{"".join(rows)}</tbody>
            </table>
        </div>
    </section>
    """


def build_chart_grid(s_idx, start, end):
    """Build a chart grid for chart indices [start, end)."""
    cards = []
    for c_idx in range(start, end):
        chart_id = f"chart-{s_idx}-{c_idx}"
        cards.append(
            f'<div class="chart-card"><canvas id="{chart_id}"></canvas></div>'
        )
    return f'<div class="chart-grid">{"".join(cards)}</div>'


def build_scenario_sections(scenarios, chart_sections, mode="main", per_partition_link=None):
    """Build scenario-level HTML. Layout differs by mode:
      - "main": summary tables + Aggregate Performance + Availability subsections.
        The Partition-Level subsection becomes a link card pointing at the
        sibling per-partition HTML when per_partition_link is provided, or a
        "Not configured" note otherwise.
      - "per_partition": only the Partition-Level Performance subsection — the
        summary tables and aggregate/availability charts live in the main HTML.
    """
    sections = []
    for s_idx, scenario in enumerate(scenarios):
        name = scenario.get("name", f"Scenario {s_idx + 1}")
        tests = scenario.get("tests", [])
        ranges = chart_sections.get(s_idx, {})

        if mode == "per_partition":
            sections.append(_build_per_partition_scenario_html(s_idx, name, ranges))
            continue

        # Test summary table
        table_rows = []
        for t in tests:
            wc = t.get("workloadConfig", {})
            table_rows.append(
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

        table_html = ""
        if table_rows:
            table_html = f"""
            <div class="config-table-wrap">
                <table class="config-table">
                    <thead><tr>
                        <th>Label</th><th>Topics</th><th>Partitions</th>
                        <th>Msg Size</th><th>Producers</th><th>Rate</th>
                        <th>Consumer Groups</th><th>Consumers/Group</th><th>Key Dist</th><th>Consumer Type</th>
                    </tr></thead>
                    <tbody>{"".join(table_rows)}</tbody>
                </table>
            </div>
            """

        # Performance summary table
        perf_rows = []
        for t in tests:
            r = t.get("results", {})
            pub_rate = avg(r.get("publishRate", []))
            con_rate = avg(r.get("consumeRate", []))
            pub_mbs = avg_mbs(t, "publish")
            con_mbs = avg_mbs(t, "consume")

            def fmt_lat(val):
                if val is None or val == 0:
                    return "—"
                return f"{val:.2f}"

            def fmt_rate(val):
                return f"{val:,.0f}"

            def fmt_mbs(val):
                return f"{val:,.1f}"

            perf_rows.append(
                f'<tr>'
                f'<td>{t.get("label", "")}</td>'
                f'<td class="num">{fmt_rate(pub_rate)}</td>'
                f'<td class="num">{fmt_mbs(pub_mbs)}</td>'
                f'<td class="num">{fmt_rate(con_rate)}</td>'
                f'<td class="num">{fmt_mbs(con_mbs)}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishLatency50pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishLatency95pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishLatency99pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishLatency999pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishLatencyMax"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedEndToEndLatency50pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedEndToEndLatency95pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedEndToEndLatency99pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedEndToEndLatency999pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedEndToEndLatencyMax"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishDelayLatency50pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishDelayLatency95pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishDelayLatency99pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishDelayLatency999pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishDelayLatencyMax"))}</td>'
                f'</tr>'
            )

        perf_table_html = ""
        if perf_rows:
            perf_table_html = f"""
            <div class="config-table-wrap">
                <table class="config-table perf-table">
                    <thead>
                        <tr>
                            <th rowspan="2">Label</th>
                            <th colspan="2">Publish Rate</th>
                            <th colspan="2">Consume Rate</th>
                            <th colspan="5">Publish Latency (ms)</th>
                            <th colspan="5">E2E Latency (ms)</th>
                            <th colspan="5">Publish Delay (ms)</th>
                        </tr>
                        <tr>
                            <th>msg/s</th><th>MB/s</th>
                            <th>msg/s</th><th>MB/s</th>
                            <th>p50</th><th>p95</th><th>p99</th><th>p99.9</th><th>max</th>
                            <th>p50</th><th>p95</th><th>p99</th><th>p99.9</th><th>max</th>
                            <th>p50</th><th>p95</th><th>p99</th><th>p99.9</th><th>max</th>
                        </tr>
                    </thead>
                    <tbody>{"".join(perf_rows)}</tbody>
                </table>
            </div>
            """

        # Aggregate Performance subsection
        agg_start, agg_end = ranges.get("aggregate", (0, 0))
        agg_html = build_chart_grid(s_idx, agg_start, agg_end) if agg_end > agg_start else ""

        # Partition-Level Performance subsection — in main mode the charts are
        # rendered in a sibling HTML to keep this page small; show a link card
        # when that sibling exists, otherwise the configuration hint.
        if per_partition_link:
            pp_html = (
                f'<p class="subsection-note">Per-partition charts are rendered in a separate file '
                f'to keep this page small. <a href="{per_partition_link}">Open per-partition charts ↗</a></p>'
            )
        else:
            pp_html = (
                '<p class="subsection-note">Not configured — set '
                '<code>perPartitionMetrics: true</code> in <code>engineConfig</code></p>'
            )

        # Availability subsection
        avail_start, avail_end = ranges.get("availability", (0, 0))
        if avail_end > avail_start:
            avail_html = build_chart_grid(s_idx, avail_start, avail_end)
        else:
            avail_html = '<p class="subsection-note">Not configured — add <code>availabilityConfig</code> to workload YAML</p>'

        sections.append(f"""
        <section id="scenario-{s_idx}" class="scenario-section">
            <h2>Scenario: {name}</h2>
            {table_html}
            {perf_table_html}
            <h3 id="scenario-{s_idx}-aggregate" class="subsection-heading">Aggregate Performance</h3>
            {agg_html}
            <h3 id="scenario-{s_idx}-partition" class="subsection-heading">Partition-Level Performance</h3>
            {pp_html}
            <h3 id="scenario-{s_idx}-availability" class="subsection-heading">Availability</h3>
            {avail_html}
        </section>
        """)

    return "\n".join(sections)


def _build_per_partition_scenario_html(s_idx, name, ranges):
    """Render the per-scenario block for the per-partition HTML.

    Only shows the Partition-Level Performance subsection — the summary tables
    and aggregate/availability charts belong to the main HTML.
    """
    pp_test_ranges = ranges.get("per_partition_tests", [])
    if pp_test_ranges:
        parts = []
        for t_label, t_start, t_end in pp_test_ranges:
            t_id = f"scenario-{s_idx}-partition-{t_label}"
            parts.append(f'<h4 id="{t_id}" class="test-label-heading">{t_label}</h4>')
            parts.append(build_chart_grid(s_idx, t_start, t_end))
        body = "\n".join(parts)
    else:
        pp_start, pp_end = ranges.get("per_partition", (0, 0))
        body = build_chart_grid(s_idx, pp_start, pp_end) if pp_end > pp_start else ""

    return f"""
    <section id="scenario-{s_idx}" class="scenario-section">
        <h2>Scenario: {name}</h2>
        <h3 id="scenario-{s_idx}-partition" class="subsection-heading">Partition-Level Performance</h3>
        {body}
    </section>
    """


def build_html(data, chart_configs, chart_sections, mode="main", sibling_link=None):
    """Render the full HTML page for either the main or per-partition output.

    Args:
        mode: "main" or "per_partition" — controls sidebar layout, scenario
              section rendering, and the page title.
        sibling_link: when set, links the two outputs together. In main mode
              this becomes the "Per-partition charts ↗" entry in the
              partition subsection and sidebar; in per-partition mode it
              becomes the back-link to the main HTML.
    """
    metadata = dict_or_empty(data.get("metadata"))
    kafka_config = dict_or_empty(data.get("baseKafkaConfig"))
    run_config = dict_or_empty(data.get("runConfig"))
    avail_config = dict_or_empty(data.get("availabilityConfig"))
    scenarios = data.get("scenarioResults", [])
    has_avail = has_availability_data(data)

    workload_name = text_or_empty(metadata.get("name")) or "Unknown Workload"
    kafka_config_name = text_or_empty(metadata.get("kafkaConfigName"))
    environment = text_or_empty(metadata.get("environment"))
    system = text_or_empty(metadata.get("system"))
    begin_time = text_or_empty(metadata.get("beginTime"))
    end_time = text_or_empty(metadata.get("endTime"))
    test_duration = run_config.get("testDurationMinutes", "")
    warmup_duration = run_config.get("warmupDurationMinutes", "")
    replication_factor = kafka_config.get("replicationFactor", "")
    annotation_meta = metadata_annotation_html(metadata)

    # Extra metadata when availability is enabled — only relevant on the main page
    avail_meta = ""
    if mode == "main" and has_avail:
        threshold = avail_config.get("unavailabilityThresholdMs", "")
        avail_meta = f'<div class="meta-item"><span class="label">Avail Threshold:</span> <span class="value">{threshold} ms</span></div>'

    sidebar_html = build_sidebar_html(scenarios, has_avail, chart_sections, mode=mode, sibling_link=sibling_link)
    config_section_html = build_config_section(data)
    # Availability summary table only appears on the main page; per-partition
    # output focuses on partition charts and stays self-contained otherwise.
    avail_summary_html = build_availability_summary(data) if (mode == "main" and has_avail) else ""
    per_partition_link = sibling_link if mode == "main" else None
    sections_html = build_scenario_sections(
        scenarios, chart_sections, mode=mode, per_partition_link=per_partition_link
    )
    configs_json = json.dumps(chart_configs)
    page_title_prefix = "Dimster Per-Partition" if mode == "per_partition" else "Dimster Results"
    page_title = f"{page_title_prefix} - {workload_name}"
    sidebar_title = "Dimster Per-Partition" if mode == "per_partition" else "Dimster Results"
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
.nav-link.nav-external,
.nav-link.nav-back {{
    color: #d4a86a;
    font-weight: 600;
}}
.nav-link.nav-back {{
    margin-bottom: 6px;
    border-bottom: 1px solid #2a2a4e;
}}
.nav-sublink {{
    display: block;
    padding: 5px 20px 5px 32px;
    color: #7a7a96;
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
.nav-sublink2 {{
    display: block;
    padding: 4px 20px 4px 44px;
    color: #6a6a86;
    text-decoration: none;
    font-size: 11px;
    transition: background 0.15s, color 0.15s;
    border-left: 3px solid transparent;
}}
.nav-sublink2:hover {{
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
.config-table td.num {{
    text-align: right;
    font-variant-numeric: tabular-nums;
}}
.perf-table thead tr:first-child th[colspan] {{
    text-align: center;
    border-bottom: 1px solid #d0d0d8;
}}
.perf-table thead tr:nth-child(2) th {{
    font-weight: 500;
    font-size: 11px;
    color: #6a6a8a;
}}
.test-label-heading {{
    font-size: 14px;
    font-weight: 600;
    color: #5a5a7a;
    margin-top: 20px;
    margin-bottom: 10px;
    padding-left: 4px;
}}
.subsection-heading {{
    font-size: 15px;
    font-weight: 600;
    color: #4a4a6a;
    margin-top: 28px;
    margin-bottom: 12px;
    padding-bottom: 6px;
    border-bottom: 1px solid #e8e8e8;
}}
.subsection-note {{
    font-size: 13px;
    color: #8a8a9a;
    font-style: italic;
    padding: 12px 16px;
    background: #f0f1f3;
    border-radius: 6px;
    margin-bottom: 16px;
}}
.subsection-note code {{
    font-size: 12px;
    background: #e0e2e6;
    padding: 1px 5px;
    border-radius: 3px;
    font-style: normal;
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
@media (max-width: 900px) {{
    .chart-grid {{ grid-template-columns: 1fr; }}
    .sidebar {{ display: none; }}
    .main {{ margin-left: 0; }}
}}
</style>
</head>
<body>

<nav class="sidebar">
    <div class="sidebar-title">{sidebar_title}</div>
    {sidebar_html}
</nav>

<div class="main">
    <div class="header">
        <h1>{workload_name}</h1>
        <div class="meta-grid">
            <div class="meta-item"><span class="label">System:</span> <span class="value">{system}</span></div>
            <div class="meta-item"><span class="label">Environment:</span> <span class="value">{environment}</span></div>
            <div class="meta-item"><span class="label">Kafka Config:</span> <span class="value">{kafka_config_name}</span></div>
            <div class="meta-item"><span class="label">Replication Factor:</span> <span class="value">{replication_factor}</span></div>
            <div class="meta-item"><span class="label">Test Duration:</span> <span class="value">{test_duration} min</span></div>
            <div class="meta-item"><span class="label">Warmup:</span> <span class="value">{warmup_duration} min</span></div>
            {avail_meta}
            {annotation_meta}
            <div class="meta-item"><span class="label">Start:</span> <span class="value">{begin_time}</span></div>
            <div class="meta-item"><span class="label">End:</span> <span class="value">{end_time}</span></div>
        </div>
    </div>

    <div id="resetBar-time" class="reset-bar">
        <span>Zoomed — drag on any time chart to adjust</span>
        <button onclick="resetZoom('time')">Reset Zoom</button>
    </div>
    <div id="resetBar-percentile" class="reset-bar">
        <span>Zoomed — drag on any percentile chart to adjust</span>
        <button onclick="resetZoom('percentile')">Reset Zoom</button>
    </div>

    {avail_summary_html}
    {config_section_html}
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
    // Deduplicate legend for floating bar charts with multiple datasets per type
    if (opts.__dedup_legend) {{
        delete opts.__dedup_legend;
        const seen = new Set();
        opts.plugins.legend.labels.filter = function(item, chartData) {{
            const lbl = chartData.datasets[item.datasetIndex].label;
            if (seen.has(lbl)) return false;
            seen.add(lbl);
            return true;
        }};
    }}
}}

var zoomGroups = {{ time: {{}}, percentile: {{}} }};

function syncZoom(sourceChart, group) {{
    var xScale = sourceChart.scales.x;
    var min = xScale.min, max = xScale.max;
    Object.values(zoomGroups[group]).forEach(function(c) {{
        if (c !== sourceChart) c.zoomScale('x', {{min: min, max: max}}, 'none');
    }});
    document.getElementById('resetBar-' + group).classList.add('visible');
}}

function resetZoom(group) {{
    Object.values(zoomGroups[group]).forEach(function(c) {{ c.resetZoom('none'); }});
    document.getElementById('resetBar-' + group).classList.remove('visible');
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
    if (c.zoomGroup) applyZoomCallbacks(c.config, c.zoomGroup);
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
        description="Generate HTML charts from Dimster run result JSON files."
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

    run_files = find_run_json_files(args.results)
    if not run_files:
        sys.exit(0)

    for path, data in run_files:
        rel = os.path.relpath(path, args.results)
        base_no_ext = os.path.splitext(rel)[0]
        out_path = os.path.join(args.output, base_no_ext + ".html")
        pp_out_path = os.path.join(args.output, base_no_ext + "-per-partition.html")

        os.makedirs(os.path.dirname(out_path), exist_ok=True)

        chart_configs, chart_sections = build_chart_configs(data)

        # When per-partition data exists we split the output into two files so
        # the main page stays lightweight. Sibling links (basename only — both
        # files live in the same directory) wire the two pages together.
        emit_pp = has_per_partition_data(data)
        main_sibling = os.path.basename(pp_out_path) if emit_pp else None
        pp_sibling = os.path.basename(out_path) if emit_pp else None

        main_configs, main_sections = filter_for_mode(chart_configs, chart_sections, "main")
        main_html = build_html(data, main_configs, main_sections, mode="main", sibling_link=main_sibling)
        with open(out_path, "w") as fh:
            fh.write(main_html)
        print(f"Generated: {out_path}")

        if emit_pp:
            pp_configs, pp_sections = filter_for_mode(chart_configs, chart_sections, "per_partition")
            pp_html = build_html(data, pp_configs, pp_sections, mode="per_partition", sibling_link=pp_sibling)
            with open(pp_out_path, "w") as fh:
                fh.write(pp_html)
            print(f"Generated: {pp_out_path}")

    print(f"Done. Processed {len(run_files)} file(s).")


if __name__ == "__main__":
    main()
