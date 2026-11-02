#!/usr/bin/env python3
"""
Generate the hardware resources report from Dimster run and drain-backlog
results.

Usage:
    python3 generate_resource_charts.py --results <dir> --output <dir>

The end-of-run metrics export (export.py) measures each test point's
resource usage in Prometheus and writes it to resource-metrics.json beside
the result: per pod from the kubelets' cAdvisor when the env sets
monitoring.cgroupMetrics, and per node from node-exporter. This script
joins those measurements with the result's throughput and, per service
(brokers, Flink, workers), writes:

    charts/<result>-resources.html         bar charts across test points,
                                           per-pod usage over time, derived
                                           figures and bottleneck flags
    <result>-resources.csv                 one row per test point and service
    <result>-resources-nodes.csv           one row per test point and node
    <result>-resources-timeseries.csv      per-pod and per-node usage over time

Results without resource-metrics.json, or none of whose test points were
measured, are skipped.
"""

import argparse
import csv
import html
import json
import os
import re
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate_run_charts import (  # noqa: E402
    COLORS,
    COLORS_ALPHA,
    avg,
    bar_options,
    dict_or_empty,
    find_run_artifact,
    is_result_of_mode,
    line_options,
    text_or_empty,
)


# Written beside the result JSON by the end-of-run metrics export (export.py).
RESOURCE_METRICS_FILE = "resource-metrics.json"

MODES = ("run", "drain-backlog")

# Services the report splits usage into, by pod name. Other pods
# (coordinator, monitoring, helper pods) are left out of every total.
SERVICES = [
    ("brokers", "Brokers", re.compile(r"^kafka-\d+$")),
    ("flink", "Flink", re.compile(r"^flink-(jobmanager|taskmanager)-")),
    ("workers", "Workers", re.compile(r"^benchmark-worker-")),
]
SERVICE_NAMES = {key: name for key, name, _ in SERVICES}

# Per-pod usage the report charts: (key in resource-metrics.json, title,
# unit shown, scale from the measured unit, CSV column). A service's value
# is the sum over its pods.
MB = 1_000_000
POD_METRICS = [
    ("cpuCores", "CPU, Sum of Pods", "cores", 1, "cpu_cores"),
    ("diskReadBytesPerSec", "Disk Read", "MB/s", 1 / MB, "disk_read_mb_s"),
    ("diskWriteBytesPerSec", "Disk Write", "MB/s", 1 / MB, "disk_write_mb_s"),
    ("diskReadIops", "Disk Read IOPS", "ops/s", 1, "disk_read_iops"),
    ("diskWriteIops", "Disk Write IOPS", "ops/s", 1, "disk_write_iops"),
    ("netRxBytesPerSec", "Network Receive", "MB/s", 1 / MB, "net_rx_mb_s"),
    ("netTxBytesPerSec", "Network Transmit", "MB/s", 1 / MB, "net_tx_mb_s"),
]
# Per-node usage, charted (the first six) and in the nodes CSV.
NODE_METRICS = [
    ("cpuUtil", "Node CPU Utilisation", "%", 100, "cpu_util_pct"),
    ("diskBusy", "Busiest Disk Busy", "%", 100, "disk_busy_pct"),
    ("diskReadBytesPerSec", "Node Disk Read", "MB/s", 1 / MB, "disk_read_mb_s"),
    ("diskWriteBytesPerSec", "Node Disk Write", "MB/s", 1 / MB, "disk_write_mb_s"),
    ("netRxBytesPerSec", "Node Network Receive", "MB/s", 1 / MB, "net_rx_mb_s"),
    ("netTxBytesPerSec", "Node Network Transmit", "MB/s", 1 / MB, "net_tx_mb_s"),
    ("cores", "Cores", "cores", 1, "cores"),
    ("cpuUtilPeak", "Peak CPU", "%", 100, "cpu_util_peak_pct"),
    ("iowaitShare", "IO Wait", "%", 100, "iowait_pct"),
    ("netSpeedBytesPerSec", "Link Speed", "MB/s", 1 / MB, "net_speed_mb_s"),
    ("psiCpuWaiting", "CPU Pressure", "%", 100, "psi_cpu_waiting_pct"),
    ("psiIoWaiting", "IO Pressure", "%", 100, "psi_io_waiting_pct"),
]
CHARTED_NODE_METRICS = NODE_METRICS[:6]

# CPU throttling: the share of CFS periods in which a container was held back
# by its CPU limit. Not a sum, so kept out of POD_METRICS: a service's share
# is its pods' throttled periods over their periods, and the time series
# holds each pod's share.
THROTTLED = ("cpuThrottledPct", "CPU Throttled", "% of periods", 1, "cpu_throttled_pct")
THROTTLED_SHARE = ("cpuThrottledShare", "CPU Throttled", "% of periods", 100, None)
# Bar charts per service across test points, and per-pod charts over time.
SERVICE_BARS = POD_METRICS[:1] + [THROTTLED] + POD_METRICS[1:]
POD_TIME_METRICS = POD_METRICS[:1] + [THROTTLED_SHARE] + POD_METRICS[1:]

# Bottleneck thresholds.
CPU_LIMIT_SATURATED = 0.90      # pod CPU / its limit
CPU_THROTTLED = 0.10            # share of CFS periods throttled
NODE_CPU_SATURATED = 0.90       # node CPU utilisation, window average
NODE_CPU_PEAK_SATURATED = 0.98  # node CPU utilisation, 1m peak
DISK_BUSY_SATURATED = 0.90      # busiest whole disk's busy time
IO_PRESSURE_HIGH = 0.25         # PSI: share of time some task waited on IO
NETWORK_SATURATED = 0.80        # node rx or tx / link speed

# resource-metrics.json holds counters as totals over each window; the report
# shows them as per-second rates over the window's measuredSeconds:
# {rate key: total key}.
POD_RATES = {
    "cpuCores": "cpuSeconds",
    "diskReadBytesPerSec": "diskReadBytes",
    "diskWriteBytesPerSec": "diskWriteBytes",
    "diskReadIops": "diskReadOps",
    "diskWriteIops": "diskWriteOps",
    "netRxBytesPerSec": "netRxBytes",
    "netTxBytesPerSec": "netTxBytes",
    "cfsPeriodsPerSec": "cfsPeriods",
    "cfsThrottledPeriodsPerSec": "cfsThrottledPeriods",
}
NODE_RATES = {
    "diskReadBytesPerSec": "diskReadBytes",
    "diskWriteBytesPerSec": "diskWriteBytes",
    "netRxBytesPerSec": "netRxBytes",
    "netTxBytesPerSec": "netTxBytes",
}


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def find_result_files(results_dir):
    """[(path, data)] of every run and drain-backlog result under a dir,
    excluding run requests and other typed helper artifacts."""
    found = []
    for root, _dirs, files in os.walk(results_dir):
        for f in sorted(files):
            if not f.endswith(".json") or f == RESOURCE_METRICS_FILE:
                continue
            path = os.path.join(root, f)
            try:
                with open(path) as fh:
                    data = json.load(fh)
            except (IOError, json.JSONDecodeError):
                continue
            if isinstance(data, dict) and any(is_result_of_mode(data, m) for m in MODES):
                found.append((path, data))
    return found


def load_resource_metrics(path):
    """The parsed resource-metrics.json, or None when unreadable."""
    try:
        with open(path) as fh:
            doc = json.load(fh)
    except (IOError, json.JSONDecodeError) as e:
        print(f"Warning: ignoring {path}: {e}", file=sys.stderr)
        return None
    return doc if isinstance(doc, dict) else None


def service_of(pod):
    """The service key a pod belongs to, or None."""
    for key, _name, pattern in SERVICES:
        if pattern.search(pod):
            return key
    return None


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def performance(data, test):
    """The test's client performance block: `results` in run mode,
    `performance` in drain-backlog mode."""
    key = "performance" if data.get("mode") == "drain-backlog" else "results"
    return dict_or_empty(test.get(key))


def with_rates(metrics, rates, seconds):
    """A pod's or node's metrics with each counter total replaced by its
    per-second rate over the window; gauges and shares are kept as they are."""
    out = {k: v for k, v in metrics.items() if k not in rates.values()}
    for rate, total in rates.items():
        if total in metrics and seconds:
            out[rate] = metrics[total] / seconds
    return out


def rated_window(window):
    """The window with its pods' and nodes' counter totals as rates, as the
    summary, charts and bottleneck checks use them."""
    seconds = window.get("measuredSeconds") or window.get("seconds")
    return dict(window,
                pods={p: with_rates(m, POD_RATES, seconds) for p, m in window.get("pods", {}).items()},
                nodes={n: with_rates(m, NODE_RATES, seconds) for n, m in window.get("nodes", {}).items()})


def ratio(numerator, denominator):
    if numerator is None or not denominator:
        return None
    return numerator / denominator


def service_usage(window, service):
    """Summed usage of a service's pods over a window: each POD_METRICS key
    in its measured unit, plus the pod count and the busiest pod's peak CPU.
    None when no pod of the service was measured."""
    pods = {p: m for p, m in window.get("pods", {}).items() if service_of(p) == service}
    if not pods:
        return None
    usage = {key: sum(m.get(key, 0.0) for m in pods.values())
             for key in [k for k, *_ in POD_METRICS] + CFS_KEYS}
    usage["pods"] = len(pods)
    peaks = [m["cpuCoresPeak"] for m in pods.values() if "cpuCoresPeak" in m]
    usage["cpuCoresPeakPod"] = max(peaks) if peaks else None
    usage["cpuThrottledPct"] = throttled_pct(usage)
    return usage


# CFS period counters, summed so a service's throttled share weighs each pod
# by its periods.
CFS_KEYS = ["cfsPeriodsPerSec", "cfsThrottledPeriodsPerSec"]


def throttled_pct(usage):
    """Percent of CFS periods throttled across summed pods; None when none
    has a CPU limit (no periods are counted without one)."""
    share = ratio(usage.get("cfsThrottledPeriodsPerSec"), usage.get("cfsPeriodsPerSec"))
    return None if share is None else share * 100


def cost_per_gb(usage, measured_s, perf, produce_mbs, consume_mbs):
    """CPU core-seconds the usage spent per GB produced and per GB consumed:
    its CPU seconds over the window's measured span divided by the bytes the
    result's clients produced and consumed (bytesSent/bytesReceived), both
    totals. Results from before those totals fall back to average cores over
    average client MB/s."""
    if "bytesSent" in perf or "bytesReceived" in perf:
        core_seconds = usage["cpuCores"] * measured_s
        return (ratio(core_seconds, perf.get("bytesSent", 0) / 1e9),
                ratio(core_seconds, perf.get("bytesReceived", 0) / 1e9))
    cores = usage["cpuCores"]
    return (ratio(cores, produce_mbs / 1000 if produce_mbs else 0),
            ratio(cores, consume_mbs / 1000 if consume_mbs else 0))


def cache_estimate(window, consumed_bytes, backlog_bytes, cgroup):
    """How broker reads split between the page cache and disk, as totals over
    the window, read straight from the measured counters.

    Logical reads are what the brokers served to consumers and followers
    (Kafka JMX bytes out plus replication bytes out). Whatever they didn't
    read from disk is assumed to have come from the page cache. Disk reads
    are the brokers' own (cgroup metrics) or, without those, every node's,
    which overstates them when brokers share disks with other services.
    Readahead and index reads can push disk reads past logical reads, so
    the cache share is clamped at 0. consumed_bytes is what the test point's
    consumers received (the result's bytesReceived; None in results from
    before it). None without Kafka JMX metrics."""
    kafka = window.get("kafka", {})
    if "bytesOut" not in kafka:
        return None
    logical = kafka["bytesOut"] + kafka.get("replicationBytesOut", 0.0)
    if cgroup:
        brokers = [m for p, m in window.get("pods", {}).items() if service_of(p) == "brokers"]
        if not brokers:
            return None
        disk = sum(m.get("diskReadBytes", 0.0) for m in brokers)
    else:
        disk = sum(n.get("diskReadBytes", 0.0) for n in window.get("nodes", {}).values())
    cached = max(0.0, logical - disk)
    return {
        "diskSource": "brokers" if cgroup else "all nodes",
        "consumedBytes": consumed_bytes,
        "logicalReadBytes": logical,
        "diskReadBytes": disk,
        "cacheReadBytes": cached,
        "cacheHitPct": ratio(cached, logical) * 100 if logical else None,
        "diskReadPerConsumedByte": ratio(disk, consumed_bytes),
        "diskReadPerBacklogByte": ratio(disk, backlog_bytes),
    }


def services_by_node(window):
    """{node: sorted service names placed on it}, from cAdvisor's pod
    placement; empty without cgroup metrics."""
    out = {}
    for pod, node in window.get("podNodes", {}).items():
        service = service_of(pod)
        if service:
            out.setdefault(node, set()).add(SERVICE_NAMES[service])
    return {node: sorted(names) for node, names in out.items()}


def find_bottlenecks(window):
    """[{resource, where, services, detail}] for every saturated resource in
    a window: pods at their CPU limit or throttled, and nodes whose CPU,
    busiest disk, IO pressure or network link is saturated. A node's
    services are those placed on it; several means the node is shared, so
    the flag can't be pinned on one of them."""
    flags = []
    for pod, m in sorted(window.get("pods", {}).items()):
        service = service_of(pod)
        if not service:
            continue
        services = [SERVICE_NAMES[service]]
        limit = m.get("cpuLimitCores")
        if limit and m.get("cpuCores", 0) >= CPU_LIMIT_SATURATED * limit:
            flags.append(_flag("CPU", f"pod {pod}", services,
                               f"{m['cpuCores']:.2f} of {limit:.2f} cores limit"))
        throttled = ratio(m.get("cfsThrottledPeriodsPerSec"), m.get("cfsPeriodsPerSec"))
        if throttled is not None and throttled > CPU_THROTTLED:
            flags.append(_flag("CPU", f"pod {pod}", services,
                               f"throttled in {throttled:.0%} of CFS periods"))

    placed = services_by_node(window)
    for node, m in sorted(window.get("nodes", {}).items()):
        services = placed.get(node, [])
        util, peak = m.get("cpuUtil"), m.get("cpuUtilPeak")
        if util is not None and util >= NODE_CPU_SATURATED:
            flags.append(_flag("CPU", f"node {node}", services, f"average {util:.0%} utilised"))
        elif peak is not None and peak >= NODE_CPU_PEAK_SATURATED:
            flags.append(_flag("CPU", f"node {node}", services, f"peaked at {peak:.0%} utilised"))
        busy = m.get("diskBusy")
        if busy is not None and busy >= DISK_BUSY_SATURATED:
            flags.append(_flag("Disk", f"node {node}", services,
                               f"busiest disk busy {busy:.0%} of the time (NVMe serves requests "
                               "in parallel, so busy time overstates its saturation)"))
        io = m.get("psiIoWaiting")
        if io is not None and io >= IO_PRESSURE_HIGH:
            flags.append(_flag("Disk", f"node {node}", services,
                               f"tasks waited on IO {io:.0%} of the time"))
        speed = m.get("netSpeedBytesPerSec")
        if speed:
            busiest = max(m.get("netRxBytesPerSec", 0.0), m.get("netTxBytesPerSec", 0.0))
            if busiest >= NETWORK_SATURATED * speed:
                flags.append(_flag("Network", f"node {node}", services,
                                   f"{busiest / speed:.0%} of the {speed * 8 / 1e9:.0f} Gbit/s link"))
    return flags


def _flag(resource, where, services, detail):
    return {"resource": resource, "where": where, "services": services, "detail": detail,
            "shared": len(services) > 1}


def analyze(data, resources):
    """[test point] for each test of the result whose window was measured:
    its usage per service, derived figures, nodes and bottleneck flags."""
    windows = {(w.get("scenario"), w.get("testIndex")): w for w in resources.get("windows", [])}
    cgroup = bool(resources.get("cgroupMetrics"))
    scenarios = data.get("scenarioResults", [])
    multi = len(scenarios) > 1
    points = []
    for scenario in scenarios:
        name = scenario.get("name", "")
        for t_idx, test in enumerate(scenario.get("tests", [])):
            raw = windows.get((name, t_idx))
            if not raw:
                continue
            window = rated_window(raw)
            perf = performance(data, test)
            produce = avg(perf.get("publishBytesRate", []))
            consume = avg(perf.get("consumeBytesRate", []))
            seconds = window.get("seconds", 0)
            measured = window.get("measuredSeconds") or seconds
            services = {}
            for key, _n, _p in SERVICES:
                usage = service_usage(window, key)
                if usage:
                    usage["cpuPerGbProduced"], usage["cpuPerGbConsumed"] = (
                        cost_per_gb(usage, measured, perf, produce, consume))
                    services[key] = usage
            if services:
                total = {key: sum(u[key] for u in services.values())
                         for key in [k for k, *_ in POD_METRICS] + CFS_KEYS}
                total["pods"] = sum(u["pods"] for u in services.values())
                total["cpuCoresPeakPod"] = None
                total["cpuThrottledPct"] = throttled_pct(total)
                total["cpuPerGbProduced"], total["cpuPerGbConsumed"] = cost_per_gb(
                    total, measured, perf, produce, consume)
                services["all"] = total
            label = test.get("label") or f"#{t_idx + 1}"
            points.append({
                "scenario": name,
                "testIndex": t_idx,
                "label": label,
                # Chart and table label; scenarios are told apart when several.
                "display": f"{name}: {label}" if multi else label,
                "start": window.get("start"),
                "end": window.get("end"),
                "seconds": seconds,
                # The span the counters were read over; longer than the
                # window for short ones (see export.py resource_eval).
                "measuredSeconds": measured,
                "produceMBs": produce,
                "consumeMBs": consume,
                "services": services,
                "nodes": window.get("nodes", {}),
                "servicesByNode": services_by_node(window),
                "cache": cache_estimate(raw, perf.get("bytesReceived"),
                                        dict_or_empty(test.get("drain")).get("backlogSizeBytes"),
                                        cgroup),
                "bottlenecks": find_bottlenecks(window),
                # The test point's own series (finer, around its window).
                "timeseries": raw.get("timeseries"),
            })
    return points


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

def _num(value, scale=1, digits=4):
    return "" if value is None else round(value * scale, digits)


def service_rows(points):
    """One CSV row per test point and service (plus an "All" row summing
    the three), with the test point's broker cache figures on its brokers row."""
    header = ["scenario", "test_index", "label", "service", "pods", "window_start",
              "window_end", "window_seconds"]
    header += [column for *_, column in POD_METRICS]
    header += ["cpu_cores_peak_pod", "cpu_throttled_pct", "produce_mb_s", "consume_mb_s",
               "cpu_core_s_per_gb_produced", "cpu_core_s_per_gb_consumed",
               "consumed_mb", "kafka_logical_read_mb", "disk_read_source", "broker_disk_read_mb",
               "cache_read_mb_est", "cache_hit_pct_est", "disk_read_per_consumed_byte",
               "disk_read_per_backlog_byte"]
    rows = [header]
    for p in points:
        for key, usage in p["services"].items():
            row = [p["scenario"], p["testIndex"], p["label"],
                   SERVICE_NAMES.get(key, "All"), usage["pods"], p["start"], p["end"],
                   _num(p["seconds"], digits=1)]
            row += [_num(usage[k], scale) for k, _t, _u, scale, _c in POD_METRICS]
            row += [_num(usage.get("cpuCoresPeakPod")), _num(usage["cpuThrottledPct"], digits=2),
                    _num(p["produceMBs"]),
                    _num(p["consumeMBs"]), _num(usage["cpuPerGbProduced"], digits=2),
                    _num(usage["cpuPerGbConsumed"], digits=2)]
            cache = p["cache"] if key == "brokers" else None
            if cache:
                row += [_num(cache["consumedBytes"], 1 / MB), _num(cache["logicalReadBytes"], 1 / MB),
                        cache["diskSource"], _num(cache["diskReadBytes"], 1 / MB),
                        _num(cache["cacheReadBytes"], 1 / MB),
                        _num(cache["cacheHitPct"], digits=1),
                        _num(cache["diskReadPerConsumedByte"]),
                        _num(cache["diskReadPerBacklogByte"])]
            else:
                row += [""] * 8
            rows.append(row)
    return rows


def node_rows(points):
    """One CSV row per test point and node."""
    rows = [["scenario", "test_index", "label", "node", "services"]
            + [column for *_, column in NODE_METRICS]]
    for p in points:
        for node, m in sorted(p["nodes"].items()):
            rows.append([p["scenario"], p["testIndex"], p["label"], node,
                         " ".join(p["servicesByNode"].get(node, []))]
                        + [_num(m.get(key), scale) for key, _t, _u, scale, _c in NODE_METRICS])
    return rows


def timeseries_rows(resources):
    """Long-format rows of per-pod and per-node usage: over the whole run
    (series "run"), then over each test point's own window and margins
    (series "test point", with its scenario and label). seconds is since
    the series' start."""
    rows = [["series", "scenario", "label", "timestamp", "seconds", "scope", "name",
             "service", "metric", "value"]]
    series = [("run", "", "", resources.get("timeseries", {}))]
    series += [("test point", w.get("scenario", ""), w.get("label", ""), w["timeseries"])
               for w in resources.get("windows", []) if w.get("timeseries")]
    for kind, scenario, label, ts in series:
        times = ts.get("times", [])
        t0 = times[0] if times else 0
        for scope, by_name in (("pod", ts.get("pods", {})), ("node", ts.get("nodes", {}))):
            for name, metrics in sorted(by_name.items()):
                service = service_of(name) if scope == "pod" else None
                if scope == "pod" and not service:
                    continue
                for metric, values in sorted(metrics.items()):
                    for t, v in zip(times, values):
                        if v is not None:
                            rows.append([kind, scenario, label, iso(t), t - t0, scope, name,
                                         SERVICE_NAMES.get(service, ""), metric,
                                         _num(v, digits=6)])
    return rows


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_csv(path, rows):
    with open(path, "w", newline="") as fh:
        csv.writer(fh).writerows(rows)
    print(f"Generated: {path}")


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def bar_chart(chart_id, title, unit, labels, datasets):
    return {"id": chart_id, "config": {
        "type": "bar",
        "data": {"labels": labels, "datasets": datasets},
        "options": bar_options(title, unit),
    }}


def bar_dataset(label, values, color_idx):
    return {"label": label, "data": values,
            "backgroundColor": COLORS[color_idx % len(COLORS)], "borderRadius": 3}


def time_chart(chart_id, title, unit, series, bands, x_label="time since run start (s)",
               x_range=None):
    """Line chart over time; series is [(name, [(seconds, value)])], and each
    band (a test point's window) is shaded by the `testPoints` plugin.
    x_range fixes the axis to (min, max) seconds rather than letting it
    round out to the next tick."""
    options = line_options(title, unit, x_label)
    options["scales"]["x"]["type"] = "linear"
    if x_range:
        options["scales"]["x"]["min"], options["scales"]["x"]["max"] = x_range
    options["plugins"]["testPoints"] = {"bands": bands}
    datasets = []
    for i, (name, points) in enumerate(series):
        datasets.append({
            "label": name,
            "data": [{"x": x, "y": y} for x, y in points],
            "borderColor": COLORS[i % len(COLORS)],
            "backgroundColor": COLORS_ALPHA[i % len(COLORS_ALPHA)],
            # Few samples (a short test point's own series): show each one.
            "borderWidth": 1.5, "pointRadius": 2 if len(points) < 60 else 0,
            "tension": 0.2, "fill": False,
            "spanGaps": True,
        })
    return {"id": chart_id, "config": {"type": "line", "data": {"datasets": datasets},
                                       "options": options}}


def time_bands(points, t0):
    """Each test point's window as {from, to, label} in seconds since t0."""
    bands = []
    for p in points:
        try:
            start = parse_iso(p["start"]) - t0
            end = parse_iso(p["end"]) - t0
        except (TypeError, ValueError):
            continue
        bands.append({"from": start, "to": end, "label": p["display"]})
    return bands


def parse_iso(text):
    """An ISO-8601 instant as written by Java's Instant.toString(), as a
    Unix timestamp. Java writes up to nanoseconds; fromisoformat before
    Python 3.11 accepts only 3 or 6 fractional digits, so the fraction is
    padded or cut to microseconds."""
    text = re.sub(r"\.(\d+)", lambda m: "." + m.group(1).ljust(6, "0")[:6],
                  text.replace("Z", "+00:00"))
    return datetime.fromisoformat(text).timestamp()


def scaled_series(times, values, scale, t0, rate_s=0):
    """(seconds since t0, scaled value) points. A rate averages the rate_s
    seconds before its sample time, so it is placed at the middle of that
    interval: otherwise the line runs rate_s/2 behind the activity."""
    shift = rate_s / 2
    return [(t - shift - t0, round(v * scale, 4)) for t, v in zip(times, values) if v is not None]


def pod_time_charts(service, ts, t0, bands, prefix, x_label="time since run start (s)",
                    x_range=None):
    """Each POD_TIME_METRICS metric of a service's pods over a series' times,
    as seconds since t0. Metrics no pod reported are left out."""
    times = ts.get("times", [])
    pods = sorted(p for p in ts.get("pods", {}) if service_of(p) == service)
    configs = []
    for i, (key, title, unit, scale, _c) in enumerate(POD_TIME_METRICS):
        series = [(pod, scaled_series(times, ts["pods"][pod].get(key, []), scale, t0,
                                      ts.get("rateSeconds", 0)))
                  for pod in pods]
        if any(s for _n, s in series):
            title = title.replace(", Sum of Pods", "")
            configs.append(time_chart(f"{prefix}-time-{i}", f"{title} per Pod ({unit})", unit,
                                      series, bands, x_label, x_range))
    return configs


def node_time_charts(ts, t0, bands, prefix, x_label="time since run start (s)", x_range=None):
    """Node CPU utilisation and busiest-disk busy time over a series' times."""
    times = ts.get("times", [])
    configs = []
    for i, (key, title, unit, scale, _c) in enumerate(CHARTED_NODE_METRICS[:2]):
        series = [(node, scaled_series(times, m.get(key, []), scale, t0,
                                       ts.get("rateSeconds", 0)))
                  for node, m in sorted(ts.get("nodes", {}).items())]
        if any(s for _n, s in series):
            configs.append(time_chart(f"{prefix}-time-{i}", f"{title} over Time ({unit})", unit,
                                      series, bands, x_label, x_range))
    return configs


def run_t0(resources):
    times = resources.get("timeseries", {}).get("times", [])
    return times[0] if times else 0


def service_charts(service, points, resources, bands):
    """Bar charts of a service across test points (one per metric), then
    each metric over the whole run per pod."""
    labels = [p["display"] for p in points]
    configs = []
    for i, (key, title, unit, scale, _c) in enumerate(SERVICE_BARS):
        values = [_num(p["services"].get(service, {}).get(key), scale) or 0 for p in points]
        configs.append(bar_chart(f"{service}-bar-{i}", f"{title} ({unit})", unit, labels,
                                 [bar_dataset(SERVICE_NAMES[service], values, i)]))
    return configs + pod_time_charts(service, resources.get("timeseries", {}),
                                     run_t0(resources), bands, service)


def node_charts(points, resources, bands):
    """Bar charts per node across test points, and node CPU and disk busy
    over the whole run."""
    labels = [p["display"] for p in points]
    nodes = sorted({n for p in points for n in p["nodes"]})
    configs = []
    for i, (key, title, unit, scale, _c) in enumerate(CHARTED_NODE_METRICS):
        datasets = [bar_dataset(node, [_num(p["nodes"].get(node, {}).get(key), scale) or 0
                                       for p in points], n_idx)
                    for n_idx, node in enumerate(nodes)]
        configs.append(bar_chart(f"nodes-bar-{i}", f"{title} ({unit})", unit, labels, datasets))
    return configs + node_time_charts(resources.get("timeseries", {}), run_t0(resources),
                                      bands, "nodes")


def test_point_charts(index, point):
    """[(heading, charts)] over a test point's own series: per service, then
    the nodes. Time is seconds since the test point's start; the series
    reaches a little either side of the window, which is shaded."""
    ts = point.get("timeseries") or {}
    try:
        t0 = parse_iso(point["start"])
    except (KeyError, TypeError, ValueError):
        return []
    bands = [{"from": 0, "to": point["seconds"], "label": "test point"}]
    x_label = "time since test point start (s)"
    margin = ts.get("marginSeconds", 30)
    x_range = (-margin, round(point["seconds"]) + margin)
    groups = []
    for key, title, _p in SERVICES:
        charts = pod_time_charts(key, ts, t0, bands, f"tp{index}-{key}", x_label, x_range)
        if charts:
            groups.append((title, charts))
    charts = node_time_charts(ts, t0, bands, f"tp{index}-nodes", x_label, x_range)
    if charts:
        groups.append(("Nodes", charts))
    return groups


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

def _cell(value, fmt="{:.2f}"):
    return '<td class="num">' + ("–" if value is None else fmt.format(value)) + "</td>"


# A test point measured over a span this much longer than its window gets a note.
MEASURED_SPAN_NOTE_RATIO = 1.10


def measured_span_note(points):
    """A note naming test points whose averages cover a longer span than
    their window: short windows (quick drains) are read to past their end,
    so all their bytes are counted, and averaged over that whole span."""
    longer = [p for p in points if p["measuredSeconds"] > MEASURED_SPAN_NOTE_RATIO * p["seconds"]]
    if not longer:
        return ""
    spans = ", ".join(f"{html.escape(p['display'])} ({p['seconds']:.0f}s window, measured over "
                      f"{p['measuredSeconds']:.0f}s)" for p in longer)
    return (f'<p class="note">Short windows are measured from before they start to past the '
            f'scrape after they end, so their totals are complete, and their averages include '
            f'that slack: {spans}. Totals (page cache vs disk) are not diluted.</p>')


def single(points):
    """One run's test points as table groups: each its own group, unlabelled."""
    return [[(None, p)] for p in points]


def _entry_cells(label, labelled):
    """The comparison's entry column, when the table compares entries."""
    return f"<td><b>{html.escape(label)}</b></td>" if labelled else ""


def _is_labelled(groups):
    return any(label is not None for group in groups for label, _p in group)


def summary_table(groups):
    """Per test point and service: average usage and CPU cost per GB.
    groups is [[(entry label, test point), ...], ...], one group per test
    point; a comparison's entries are listed side by side within each test
    point and service, a single run (see single()) has no entry column."""
    labelled = _is_labelled(groups)
    head = ("<tr><th>Test point</th><th>Service</th>" + ("<th>Entry</th>" if labelled else "")
            + "<th>Pods</th><th>CPU (cores, sum of pods)</th><th>Peak pod CPU (cores)</th>"
            "<th>CPU throttled</th><th>Disk read MB/s</th><th>Disk write MB/s</th>"
            "<th>Read IOPS</th><th>Write IOPS</th><th>Net rx MB/s</th><th>Net tx MB/s</th>"
            "<th>Core-s / GB produced</th><th>Core-s / GB consumed</th></tr>")
    rows = []
    for group in groups:
        keys = list(dict.fromkeys(k for _l, p in group for k in p["services"]))
        for key in keys:
            for label, p in group:
                u = p["services"].get(key)
                if not u:
                    continue
                rows.append(
                    f"<tr><td>{html.escape(p['display'])}</td>"
                    f"<td>{SERVICE_NAMES.get(key, '<b>All</b>')}</td>{_entry_cells(label, labelled)}"
                    f"<td class=\"num\">{u['pods']}</td>"
                    + _cell(u["cpuCores"]) + _cell(u.get("cpuCoresPeakPod"))
                    + _cell(u["cpuThrottledPct"], "{:.1f}%")
                    + _cell(u["diskReadBytesPerSec"] / MB) + _cell(u["diskWriteBytesPerSec"] / MB)
                    + _cell(u["diskReadIops"], "{:.0f}") + _cell(u["diskWriteIops"], "{:.0f}")
                    + _cell(u["netRxBytesPerSec"] / MB) + _cell(u["netTxBytesPerSec"] / MB)
                    + _cell(u["cpuPerGbProduced"], "{:.1f}") + _cell(u["cpuPerGbConsumed"], "{:.1f}")
                    + "</tr>")
    return table(head, rows)


def cache_table(groups):
    """Page cache vs disk: totals over each test point's window (groups as
    for summary_table)."""
    labelled = _is_labelled(groups)
    mb = lambda v: None if v is None else v / MB  # noqa: E731
    rows = []
    for group in groups:
        for label, p in group:
            c = p["cache"]
            if not c:
                continue
            rows.append(
                f"<tr><td>{html.escape(p['display'])}</td>{_entry_cells(label, labelled)}"
                + _cell(p["seconds"], "{:.1f}") + _cell(mb(c["consumedBytes"]), "{:,.1f}")
                + _cell(mb(c["logicalReadBytes"]), "{:,.1f}") + _cell(mb(c["diskReadBytes"]), "{:,.1f}")
                + f"<td>{c['diskSource']}</td>" + _cell(mb(c["cacheReadBytes"]), "{:,.1f}")
                + _cell(c["cacheHitPct"], "{:.1f}%")
                + _cell(c["diskReadPerConsumedByte"], "{:.3f}")
                + _cell(c["diskReadPerBacklogByte"], "{:.3f}") + "</tr>")
    if not rows:
        return ""
    head = ("<tr><th>Test point</th>" + ("<th>Entry</th>" if labelled else "")
            + "<th>Window (s)</th><th>Consumed MB</th>"
            "<th>Broker logical reads MB</th><th>Disk reads MB</th><th>Disk reads of</th>"
            "<th>Page cache reads MB (est.)</th><th>Page cache hit (est.)</th>"
            "<th>Disk read / consumed byte</th><th>Disk read / backlog byte</th></tr>")
    return table(head, rows)


def bottleneck_table(groups):
    """Every bottleneck flag (groups as for summary_table)."""
    labelled = _is_labelled(groups)
    rows = []
    for group in groups:
        for label, p in group:
            for f in p["bottlenecks"]:
                services = ", ".join(f["services"]) or "–"
                if f["shared"]:
                    services += " (shared node)"
                rows.append(f"<tr><td>{html.escape(p['display'])}</td>{_entry_cells(label, labelled)}"
                            f"<td>{f['resource']}</td>"
                            f"<td>{html.escape(f['where'])}</td><td>{html.escape(services)}</td>"
                            f"<td>{html.escape(f['detail'])}</td></tr>")
    if not rows:
        return '<div class="note">No saturated CPU, disk or network detected.</div>'
    head = ("<tr><th>Test point</th>" + ("<th>Entry</th>" if labelled else "")
            + "<th>Resource</th><th>Where</th><th>Services</th><th>Detail</th></tr>")
    return table(head, rows)


def node_table(points):
    rows = []
    for p in points:
        for node, m in sorted(p["nodes"].items()):
            pct = lambda k: None if m.get(k) is None else m[k] * 100  # noqa: E731
            mbs = lambda k: None if m.get(k) is None else m[k] / MB  # noqa: E731
            rows.append(
                f"<tr><td>{html.escape(p['display'])}</td><td>{html.escape(node)}</td>"
                f"<td>{html.escape(', '.join(p['servicesByNode'].get(node, [])) or '–')}</td>"
                + _cell(m.get("cores"), "{:.0f}") + _cell(pct("cpuUtil"), "{:.0f}%")
                + _cell(pct("cpuUtilPeak"), "{:.0f}%") + _cell(pct("diskBusy"), "{:.0f}%")
                + _cell(mbs("diskReadBytesPerSec")) + _cell(mbs("diskWriteBytesPerSec"))
                + _cell(mbs("netRxBytesPerSec")) + _cell(mbs("netTxBytesPerSec")) + "</tr>")
    head = ("<tr><th>Test point</th><th>Node</th><th>Services</th><th>Cores</th><th>CPU</th>"
            "<th>Peak CPU</th><th>Disk busy</th><th>Disk read MB/s</th><th>Disk write MB/s</th>"
            "<th>Net rx MB/s</th><th>Net tx MB/s</th></tr>")
    return table(head, rows)


def table(head, rows):
    return (f'<div class="table-wrap"><table><thead>{head}</thead>'
            f'<tbody>{"".join(rows)}</tbody></table></div>')


def chart_grid(configs):
    cards = "".join(f'<div class="chart-card"><canvas id="{c["id"]}"></canvas></div>'
                    for c in configs)
    return f'<div class="chart-grid">{cards}</div>'


def build_html(data, points, resources):
    """The resources report page for one result."""
    metadata = dict_or_empty(data.get("metadata"))
    name = text_or_empty(metadata.get("name")) or "Unknown Workload"
    cgroup = bool(resources.get("cgroupMetrics"))
    times = resources.get("timeseries", {}).get("times", [])
    bands = time_bands(points, times[0] if times else 0)

    nav = ['<a class="nav-link" href="#summary">Summary</a>',
           '<a class="nav-link" href="#bottlenecks">Bottlenecks</a>']
    sections = []
    configs = []
    if cgroup:
        sections.append(f"""
<section id="summary"><h2>Summary</h2>
<p class="note">Averages over each test point's measured window, summed over each service's pods;
peak pod CPU is the busiest pod's 1-minute peak. CPU throttled is the share of CFS periods in which
the service's pods were held back by their CPU limits (– without limits). CPU cost is core-seconds
per GB of client throughput. <b>All</b> sums brokers, Flink and workers.</p>
{measured_span_note(points)}
{summary_table(single(points))}
<h3>Page cache vs disk</h3>
<p class="note">Totals over each test point's window. Estimates: broker logical reads are what
brokers served to consumers and followers (Kafka bytes out plus replication bytes out); what they did
not read from disk is assumed to come from the page cache. Consumed is what the Kafka consumers
received.</p>
{cache_table(single(points)) or '<div class="note">No Kafka broker metrics (external Kafka).</div>'}
</section>""")
    else:
        sections.append(f"""
<section id="summary"><h2>Summary</h2>
<div class="note">No per-pod metrics were collected, so usage can't be split by service and only
node totals are shown. Set <code>monitoring.cgroupMetrics: true</code> in the environment and
redeploy to collect them.</div>
<h3>Page cache vs disk</h3>
{cache_table(single(points)) or '<div class="note">No Kafka broker metrics (external Kafka).</div>'}
</section>""")
    sections.append(f'<section id="bottlenecks"><h2>Bottlenecks</h2>{bottleneck_table(single(points))}</section>')

    # The whole run: across test points, and over the run with each test
    # point's window shaded.
    nav.append('<div class="nav-label">Whole run</div>')
    for key, title, _p in SERVICES:
        if not any(key in p["services"] for p in points):
            continue
        charts = service_charts(key, points, resources, bands)
        configs += charts
        nav.append(f'<a class="nav-link" href="#{key}">{title}</a>')
        sections.append(f'<section id="{key}"><h2>{title}: whole run</h2>{chart_grid(charts)}</section>')

    if any(p["nodes"] for p in points):
        charts = node_charts(points, resources, bands)
        configs += charts
        nav.append('<a class="nav-link" href="#nodes">Nodes</a>')
        sections.append(f"""<section id="nodes"><h2>Nodes: whole run</h2>
<p class="note">Whole-node usage from node-exporter. On a single-machine cluster every node shares
one kernel and reports the same machine-wide CPU and disk.</p>
{node_table(points)}{chart_grid(charts)}</section>""")

    # Each test point over its own window, at the series' finer step.
    tp_sections = []
    for index, p in enumerate(points):
        groups = test_point_charts(index, p)
        if not groups:
            continue
        step = (p.get("timeseries") or {}).get("step")
        body = "".join(f"<h3>{title}</h3>{chart_grid(charts)}" for title, charts in groups)
        configs += [c for _t, charts in groups for c in charts]
        tp_sections.append(f"""<section id="tp-{index}"><h2>Test point: {html.escape(p['display'])}</h2>
<p class="note">The test point's {p['seconds']:.0f}s window (shaded) and 30s either side, sampled every
{step}s. Each sample is a rate over the {(p.get("timeseries") or {}).get("rateSeconds", 30)}s around it.
Prometheus scrapes every 10s, so a short test point has only a few samples.</p>
{body}</section>""")
        if len(tp_sections) == 1:
            nav.append('<div class="nav-label">Test points</div>')
        nav.append(f'<a class="nav-link" href="#tp-{index}">{html.escape(p["display"])}</a>')
    sections += tp_sections

    meta = [("Mode", data.get("mode")), ("Environment", metadata.get("environment")),
            ("Kafka Config", metadata.get("kafkaConfigName")), ("Start", metadata.get("beginTime")),
            ("End", metadata.get("endTime")),
            ("Per-pod metrics", "cgroup (cAdvisor)" if cgroup else "not collected")]
    meta_html = "".join(f'<div><span class="label">{k}:</span> {html.escape(text_or_empty(v))}</div>'
                        for k, v in meta)
    return PAGE.format(
        title=html.escape(f"Dimster Resources - {name}"),
        name=html.escape(name),
        meta=meta_html,
        nav="".join(nav),
        sections="".join(sections),
        configs=json.dumps(configs),
    )


PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
       background: #f8f9fa; color: #1a1a2e; }}
.sidebar {{ position: fixed; top: 0; left: 0; width: 200px; height: 100vh; background: #1a1a2e;
           padding: 24px 0; overflow-y: auto; }}
.sidebar-title {{ font-size: 15px; font-weight: 700; color: #fff; padding: 0 20px 16px;
                 border-bottom: 1px solid #2a2a4e; margin-bottom: 12px; }}
.nav-link {{ display: block; padding: 8px 20px; color: #a0a0b8; text-decoration: none; font-size: 13px;
            border-left: 3px solid transparent; }}
.nav-link:hover {{ background: #2a2a4e; color: #fff; border-left-color: #4e79a7; }}
.nav-label {{ font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #6c6c8a;
             padding: 14px 20px 6px; }}
.main {{ margin-left: 200px; padding: 32px 40px; max-width: 1500px; }}
.header {{ background: #fff; border-radius: 10px; box-shadow: 0 1px 4px rgba(0,0,0,0.06);
          padding: 24px 32px; margin-bottom: 32px; }}
.header h1 {{ font-size: 22px; margin-bottom: 14px; }}
.meta {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 8px 24px;
        font-size: 13px; font-weight: 600; }}
.meta .label {{ color: #6c6c8a; font-weight: 500; }}
section {{ margin-bottom: 48px; }}
h2 {{ font-size: 18px; margin-bottom: 16px; padding-bottom: 8px; border-bottom: 2px solid #e0e0e0; }}
h3 {{ font-size: 15px; color: #4a4a6a; margin: 24px 0 12px; }}
.note {{ font-size: 13px; color: #6a6a7a; padding: 10px 14px; background: #f0f1f3; border-radius: 6px;
        margin-bottom: 14px; }}
.note code {{ background: #e0e2e6; padding: 1px 5px; border-radius: 3px; }}
.table-wrap {{ overflow-x: auto; margin-bottom: 20px; }}
table {{ width: 100%; border-collapse: collapse; font-size: 12px; background: #fff; border-radius: 8px;
        overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }}
th {{ background: #f0f1f3; padding: 8px 12px; text-align: left; font-weight: 600; color: #4a4a6a; }}
td {{ padding: 7px 12px; border-top: 1px solid #eee; white-space: nowrap; }}
td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
.chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
.chart-card {{ background: #fff; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,0.06); padding: 20px; }}
@media (max-width: 900px) {{ .chart-grid {{ grid-template-columns: 1fr; }} .sidebar {{ display: none; }}
                            .main {{ margin-left: 0; padding: 16px; }} }}
</style>
</head>
<body>
<nav class="sidebar"><div class="sidebar-title">Dimster Resources</div>{nav}</nav>
<div class="main">
<div class="header"><h1>{name}</h1><div class="meta">{meta}</div></div>
{sections}
</div>
<script>
// Shades each test point's measured window on the over-time charts.
Chart.register({{
    id: 'testPoints',
    beforeDatasetsDraw(chart, _args, opts) {{
        if (!opts || !opts.bands) return;
        const {{ctx, chartArea, scales}} = chart;
        ctx.save();
        opts.bands.forEach((b, i) => {{
            const x0 = Math.max(scales.x.getPixelForValue(b.from), chartArea.left);
            const x1 = Math.min(scales.x.getPixelForValue(b.to), chartArea.right);
            if (x1 <= x0) return;
            ctx.fillStyle = i % 2 ? 'rgba(78,121,167,0.07)' : 'rgba(242,142,43,0.07)';
            ctx.fillRect(x0, chartArea.top, x1 - x0, chartArea.bottom - chartArea.top);
            ctx.fillStyle = '#8a8a9a';
            ctx.font = '10px sans-serif';
            ctx.fillText(b.label, x0 + 3, chartArea.top + 11, x1 - x0 - 6);
        }});
        ctx.restore();
    }}
}});
const chartConfigs = {configs};
chartConfigs.forEach(c => {{
    const el = document.getElementById(c.id);
    if (el) new Chart(el, c.config);
}});
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate the hardware resources report from Dimster results.")
    parser.add_argument("--results", required=True, help="Directory to scan for result JSON files")
    parser.add_argument("--output", required=True, help="Directory to write HTML files")
    args = parser.parse_args()

    if not os.path.isdir(args.results):
        print(f"Error: results directory does not exist: {args.results}", file=sys.stderr)
        sys.exit(1)

    generated = 0
    for path, data in find_result_files(args.results):
        metrics_path = find_run_artifact(path, args.results, RESOURCE_METRICS_FILE)
        resources = load_resource_metrics(metrics_path) if metrics_path else None
        if not resources:
            continue
        points = analyze(data, resources)
        if not points:
            continue
        rel = os.path.splitext(os.path.relpath(path, args.results))[0]
        out_path = os.path.join(args.output, rel + "-resources.html")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w") as fh:
            fh.write(build_html(data, points, resources))
        print(f"Generated: {out_path}")

        base = os.path.splitext(path)[0]
        if resources.get("cgroupMetrics"):
            write_csv(base + "-resources.csv", service_rows(points))
        write_csv(base + "-resources-nodes.csv", node_rows(points))
        write_csv(base + "-resources-timeseries.csv", timeseries_rows(resources))
        generated += 1

    if generated:
        print(f"Done. Generated resources reports for {generated} result(s).")


if __name__ == "__main__":
    main()
