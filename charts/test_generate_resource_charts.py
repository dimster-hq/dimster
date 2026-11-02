"""Tests for the resources report: how measured pod and node usage is
grouped into services, the derived figures, bottleneck flags and outputs.

Run with:  python3 test_generate_resource_charts.py
"""

import csv
import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import generate_resource_charts as charts

START = "2026-10-28T10:00:00Z"
END = "2026-10-28T10:05:00Z"
T0 = 1793181600  # START


SECONDS = 300  # every fixture window's measuredSeconds


def totals(metrics, rates):
    """Fixture metrics written as per-second rates, stored as the totals over
    the window that resource-metrics.json holds."""
    return {rates.get(k, k): v * SECONDS if k in rates else v for k, v in metrics.items()}


def pod(cpu=0.0, read=0.0, write=0.0, **extra):
    m = {"cpuCores": cpu, "diskReadBytesPerSec": read, "diskWriteBytesPerSec": write,
         "diskReadIops": 0.0, "diskWriteIops": 0.0, "netRxBytesPerSec": 0.0,
         "netTxBytesPerSec": 0.0}
    m.update(extra)
    return totals(m, charts.POD_RATES)


def window(scenario="sweep", index=0, pods=None, nodes=None, pod_nodes=None, kafka=None):
    """kafka holds the brokers' byte totals over the window."""
    return {"scenario": scenario, "testIndex": index, "label": f"p{index}",
            "start": START, "end": END, "seconds": 300, "measuredSeconds": SECONDS,
            "pods": pods or {}, "podNodes": pod_nodes or {},
            "nodes": {n: totals(m, charts.NODE_RATES) for n, m in (nodes or {}).items()},
            "kafka": kafka or {}}


def run_result(*labels, publish=100.0, consume=100.0, received=None):
    """A run result whose test points produce and consume at the given MB/s,
    their consumers receiving `received` bytes in total."""
    results = {"publishBytesRate": [publish, publish], "consumeBytesRate": [consume, consume]}
    if received is not None:
        results["bytesReceived"] = received
    return {"fileType": "dimster-result", "mode": "run", "metadata": {"name": "w"},
            "scenarioResults": [{"name": "sweep", "tests": [
                {"label": label, "results": dict(results)} for label in labels]}]}


def resources(*windows, cgroup=True, timeseries=None):
    return {"cgroupMetrics": cgroup, "step": 15, "windows": list(windows),
            "timeseries": timeseries or {"times": [], "pods": {}, "nodes": {}}}


class ServicesTest(unittest.TestCase):

    def test_pods_are_grouped_by_service_and_others_left_out(self):
        self.assertEqual(charts.service_of("kafka-0"), "brokers")
        self.assertEqual(charts.service_of("kafka-12"), "brokers")
        self.assertEqual(charts.service_of("flink-taskmanager-54bb89cd7-dv9h7"), "flink")
        self.assertEqual(charts.service_of("flink-jobmanager-b5464f7f4-8jvzr"), "flink")
        self.assertEqual(charts.service_of("benchmark-worker-85bccb976d-z8pgs"), "workers")
        for other in ("benchmark-coordinator-58dd6c5b89-5nsq4", "prometheus-7757747f48-rkqst",
                      "kafka-ui", "dashboard-export-1793181600"):
            self.assertIsNone(charts.service_of(other), other)

    def test_rates_divide_totals_by_the_measured_seconds(self):
        # Deltas cover the span between the scrapes at the window's bounds.
        w = dict(window(pods={"kafka-0": {"cpuSeconds": 598.0, "cpuCoresPeak": 2.5}},
                        nodes={"n1": {"diskReadBytes": 2.99e9, "cpuUtil": 0.4}}),
                 measuredSeconds=299)
        rated = charts.rated_window(w)
        self.assertEqual(rated["pods"]["kafka-0"], {"cpuCores": 2.0, "cpuCoresPeak": 2.5})
        self.assertEqual(rated["nodes"]["n1"], {"diskReadBytesPerSec": 1e7, "cpuUtil": 0.4})

    def test_service_usage_sums_its_pods_and_keeps_the_busiest_peak(self):
        w = window(pods={"kafka-0": pod(2.0, read=1e6, cpuCoresPeak=3.0),
                         "kafka-1": pod(1.0, read=3e6, cpuCoresPeak=1.5),
                         "benchmark-worker-a": pod(4.0)})
        usage = charts.service_usage(charts.rated_window(w), "brokers")
        self.assertEqual(usage["cpuCores"], 3.0)
        self.assertEqual(usage["diskReadBytesPerSec"], 4e6)
        self.assertEqual(usage["pods"], 2)
        self.assertEqual(usage["cpuCoresPeakPod"], 3.0)
        self.assertIsNone(charts.service_usage(charts.rated_window(w), "flink"))


    def test_throttling_is_the_share_of_the_services_periods(self):
        # 3 of 10 periods and 1 of 30: 4 of 40, weighted by periods, not averaged per pod.
        w = window(pods={
            "kafka-0": pod(1.0, cfsPeriodsPerSec=10.0, cfsThrottledPeriodsPerSec=3.0),
            "kafka-1": pod(1.0, cfsPeriodsPerSec=30.0, cfsThrottledPeriodsPerSec=1.0),
            "benchmark-worker-a": pod(1.0)})
        self.assertAlmostEqual(charts.service_usage(charts.rated_window(w), "brokers")["cpuThrottledPct"], 10.0)
        # Without a CPU limit there are no periods, so throttling is unknown, not 0.
        self.assertIsNone(charts.service_usage(charts.rated_window(w), "workers")["cpuThrottledPct"])
        point = charts.analyze(run_result("p0"), resources(w))[0]
        self.assertAlmostEqual(point["services"]["all"]["cpuThrottledPct"], 10.0)
        rows = charts.service_rows([point])
        col = rows[0].index("cpu_throttled_pct")
        self.assertEqual([r[col] for r in rows[1:]], [10.0, "", 10.0])


class DerivedFiguresTest(unittest.TestCase):

    def test_cpu_cost_is_core_seconds_per_gb_of_client_throughput(self):
        data = run_result("p0", publish=200.0, consume=400.0)
        w = window(pods={"kafka-0": pod(2.0), "benchmark-worker-a": pod(1.0)})
        point = charts.analyze(data, resources(w))[0]
        # 2 cores at 0.2 GB/s produced: 10 core-seconds per GB.
        self.assertAlmostEqual(point["services"]["brokers"]["cpuPerGbProduced"], 10.0)
        self.assertAlmostEqual(point["services"]["brokers"]["cpuPerGbConsumed"], 5.0)
        self.assertAlmostEqual(point["services"]["all"]["cpuCores"], 3.0)
        self.assertAlmostEqual(point["services"]["all"]["cpuPerGbProduced"], 15.0)

    def test_cpu_cost_from_totals_when_the_result_has_byte_totals(self):
        data = run_result("p0", publish=1.0, consume=1.0, received=30e9)
        data["scenarioResults"][0]["tests"][0]["results"]["bytesSent"] = 60e9
        # 2 cores over the 300s measured span is 600 core-seconds.
        point = charts.analyze(data, resources(window(pods={"kafka-0": pod(2.0)})))[0]
        self.assertAlmostEqual(point["services"]["brokers"]["cpuPerGbProduced"], 10.0)
        self.assertAlmostEqual(point["services"]["brokers"]["cpuPerGbConsumed"], 20.0)

    def test_no_throughput_has_no_cpu_cost(self):
        data = run_result("p0", publish=0.0, consume=0.0)
        point = charts.analyze(data, resources(window(pods={"kafka-0": pod(2.0)})))[0]
        self.assertIsNone(point["services"]["brokers"]["cpuPerGbProduced"])

    def test_cache_figures_are_the_windows_totals(self):
        data = run_result("p0", received=30e9)
        # 30 MB/s read from disk over the 300 s window: 9 GB.
        w = window(pods={"kafka-0": pod(1.0, read=30e6)},
                   kafka={"bytesOut": 30e9, "replicationBytesOut": 6e9})
        cache = charts.analyze(data, resources(w))[0]["cache"]
        self.assertEqual(cache["diskSource"], "brokers")
        self.assertEqual(cache["consumedBytes"], 30e9)
        self.assertEqual(cache["logicalReadBytes"], 36e9)
        self.assertEqual(cache["diskReadBytes"], 9e9)
        self.assertEqual(cache["cacheReadBytes"], 27e9)
        self.assertAlmostEqual(cache["cacheHitPct"], 75.0)
        self.assertAlmostEqual(cache["diskReadPerConsumedByte"], 0.3)

    def test_consumed_bytes_are_the_results_total_not_an_average_of_rates(self):
        # As in a drain whose samples start with the backlog build: no
        # consumption in most samples, which an average of rates divides by.
        data = {"fileType": "dimster-result", "mode": "drain-backlog",
                "scenarioResults": [{"name": "d", "tests": [{
                    "label": "b", "drain": {"backlogSizeBytes": 2e9},
                    "performance": {"consumeBytesRate": [0.0] * 14 + [15.0] * 5,
                                    "bytesReceived": 4e8}}]}]}
        w = window(scenario="d", pods={"kafka-0": pod(1.0)}, kafka={"bytesOut": 4e8})
        cache = charts.analyze(data, resources(w))[0]["cache"]
        self.assertEqual(cache["consumedBytes"], 4e8)

    def test_results_without_byte_totals_have_no_consumed_figure(self):
        data = run_result("p0")
        w = window(pods={"kafka-0": pod(1.0, read=1e6)}, kafka={"bytesOut": 1e9})
        cache = charts.analyze(data, resources(w))[0]["cache"]
        self.assertIsNone(cache["consumedBytes"])
        self.assertIsNone(cache["diskReadPerConsumedByte"])

    def test_cache_share_is_clamped_when_disk_reads_exceed_logical_reads(self):
        data = run_result("p0")
        w = window(pods={"kafka-0": pod(1.0, read=150e6)}, kafka={"bytesOut": 30e9})
        cache = charts.analyze(data, resources(w))[0]["cache"]
        self.assertEqual(cache["cacheReadBytes"], 0.0)
        self.assertEqual(cache["cacheHitPct"], 0.0)

    def test_without_cgroup_metrics_cache_uses_every_nodes_disk_reads(self):
        data = run_result("p0")
        w = window(nodes={"n1": {"diskReadBytesPerSec": 10e6}, "n2": {"diskReadBytesPerSec": 30e6}},
                   kafka={"bytesOut": 30e9})
        point = charts.analyze(data, resources(w, cgroup=False))[0]
        self.assertEqual(point["services"], {})
        self.assertEqual(point["cache"]["diskSource"], "all nodes")
        self.assertEqual(point["cache"]["diskReadBytes"], 12e9)

    def test_without_kafka_metrics_there_is_no_cache_estimate(self):
        data = run_result("p0")
        point = charts.analyze(data, resources(window(pods={"kafka-0": pod(1.0)})))[0]
        self.assertIsNone(point["cache"])

    def test_drain_disk_reads_are_compared_with_the_backlog(self):
        data = {"fileType": "dimster-result", "mode": "drain-backlog",
                "scenarioResults": [{"name": "d", "tests": [{
                    "label": "b", "drain": {"backlogSizeBytes": 6e9},
                    "performance": {"publishBytesRate": [0.0], "consumeBytesRate": [500.0]}}]}]}
        w = window(scenario="d", pods={"kafka-0": pod(1.0, read=10e6)},
                   kafka={"bytesOut": 150e9})
        cache = charts.analyze(data, resources(w))[0]["cache"]
        # 10 MB/s over 300s is 3 GB of the 6 GB backlog.
        self.assertAlmostEqual(cache["diskReadPerBacklogByte"], 0.5)

    def test_only_measured_test_points_are_reported(self):
        data = run_result("p0", "p1")
        points = charts.analyze(data, resources(window(index=1, pods={"kafka-0": pod(1.0)})))
        self.assertEqual([p["label"] for p in points], ["p1"])


class BottleneckTest(unittest.TestCase):

    def flags(self, **kwargs):
        return charts.find_bottlenecks(charts.rated_window(window(**kwargs)))

    def test_a_pod_at_its_cpu_limit_is_flagged(self):
        flags = self.flags(pods={"kafka-0": pod(1.85, cpuLimitCores=2.0)})
        self.assertEqual([(f["resource"], f["where"], f["services"]) for f in flags],
                         [("CPU", "pod kafka-0", ["Brokers"])])

    def test_a_pod_below_its_limit_or_without_one_is_not(self):
        self.assertEqual(self.flags(pods={"kafka-0": pod(1.5, cpuLimitCores=2.0),
                                          "benchmark-worker-a": pod(8.0)}), [])

    def test_throttling_is_flagged(self):
        flags = self.flags(pods={"benchmark-worker-a": pod(
            1.0, cfsPeriodsPerSec=10.0, cfsThrottledPeriodsPerSec=2.0)})
        self.assertIn("throttled in 20%", flags[0]["detail"])
        self.assertEqual(self.flags(pods={"benchmark-worker-a": pod(
            1.0, cfsPeriodsPerSec=10.0, cfsThrottledPeriodsPerSec=0.5)}), [])

    def test_saturated_node_cpu_names_the_services_on_it(self):
        flags = self.flags(nodes={"n1": {"cpuUtil": 0.95}},
                           pod_nodes={"kafka-0": "n1", "kafka-1": "n2"})
        self.assertEqual(len(flags), 1)
        self.assertEqual(flags[0]["services"], ["Brokers"])
        self.assertFalse(flags[0]["shared"])

    def test_a_peak_alone_flags_node_cpu(self):
        flags = self.flags(nodes={"n1": {"cpuUtil": 0.6, "cpuUtilPeak": 0.99}})
        self.assertIn("peaked at 99%", flags[0]["detail"])
        self.assertEqual(self.flags(nodes={"n1": {"cpuUtil": 0.6, "cpuUtilPeak": 0.9}}), [])

    def test_a_node_hosting_several_services_is_shared(self):
        flags = self.flags(nodes={"n1": {"diskBusy": 0.97}},
                           pod_nodes={"kafka-0": "n1", "benchmark-worker-a": "n1",
                                      "prometheus-x": "n1"})
        self.assertEqual(flags[0]["resource"], "Disk")
        self.assertEqual(flags[0]["services"], ["Brokers", "Workers"])
        self.assertTrue(flags[0]["shared"])

    def test_io_pressure_and_network_saturation(self):
        flags = self.flags(nodes={"n1": {"psiIoWaiting": 0.4, "netSpeedBytesPerSec": 1.25e9,
                                         "netRxBytesPerSec": 1.1e9, "netTxBytesPerSec": 0.1e9}})
        self.assertEqual(sorted(f["resource"] for f in flags), ["Disk", "Network"])
        self.assertEqual(self.flags(nodes={"n1": {"netRxBytesPerSec": 1e12}}), [])


class OutputTest(unittest.TestCase):

    def test_service_csv_has_a_row_per_test_point_and_service(self):
        data = run_result("p0", "p1")
        res = resources(window(index=0, pods={"kafka-0": pod(1.0)}),
                        window(index=1, pods={"kafka-0": pod(2.0), "benchmark-worker-a": pod(1.0)},
                               kafka={"bytesOut": 1e6}))
        rows = charts.service_rows(charts.analyze(data, res))
        header, body = rows[0], rows[1:]
        self.assertEqual([(r[2], r[3]) for r in body],
                         [("p0", "Brokers"), ("p0", "All"),
                          ("p1", "Brokers"), ("p1", "Workers"), ("p1", "All")])
        cpu = header.index("cpu_cores")
        self.assertEqual([r[cpu] for r in body], [1.0, 1.0, 2.0, 1.0, 3.0])
        logical = header.index("kafka_logical_read_mb")
        self.assertEqual([r[logical] for r in body], ["", "", 1.0, "", ""])

    def test_node_csv_is_in_percent_and_mb_with_the_raw_test_label(self):
        data = run_result("p0")
        data["scenarioResults"].append({"name": "other", "tests": []})
        res = resources(window(nodes={"n1": {"cpuUtil": 0.5, "diskReadBytesPerSec": 2e6}},
                               pod_nodes={"kafka-0": "n1"}, pods={"kafka-0": pod(1.0)}))
        point = charts.analyze(data, res)[0]
        self.assertEqual(point["display"], "sweep: p0")
        rows = charts.node_rows([point])
        row = dict(zip(rows[0], rows[1]))
        self.assertEqual((row["label"], row["node"], row["services"]), ("p0", "n1", "Brokers"))
        self.assertEqual((row["cpu_util_pct"], row["disk_read_mb_s"]), (50.0, 2.0))

    def test_timeseries_rows_skip_gaps_and_other_pods(self):
        ts = {"times": [T0, T0 + 15], "nodes": {"n1": {"cpuUtil": [0.5, None]}},
              "pods": {"kafka-0": {"cpuCores": [1.0, 2.0]}, "prometheus-x": {"cpuCores": [9.0, 9.0]}}}
        rows = charts.timeseries_rows({"timeseries": ts})
        self.assertEqual(rows[1:], [
            ["run", "", "", START, 0, "pod", "kafka-0", "Brokers", "cpuCores", 1.0],
            ["run", "", "", "2026-10-28T10:00:15Z", 15, "pod", "kafka-0", "Brokers", "cpuCores", 2.0],
            ["run", "", "", START, 0, "node", "n1", "", "cpuUtil", 0.5],
        ])

    def test_timeseries_rows_include_each_test_points_own_series(self):
        w = window(index=1)
        w["timeseries"] = {"times": [T0 - 30, T0 - 20], "nodes": {},
                           "pods": {"kafka-0": {"cpuCores": [None, 0.4]}}}
        rows = charts.timeseries_rows({"timeseries": {}, "windows": [w]})
        self.assertEqual(rows[1:], [["test point", "sweep", "p1", "2026-10-28T09:59:40Z", 10,
                                     "pod", "kafka-0", "Brokers", "cpuCores", 0.4]])

    def test_rates_are_placed_at_the_middle_of_what_they_average(self):
        # A 30s rate sampled at 40s covers 10s..40s.
        self.assertEqual(charts.scaled_series([T0 + 40], [2.0], 1, T0, rate_s=30), [(25.0, 2.0)])
        self.assertEqual(charts.scaled_series([T0 + 40, T0 + 50], [None, 1.0], 100, T0), [(50, 100.0)])

    def test_each_test_point_gets_its_own_over_time_sections(self):
        data = run_result("p0", "p1")
        own = {"times": [T0 - 30, T0, T0 + 30], "step": 10,
               "pods": {"kafka-0": {"cpuCores": [0.1, 1.0, 1.2]},
                        "benchmark-worker-a": {"cpuCores": [0.0, 0.5, 0.5]}},
               "nodes": {"n1": {"cpuUtil": [0.1, 0.4, 0.5]}}}
        w0, w1 = window(index=0, pods={"kafka-0": pod(1.0)}), window(index=1, pods={"kafka-0": pod(1.0)})
        w0["timeseries"] = own
        page = charts.build_html(data, charts.analyze(data, resources(w0, w1)), resources(w0, w1))
        self.assertIn('href="#tp-0"', page)
        self.assertNotIn('href="#tp-1"', page)      # no series of its own
        self.assertIn("Test point: p0", page)
        self.assertIn('"id": "tp0-brokers-time-0"', page)
        self.assertIn('"id": "tp0-workers-time-0"', page)
        self.assertIn('"id": "tp0-nodes-time-0"', page)
        # Seconds since the test point's start; its window shaded from 0.
        self.assertIn('{"x": -30.0, "y": 0.1}', page)
        self.assertIn('"bands": [{"from": 0, "to": 300, "label": "test point"}]', page)
        self.assertIn("time since test point start (s)", page)
        # The axis spans exactly the window and its margins, not rounded out.
        self.assertIn('"min": -30, "max": 330', page)

    def test_page_shows_services_with_pods_and_bands_each_window(self):
        data = run_result("p0")
        ts = {"times": [T0, T0 + 15], "nodes": {},
              "pods": {"kafka-0": {"cpuCores": [1.0, 2.0]}}}
        res = resources(window(pods={"kafka-0": pod(1.0)}, nodes={"n1": {"cpuUtil": 0.2}}),
                        timeseries=ts)
        page = charts.build_html(data, charts.analyze(data, res), res)
        self.assertIn('href="#brokers"', page)
        self.assertNotIn('href="#flink"', page)
        self.assertIn('href="#nodes"', page)
        self.assertIn('"bands": [{"from": 0.0, "to": 300.0, "label": "p0"}]', page)
        self.assertIn("CPU Throttled (% of periods)", page)
        self.assertIn("CPU, Sum of Pods (cores)", page)

    def test_bands_accept_java_nanosecond_instants(self):
        # Instant.toString() writes up to nine fractional digits.
        point = {"start": "2026-10-28T10:00:00.973920449Z", "end": "2026-10-28T10:05:00.5Z",
                 "display": "p0"}
        [band] = charts.time_bands([point], T0)
        self.assertAlmostEqual(band["from"], 0.97392, places=5)
        self.assertAlmostEqual(band["to"], 300.5, places=5)

    def test_short_windows_measured_over_a_longer_span_are_noted(self):
        data = run_result("p0", "p1")
        short = dict(window(index=0, pods={"kafka-0": pod(1.0)}), seconds=26.0, measuredSeconds=42)
        long = window(index=1, pods={"kafka-0": pod(1.0)})
        res = resources(short, long)
        page = charts.build_html(data, charts.analyze(data, res), res)
        self.assertIn("p0 (26s window, measured over 42s)", page)
        self.assertNotIn("p1 (", page)

    def test_page_without_cgroup_metrics_says_how_to_enable_them(self):
        data = run_result("p0")
        res = resources(window(nodes={"n1": {"cpuUtil": 0.2}}), cgroup=False)
        page = charts.build_html(data, charts.analyze(data, res), res)
        self.assertIn("monitoring.cgroupMetrics: true", page)
        self.assertNotIn('href="#brokers"', page)


class MainTest(unittest.TestCase):
    """The script as `dimster chart` runs it, on a run directory."""

    def run_script(self, results_dir):
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "generate_resource_charts.py")
        out = os.path.join(results_dir, "charts")
        proc = subprocess.run([sys.executable, script, "--results", results_dir, "--output", out],
                              capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return out

    def test_writes_page_and_csvs_beside_the_result(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "w-run.json"), "w") as fh:
                json.dump(run_result("p0"), fh)
            # A run request also carries the mode and must be ignored.
            with open(os.path.join(d, "run-request.json"), "w") as fh:
                json.dump({"fileType": "dimster-run-request", "mode": "run"}, fh)
            with open(os.path.join(d, charts.RESOURCE_METRICS_FILE), "w") as fh:
                json.dump(resources(window(pods={"kafka-0": pod(1.0)}, nodes={"n1": {}})), fh)

            out = self.run_script(d)

            self.assertEqual(os.listdir(out), ["w-run-resources.html"])
            for suffix in ("-resources.csv", "-resources-nodes.csv", "-resources-timeseries.csv"):
                self.assertTrue(os.path.isfile(os.path.join(d, "w-run" + suffix)), suffix)
            with open(os.path.join(d, "w-run-resources.csv")) as fh:
                self.assertEqual(len(list(csv.reader(fh))), 3)  # header, Brokers, All

    def test_results_without_resource_metrics_are_skipped(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "w-run.json"), "w") as fh:
                json.dump(run_result("p0"), fh)
            out = self.run_script(d)
            self.assertFalse(os.path.exists(out))
            self.assertEqual(os.listdir(d), ["w-run.json"])


if __name__ == "__main__":
    unittest.main()
