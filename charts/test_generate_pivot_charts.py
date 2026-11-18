"""Tests for the pivot view: one section per test point with the scenarios as
the comparison axis, for one result (`dimster pivot`) or several
(`dimster compare --pivot`), over run and drain-backlog results."""

import colorsys
import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import generate_pivot_charts as pivot

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "generate_pivot_charts.py")
SCENARIOS = ("1-consumer", "4-consumers")
TEST_LABELS = ("16KB", "64KB")


def _run_test(label, rate, quantiles=True):
    """A run test point whose rates and latencies all derive from `rate`."""
    results = {
        "publishRate": [rate, rate],
        "consumeRate": [rate - 1, rate + 1],
        "publishLatency99pct": [2.0, 2.1],
        "endToEndLatency99pct": [5.0, 5.2],
        "publishDelayLatency99pct": [1.0, 1.1],
        "aggregatedPublishLatency50pct": rate / 1000,
        "aggregatedEndToEndLatency50pct": rate / 500,
    }
    if quantiles:
        results["aggregatedPublishLatencyQuantiles"] = {"0.5": 1.0, "0.99": 2.0}
        results["aggregatedEndToEndLatencyQuantiles"] = {"0.5": 3.0, "0.99": 5.0}
    return {"label": label,
            "workloadConfig": {"partitionsPerTopic": 1, "consumersPerGroup": 1,
                               "messageSize": 1000},
            "results": results}


def _run_result(base_rate, quantiles=True):
    """A run result of 2 scenarios x 2 test points. Scenario si, test ti
    publishes at base_rate + 100*si + 10*ti, so every cell is identifiable."""
    return {
        "fileType": "dimster-result",
        "mode": "run",
        "metadata": {"name": "batching", "environment": "local"},
        "runConfig": {"sampleRateMillis": 10000, "testDurationMinutes": 1,
                      "warmupDurationMinutes": 0},
        "baseKafkaConfig": {"replicationFactor": 1},
        "scenarioResults": [
            {"name": name, "tests": [_run_test(label, base_rate + 100 * si + 10 * ti, quantiles)
                                     for ti, label in enumerate(TEST_LABELS)]}
            for si, name in enumerate(SCENARIOS)],
    }


def _drain_result(base_ms):
    """A drain-backlog result of 2 scenarios x 2 test points. Scenario si,
    test ti drains in base_ms + 10s*si + 1s*ti, reading 400 MB."""
    def test(si, ti, label):
        return {"label": label,
                "workloadConfig": {"partitionsPerTopic": 1, "consumersPerGroup": 1 + 3 * si},
                "drain": {"drainDurationMs": base_ms + 10_000 * si + 1_000 * ti,
                          "buildDurationMs": 70_000,
                          "drainStartTime": f"2026-11-02T1{si}:5{ti}:00Z",
                          "drainEndTime": f"2026-11-02T1{si}:5{ti}:30Z",
                          "backlogMessages": 7_500_000, "backlogSizeBytes": 2_000_000_000,
                          "backlogMessagesOverTime": [7_500_000, 2_000_000, 0]},
                "performance": {"consumeRate": [0, 250_000, 300_000],
                                "bytesReceived": 400e6}}

    return {
        "fileType": "dimster-result",
        "mode": "drain-backlog",
        "metadata": {"name": "drain-batching", "environment": "local"},
        "config": {"baseKafkaConfig": {"replicationFactor": 3},
                   "drainBacklogConfig": {"backlogSizeGB": 2.0, "sampleRateMillis": 5000}},
        "scenarioResults": [
            {"name": name, "tests": [test(si, ti, label) for ti, label in enumerate(TEST_LABELS)]}
            for si, name in enumerate(SCENARIOS)],
    }


def _write_entries(directory, results):
    """Write each (label, result) to its own run directory, as runs lay them out."""
    entries = []
    for label, data in results:
        os.makedirs(os.path.join(directory, label))
        path = os.path.join(directory, label, "result.json")
        with open(path, "w") as fh:
            json.dump(data, fh)
        entries.append((label, path, data))
    return entries


def _charts_by_title(configs, t_idx):
    """{title: chart config} of one section's charts."""
    return {c["config"]["options"]["plugins"]["title"]["text"]: c for c in configs
            if c["id"].startswith(f"chart-{t_idx}-")}


def _datasets(chart):
    return [(ds["label"], ds["data"]) for ds in chart["config"]["data"]["datasets"]]


def _hls(color):
    return colorsys.rgb_to_hls(*(int(color[i:i + 2], 16) / 255 for i in (1, 3, 5)))


class RunPivotTest(unittest.TestCase):

    def test_single_result_pivots_scenarios_onto_the_x_axis(self):
        entries = [("r", "r.json", _run_result(1000))]
        pivot.validate_pivot(entries)
        configs, sections = pivot.build_pivot_chart_configs(entries)

        self.assertEqual(sorted(k for k in sections), [0, 1])
        tp = _charts_by_title(configs, 1)["Throughput (msg/s)"]
        self.assertEqual(tp["config"]["data"]["labels"], list(SCENARIOS))
        # Second test point (64KB): 1010 for the first scenario, 1110 for the second.
        self.assertEqual(_datasets(tp), [("Publish", [1010.0, 1110.0]),
                                         ("Consume", [1010.0, 1110.0])])
        rate = _charts_by_title(configs, 1)["Publish Rate over Time"]
        self.assertEqual([ds["label"] for ds in rate["config"]["data"]["datasets"]],
                         list(SCENARIOS))
        self.assertEqual({tuple(ds.get("borderDash", [])) for ds in
                          rate["config"]["data"]["datasets"]}, {()})

    def test_compare_pivot_has_a_bar_per_scenario_per_entry(self):
        entries = [("a", "a.json", _run_result(1000)), ("b", "b.json", _run_result(2000))]
        pivot.validate_pivot(entries)
        configs, _ = pivot.build_pivot_chart_configs(entries)

        tp = _charts_by_title(configs, 0)["Throughput (msg/s)"]
        self.assertEqual(tp["config"]["data"]["labels"], list(SCENARIOS))
        self.assertEqual(_datasets(tp)[0], ("a Publish", [1000.0, 1100.0]))
        self.assertEqual(_datasets(tp)[2], ("b Publish", [2000.0, 2100.0]))
        p50 = _charts_by_title(configs, 0)["Aggregated Latency - Publish"]
        self.assertEqual(_datasets(p50)[5], ("b p50", [2.0, 2.1]))

    def test_compare_pivot_lines_take_colour_from_scenario_and_dash_from_entry(self):
        entries = [("a", "a.json", _run_result(1000)), ("b", "b.json", _run_result(2000))]
        configs, _ = pivot.build_pivot_chart_configs(entries)

        lines = _charts_by_title(configs, 0)["Publish Rate over Time"]["config"]["data"]["datasets"]
        self.assertEqual([ds["label"] for ds in lines],
                         ["1-consumer / a", "1-consumer / b", "4-consumers / a", "4-consumers / b"])
        self.assertEqual(lines[0]["borderColor"], lines[1]["borderColor"])
        self.assertNotEqual(lines[0]["borderColor"], lines[2]["borderColor"])
        self.assertEqual(lines[0].get("borderDash", []), lines[2].get("borderDash", []))
        self.assertNotEqual(lines[0].get("borderDash", []), lines[1].get("borderDash"))
        self.assertEqual(lines[3]["data"], [2100.0, 2100.0])

    def test_every_scenario_has_its_own_colour(self):
        data = _run_result(1000)
        data["scenarioResults"] = [dict(data["scenarioResults"][0], name=f"s{i}")
                                   for i in range(16)]
        configs, _ = pivot.build_pivot_chart_configs([("r", "r.json", data)])

        lines = _charts_by_title(configs, 0)["Publish Rate over Time"]["config"]["data"]["datasets"]
        colors = [ds["borderColor"] for ds in lines]
        self.assertEqual(len(set(colors)), 16)
        # Two scenarios get different hues, not two shades of one.
        hue_gap = abs(_hls(colors[0])[0] - _hls(colors[1])[0])
        self.assertGreater(min(hue_gap, 1 - hue_gap), 0.1)

    def test_entries_past_the_dash_styles_take_a_shade_of_the_scenario_hue(self):
        entries = [(f"e{i}", f"e{i}.json", _run_result(1000 + i)) for i in range(6)]
        configs, _ = pivot.build_pivot_chart_configs(entries)

        lines = _charts_by_title(configs, 0)["Publish Rate over Time"]["config"]["data"]["datasets"]
        first_scenario = lines[:6]
        dashes = [tuple(ds.get("borderDash", [])) for ds in first_scenario]
        colors = [ds["borderColor"] for ds in first_scenario]
        self.assertEqual(len(set(dashes[:4])), 4)
        self.assertEqual(colors[:4], [colors[0]] * 4)
        # e4 reuses e0's dash in a darker shade of the same hue.
        self.assertEqual(dashes[4], dashes[0])
        self.assertNotEqual(colors[4], colors[0])
        self.assertAlmostEqual(_hls(colors[4])[0], _hls(colors[0])[0], places=2)
        self.assertLess(_hls(colors[4])[1], _hls(colors[0])[1])

    def test_line_legends_draw_each_series_dash(self):
        entries = [("a", "a.json", _run_result(1000)), ("b", "b.json", _run_result(2000))]
        configs, sections = pivot.build_pivot_chart_configs(entries)
        page = pivot.build_pivot_html(entries, configs, sections)

        for chart in configs:
            if chart["config"]["type"] in ("line", "scatter"):
                labels = chart["config"]["options"]["plugins"]["legend"]["labels"]
                self.assertEqual(labels["pointStyle"], "line", chart["id"])
        self.assertIn("Line colour is the scenario; line style is the entry", page)

    def test_series_without_quantiles_are_left_out_and_noted(self):
        entries = [("a", "a.json", _run_result(1000)),
                   ("b", "b.json", _run_result(2000, quantiles=False))]
        configs, sections = pivot.build_pivot_chart_configs(entries)

        pct = _charts_by_title(configs, 0)["Publish Latency Percentile Distribution"]
        self.assertEqual([ds["label"] for ds in pct["config"]["data"]["datasets"]],
                         ["1-consumer / a", "4-consumers / a"])
        self.assertEqual(sections[0]["notes"][pct["id"]],
                         "Omitted (no quantiles): 1-consumer / b, 4-consumers / b")

    def test_page_has_a_section_per_test_point_and_a_table_per_entry(self):
        entries = [("a", "a.json", _run_result(1000)), ("b", "b.json", _run_result(2000))]
        configs, sections = pivot.build_pivot_chart_configs(entries)
        page = pivot.build_pivot_html(entries, configs, sections)

        self.assertIn("<h2>Test point: 16KB</h2>", page)
        self.assertIn("<h2>Test point: 64KB</h2>", page)
        self.assertEqual(page.count("<th>Scenario</th>"), 4)
        self.assertIn('<h4 class="table-heading">b</h4>', page)
        self.assertIn("a (solid)", page)
        self.assertIn("b (dashed)", page)
        self.assertIn("<title>Dimster Compare (pivot) - a vs b - batching</title>", page)


class DrainPivotTest(unittest.TestCase):

    def test_single_drain_result_pivots_drain_bars_by_scenario(self):
        with tempfile.TemporaryDirectory() as d:
            entries = _write_entries(d, [("r", _drain_result(20_000))])
            pivot.validate_pivot(entries)
            configs, sections = pivot.build_pivot_chart_configs(entries)
            page = pivot.build_pivot_html(entries, configs, sections)

        charts = _charts_by_title(configs, 1)
        self.assertEqual(set(charts), {"Drain Duration", "Consume Throughput during the Drain",
                                       "Backlog Messages over Time", "Consume Rate over Time"})
        duration = charts["Drain Duration"]
        self.assertEqual(duration["config"]["data"]["labels"], list(SCENARIOS))
        self.assertEqual(_datasets(duration), [("r", [21.0, 31.0])])
        # 400 MB over 21s and 31s.
        self.assertEqual(_datasets(charts["Consume Throughput during the Drain"]),
                         [("r", [19.05, 12.9])])
        self.assertEqual([ds["label"] for ds in
                          charts["Backlog Messages over Time"]["config"]["data"]["datasets"]],
                         list(SCENARIOS))
        # The drain table lists the scenarios at each test point.
        self.assertEqual(page.count("<th>Drain Time</th>"), 2)
        self.assertIn("<td>4-consumers</td>", page)
        self.assertIn("<td>31.0s</td>", page)

    def test_compare_drain_pivot_with_disk_reads_for_one_entry(self):
        with tempfile.TemporaryDirectory() as d:
            entries = _write_entries(d, [("a", _drain_result(20_000)),
                                         ("b", _drain_result(40_000))])
            # b measured its second scenario's first drain: 1 GB of 2 GB.
            drain = entries[1][2]["scenarioResults"][1]["tests"][0]["drain"]
            with open(os.path.join(d, "b", "drain-disk-reads.json"), "w") as fh:
                json.dump({"drains": [{"drainStartTime": drain["drainStartTime"],
                                       "drainEndTime": drain["drainEndTime"],
                                       "diskReadBytes": 1_000_000_000}]}, fh)
            pivot.validate_pivot(entries)
            configs, _ = pivot.build_pivot_chart_configs(entries)

        first, second = _charts_by_title(configs, 0), _charts_by_title(configs, 1)
        self.assertEqual(_datasets(first["Drain Duration"]),
                         [("a", [20.0, 30.0]), ("b", [40.0, 50.0])])
        self.assertEqual(_datasets(first["Disk Read Fraction (disk reads / bytes drained)"]),
                         [("a", [None, None]), ("b", [None, 0.5])])
        # Only measured test points get the fraction chart.
        self.assertNotIn("Disk Read Fraction (disk reads / bytes drained)", second)
        self.assertEqual(len(first["Consume Rate over Time"]["config"]["data"]["datasets"]), 4)


class ValidationTest(unittest.TestCase):

    def assert_rejected(self, entries):
        with self.assertRaises(SystemExit):
            pivot.validate_pivot(entries)

    def test_correctness_results_are_rejected(self):
        data = _run_result(1000)
        data["mode"] = "correctness"
        self.assert_rejected([("c", "c.json", data)])

    def test_a_single_scenario_is_rejected(self):
        data = _run_result(1000)
        del data["scenarioResults"][1]
        self.assert_rejected([("r", "r.json", data)])

    def test_unequal_test_counts_within_an_entry_are_rejected(self):
        data = _run_result(1000)
        del data["scenarioResults"][1]["tests"][1]
        self.assert_rejected([("a", "a.json", _run_result(1000)), ("b", "b.json", data)])

    def test_entries_with_different_scenarios_are_rejected(self):
        data = _run_result(1000)
        data["scenarioResults"][0]["name"] = "other"
        self.assert_rejected([("a", "a.json", _run_result(1000)), ("b", "b.json", data)])

    def test_a_run_and_a_drain_result_are_rejected(self):
        self.assert_rejected([("a", "a.json", _run_result(1000)),
                              ("b", "b.json", _drain_result(20_000))])


class ScriptTest(unittest.TestCase):
    """The script's arguments and output file names."""

    def run_script(self, *args):
        return subprocess.run([sys.executable, SCRIPT, *args], capture_output=True, text=True)

    def test_single_input_writes_the_workload_pivot_page(self):
        with tempfile.TemporaryDirectory() as d:
            [(_l, path, _d)] = _write_entries(d, [("r", _run_result(1000))])
            done = self.run_script("--input", path, "--output", d, "--scenarios", "1,0")
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertTrue(os.path.isfile(os.path.join(d, "pivot-batching-scenarios-1,0.html")))

    def test_entries_write_the_named_compare_pivot_page(self):
        with tempfile.TemporaryDirectory() as d:
            entries = _write_entries(d, [("a", _drain_result(20_000)),
                                         ("b", _drain_result(40_000))])
            done = self.run_script(*[arg for label, path, _ in entries
                                     for arg in ("--entry", f"{label}={path}")],
                                   "--name", "drains", "--output", d)
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertTrue(os.path.isfile(os.path.join(d, "compare-drains-pivot.html")))
            self.assertFalse(os.path.exists(os.path.join(d, "compare-drains.html")))

    def test_scenarios_filter_applies_to_every_entry(self):
        with tempfile.TemporaryDirectory() as d:
            first, second = _run_result(1000), _run_result(2000)
            # A third scenario with fewer test points would fail validation
            # unless the filter leaves it out.
            for data in (first, second):
                data["scenarioResults"].append({"name": "short", "tests": [_run_test("16KB", 1)]})
            entries = _write_entries(d, [("a", first), ("b", second)])
            done = self.run_script("--entry", f"a={entries[0][1]}", "--entry",
                                   f"b={entries[1][1]}", "--scenarios", "1,0",
                                   "--name", "n", "--output", d)
            self.assertEqual(done.returncode, 0, done.stderr)
            with open(os.path.join(d, "compare-n-pivot.html")) as fh:
                page = fh.read()
        self.assertIn('"labels": ["4-consumers", "1-consumer"]', page)
        self.assertNotIn("<td>short</td>", page)

    def test_scenarios_filter_needs_matching_scenario_counts(self):
        with tempfile.TemporaryDirectory() as d:
            short = _run_result(2000)
            del short["scenarioResults"][1]
            entries = _write_entries(d, [("a", _run_result(1000)), ("b", short)])
            done = self.run_script("--entry", f"a={entries[0][1]}", "--entry",
                                   f"b={entries[1][1]}", "--scenarios", "0", "--output", d)
        self.assertEqual(done.returncode, 1)
        self.assertIn("--scenarios needs every entry to have the same scenarios", done.stderr)


if __name__ == "__main__":
    unittest.main()
