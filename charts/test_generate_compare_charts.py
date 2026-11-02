import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import generate_compare_charts
import generate_correctness_charts


def _run_result(name):
    """Build the smallest run result shape the compare chart builder needs."""
    return {
        "mode": "run",
        "metadata": {
            "name": name,
            "system": "Kafka",
            "environment": "local",
            "kafkaConfigName": "safe",
            "beginTime": "2026-07-17T12:00:00Z",
            "endTime": "2026-07-17T12:10:00Z",
        },
        "runConfig": {
            "sampleRateMillis": 10000,
            "testDurationMinutes": 1,
            "warmupDurationMinutes": 0,
        },
        "baseKafkaConfig": {"replicationFactor": 1},
        "scenarioResults": [
            {
                "name": "default",
                "tests": [
                    {
                        "label": "baseline",
                        "workloadConfig": {
                            "topics": 1,
                            "partitionsPerTopic": 1,
                            "messageSize": 1000,
                            "producersPerTopic": 1,
                            "producerRate": 1000,
                            "consumerGroupsPerTopic": 1,
                            "consumersPerGroup": 1,
                            "messageDistributor": "NO_KEY",
                            "consumerType": "CONSUMER_GROUP",
                        },
                        "results": {
                            "publishRate": [1000.0, 1001.0],
                            "consumeRate": [998.0, 1002.0],
                            "publishLatency99pct": [2.0, 2.1],
                            "endToEndLatency99pct": [5.0, 5.2],
                            "publishDelayLatency99pct": [1.0, 1.1],
                            "aggregatedPublishLatencyQuantiles": {"0.5": 1.0, "0.99": 2.0},
                            "aggregatedEndToEndLatencyQuantiles": {"0.5": 3.0, "0.99": 5.0},
                        },
                    }
                ],
            }
        ],
    }


def _correctness_result(name, duplicate_count, violations, samples=None):
    """Build a correctness result with enough timing data for event bins."""
    return {
        "mode": "correctness",
        "metadata": {
            "name": name,
            "system": "Kafka",
            "environment": "local",
            "kafkaConfigName": "safe",
            "beginTime": "2026-07-21-12-00-00",
            "endTime": "2026-07-21-12-01-00",
        },
        "config": {
            "correctnessConfig": {
                "sampleRateMillis": 10000,
                "testDurationMinutes": 1,
                "gracePeriodSeconds": 5,
            },
            "baseKafkaConfig": {"replicationFactor": 3},
        },
        "scenarioResults": [
            {
                "name": "default",
                "beginTime": "2026-07-21-12-00-00",
                "endTime": "2026-07-21-12-01-00",
                "tests": [
                    {
                        "label": "default",
                        "workloadConfig": {
                            "topics": 1,
                            "partitionsPerTopic": 4,
                            "messageSize": 1000,
                            "producersPerTopic": 4,
                            "producerRate": 1000,
                            "consumerGroupsPerTopic": 1,
                            "consumersPerGroup": 4,
                            "messageDistributor": "PINNED_PARTITIONS",
                            "consumerType": "CONSUMER_GROUP",
                        },
                        "correctness": {
                            "passed": duplicate_count == 0,
                            "lostMessages": 2,
                            "duplicateCount": duplicate_count,
                            "outOfOrderCount": 1,
                            "crcFailureCount": 0,
                            "violations": violations,
                            **(samples or {}),
                        },
                    }
                ],
            }
        ],
    }


class GenerateCompareChartsTest(unittest.TestCase):
    def test_correctness_file_discovery_skips_typed_run_request(self):
        result = _correctness_result("consumer", 0, [])
        result["fileType"] = "dimster-result"
        run_request = {
            "fileType": "dimster-run-request",
            "mode": "correctness",
        }

        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "result.json"), "w") as fh:
                json.dump(result, fh)
            with open(os.path.join(directory, "run-request.json"), "w") as fh:
                json.dump(run_request, fh)

            found = generate_correctness_charts.find_correctness_json_files(directory)

        self.assertEqual(1, len(found))
        self.assertTrue(found[0][0].endswith("result.json"))

    def test_build_compare_charts_accepts_more_than_four_entries(self):
        entries = [
            (f"run-{i}", f"/tmp/run-{i}.json", _run_result(f"run-{i}"))
            for i in range(5)
        ]

        generate_compare_charts.validate_compare(entries)
        chart_configs, chart_sections = generate_compare_charts.build_compare_chart_configs(entries)
        html = generate_compare_charts.build_compare_html(entries, chart_configs, chart_sections)

        self.assertGreater(len(chart_configs), 0)
        self.assertIn("run-4", html)
        self.assertIn("Dimster Compare", html)

    def test_build_correctness_compare_charts_uses_totals_and_weighted_events(self):
        start_ms = 1784635200000
        first = _correctness_result(
            "consumer",
            5,
            [
                {
                    "timestampMs": start_ms + 12000,
                    "type": "DUPLICATE",
                    "count": 3,
                },
                {
                    "timestampMs": start_ms + 22000,
                    "type": "OUT_OF_ORDER",
                    "count": 1,
                },
            ],
        )
        second = _correctness_result(
            "classic",
            1,
            [{"timestampMs": start_ms + 12000, "type": "DUPLICATE", "count": 1}],
        )
        entries = [
            ("consumer", "/tmp/consumer.json", first),
            ("classic", "/tmp/classic.json", second),
        ]

        generate_compare_charts.validate_compare(entries)
        configs, sections = generate_compare_charts.build_compare_chart_configs(entries)
        html = generate_compare_charts.build_compare_html(entries, configs, sections)

        self.assertEqual(7, len(configs))
        self.assertEqual("Message Loss", configs[0]["config"]["options"]["plugins"]["title"]["text"])
        self.assertEqual([2, 2], configs[0]["config"]["data"]["datasets"][0]["data"])
        self.assertEqual([5, 1], configs[1]["config"]["data"]["datasets"][0]["data"])
        duplicate_timeline = configs[4]["config"]["data"]["datasets"][0]["data"]
        self.assertEqual(3, duplicate_timeline[-1])
        self.assertIn("represents 3 of 5 total violations", html)
        self.assertIn(
            "Message loss is intentionally shown only as a final aggregate", html
        )

    def test_correctness_compare_prefers_complete_sampled_counter_arrays(self):
        first = _correctness_result(
            "consumer",
            5,
            [],
            {
                "lostMessagesOverTime": [8, 2],
                "duplicateCountOverTime": [0, 3, 5],
                "outOfOrderCountOverTime": [0, 1, 1],
                "crcFailureCountOverTime": [0, 0, 0],
            },
        )
        second = _correctness_result(
            "classic",
            1,
            [],
            {
                "lostMessagesOverTime": [9, 2],
                "duplicateCountOverTime": [0, 1],
                "outOfOrderCountOverTime": [0, 1],
                "crcFailureCountOverTime": [0, 0],
            },
        )
        entries = [
            ("consumer", "/tmp/consumer.json", first),
            ("classic", "/tmp/classic.json", second),
        ]

        configs, sections = generate_compare_charts.build_compare_chart_configs(entries)
        html = generate_compare_charts.build_compare_html(entries, configs, sections)

        self.assertEqual(7, len(configs))
        self.assertEqual(
            [0, 3, 5], configs[4]["config"]["data"]["datasets"][0]["data"]
        )
        self.assertNotIn("Message Loss over Time", html)
        self.assertNotIn("may be capped", html)

    def test_single_correctness_chart_includes_sampled_counter_arrays(self):
        result = _correctness_result(
            "consumer",
            5,
            [],
            {
                "lostMessagesOverTime": [8, 2],
                "duplicateCountOverTime": [0, 3, 5],
                "outOfOrderCountOverTime": [0, 1, 1],
                "crcFailureCountOverTime": [0, 0, 0],
            },
        )

        configs, sections = generate_correctness_charts.build_chart_configs(result)
        charts_by_title = {
            config["config"]["options"]["plugins"]["title"]["text"]: config
            for config in configs
        }

        self.assertEqual(
            [0, 3, 5],
            charts_by_title["Duplicates over Time"]["config"]["data"]["datasets"][0]["data"],
        )
        self.assertNotIn("Message Loss over Time", charts_by_title)

        # Correctness evidence is grouped ahead of secondary performance data
        # both in the chart model and in the rendered page.
        self.assertEqual((0, 3), sections[0]["violations"])
        self.assertEqual((3, len(configs)), sections[0]["performance"])
        html = generate_correctness_charts.build_html(result, configs, sections)
        violations_heading = html.index('id="scenario-0-violations"')
        first_violation_chart = html.index('<canvas id="chart-0-0"')
        performance_heading = html.index('id="scenario-0-performance"')
        first_performance_chart = html.index('<canvas id="chart-0-3"')
        self.assertLess(violations_heading, first_violation_chart)
        self.assertLess(first_violation_chart, performance_heading)
        self.assertLess(performance_heading, first_performance_chart)


def _drain_result(name, drain_ms=(20_000, 30_000), received=(400e6, 600e6)):
    """A drain-backlog result with one scenario of two test points."""
    return {
        "fileType": "dimster-result",
        "mode": "drain-backlog",
        "metadata": {"name": name, "environment": "local"},
        "config": {"baseKafkaConfig": {"replicationFactor": 3},
                   "drainBacklogConfig": {"backlogSizeGB": 2.0, "sampleRateMillis": 5000,
                                          "reuseBacklog": True, "warmup": True}},
        "scenarioResults": [{"name": "batch.size", "tests": [
            {"label": label,
             "workloadConfig": {"partitionsPerTopic": 1, "consumersPerGroup": 1},
             "drain": {"drainDurationMs": ms, "buildDurationMs": 70_000,
                       "drainStartTime": f"2026-11-02T11:5{i}:00Z",
                       "drainEndTime": f"2026-11-02T11:5{i}:30Z",
                       "backlogMessages": 7_500_000, "backlogSizeBytes": 2_000_000_000,
                       "backlogMessagesOverTime": [7_500_000, 2_000_000, 0]},
             "performance": {"consumeRate": [0, 250_000, 300_000], "bytesReceived": rx,
                             "consumeBytesRate": [0.0, 12.0, 15.0]}}
            for i, (label, ms, rx) in enumerate(zip(("16KB", "64KB"), drain_ms, received))]}],
    }


class DrainCompareTest(unittest.TestCase):
    """Comparing drain-backlog results: drain bars per entry, backlog and
    consume rate overlays per test point, and each entry's drain table."""

    def entries(self, directory, disk_reads_for=()):
        entries = []
        for label, ms in (("run1", (20_000, 30_000)), ("run2", (25_000, 40_000))):
            os.makedirs(os.path.join(directory, label))
            path = os.path.join(directory, label, "drain.json")
            data = _drain_result("w", drain_ms=ms)
            with open(path, "w") as fh:
                json.dump(data, fh)
            if label in disk_reads_for:
                t = data["scenarioResults"][0]["tests"][0]["drain"]
                with open(os.path.join(directory, label, "drain-disk-reads.json"), "w") as fh:
                    json.dump({"drains": [{"drainStartTime": t["drainStartTime"],
                                           "drainEndTime": t["drainEndTime"],
                                           "diskReadBytes": 1_000_000_000}]}, fh)
            entries.append((label, path, data))
        return entries

    def test_drain_bars_compare_each_entry_across_test_points(self):
        with tempfile.TemporaryDirectory() as d:
            entries = self.entries(d)
            generate_compare_charts.validate_compare(entries)
            configs, sections = generate_compare_charts.build_compare_chart_configs(entries)
        bars = {c["config"]["options"]["plugins"]["title"]["text"]: c["config"]["data"]
                for c in configs if c["config"]["type"] == "bar"}
        self.assertEqual(set(bars), {"Drain Duration", "Consume Throughput during the Drain"})
        duration = bars["Drain Duration"]
        self.assertEqual(duration["labels"], ["16KB", "64KB"])
        self.assertEqual([(ds["label"], ds["data"]) for ds in duration["datasets"]],
                         [("run1", [20.0, 30.0]), ("run2", [25.0, 40.0])])
        # bytesReceived over the drain's duration: 400 MB in 20s.
        self.assertEqual(bars["Consume Throughput during the Drain"]["datasets"][0]["data"],
                         [20.0, 20.0])
        self.assertEqual([label for label, _s, _e in sections[0]["per_dimension"]], ["16KB", "64KB"])
        titles = [c["config"]["options"]["plugins"]["title"]["text"] for c in configs
                  if c["config"]["type"] == "line"]
        self.assertEqual(titles, ["Backlog Messages over Time", "Consume Rate over Time"] * 2)

    def test_disk_read_fraction_shown_when_an_entry_measured_it(self):
        with tempfile.TemporaryDirectory() as d:
            configs, _ = generate_compare_charts.build_compare_chart_configs(
                self.entries(d, disk_reads_for=("run2",)))
        fraction = next(c for c in configs if "Disk Read Fraction" in
                        c["config"]["options"]["plugins"]["title"]["text"])
        # 1 GB read of the 2 GB backlog, for run2's first drain only.
        self.assertEqual([ds["data"] for ds in fraction["config"]["data"]["datasets"]],
                         [[None, None], [0.5, None]])

    def test_page_shows_each_entrys_drain_table(self):
        with tempfile.TemporaryDirectory() as d:
            entries = self.entries(d)
            configs, sections = generate_compare_charts.build_compare_chart_configs(entries)
            page = generate_compare_charts.build_compare_html(entries, configs, sections)
        self.assertEqual(page.count("<th>Drain Time</th>"), 2)
        self.assertIn("<td>20.0s</td>", page)
        self.assertIn("Backlog Size:</span> <span class=\"value\">2.0 GB", page)
        self.assertNotIn("Test Duration:", page)

    def test_a_run_and_a_drain_backlog_result_cannot_be_compared(self):
        with tempfile.TemporaryDirectory() as d:
            drain = self.entries(d)[0]
        with self.assertRaises(SystemExit):
            generate_compare_charts.validate_compare([("r", "r.json", _run_result("r")), drain])


if __name__ == "__main__":
    unittest.main()
