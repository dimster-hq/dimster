import unittest
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import generate_run_charts


class GenerateRunChartsTest(unittest.TestCase):
    def test_build_html_allows_null_top_level_config_blocks(self):
        """Interrupted runs can have null config blocks but still chart completed tests."""
        data = {
            "mode": "run",
            "metadata": {
                "name": "stopped run",
                "kafkaConfigName": None,
                "environment": "localBeefy",
                "system": "Kafka",
                "beginTime": "2026-07-02-16-09-41",
                "endTime": "2026-07-02-16-15-00",
                "series": "broker-count",
                "extraDimensions": {"brokers": 5},
            },
            "baseKafkaConfig": None,
            "runConfig": {
                "testDurationMinutes": 3,
                "warmupDurationMinutes": 2,
                "sampleRateMillis": 10000,
            },
            "availabilityConfig": None,
            "scenarioResults": [
                {
                    "name": "partial",
                    "tests": [
                        {
                            "label": "1K/s",
                            "kafkaConfig": {"replicationFactor": 3},
                            "workloadConfig": {"messageSize": 500},
                            "results": {
                                "publishRate": [1000.0],
                                "consumeRate": [1000.0],
                            },
                        }
                    ],
                }
            ],
        }

        chart_configs, chart_sections = generate_run_charts.build_chart_configs(data)
        main_configs, main_sections = generate_run_charts.filter_for_mode(
            chart_configs, chart_sections, "main"
        )
        html = generate_run_charts.build_html(data, main_configs, main_sections)

        self.assertIn("stopped run", html)
        self.assertIn("localBeefy", html)
        self.assertIn("Metadata series", html)
        self.assertIn("broker-count", html)
        self.assertIn("Dimension brokers", html)
        self.assertNotIn(">None<", html)

    def test_flink_latency_split_is_charted_only_when_present(self):
        """Results of Flink test points carry the e2e split at the Flink job."""
        def run(results):
            return {"mode": "run", "runConfig": {"sampleRateMillis": 1000},
                    "scenarioResults": [{"name": "s", "tests": [{"label": "t", "results": results}]}]}

        def titles(data):
            configs, _ = generate_run_charts.build_chart_configs(data)
            return [c["config"]["options"]["plugins"]["title"]["text"] for c in configs]

        base = {"publishRate": [1.0], "consumeRate": [1.0]}
        self.assertFalse(any("Flink" in t for t in titles(run(base))))

        flink = dict(base,
                     producerToFlinkLatency99pct=[30.0], flinkToConsumerLatency99pct=[70.0],
                     aggregatedProducerToFlinkLatency99pct=30.0,
                     aggregatedFlinkToConsumerLatency99pct=70.0,
                     aggregatedProducerToFlinkLatencyQuantiles={"50.0": 30.0, "99.0": 30.0},
                     aggregatedFlinkToConsumerLatencyQuantiles={"50.0": 70.0, "99.0": 70.0})
        flink_titles = [t for t in titles(run(flink)) if "Flink" in t]
        self.assertEqual([
            "Aggregated Producer → Flink Latency",
            "Aggregated Flink → Consumer Latency",
            "Producer → Flink Latency p99 over Time",
            "Flink → Consumer Latency p99 over Time",
            "Producer → Flink Latency Percentile Distribution",
            "Flink → Consumer Latency Percentile Distribution",
        ], flink_titles)

    def test_avg_mbs_prefers_measured_byte_rates(self):
        """Generated datasets have variable record sizes, so measured MB/s wins."""
        test = {
            "workloadConfig": {"messageSize": 1000},
            "results": {
                "publishRate": [1000.0, 3000.0],
                "consumeRate": [1000.0, 3000.0],
                "publishBytesRate": [0.5, 1.5],
                "consumeBytesRate": [0.25, 0.75],
            },
        }
        self.assertEqual(1.0, generate_run_charts.avg_mbs(test, "publish"))
        self.assertEqual(0.5, generate_run_charts.avg_mbs(test, "consume"))

    def test_avg_mbs_falls_back_to_rate_times_message_size(self):
        """Results written before measured byte rates existed still chart."""
        test = {
            "workloadConfig": {"messageSize": 500},
            "results": {"publishRate": [2000.0], "consumeRate": [4000.0]},
        }
        self.assertEqual(1.0, generate_run_charts.avg_mbs(test, "publish"))
        self.assertEqual(2.0, generate_run_charts.avg_mbs(test, "consume"))

    def test_message_size_cell_marks_dataset_estimates(self):
        self.assertEqual(1000, generate_run_charts.message_size_cell({"messageSize": 1000}))
        self.assertEqual(1000, generate_run_charts.message_size_cell(
            {"messageSize": 1000, "datasetPreset": "payload", "datasetFormat": "RAW"}))
        self.assertEqual("~281 (clicks/AVRO)", generate_run_charts.message_size_cell(
            {"messageSize": 281, "datasetPreset": "clicks", "datasetFormat": "AVRO"}))


if __name__ == "__main__":
    unittest.main()
