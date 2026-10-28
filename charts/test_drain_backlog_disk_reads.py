import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import generate_drain_backlog_charts as charts

START = "2026-10-28T10:00:00.000Z"
END = "2026-10-28T10:00:10Z"


def _result(*drains):
    return {"scenarioResults": [{"name": "s", "tests": [
        {"label": f"t{i}", "drain": d} for i, d in enumerate(drains)]}]}


class DiskReadsTest(unittest.TestCase):
    """Drain disk reads come from drain-disk-reads.json, written by the metrics export."""

    def test_drains_are_matched_by_their_window(self):
        with tempfile.TemporaryDirectory() as run_dir:
            with open(os.path.join(run_dir, charts.DISK_READS_FILE), "w") as fh:
                json.dump({"drains": [{"drainStartTime": START, "drainEndTime": END,
                                       "diskReadBytes": 2000}]}, fh)
            result = os.path.join(run_dir, "w-drain-backlog.json")
            data = _result({"drainStartTime": START, "drainEndTime": END, "backlogSizeBytes": 4000},
                           {"drainStartTime": END, "drainEndTime": END, "backlogSizeBytes": 4000},
                           {"backlogSizeBytes": 4000})

            reads = charts.load_disk_reads(charts.find_disk_reads_file(result, run_dir))

            self.assertEqual(charts.measure_disk_reads(data, reads), {(0, 0): (2000, 0.5)})

    def test_measured_drains_get_the_chart_and_column(self):
        data = _result({"drainStartTime": START, "drainEndTime": END, "backlogSizeBytes": 4_000_000})
        disk_reads = charts.measure_disk_reads(data, {(START, END): 2_000_000})
        configs, counts = charts.build_chart_configs(data, disk_reads)
        self.assertIn("Disk Read Fraction", str(configs))
        self.assertIn("2 MB (50%)", charts.build_scenario_sections(data["scenarioResults"], counts, disk_reads))

    def test_no_disk_reads_file_means_no_disk_reads(self):
        with tempfile.TemporaryDirectory() as run_dir:
            self.assertIsNone(charts.find_disk_reads_file(os.path.join(run_dir, "r.json"), run_dir))
        data = _result({"drainStartTime": START, "drainEndTime": END, "backlogSizeBytes": 1})
        configs, _ = charts.build_chart_configs(data, charts.measure_disk_reads(data, None))
        self.assertNotIn("Disk Read Fraction", str(configs))


if __name__ == "__main__":
    unittest.main()
