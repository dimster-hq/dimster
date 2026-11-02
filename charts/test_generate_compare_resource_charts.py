"""Tests for the resources comparison `dimster compare` writes beside its
charts: which entries have resource data, and how their rows are set side
by side.

Run with:  python3 test_generate_compare_resource_charts.py
"""

import csv
import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import generate_compare_resource_charts as compare
import generate_resource_charts as charts
from test_generate_resource_charts import pod, resources, run_result, window

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "generate_compare_resource_charts.py")


def write_run(directory, name, resource_doc=None, labels=("p0", "p1")):
    """A run result in its own directory, with resource-metrics.json beside
    it when resource_doc is given. Returns the result's path."""
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"{name}.json")
    with open(path, "w") as fh:
        json.dump(run_result(*labels), fh)
    if resource_doc is not None:
        with open(os.path.join(directory, charts.RESOURCE_METRICS_FILE), "w") as fh:
            json.dump(resource_doc, fh)
    return path


def doc(cpu0, cpu1):
    """Resource data whose two test points' brokers use cpu0 and cpu1 cores."""
    return resources(window(index=0, pods={"kafka-0": pod(cpu0)}, kafka={"bytesOut": 1e9}),
                     window(index=1, pods={"kafka-0": pod(cpu1)}))


class CompareResourcesTest(unittest.TestCase):

    def run_script(self, *entries, output):
        args = [sys.executable, SCRIPT, "--output", output]
        for label, path in entries:
            args += ["--entry", f"{label}={path}"]
        proc = subprocess.run(args, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return proc.stdout

    def test_entries_without_resource_data_are_left_out_and_named(self):
        with tempfile.TemporaryDirectory() as d:
            entries = compare.parse_entries([
                "A=" + write_run(os.path.join(d, "a"), "a", doc(1.0, 2.0)),
                "B=" + write_run(os.path.join(d, "b"), "b"),
                "C=" + write_run(os.path.join(d, "c"), "c", doc(3.0, 4.0))])
            measured, missing = compare.entry_resources(entries)
        self.assertEqual([m[0] for m in measured], ["A", "C"])
        self.assertEqual(missing, ["B"])

    def test_rows_set_entries_side_by_side_per_test_point_and_service(self):
        with tempfile.TemporaryDirectory() as d:
            entries = compare.parse_entries([
                "A=" + write_run(os.path.join(d, "a"), "a", doc(1.0, 2.0)),
                "B=" + write_run(os.path.join(d, "b"), "b", doc(3.0, 4.0))])
            measured, _ = compare.entry_resources(entries)
        groups = compare.test_point_groups(measured)
        self.assertEqual([[(label, p["label"]) for label, p in g] for g in groups],
                         [[("A", "p0"), ("B", "p0")], [("A", "p1"), ("B", "p1")]])
        page = charts.summary_table(groups)
        # Test point p0: Brokers for A then B, then All for A then B.
        order = [page.index(f"<td>{svc}</td><td><b>{label}</b></td><td class=\"num\">1</td>"
                            f"<td class=\"num\">{cpu}</td>")
                 for svc, label, cpu in (("Brokers", "A", "1.00"), ("Brokers", "B", "3.00"),
                                         ("<b>All</b>", "A", "1.00"), ("<b>All</b>", "B", "3.00"))]
        self.assertEqual(order, sorted(order))
        self.assertIn("<th>Entry</th>", charts.cache_table(groups))

    def test_writes_the_page_and_labelled_csv(self):
        with tempfile.TemporaryDirectory() as d:
            a = write_run(os.path.join(d, "a"), "a", doc(1.0, 2.0))
            b = write_run(os.path.join(d, "b"), "b", doc(3.0, 4.0))
            out = os.path.join(d, "out")
            self.run_script(("A", a), ("B", b), output=out)
            self.assertEqual(sorted(os.listdir(out)),
                             ["compare-A-vs-B-resources.csv", "compare-A-vs-B-resources.html"])
            with open(os.path.join(out, "compare-A-vs-B-resources.csv")) as fh:
                rows = list(csv.reader(fh))
            with open(os.path.join(out, "compare-A-vs-B-resources.html")) as fh:
                page = fh.read()
        self.assertEqual(rows[0][:3], ["compare_label", "scenario", "test_index"])
        # Per entry: p0 and p1, each Brokers and All.
        self.assertEqual([r[0] for r in rows[1:]], ["A"] * 4 + ["B"] * 4)
        for heading in ("Summary", "Page cache vs disk", "Bottlenecks"):
            self.assertIn(f"<h2>{heading}</h2>", page)
        self.assertNotIn("<canvas", page)

    def test_fewer_than_two_entries_with_data_write_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            a = write_run(os.path.join(d, "a"), "a", doc(1.0, 2.0))
            b = write_run(os.path.join(d, "b"), "b")
            out = os.path.join(d, "out")
            stdout = self.run_script(("A", a), ("B", b), output=out)
            self.assertFalse(os.path.exists(out))
        self.assertIn("only A has resource data", stdout)

    def test_drain_backlog_entries_are_compared_too(self):
        with tempfile.TemporaryDirectory() as d:
            paths = []
            for name, cpu in (("a", 1.0), ("b", 2.0)):
                directory = os.path.join(d, name)
                os.makedirs(directory)
                path = os.path.join(directory, f"{name}.json")
                with open(path, "w") as fh:
                    json.dump({"fileType": "dimster-result", "mode": "drain-backlog",
                               "scenarioResults": [{"name": "sweep", "tests": [
                                   {"label": "p0", "drain": {"backlogSizeBytes": 2e9},
                                    "performance": {"bytesReceived": 4e8}}]}]}, fh)
                with open(os.path.join(directory, charts.RESOURCE_METRICS_FILE), "w") as fh:
                    json.dump(resources(window(index=0, pods={"kafka-0": pod(cpu)})), fh)
                paths.append(path)
            out = os.path.join(d, "out")
            self.run_script(("A", paths[0]), ("B", paths[1]), output=out)
            self.assertIn("compare-A-vs-B-resources.html", os.listdir(out))

    def test_correctness_comparisons_write_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            paths = []
            for name in ("a", "b"):
                path = os.path.join(d, f"{name}.json")
                with open(path, "w") as fh:
                    json.dump({"fileType": "dimster-result", "mode": "correctness",
                               "scenarioResults": []}, fh)
                paths.append(path)
            out = os.path.join(d, "out")
            self.run_script(("A", paths[0]), ("B", paths[1]), output=out)
            self.assertFalse(os.path.exists(out))


if __name__ == "__main__":
    unittest.main()
