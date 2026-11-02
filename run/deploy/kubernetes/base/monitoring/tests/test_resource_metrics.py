"""Tests for the per-test-point resource metrics export.py writes for the
resources report.

Loads the script out of dashboard-export-configmap.yaml and swaps its
Prometheus helpers for fakes that answer by query text, so nothing needs a
live Prometheus.

Run with:  python3 test_resource_metrics.py
"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from test_snapshot import load_export_namespace  # noqa: E402


def vector(series):
    """An instant query response from [(labels, value), ...]."""
    return {'status': 'success', 'data': {'resultType': 'vector', 'result': [
        {'metric': labels, 'value': [0, str(value)]} for labels, value in series]}}


def matrix(series):
    """A range query response from [(labels, [(ts, value), ...]), ...]."""
    return {'status': 'success', 'data': {'resultType': 'matrix', 'result': [
        {'metric': labels, 'values': [[ts, str(v)] for ts, v in values]}
        for labels, values in series]}}


class FakePrometheus:
    """Answers each query with the series of the first registered fragment
    the query contains; anything else gets an empty result."""

    def __init__(self):
        self.instant = []
        self.ranged = []
        self.queries = []

    def on_instant(self, fragment, series):
        self.instant.append((fragment, series))

    def on_range(self, fragment, series):
        self.ranged.append((fragment, series))

    def query_instant(self, _url, expr, time):
        self.queries.append((expr, time))
        for fragment, series in self.instant:
            if fragment in expr:
                return vector(series)
        return vector([])

    def query_range(self, _url, expr, start, end, step):
        self.queries.append((expr, (start, end, step)))
        for fragment, series in self.ranged:
            if fragment in expr:
                return matrix(series)
        return matrix([])


WINDOW = {'scenario': 'sweep', 'testIndex': 1, 'label': 'p2',
          'start': '2026-10-28T10:00:00Z', 'end': '2026-10-28T10:05:00Z'}
START = 1793181600  # 2026-10-28T10:00:00Z


class ResourceMetricsTests(unittest.TestCase):

    def setUp(self):
        self.ns = load_export_namespace()
        self.prom = FakePrometheus()
        self.ns['prom_query_instant'] = self.prom.query_instant
        self.ns['prom_query_range'] = self.prom.query_range

    def test_counter_delta_groups_and_dedupes_both_sides(self):
        q = self.ns['counter_delta_query']('m{a="b"}', 30, by='pod', dedupe_by='pod, interface')
        self.assertEqual(q, 'sum by (pod) (max by (pod, interface) (m{a="b"})) - '
                            'sum by (pod) (max by (pod, interface) (m{a="b"} offset 30s))')
        self.assertEqual(self.ns['counter_delta_query']('m', 30), 'sum(m) - sum(m offset 30s)')

    def test_instants_may_carry_java_nanoseconds(self):
        parse = self.ns['parse_instant']
        self.assertAlmostEqual(parse('2026-10-28T10:00:00.973920449Z'), START + 0.97392, places=5)
        self.assertEqual(parse('2026-10-28T10:00:00Z'), START)

    def test_long_windows_are_read_between_their_bounds(self):
        at, offset_s, divisor = self.ns['resource_eval'](START + 0.4, START + 300.7)
        self.assertEqual((at, offset_s, divisor), (START + 300, 299, 299))

    def test_short_windows_read_past_their_end_and_are_measured_over_that_span(self):
        # A 7s drain: read from before its start to past the scrape after its
        # end, so its totals are complete, and measured over that whole span.
        at, offset_s, measured_s = self.ns['resource_eval'](START + 0.5, START + 7.5)
        slack = self.ns['DISK_READ_SCRAPE_SLACK_S']
        self.assertEqual(at, START + 8 + slack)
        self.assertEqual(offset_s, 8 + slack)
        self.assertEqual(measured_s, offset_s)

    def test_node_utilisation_stays_positive_when_idle_time_follows_a_short_window(self):
        # A 26s drain on 8 cores, busy 2 cores during it and idle after: the
        # idle delta spans the drain plus the slack, about 42s.
        self.prom.on_instant('count by (instance)', [({'instance': 'n:9100'}, 8)])
        at, offset_s, measured_s = self.ns['resource_eval'](START, START + 26)
        idle = 26 * 6 + (offset_s - 26) * 8
        self.prom.on_instant('node_cpu_seconds_total{mode="idle"}) - ', [({'instance': 'n:9100'}, idle)])
        nodes = self.ns['node_window_metrics']('p', at, offset_s, measured_s)
        self.assertTrue(0 < nodes['n:9100']['cpuUtil'] < 0.25, nodes)

    def test_deltas_are_raw_totals_and_drop_counter_resets(self):
        self.prom.on_instant('container_cpu_usage_seconds_total', [
            ({'pod': 'kafka-0'}, 600.0), ({'pod': 'kafka-1'}, -5.0)])
        totals = self.ns['deltas']('p', 'container_cpu_usage_seconds_total{}', 0, 300, 'pod')
        self.assertEqual(totals, {'kafka-0': 600.0})

    def test_pod_metrics_count_containers_once_and_dedupe_network(self):
        self.ns['pod_window_metrics']('p', 'bench', START, 300)
        exprs = [q for q, _ in self.prom.queries]
        cpu = next(q for q in exprs if q.startswith('sum by (pod) (container_cpu_usage'))
        self.assertIn('namespace="bench", container!="", container!="POD"', cpu)
        rx = next(q for q in exprs if 'container_network_receive_bytes_total' in q)
        # Network is per pod (container=""), reported more than once.
        self.assertNotIn('container!=""', rx)
        self.assertIn('max by (pod, interface)', rx)

    def test_window_places_pods_and_names_nodes(self):
        self.prom.on_instant('group by (pod, node)', [
            ({'pod': 'kafka-0', 'node': 'k3d-dimster-server-0'}, 1)])
        self.prom.on_instant('container_cpu_usage_seconds_total{namespace="dimster", container!="",'
                             ' container!="POD"}) - ', [({'pod': 'kafka-0'}, 900.0)])
        self.prom.on_instant('node_uname_info', [
            ({'instance': '172.18.0.2:9100', 'nodename': 'k3d-dimster-server-0'}, 1)])
        self.prom.on_instant('count by (instance)', [({'instance': '172.18.0.2:9100'}, 8)])
        self.prom.on_instant('node_cpu_seconds_total{mode="idle"}) - ', [
            ({'instance': '172.18.0.2:9100'}, 1200.0)])
        self.prom.on_instant('node_disk_io_time_seconds_total', [
            ({'instance': '172.18.0.2:9100', 'device': 'nvme0n1'}, 270.0),
            ({'instance': '172.18.0.2:9100', 'device': 'nvme1n1'}, 3.0)])

        w = self.ns['window_resources']('p', 'dimster', WINDOW)

        self.assertEqual((w['scenario'], w['testIndex'], w['label']), ('sweep', 1, 'p2'))
        self.assertEqual(w['seconds'], 300)
        self.assertEqual(w['measuredSeconds'], 300)
        # Counters are the window's totals; rates are left to the report.
        self.assertEqual(w['pods']['kafka-0']['cpuSeconds'], 900.0)
        self.assertEqual(w['podNodes'], {'kafka-0': 'k3d-dimster-server-0'})
        node = w['nodes']['k3d-dimster-server-0']
        self.assertEqual(node['cores'], 8)
        self.assertEqual(node['cpuUtil'], 0.5)      # 4 of 8 cores idle
        self.assertEqual(node['diskBusy'], 0.9)     # the busiest disk
        self.assertEqual(w['kafka'], {})

    def test_kafka_totals_sum_the_brokers_aggregate_series(self):
        self.prom.on_instant('brokertopicmetrics_bytesoutpersec_count{topic=""}', [({'job': 'kafka-jmx'}, 3e9)])
        kafka = self.ns['kafka_window_metrics']('p', START, 300)
        self.assertEqual(kafka, {'bytesOut': 3e9})

    def test_timeseries_align_to_shared_times(self):
        self.prom.on_range('container_cpu_usage_seconds_total', [
            ({'pod': 'kafka-0'}, [(START, 1.5), (START + 30, 2.0)])])
        self.prom.on_range('node_cpu_seconds_total', [
            ({'instance': '10.0.0.1:9100'}, [(START + 15, 0.25)])])
        self.prom.on_instant('node_uname_info', [
            ({'instance': '10.0.0.1:9100', 'nodename': 'ip-10-0-0-1'}, 1)])

        ts = self.ns['resource_timeseries']('p', 'dimster', START, START + 30, 15)

        self.assertEqual(ts['times'], [START, START + 15, START + 30])
        self.assertEqual(ts['pods']['kafka-0']['cpuCores'], [1.5, None, 2.0])
        self.assertEqual(ts['nodes']['ip-10-0-0-1']['cpuUtil'], [None, 0.25, None])

    def test_timeseries_hold_each_pods_throttled_share(self):
        self.prom.on_range('container_cpu_cfs_throttled_periods_total', [
            ({'pod': 'kafka-0'}, [(START, 0.25)])])
        ts = self.ns['resource_timeseries']('p', 'dimster', START, START, 15)
        self.assertEqual(ts['pods']['kafka-0']['cpuThrottledShare'], [0.25])
        expr = next(q for q, _ in self.prom.queries if 'cfs_throttled' in q)
        self.assertIn(' / sum by (pod) (rate(container_cpu_cfs_periods_total', expr)

    def test_a_test_points_series_covers_its_window_and_margins_finely(self):
        # A 25.7s drain: 30s either side, at the 10s scrape step, rate()
        # over three scrapes rather than the whole run's 1m. Samples are
        # half a rate window (15s) late, so centred they span -30s..+56s.
        self.prom.on_range('container_cpu_usage_seconds_total', [
            ({'pod': 'kafka-0'}, [(START - 15, 0.1), (START + 25, 1.5)])])
        ts = self.ns['window_series']('p', 'dimster', START + 0.5, START + 26.2)
        self.assertEqual(ts['step'], 10)
        self.assertEqual(ts['rateSeconds'], 30)
        self.assertEqual(ts['marginSeconds'], 30)
        self.assertEqual(ts['times'][0], START - 15)
        self.assertEqual(ts['times'][-1], START + 65)   # last step at or before end + 30s + 15s
        self.assertEqual(ts['pods']['kafka-0']['cpuCores'][:5], [0.1, None, None, None, 1.5])
        cpu = next(q for q, _ in self.prom.queries if 'container_cpu_usage' in q)
        self.assertIn('[30s]', cpu)
        self.assertNotIn('[1m]', cpu)

    def test_long_test_points_are_capped_in_points(self):
        ts = self.ns['window_series']('p', 'dimster', START, START + 3 * 3600)
        self.assertEqual(ts['step'], 37)    # ceil((3h + 60s) / 300)
        expr = next(q for q, _ in self.prom.queries if 'container_cpu_usage' in q)
        self.assertIn('[74s]', expr)        # at least two steps, so no sample falls between

    def test_windows_carry_their_own_series(self):
        w = self.ns['window_resources']('p', 'dimster', WINDOW)
        self.assertEqual(w['timeseries']['times'][0], START - 15)

    def test_file_records_whether_cgroup_metrics_were_found(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'resource-metrics.json')
            write = self.ns['write_resource_metrics']
            write('p', 'dimster', [WINDOW, {'start': 'garbage', 'end': 'garbage'}],
                  START, START + 300, 15, path)
            with open(path) as f:
                without = json.load(f)
            self.prom.on_instant('container_cpu_usage_seconds_total', [({'pod': 'kafka-0'}, 30.0)])
            write('p', 'dimster', [WINDOW], START, START + 300, 15, path)
            with open(path) as f:
                with_cgroups = json.load(f)

        self.assertFalse(without['cgroupMetrics'])
        self.assertEqual(len(without['windows']), 1)   # the garbage window is skipped
        self.assertEqual(without['step'], 15)
        self.assertTrue(with_cgroups['cgroupMetrics'])
        self.assertIn('kafka-0', with_cgroups['windows'][0]['pods'])


if __name__ == '__main__':
    unittest.main()
