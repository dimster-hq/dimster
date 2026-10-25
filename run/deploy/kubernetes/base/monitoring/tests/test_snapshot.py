"""Tests for the Prometheus snapshot output of export.py.

Loads the script out of dashboard-export-configmap.yaml the same way
test_compute_windows.py does, then swaps its Prometheus helpers for fakes so
nothing needs a live Prometheus.

Run with:  python3 test_snapshot.py
"""

import gzip
import io
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from test_compute_windows import load_export_script  # noqa: E402


def load_export_namespace():
    """Exec the script body and return its globals, so tests can both call
    functions and replace the Prometheus helpers they look up at call time."""
    ns = {'__name__': 'export_module'}
    exec(compile(load_export_script(), '<dashboard-export>', 'exec'), ns)
    return ns


def raw_result(series):
    """Build a Prometheus range-vector query response from
    [(labels, [(ts, value), ...]), ...]."""
    return {
        'status': 'success',
        'data': {
            'resultType': 'matrix',
            'result': [
                {'metric': labels, 'values': [[ts, v] for ts, v in values]}
                for labels, values in series
            ],
        },
    }


class ReferencedMetricNamesTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ns = load_export_namespace()

    def names(self, exprs, known):
        return self.ns['referenced_metric_names'](exprs, set(known))

    def test_functions_aggregations_and_label_names_are_ignored(self):
        exprs = [
            'sum by (instance) (rate(node_cpu_seconds_total{mode!="idle",instance=~"$instance"}[1m]))',
            'histogram_quantile(0.99, sum by (le) (rate(req_latency_bucket[5m])))',
            'node_load1{instance=~"$instance"} / on(instance) count(node_cpu_seconds_total)',
        ]
        known = ['node_cpu_seconds_total', 'req_latency_bucket', 'node_load1',
                 'node_load5', 'up']
        self.assertEqual(
            self.names(exprs, known),
            ['node_cpu_seconds_total', 'node_load1', 'req_latency_bucket'])

    def test_metric_named_like_a_label_inside_matchers_is_not_picked_up(self):
        # "mode" is both a label here and, contrived, a metric name. Tokens
        # inside {...} and string literals are stripped before matching.
        exprs = ['node_cpu_seconds_total{mode="mode"}']
        self.assertEqual(
            self.names(exprs, ['node_cpu_seconds_total', 'mode']),
            ['node_cpu_seconds_total'])

    def test_unknown_names_are_dropped(self):
        self.assertEqual(self.names(['rate(missing_metric[1m])'], ['other']), [])

    def test_template_variable_definitions_are_parsed(self):
        exprs = ['label_values(node_uname_info, instance)']
        self.assertEqual(
            self.names(exprs, ['node_uname_info', 'instance_count']),
            ['node_uname_info'])


class CollectDashboardExprsTests(unittest.TestCase):

    def test_collects_targets_from_rows_and_template_variables(self):
        ns = load_export_namespace()
        dashboard = {
            'panels': [
                {'type': 'timeseries', 'targets': [{'expr': 'a_total'}, {'refId': 'B'}]},
                {'type': 'row', 'panels': [
                    {'type': 'timeseries', 'targets': [{'expr': 'b_bytes'}]},
                ]},
            ],
            'templating': {'list': [
                {'name': 'instance', 'query': {'query': 'label_values(c_info, instance)'}},
                {'name': 'DS_PROMETHEUS', 'type': 'datasource', 'query': 'prometheus'},
            ]},
        }
        self.assertEqual(
            ns['collect_dashboard_exprs'](dashboard),
            ['a_total', 'b_bytes', 'label_values(c_info, instance)'])


class FetchRawSeriesTests(unittest.TestCase):

    def test_chunks_cover_range_and_boundary_duplicates_are_dropped(self):
        ns = load_export_namespace()
        calls = []

        def fake_instant(_url, expr, time):
            calls.append((expr, time))
            # Both chunks return the boundary sample at t=3600.
            if time == 3600:
                return raw_result([({'__name__': 'm', 'pod': 'a'},
                                    [(0, '1'), (1800, '2'), (3600, '3')])])
            return raw_result([({'__name__': 'm', 'pod': 'a'},
                                [(3600, '3'), (5400, '4')])])

        ns['prom_query_instant'] = fake_instant
        series = ns['fetch_raw_series']('http://p', 'm', 0, 5400, chunk_s=3600)

        self.assertEqual(calls, [('{__name__="m"}[3600s]', 3600),
                                 ('{__name__="m"}[1800s]', 5400)])
        key = (('__name__', 'm'), ('pod', 'a'))
        self.assertEqual(series, {key: {0: '1', 1800: '2', 3600: '3', 5400: '4'}})

    def test_failed_chunk_is_skipped(self):
        ns = load_export_namespace()
        ns['prom_query_instant'] = lambda *_: None
        self.assertEqual(ns['fetch_raw_series']('http://p', 'm', 0, 100), {})


class WriteOpenMetricsTests(unittest.TestCase):

    def test_format_ordering_and_escaping(self):
        ns = load_export_namespace()
        series = {
            (('__name__', 'm_total'), ('path', 'C:\\x "q"\nz')): {20: '2', 10: '1'},
            (('__name__', 'm_total'), ('path', 'a')): {10: 'NaN'},
        }
        out = io.StringIO()
        ns['write_openmetrics'](out, 'm_total', series)
        self.assertEqual(out.getvalue(), (
            '# TYPE m_total unknown\n'
            'm_total{path="C:\\\\x \\"q\\"\\nz"} 1 10\n'
            'm_total{path="C:\\\\x \\"q\\"\\nz"} 2 20\n'
            'm_total{path="a"} NaN 10\n'
        ))

    def test_series_without_labels_has_bare_name(self):
        ns = load_export_namespace()
        out = io.StringIO()
        ns['write_openmetrics'](out, 'up', {(('__name__', 'up'),): {5: '1'}})
        self.assertEqual(out.getvalue(), '# TYPE up unknown\nup 1 5\n')


class WriteSnapshotTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        dash_dir = os.path.join(self.tmp.name, 'dash')
        os.makedirs(dash_dir)
        self.dash_path = os.path.join(dash_dir, 'kafka.json')
        with open(self.dash_path, 'w') as f:
            json.dump({'uid': 'dimster-kafka', 'panels': [
                {'type': 'timeseries', 'targets': [{'expr': 'rate(bytes_total[1m])'}]},
            ]}, f)
        self.snapshot_dir = os.path.join(self.tmp.name, 'prometheus-snapshot')

    def test_writes_gzipped_openmetrics_and_copies_dashboards(self):
        ns = load_export_namespace()
        ns['prom_get'] = lambda *_: {'status': 'success',
                                     'data': ['bytes_total', 'unrelated']}
        queried = []

        def fake_instant(_url, expr, _time):
            queried.append(expr)
            return raw_result([({'__name__': 'bytes_total', 'pod': 'p'}, [(100, '7')])])

        ns['prom_query_instant'] = fake_instant
        n = ns['write_snapshot']('http://p', 0, 200, [(self.dash_path, 'kafka')],
                                 self.snapshot_dir)

        self.assertEqual(n, 1)
        # Only the referenced metric is fetched.
        self.assertTrue(all('bytes_total' in q for q in queried))
        with gzip.open(os.path.join(self.snapshot_dir, 'metrics.om.gz'), 'rt') as f:
            self.assertEqual(f.read(), (
                '# TYPE bytes_total unknown\n'
                'bytes_total{pod="p"} 7 100\n'
                '# EOF\n'
            ))
        with open(os.path.join(self.snapshot_dir, 'dashboards', 'kafka.json')) as f:
            self.assertEqual(json.load(f)['uid'], 'dimster-kafka')

    def test_metric_name_listing_failure_skips_snapshot(self):
        ns = load_export_namespace()
        ns['prom_get'] = lambda *_: None
        n = ns['write_snapshot']('http://p', 0, 200, [(self.dash_path, 'kafka')],
                                 self.snapshot_dir)
        self.assertEqual(n, 0)
        self.assertFalse(os.path.exists(os.path.join(self.snapshot_dir, 'metrics.om.gz')))


class MainOutputsTests(unittest.TestCase):

    def run_main(self, outputs):
        ns = load_export_namespace()
        calls = []
        ns['write_html_dashboards'] = lambda *_: calls.append('html')
        ns['write_snapshot'] = lambda *_: calls.append('snapshot')
        tmp = tempfile.mkdtemp()
        argv = ['export.py', '--prometheus-url', 'http://p', '--start', '0',
                '--end', '10', '--dashboard-dirs', tmp,
                '--output-dir', os.path.join(tmp, 'dashboards')]
        if outputs is not None:
            argv += ['--outputs', outputs]
        old_argv = sys.argv
        sys.argv = argv
        try:
            ns['main']()
        finally:
            sys.argv = old_argv
        return calls

    def test_default_is_html_only(self):
        self.assertEqual(self.run_main(None), ['html'])

    def test_snapshot_only_skips_html(self):
        self.assertEqual(self.run_main('snapshot'), ['snapshot'])

    def test_both(self):
        self.assertEqual(self.run_main('html,snapshot'), ['html', 'snapshot'])

    def test_unknown_output_is_rejected(self):
        with self.assertRaises(SystemExit):
            self.run_main('pdf')


if __name__ == '__main__':
    unittest.main()
