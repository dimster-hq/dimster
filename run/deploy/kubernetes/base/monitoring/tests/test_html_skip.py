"""Tests that export.py skips the HTML page of a dashboard none of whose
queries returned data (such as the Flink dashboard for a run without Flink),
and still writes dashboards with data. Prometheus is faked.

Run with:  python3 test_html_skip.py
"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from test_snapshot import load_export_namespace, raw_result  # noqa: E402


DASHBOARD = {
    'title': 'Test',
    'panels': [
        {'type': 'timeseries', 'title': 'rate', 'targets': [{'expr': 'some_metric', 'legendFormat': ''}]},
        {'type': 'stat', 'title': 'count', 'targets': [{'expr': 'max(some_metric)', 'legendFormat': ''}]},
    ],
}


class HtmlSkipTests(unittest.TestCase):

    def setUp(self):
        self.ns = load_export_namespace()
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, 'test.json')
        with open(self.path, 'w') as f:
            json.dump(DASHBOARD, f)

    def tearDown(self):
        self.tmp.cleanup()

    def render(self, range_series, instant_value):
        self.ns['prom_query_range'] = lambda url, expr, start, end, step: raw_result(range_series)
        self.ns['prom_query_instant'] = lambda url, expr, time: {
            'status': 'success',
            'data': {'result': [] if instant_value is None else [{'metric': {}, 'value': [time, instant_value]}]},
        }
        return self.ns['process_dashboard_file'](self.path, 'http://prom', 0, 600, 15, {})

    def test_dashboard_without_any_data_is_skipped(self):
        self.assertIsNone(self.render([], None))

    def test_dashboard_with_data_is_rendered(self):
        html = self.render([({'job': 'x'}, [(0, '1'), (15, '2')])], None)
        self.assertIsNotNone(html)
        self.assertIn('Test', html)

    def test_a_stat_value_alone_counts_as_data(self):
        self.assertIsNotNone(self.render([], '3'))


    def test_rows_head_their_panels_and_are_not_data(self):
        with open(self.path, 'w') as f:
            json.dump({'title': 'Rows', 'panels': [
                {'type': 'row', 'title': 'Throughput', 'collapsed': False},
                DASHBOARD['panels'][0],
                {'type': 'row', 'title': 'Hidden', 'collapsed': True,
                 'panels': [DASHBOARD['panels'][1]]},
            ]}, f)
        self.assertIsNone(self.render([], None))   # headings alone are no data
        html = self.render([({'job': 'x'}, [(0, '1'), (15, '2')])], None)
        throughput = html.index('<h3 class="row-heading">Throughput</h3>')
        hidden = html.index('<h3 class="row-heading">Hidden</h3>')
        # Each heading comes before its panels, a collapsed row's included.
        self.assertLess(throughput, html.index('chart_all_0'))
        self.assertLess(hidden, html.index('count'))

if __name__ == '__main__':
    unittest.main()
