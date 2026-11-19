"""Checks the dashboards' broker queries avoid Kafka metrics that misreport:

- Yammer histogram and timer mean and max cover the broker's lifetime, so
  they mostly reflect earlier runs: a mean must be windowed with its count,
  and a max (which can't be windowed) isn't charted.
- With broker and controller in one process (combined mode), both register
  the same un-tagged network gauges and the controller's, nearly always
  idle, is the one exported: NetworkProcessorAvgIdlePercent and the
  per-processor IdlePercent read 1.0 whatever the load.

Run with:  python3 test_dashboard_queries.py
"""

import json
import os
import re
import unittest

DASHBOARDS = os.path.join(os.path.dirname(__file__), '..', 'grafana-dashboards.yaml')

# Yammer histograms/timers exported with lifetime _mean and _max.
YAMMER_HISTOGRAMS = re.compile(
    r'(kafka_network_requestmetrics_[a-z]+|kafka_server_sharepartitionmetrics'
    r'|kafka_log_logflushstats_logflushrateandtimems)_(mean|max)\b')

# Network gauges that report the controller's processors in combined mode.
COMBINED_MODE_GAUGES = re.compile(
    r'kafka_network_socketserver_networkprocessoravgidlepercent|kafka_network_processor_idlepercent')


def dashboard_exprs():
    """(dashboard, panel title, expr) for every target in the ConfigMap's
    dashboards: `  name.json: |` literal blocks indented 4 spaces."""
    blocks, name, buf = {}, None, []
    with open(DASHBOARDS) as f:
        for line in f.read().split('\n'):
            m = re.match(r'^  ([\w.-]+\.json): \|$', line)
            if m:
                if name:
                    blocks[name] = buf
                name, buf = m.group(1), []
            elif name is not None:
                if line.startswith('    ') or line == '':
                    buf.append(line[4:])
                else:
                    blocks[name], name = buf, None
    if name:
        blocks[name] = buf
    for dash, lines in blocks.items():
        for panel in json.loads('\n'.join(lines)).get('panels', []):
            for target in panel.get('targets', []):
                if 'expr' in target:
                    yield dash, panel.get('title'), target['expr']


class DashboardQueryTests(unittest.TestCase):

    def setUp(self):
        self.exprs = list(dashboard_exprs())

    def test_finds_the_broker_dashboard_queries(self):
        self.assertTrue(any(d == 'kafka.json' for d, _, _ in self.exprs))

    def test_histogram_means_are_windowed_with_their_count(self):
        for dash, title, expr in self.exprs:
            for m in YAMMER_HISTOGRAMS.finditer(expr):
                if m.group(2) != 'mean':
                    continue
                with self.subTest(dash=dash, panel=title):
                    self.assertIn(m.group(1) + '_count', expr)
                    self.assertIn('offset', expr)

    def test_no_lifetime_histogram_max(self):
        for dash, title, expr in self.exprs:
            with self.subTest(dash=dash, panel=title):
                self.assertNotIn('max', [m.group(2) for m in YAMMER_HISTOGRAMS.finditer(expr)])

    def test_no_network_gauges_that_report_the_controller_in_combined_mode(self):
        for dash, title, expr in self.exprs:
            with self.subTest(dash=dash, panel=title):
                self.assertIsNone(COMBINED_MODE_GAUGES.search(expr))

    def test_network_idle_comes_from_the_client_listeners_selectors(self):
        idle = [e for _, t, e in self.exprs if t == 'Network Processor Avg Idle %']
        self.assertEqual(1, len(idle))
        self.assertIn('kafka_server_socketservermetrics_io_wait_time_ns_total', idle[0])
        self.assertIn('listener!="CONTROLLER"', idle[0])


if __name__ == '__main__':
    unittest.main()
