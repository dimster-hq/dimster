"""Checks the dashboards' broker queries avoid Kafka metrics that misreport:

- Yammer histogram and timer mean and max cover the broker's lifetime, so
  they mostly reflect earlier runs: a mean must be windowed with its count,
  and a max (which can't be windowed) isn't charted.
- With broker and controller in one process (combined mode), both register
  the same un-tagged network gauges and the controller's, nearly always
  idle, is the one exported: NetworkProcessorAvgIdlePercent and the
  per-processor IdlePercent read 1.0 whatever the load, and the
  RequestChannel's RequestQueueSize and ResponseQueueSize report the
  controller's queues. The broker's listeners' selector and acceptor metrics
  are tagged by listener, so queries must exclude CONTROLLER.
- Broker-measured request timings for Fetch also count the KRaft voters'
  fetches, so the saturation panels use FetchConsumer.
- Moving rows shifts every later panel, so no two panels may overlap.

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
    r'kafka_network_socketserver_networkprocessoravgidlepercent|kafka_network_processor_idlepercent'
    r'|kafka_network_requestchannel_value')

# A selector on a metric tagged by listener, which in combined mode includes
# the controller's listener; group 2 is the label matchers.
LISTENER_TAGGED = re.compile(r'(kafka_server_socketservermetrics_\w+|kafka_network_acceptor_\w+)(\{[^}]*\})?')

# A request filter on a broker request-timing metric.
REQUEST_TIMING = re.compile(r'kafka_network_requestmetrics_\w+?timems_\w+\{[^}]*request=~?"([^"]*)"')


def dashboards():
    """{name: parsed dashboard} for the ConfigMap's dashboards: `  name.json: |`
    literal blocks indented 4 spaces."""
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
    return {dash: json.loads('\n'.join(lines)) for dash, lines in blocks.items()}


def section_panels(dashboard, row_title):
    """Panels between the row titled `row_title` and the next row."""
    panels, inside = [], False
    for panel in dashboard['panels']:
        if panel.get('type') == 'row':
            inside = panel.get('title') == row_title
        elif inside:
            panels.append(panel)
    return panels


def dashboard_exprs():
    """(dashboard, panel title, expr) for every target in the dashboards."""
    for dash, dashboard in dashboards().items():
        for panel in dashboard.get('panels', []):
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
        self.assertEqual(2, len(idle))  # average and hottest processor
        for expr in idle:
            self.assertIn('kafka_server_socketservermetrics_io_wait_time_ns_total', expr)
            self.assertIn('listener!="CONTROLLER"', expr)

    def test_listener_tagged_metrics_exclude_the_controller_listener(self):
        for dash, title, expr in self.exprs:
            for m in LISTENER_TAGGED.finditer(expr):
                with self.subTest(dash=dash, panel=title, metric=m.group(1)):
                    self.assertIn('listener!="CONTROLLER"', m.group(2) or '')

    def test_saturation_request_timings_exclude_kraft_voter_fetches(self):
        kafka = dashboards()['kafka.json']
        filters = [m.group(1)
                   for panel in section_panels(kafka, 'Broker Saturation')
                   for target in panel['targets']
                   for m in REQUEST_TIMING.finditer(target['expr'])]
        self.assertTrue(filters)
        for f in filters:
            self.assertNotIn('Fetch', re.sub('FetchConsumer', '', f))

    def test_saturation_section_follows_throughput(self):
        rows = [p['title'] for p in dashboards()['kafka.json']['panels'] if p.get('type') == 'row']
        self.assertEqual(rows.index('Throughput') + 1, rows.index('Broker Saturation'))
        self.assertNotIn('Network & Handler Saturation', rows)

    def test_no_overlapping_panels(self):
        for dash, dashboard in dashboards().items():
            cells = {}
            for panel in dashboard.get('panels', []):
                g = panel['gridPos']
                own = {(x, y) for y in range(g['y'], g['y'] + g['h']) for x in range(g['x'], g['x'] + g['w'])}
                with self.subTest(dash=dash, panel=panel.get('title')):
                    self.assertEqual(set(), {cells[c] for c in own if c in cells})
                cells.update({c: panel.get('title') for c in own})


if __name__ == '__main__':
    unittest.main()
