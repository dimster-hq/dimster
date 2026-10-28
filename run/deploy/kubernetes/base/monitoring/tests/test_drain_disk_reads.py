"""Tests for the drain disk reads export.py writes for drain-backlog charts.

Loads the script out of dashboard-export-configmap.yaml and swaps its
Prometheus helper for a fake, so nothing needs a live Prometheus.

Run with:  python3 test_drain_disk_reads.py
"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from test_snapshot import load_export_namespace  # noqa: E402


def instant_result(value):
    return {'status': 'success', 'data': {'resultType': 'vector', 'result': (
        [{'metric': {}, 'value': [0, str(value)]}] if value is not None else [])}}


class DrainDiskReadsTests(unittest.TestCase):

    def setUp(self):
        self.ns = load_export_namespace()
        self.queries = []
        self.answers = {}

        def fake_instant(_url, expr, time):
            self.queries.append((expr, time))
            return instant_result(self.answers.get(time))
        self.ns['prom_query_instant'] = fake_instant

    def test_query_is_a_counter_difference_over_whole_disks(self):
        q = self.ns['disk_read_query'](75)
        sel = 'node_disk_read_bytes_total{device=~"%s"}' % self.ns['WHOLE_DISK_DEVICES']
        self.assertEqual(q, f'sum({sel}) - sum({sel} offset 75s)')

    def test_whole_disk_devices(self):
        import re
        regex = re.compile(self.ns['WHOLE_DISK_DEVICES'])
        for device in ('nvme0n1', 'nvme12n3', 'sda', 'xvdb', 'vda'):
            self.assertTrue(regex.fullmatch(device), device)
        for device in ('nvme0n1p1', 'sda1', 'dm-0', 'md0', 'loop0'):
            self.assertFalse(regex.fullmatch(device), device)

    def test_evaluates_past_the_drain_end_over_the_whole_drain(self):
        # 10:00:00.5Z to 10:00:07.2Z: evaluate at 10:00:08 + 15s, over 23s.
        start = 1793181600
        self.answers[start + 23] = 5000.0
        read = self.ns['drain_disk_reads']('p', start + 0.5, start + 7.2)
        self.assertEqual(read, 5000)
        self.assertEqual(self.queries[0], (self.ns['disk_read_query'](23), start + 23))

    def test_counter_reset_or_no_data_is_unmeasured(self):
        self.answers[115] = -10.0
        self.assertIsNone(self.ns['drain_disk_reads']('p', 0, 100))
        self.assertIsNone(self.ns['drain_disk_reads']('p', 1000, 1100))

    def test_writes_each_measured_drain_keyed_by_its_window(self):
        start = 1793181600
        self.answers[start + 25] = 1234.0
        windows = [{'start': '2026-10-28T10:00:00Z', 'end': '2026-10-28T10:00:10Z'},
                   {'start': '2026-10-28T11:00:00Z', 'end': '2026-10-28T11:00:10Z'},
                   {'start': 'garbage', 'end': 'garbage'}]
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'drain-disk-reads.json')
            self.ns['write_drain_disk_reads']('p', windows, path)
            with open(path) as f:
                written = json.load(f)
        self.assertEqual(written, {'drains': [{'drainStartTime': '2026-10-28T10:00:00Z',
                                               'drainEndTime': '2026-10-28T10:00:10Z',
                                               'diskReadBytes': 1234}]})


if __name__ == '__main__':
    unittest.main()
