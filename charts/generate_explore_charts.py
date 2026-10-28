#!/usr/bin/env python3
"""Generate Chart.js bar charts from explore-mode benchmark results.

Self-contained: uses only Python stdlib. Produces one HTML file per
explore-mode JSON result, with interactive Chart.js bar charts.

Usage:
    python3 generate_explore_charts.py --results <dir> --output <dir>
"""

import argparse
import glob
import html
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate_run_charts import BUILTIN_METADATA_KEYS, format_meta_value, is_result_of_mode

# ---------------------------------------------------------------------------
# Metrics to chart: (extractor, title, y_axis_label)
# ---------------------------------------------------------------------------
METRICS = [
    (lambda t: t['peakSustainableRate'],
     'Peak Sustainable Rate', 'msgs/s'),
    (lambda t: t['peakSustainableThroughputMBs'],
     'Peak Sustainable Throughput', 'MB/s'),
    (lambda t: t['publishLatency']['p75'],
     'Publish Latency p75', 'ms'),
    (lambda t: t['publishLatency']['p999'],
     'Publish Latency p99.9', 'ms'),
    (lambda t: t['endToEndLatency']['p75'],
     'E2E Latency p75', 'ms'),
    (lambda t: t['endToEndLatency']['p999'],
     'E2E Latency p99.9', 'ms'),
]

COLOR_PALETTE = [
    '#4e79a7',  # steel blue
    '#e15759',  # coral
    '#59a14f',  # green
    '#f28e2b',  # amber
    '#9c755f',  # taupe
    '#76b7b2',  # teal
]


def _strip_heavy_data(data):
    """Return a copy of the result JSON with scenarioResults removed (config only)."""
    stripped = {k: v for k, v in data.items() if k != 'scenarioResults'}
    return stripped


def find_explore_results(results_dir):
    """Recursively find explore results, excluding typed helper artifacts."""
    results = []
    for json_path in sorted(glob.glob(os.path.join(results_dir, '**', '*.json'), recursive=True)):
        try:
            with open(json_path) as f:
                data = json.load(f)
            if is_result_of_mode(data, 'explore'):
                results.append((json_path, data))
        except json.JSONDecodeError as e:
            print(f"Warning: skipping {json_path}: invalid JSON: {e}", file=sys.stderr)
        except KeyError:
            continue
    return results


def _safe_id(text):
    """Turn arbitrary text into a CSS-safe id."""
    return re.sub(r'[^a-zA-Z0-9_-]', '-', text).strip('-')


def _fmt_val(v):
    """Format a numeric value for display."""
    if v is None or v == 0:
        return '0'
    if v < 1:
        return f'{v:.3f}'
    if v < 1000:
        return f'{v:,.1f}'
    return f'{v:,.0f}'


def _build_chart_js(canvas_id, title, y_label, labels, values):
    """Return a JS snippet that creates one Chart.js bar chart."""
    bar_colors = [COLOR_PALETTE[i % len(COLOR_PALETTE)] for i in range(len(labels))]
    return f"""
    new Chart(document.getElementById('{canvas_id}'), {{
        type: 'bar',
        data: {{
            labels: {json.dumps(labels)},
            datasets: [{{
                data: {json.dumps(values)},
                backgroundColor: {json.dumps(bar_colors)},
                borderColor: {json.dumps(bar_colors)},
                borderWidth: 1,
                borderRadius: 4
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{
                title: {{
                    display: true,
                    text: {json.dumps(title)},
                    font: {{ size: 15, weight: '600' }},
                    color: '#1a1a2e',
                    padding: {{ bottom: 12 }}
                }},
                legend: {{ display: false }},
                tooltip: {{
                    callbacks: {{
                        label: function(ctx) {{
                            let v = ctx.parsed.y;
                            if (v >= 1000) return v.toLocaleString(undefined, {{maximumFractionDigits: 0}}) + ' {y_label}';
                            if (v < 1) return v.toFixed(3) + ' {y_label}';
                            return v.toFixed(1) + ' {y_label}';
                        }}
                    }}
                }}
            }},
            scales: {{
                y: {{
                    beginAtZero: true,
                    title: {{
                        display: true,
                        text: {json.dumps(y_label)},
                        font: {{ size: 13 }},
                        color: '#555'
                    }},
                    ticks: {{
                        color: '#555',
                        callback: function(v) {{
                            if (v >= 1000) return v.toLocaleString();
                            return v;
                        }}
                    }},
                    grid: {{ color: 'rgba(0,0,0,0.06)' }}
                }},
                x: {{
                    ticks: {{ color: '#555', maxRotation: 45 }},
                    grid: {{ display: false }}
                }}
            }}
        }}
    }});"""


def generate_html(json_path, data, output_dir):
    """Generate a single self-contained HTML file for one explore result."""
    metadata = data.get('metadata', {})
    result_name = metadata.get('name', data.get('name', os.path.basename(json_path)))
    scenarios = data.get('scenarioResults', data.get('scenarios', []))

    if not scenarios:
        return None

    # -- Build sidebar links and chart sections --
    sidebar_links = []
    chart_sections = []
    chart_scripts = []
    chart_counter = 0

    for scenario in scenarios:
        scenario_name = scenario.get('name', 'unknown')
        tests = scenario.get('tests', [])
        if not tests:
            continue

        section_id = _safe_id(scenario_name)
        sidebar_links.append(f'<a href="#{section_id}">{html.escape(scenario_name)}</a>')

        labels = [t.get('label', f'test-{i}') for i, t in enumerate(tests)]

        cards_html = []
        for extractor, metric_title, y_label in METRICS:
            values = []
            for t in tests:
                try:
                    v = extractor(t)
                    values.append(round(v, 3) if v is not None else 0)
                except (KeyError, TypeError):
                    values.append(0)

            canvas_id = f'chart_{chart_counter}'
            chart_counter += 1

            cards_html.append(f'''<div class="chart-card">
                <canvas id="{canvas_id}"></canvas>
            </div>''')

            chart_scripts.append(_build_chart_js(
                canvas_id, f'{scenario_name} -- {metric_title}', y_label, labels, values))

        # Configuration table
        config_rows = []
        for t in tests:
            config_rows.append(f'''<tr>
                <td>{html.escape(t.get('label', ''))}</td>
                <td>{t.get('topics', '')}</td>
                <td>{t.get('partitionsPerTopic', '')}</td>
                <td>{t.get('messageSize', '')}</td>
                <td>{t.get('producersPerTopic', '')}</td>
                <td>{t.get('consumerGroupsPerTopic', '')}</td>
                <td>{t.get('consumersPerGroup', '')}</td>
                <td>{t.get('messageDistributor', '')}</td>
                <td>{t.get('consumerType', 'CONSUMER_GROUP')}</td>
            </tr>''')

        config_table_html = f'''<div class="summary-table">
            <table>
                <thead><tr>
                    <th>Label</th><th>Topics</th><th>Partitions</th>
                    <th>Msg Size</th><th>Producers</th>
                    <th>Consumer Groups</th><th>Consumers/Group</th>
                    <th>Key Dist</th><th>Consumer Type</th>
                </tr></thead>
                <tbody>{''.join(config_rows)}</tbody>
            </table>
        </div>'''

        # Convergence summary table
        table_rows = []
        for t in tests:
            label = html.escape(t.get('label', ''))
            converged = t.get('converged', False)
            reason = html.escape(t.get('reason', ''))
            rate = _fmt_val(t.get('peakSustainableRate', 0))
            throughput = _fmt_val(t.get('peakSustainableThroughputMBs', 0))
            badge_cls = 'badge-ok' if converged else 'badge-fail'
            badge_text = 'converged' if converged else 'not converged'

            pub_lat = t.get('publishLatency', {})
            e2e_lat = t.get('endToEndLatency', {})

            table_rows.append(f'''<tr>
                <td>{label}</td>
                <td><span class="badge {badge_cls}">{badge_text}</span></td>
                <td class="num">{rate}</td>
                <td class="num">{throughput}</td>
                <td class="num">{_fmt_val(pub_lat.get('p99', 0))}</td>
                <td class="num">{_fmt_val(pub_lat.get('p999', 0))}</td>
                <td class="num">{_fmt_val(e2e_lat.get('p99', 0))}</td>
                <td class="num">{_fmt_val(e2e_lat.get('p999', 0))}</td>
                <td>{reason}</td>
            </tr>''')

        table_html = f'''<div class="summary-table">
            <table>
                <thead>
                    <tr>
                        <th rowspan="2">Test</th><th rowspan="2">Status</th>
                        <th colspan="2">Peak Sustainable</th>
                        <th colspan="2">Publish Latency (ms)</th>
                        <th colspan="2">E2E Latency (ms)</th>
                        <th rowspan="2">Reason</th>
                    </tr>
                    <tr>
                        <th>msgs/s</th><th>MB/s</th>
                        <th>p99</th><th>p99.9</th>
                        <th>p99</th><th>p99.9</th>
                    </tr>
                </thead>
                <tbody>{''.join(table_rows)}</tbody>
            </table>
        </div>'''

        chart_sections.append(f'''
        <div class="scenario-section" id="{section_id}">
            <h2>{html.escape(scenario_name)}</h2>
            {config_table_html}
            {table_html}
            <div class="chart-grid">
                {''.join(cards_html)}
            </div>
        </div>''')

    # -- Configuration section (stripped JSON) --
    sidebar_links.append('<a href="#config-section">Configuration</a>')
    stripped = _strip_heavy_data(data)
    config_section = f'''
    <div class="scenario-section" id="config-section">
        <h2>Configuration</h2>
        <div class="config-json-wrap">
            <pre class="config-json">{html.escape(json.dumps(stripped, indent=2))}</pre>
        </div>
    </div>'''
    chart_sections.append(config_section)

    # -- Metadata header --
    meta_items = []
    for key, label in [('name', 'Workload'), ('environment', 'Environment'),
                        ('system', 'System'), ('kafkaConfigName', 'Kafka Config'),
                        ('beginTime', 'Begin'), ('endTime', 'End')]:
        val = metadata.get(key, '')
        if val:
            meta_items.append(f'<div class="meta-item"><span class="meta-label">{label}</span>'
                              f'<span class="meta-value">{html.escape(str(val))}</span></div>')
    for key in sorted(k for k in metadata.keys() if k not in BUILTIN_METADATA_KEYS):
        val = format_meta_value(metadata.get(key))
        meta_items.append(f'<div class="meta-item"><span class="meta-label">Metadata {html.escape(key)}</span>'
                          f'<span class="meta-value">{html.escape(val)}</span></div>')
    dimensions = metadata.get('extraDimensions')
    if isinstance(dimensions, dict):
        for key in sorted(dimensions.keys()):
            val = format_meta_value(dimensions.get(key))
            meta_items.append(f'<div class="meta-item"><span class="meta-label">Dimension {html.escape(key)}</span>'
                              f'<span class="meta-value">{html.escape(val)}</span></div>')

    page_html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Explore Results -- {html.escape(result_name)}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
*, *::before, *::after {{ box-sizing: border-box; }}
body {{
    margin: 0;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    background: #f8f9fa;
    color: #1a1a2e;
    display: flex;
    min-height: 100vh;
}}
.sidebar {{
    width: 220px;
    min-width: 220px;
    background: #1a1a2e;
    padding: 24px 0;
    position: fixed;
    top: 0;
    left: 0;
    bottom: 0;
    overflow-y: auto;
    z-index: 10;
}}
.sidebar h3 {{
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #6c6c8a;
    margin: 0;
    padding: 12px 20px 6px;
}}
.sidebar-title {{
    font-size: 15px;
    font-weight: 700;
    color: #fff;
    padding: 0 20px 16px;
    border-bottom: 1px solid #2a2a4e;
    margin-bottom: 12px;
    letter-spacing: 0.5px;
}}
.sidebar a {{
    display: block;
    padding: 8px 20px;
    color: #a0a0b8;
    text-decoration: none;
    font-size: 13px;
    transition: background 0.15s, color 0.15s;
    border-left: 3px solid transparent;
}}
.sidebar a:hover {{
    background: #2a2a4e;
    color: #fff;
    border-left-color: #4e79a7;
}}
.main {{
    margin-left: 220px;
    flex: 1;
    padding: 32px 40px;
    max-width: 1400px;
}}
.header {{
    background: #fff;
    border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    padding: 24px 28px;
    margin-bottom: 32px;
}}
.header h1 {{
    margin: 0 0 16px 0;
    font-size: 22px;
    font-weight: 700;
}}
.meta-grid {{
    display: flex;
    flex-wrap: wrap;
    gap: 20px;
}}
.meta-item {{
    display: flex;
    flex-direction: column;
}}
.meta-label {{
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    color: #888;
    margin-bottom: 2px;
}}
.meta-value {{
    font-size: 14px;
    font-weight: 500;
}}
.scenario-section {{
    margin-bottom: 48px;
}}
.scenario-section h2 {{
    font-size: 18px;
    font-weight: 600;
    margin: 0 0 16px 0;
    padding-bottom: 8px;
    border-bottom: 2px solid #e0e0e0;
}}
.summary-table {{
    margin-bottom: 20px;
    overflow-x: auto;
}}
.summary-table table {{
    border-collapse: collapse;
    width: 100%;
    font-size: 13px;
    background: #fff;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}}
.summary-table th, .summary-table td {{
    padding: 10px 14px;
    text-align: left;
    border-bottom: 1px solid #eee;
}}
.summary-table td.num {{
    text-align: right;
    font-variant-numeric: tabular-nums;
}}
.summary-table th {{
    background: #fafafa;
    font-weight: 600;
    color: #555;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.3px;
}}
.badge {{
    display: inline-block;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 600;
}}
.badge-ok {{
    background: #e8f5e9;
    color: #2e7d32;
}}
.badge-fail {{
    background: #fce4ec;
    color: #c62828;
}}
.chart-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
}}
.chart-card {{
    background: #fff;
    border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    padding: 20px;
}}
.chart-card canvas {{
    width: 100% !important;
    height: 300px !important;
}}
.config-json-wrap {{
    margin-top: 12px;
    overflow-x: auto;
}}
.config-json {{
    background: #fff;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    padding: 16px;
    overflow-x: auto;
    font-size: 12px;
    max-height: 600px;
    overflow-y: auto;
    white-space: pre-wrap;
    word-break: break-word;
}}
@media (max-width: 900px) {{
    .sidebar {{ display: none; }}
    .main {{ margin-left: 0; padding: 16px; }}
    .chart-grid {{ grid-template-columns: 1fr; }}
}}
</style>
</head>
<body>
<nav class="sidebar">
    <div class="sidebar-title">Dimster Results</div>
    <h3>Scenarios</h3>
    {''.join(sidebar_links)}
</nav>
<div class="main">
    <div class="header">
        <h1>Explore Results</h1>
        <div class="meta-grid">
            {''.join(meta_items)}
        </div>
    </div>
    {''.join(chart_sections)}
</div>
<script>
document.addEventListener('DOMContentLoaded', function() {{
{chr(10).join(chart_scripts)}
}});
</script>
</body>
</html>'''

    # Write output file
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(json_path))[0]
    out_path = os.path.join(output_dir, f'explore-{base_name}.html')
    with open(out_path, 'w') as f:
        f.write(page_html)

    return out_path


def main():
    parser = argparse.ArgumentParser(description='Generate explore-mode bar charts (Chart.js)')
    parser.add_argument('--results', required=True, help='Results directory')
    parser.add_argument('--output', required=True, help='Output directory')
    args = parser.parse_args()

    results = find_explore_results(args.results)
    if not results:
        sys.exit(0)

    print(f'Found {len(results)} explore result file(s)')

    total_charts = 0
    for json_path, data in results:
        out_path = generate_html(json_path, data, args.output)
        if out_path:
            scenarios = data.get('scenarioResults', data.get('scenarios', []))
            n_charts = sum(len(METRICS) for s in scenarios if s.get('tests'))
            total_charts += n_charts
            print(f'  Wrote {n_charts} charts to {out_path}')

    print(f'Total: {total_charts} charts across {len(results)} file(s)')


if __name__ == '__main__':
    main()
