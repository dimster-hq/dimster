use std::path::Path;

use anyhow::{Result, bail};
use serde::Deserialize;

#[derive(Debug)]
pub struct CsvDocument {
    pub header: String,
    pub rows: Vec<String>,
}

// --- Explore result types ---

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExploreResult {
    #[serde(default)]
    metadata: ExploreMetadata,
    // New format uses scenarioResults, old format uses scenarios
    #[serde(default, alias = "scenarios")]
    scenario_results: Vec<ScenarioResult>,
    // Legacy top-level fields (old format) — used as fallbacks
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    begin_time: Option<String>,
    #[serde(default)]
    end_time: Option<String>,
    #[serde(default)]
    environment: Option<String>,
    #[serde(default)]
    system: Option<String>,
}

impl ExploreResult {
    fn effective_name(&self) -> &str {
        if !self.metadata.name.is_empty() {
            &self.metadata.name
        } else {
            self.name.as_deref().unwrap_or("")
        }
    }
    fn effective_environment(&self) -> &str {
        self.metadata
            .environment
            .as_deref()
            .or(self.environment.as_deref())
            .unwrap_or("")
    }
    fn effective_system(&self) -> &str {
        self.metadata
            .system
            .as_deref()
            .or(self.system.as_deref())
            .unwrap_or("")
    }
    fn effective_begin_time(&self) -> &str {
        self.metadata
            .begin_time
            .as_deref()
            .or(self.begin_time.as_deref())
            .unwrap_or("")
    }
    fn effective_end_time(&self) -> &str {
        self.metadata
            .end_time
            .as_deref()
            .or(self.end_time.as_deref())
            .unwrap_or("")
    }
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ExploreMetadata {
    #[serde(default)]
    name: String,
    #[serde(default)]
    #[allow(dead_code)]
    kafka_config_name: Option<String>,
    #[serde(default)]
    begin_time: Option<String>,
    #[serde(default)]
    end_time: Option<String>,
    #[serde(default)]
    environment: Option<String>,
    #[serde(default)]
    system: Option<String>,
}

#[derive(Deserialize)]
struct ScenarioResult {
    name: String,
    tests: Vec<TestOutcome>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestOutcome {
    label: Option<String>,
    topics: i32,
    partitions_per_topic: i32,
    producers_per_topic: i32,
    #[serde(default, alias = "subscriptionsPerTopic")]
    consumer_groups_per_topic: i32,
    #[serde(alias = "consumerPerSubscription")]
    consumers_per_group: i32,
    message_size: i32,
    key_distributor: Option<String>,
    consumer_type: Option<String>,
    peak_sustainable_rate: f64,
    #[serde(rename = "peakSustainableThroughputMBs")]
    peak_sustainable_throughput_mbs: f64,
    #[serde(default)]
    found_rate_upper: f64,
    converged: bool,
    reason: Option<String>,
    publish_latency: LatencyBreakdown,
    end_to_end_latency: LatencyBreakdown,
}

#[derive(Deserialize)]
struct LatencyBreakdown {
    p50: f64,
    p75: f64,
    p99: f64,
    p999: f64,
    p9999: f64,
}

// --- Run result types (consolidated format) ---

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunResult {
    metadata: RunMetadata,
    scenario_results: Vec<RunScenarioResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunMetadata {
    name: Option<String>,
    kafka_config_name: Option<String>,
    environment: Option<String>,
    system: Option<String>,
    begin_time: Option<String>,
    end_time: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunScenarioResult {
    name: String,
    tests: Vec<RunTestEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunTestEntry {
    label: Option<String>,
    workload_config: RunWorkloadConfig,
    results: RunPerformanceResults,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct RunWorkloadConfig {
    #[serde(default, alias = "messageDistributor")]
    key_distributor: Option<String>,
    #[serde(default)]
    key_distributor_skew: f64,
    #[serde(default)]
    consumer_type: Option<String>,
    #[serde(default)]
    message_size: i64,
    #[serde(default)]
    topics: i32,
    #[serde(default)]
    partitions_per_topic: i32,
    #[serde(default)]
    producers_per_topic: i32,
    #[serde(default)]
    producer_rate: f64,
    #[serde(default)]
    consumer_groups_per_topic: i32,
    #[serde(default)]
    consumers_per_group: i32,
    #[serde(default)]
    consumer_proc_ms_min: i32,
    #[serde(default)]
    consumer_proc_ms_max: i32,
    #[serde(default)]
    test_duration_minutes: i32,
    #[serde(default)]
    warmup_duration_minutes: i32,
    #[serde(default)]
    sample_rate_millis: i32,
    // Dataset of the test point: "payload" (RAW) when the workload sets none,
    // absent in older results. For other datasets, message_size is the
    // estimated average record size.
    #[serde(default)]
    dataset_preset: Option<String>,
    #[serde(default)]
    dataset_format: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct RunPerformanceResults {
    #[serde(default)]
    publish_rate: Vec<f64>,
    #[serde(default)]
    consume_rate: Vec<f64>,
    // Measured MB/s per period; empty in results that predate them.
    #[serde(default)]
    publish_bytes_rate: Vec<f64>,
    #[serde(default)]
    consume_bytes_rate: Vec<f64>,
    #[serde(default)]
    avg_record_bytes: f64,

    #[serde(default)]
    aggregated_publish_latency_avg: f64,
    #[serde(default, rename = "aggregatedPublishLatency50pct")]
    aggregated_publish_latency_p50: f64,
    #[serde(default, rename = "aggregatedPublishLatency75pct")]
    aggregated_publish_latency_p75: f64,
    #[serde(default, rename = "aggregatedPublishLatency95pct")]
    aggregated_publish_latency_p95: f64,
    #[serde(default, rename = "aggregatedPublishLatency99pct")]
    aggregated_publish_latency_p99: f64,
    #[serde(default, rename = "aggregatedPublishLatency999pct")]
    aggregated_publish_latency_p999: f64,
    #[serde(default, rename = "aggregatedPublishLatency9999pct")]
    aggregated_publish_latency_p9999: f64,
    #[serde(default)]
    aggregated_publish_latency_max: f64,

    #[serde(default)]
    aggregated_publish_delay_latency_avg: f64,
    #[serde(default, rename = "aggregatedPublishDelayLatency50pct")]
    aggregated_publish_delay_latency_p50: f64,
    #[serde(default, rename = "aggregatedPublishDelayLatency75pct")]
    aggregated_publish_delay_latency_p75: f64,
    #[serde(default, rename = "aggregatedPublishDelayLatency95pct")]
    aggregated_publish_delay_latency_p95: f64,
    #[serde(default, rename = "aggregatedPublishDelayLatency99pct")]
    aggregated_publish_delay_latency_p99: f64,
    #[serde(default, rename = "aggregatedPublishDelayLatency999pct")]
    aggregated_publish_delay_latency_p999: f64,
    #[serde(default, rename = "aggregatedPublishDelayLatency9999pct")]
    aggregated_publish_delay_latency_p9999: f64,
    #[serde(default)]
    aggregated_publish_delay_latency_max: f64,

    #[serde(default)]
    aggregated_end_to_end_latency_avg: f64,
    #[serde(default, rename = "aggregatedEndToEndLatency50pct")]
    aggregated_end_to_end_latency_p50: f64,
    #[serde(default, rename = "aggregatedEndToEndLatency75pct")]
    aggregated_end_to_end_latency_p75: f64,
    #[serde(default, rename = "aggregatedEndToEndLatency95pct")]
    aggregated_end_to_end_latency_p95: f64,
    #[serde(default, rename = "aggregatedEndToEndLatency99pct")]
    aggregated_end_to_end_latency_p99: f64,
    #[serde(default, rename = "aggregatedEndToEndLatency999pct")]
    aggregated_end_to_end_latency_p999: f64,
    #[serde(default, rename = "aggregatedEndToEndLatency9999pct")]
    aggregated_end_to_end_latency_p9999: f64,
    #[serde(default)]
    aggregated_end_to_end_latency_max: f64,
}

// --- Correctness result types ---

/**
 * Minimal correctness projection for comparison CSV output.
 *
 * Aggregate counters are authoritative. Individual violation records are
 * retained only to report how much of each total is represented in the capped
 * event list used by the comparison timelines.
 */
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CorrectnessResult {
    metadata: RunMetadata,
    scenario_results: Vec<CorrectnessScenarioResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CorrectnessScenarioResult {
    name: String,
    tests: Vec<CorrectnessTestEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CorrectnessTestEntry {
    label: Option<String>,
    workload_config: RunWorkloadConfig,
    correctness: CorrectnessData,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CorrectnessData {
    #[serde(default)]
    passed: bool,
    #[serde(default)]
    total_sent: i64,
    #[serde(default)]
    total_received: i64,
    #[serde(default)]
    lost_messages: i64,
    #[serde(default)]
    duplicate_count: i64,
    #[serde(default)]
    out_of_order_count: i64,
    #[serde(default)]
    crc_failure_count: i64,
    #[serde(default)]
    violations: Vec<CorrectnessViolation>,
}

#[derive(Deserialize)]
struct CorrectnessViolation {
    #[serde(rename = "type")]
    violation_type: String,
    #[serde(default = "default_violation_count")]
    count: i64,
}

fn default_violation_count() -> i64 {
    1
}

// --- Drain-backlog result types ---

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrainBacklogResult {
    metadata: DrainBacklogMetadata,
    scenario_results: Vec<DrainBacklogScenarioResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrainBacklogMetadata {
    name: Option<String>,
    kafka_config_name: Option<String>,
    environment: Option<String>,
    system: Option<String>,
    // hardware string is drain-backlog-specific (records cluster shape)
    hardware: Option<String>,
    begin_time: Option<String>,
    end_time: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrainBacklogScenarioResult {
    name: String,
    tests: Vec<DrainBacklogTestEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrainBacklogTestEntry {
    label: Option<String>,
    workload_config: DrainBacklogWorkloadConfig,
    drain: DrainStats,
    // `performance` field is intentionally not deserialized — drain-phase
    // latency/rate stats are out of scope for the CSV (drain summary only).
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct DrainBacklogWorkloadConfig {
    #[serde(default)]
    key_distributor: Option<String>,
    #[serde(default)]
    consumer_type: Option<String>,
    #[serde(default)]
    message_size: i64,
    #[serde(default)]
    topics: i32,
    #[serde(default)]
    partitions_per_topic: i32,
    #[serde(default)]
    producers_per_topic: i32,
    #[serde(default)]
    producer_rate: f64,
    #[serde(default)]
    consumer_groups_per_topic: i32,
    #[serde(default)]
    consumers_per_group: i32,
    #[serde(default)]
    consumer_proc_ms_min: i32,
    #[serde(default)]
    consumer_proc_ms_max: i32,
    #[serde(default)]
    warmup_duration_minutes: i32,
    #[serde(default)]
    warmup_producer_rate: f64,
    #[serde(default)]
    sample_rate_millis: i32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct DrainStats {
    #[serde(default)]
    backlog_messages: i64,
    #[serde(default)]
    backlog_size_bytes: i64,
    // JSON key uses uppercase "GB" so the camelCase rule alone won't match.
    #[serde(default, rename = "backlogSizeGB")]
    backlog_size_gb: f64,
    #[serde(default)]
    build_rate: f64,
    // -1 in JSON when no producer is running during drain (pure-drain test)
    #[serde(default)]
    backlog_producer_rate: f64,
    #[serde(default)]
    build_duration_ms: i64,
    // 0 when the drain timed out before completing
    #[serde(default)]
    drain_duration_ms: i64,
    #[serde(default)]
    drain_timeout_minutes: i32,
}

// --- CSV headers ---

const EXPLORE_HEADER: &str = "name,environment,system,begin_time,end_time,\
scenario,label,topics,partitions,producers,consumer_groups,consumers_per_group,\
message_size,key_distributor,consumer_type,\
peak_rate,peak_throughput_mbs,found_rate_upper,converged,reason,\
publish_latency_p50,publish_latency_p75,publish_latency_p99,\
publish_latency_p999,publish_latency_p9999,\
e2e_latency_p50,e2e_latency_p75,e2e_latency_p99,\
e2e_latency_p999,e2e_latency_p9999";

const RUN_HEADER: &str = "workload,kafka_config,environment,system,label,scenario,\
begin_time,end_time,\
topics,partitions,producers_per_topic,consumer_groups_per_topic,consumers_per_group,\
message_size,producer_rate,key_distributor,key_distributor_skew,consumer_type,\
consumer_proc_ms_min,consumer_proc_ms_max,\
test_duration_minutes,warmup_duration_minutes,sample_rate_millis,\
prod_rate_avg,prod_rate_min,prod_rate_max,\
con_rate_avg,con_rate_min,con_rate_max,\
publish_latency_avg,publish_latency_p50,publish_latency_p75,publish_latency_p95,\
publish_latency_p99,publish_latency_p999,publish_latency_p9999,publish_latency_max,\
publish_delay_avg,publish_delay_p50,publish_delay_p75,publish_delay_p95,\
publish_delay_p99,publish_delay_p999,publish_delay_p9999,publish_delay_max,\
e2e_latency_avg,e2e_latency_p50,e2e_latency_p75,e2e_latency_p95,\
e2e_latency_p99,e2e_latency_p999,e2e_latency_p9999,e2e_latency_max,\
dataset_preset,dataset_format,avg_record_bytes,prod_mbs_avg,con_mbs_avg";

const CORRECTNESS_HEADER: &str = "workload,kafka_config,environment,system,\
begin_time,end_time,scenario,label,\
topics,partitions,producers_per_topic,consumer_groups_per_topic,consumers_per_group,\
message_size,producer_rate,key_distributor,consumer_type,\
passed,total_sent,total_received,lost_messages,duplicate_count,out_of_order_count,crc_failure_count,\
recorded_duplicate_count,recorded_out_of_order_count,recorded_crc_failure_count";

const DRAIN_BACKLOG_HEADER: &str = "workload,kafka_config,environment,system,hardware,\
begin_time,end_time,scenario,label,\
topics,partitions,producers_per_topic,consumer_groups_per_topic,consumers_per_group,\
message_size,producer_rate,key_distributor,consumer_type,\
consumer_proc_ms_min,consumer_proc_ms_max,\
warmup_duration_minutes,warmup_producer_rate,sample_rate_millis,\
backlog_messages,backlog_size_bytes,backlog_size_gb,\
build_rate,backlog_producer_rate,\
build_duration_ms,drain_duration_ms,drain_timeout_minutes,\
drain_minutes,drain_throughput_mbs";

// --- Main entry point ---

pub fn run(path: &str, output: Option<&str>) -> Result<()> {
    let p = Path::new(path);

    let files: Vec<std::path::PathBuf> = if p.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(p)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
            .collect();
        entries.sort();
        entries
    } else if p.is_file() {
        vec![p.to_path_buf()]
    } else {
        bail!("Path not found: {}", path);
    };

    if files.is_empty() {
        bail!("No JSON files found in: {}", path);
    }

    let mut converted = 0;

    for file in &files {
        let Some(doc) = convert_file(file)? else {
            let content = std::fs::read_to_string(file)?;
            let raw: serde_json::Value = serde_json::from_str(&content)
                .map_err(|e| anyhow::anyhow!("Cannot parse {}: {}", file.display(), e))?;
            let mode = raw.get("mode").and_then(|v| v.as_str()).unwrap_or("");
            eprintln!("Skipping {} (unknown mode={:?})", file.display(), mode);
            continue;
        };

        let out_path = match output {
            Some(o) => o.to_string(),
            None => file.with_extension("csv").to_string_lossy().to_string(),
        };

        let csv = doc.to_csv();
        std::fs::write(&out_path, &csv)?;
        println!("Wrote {} rows to {}", doc.rows.len(), out_path);
        converted += 1;
    }

    if converted == 0 {
        bail!("No results found in: {}", path);
    }

    Ok(())
}

impl CsvDocument {
    pub fn to_csv(&self) -> String {
        format!("{}\n{}\n", self.header, self.rows.join("\n"))
    }
}

pub fn convert_file(file: &Path) -> Result<Option<CsvDocument>> {
    let content = std::fs::read_to_string(file)?;
    let raw: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Cannot parse {}: {}", file.display(), e))?;
    convert_value(&raw, file)
}

fn convert_value(raw: &serde_json::Value, file: &Path) -> Result<Option<CsvDocument>> {
    // Legacy result files have no fileType. When a type is present, require the
    // canonical result type so directory conversion does not deserialize run
    // requests or topology snapshots merely because they share a mode value.
    if raw
        .get("fileType")
        .and_then(|value| value.as_str())
        .is_some_and(|file_type| file_type != "dimster-result")
    {
        return Ok(None);
    }

    let annotation_columns = annotation_columns(raw);
    let mode = raw.get("mode").and_then(|v| v.as_str()).unwrap_or("");

    let (header, rows) = match mode {
        "explore" => {
            let result: ExploreResult = serde_json::from_value(raw.clone())
                .map_err(|e| anyhow::anyhow!("Cannot deserialize {}: {}", file.display(), e))?;
            let mut rows = Vec::new();
            convert_explore(&result, &mut rows);
            (EXPLORE_HEADER, rows)
        }
        "run" => {
            let result: RunResult = serde_json::from_value(raw.clone())
                .map_err(|e| anyhow::anyhow!("Cannot deserialize {}: {}", file.display(), e))?;
            let mut rows = Vec::new();
            convert_run(&result, &mut rows);
            (RUN_HEADER, rows)
        }
        "correctness" => {
            let result: CorrectnessResult = serde_json::from_value(raw.clone())
                .map_err(|e| anyhow::anyhow!("Cannot deserialize {}: {}", file.display(), e))?;
            let mut rows = Vec::new();
            convert_correctness(&result, &mut rows);
            (CORRECTNESS_HEADER, rows)
        }
        "drain-backlog" => {
            let result: DrainBacklogResult = serde_json::from_value(raw.clone())
                .map_err(|e| anyhow::anyhow!("Cannot deserialize {}: {}", file.display(), e))?;
            let mut rows = Vec::new();
            convert_drain_backlog(&result, &mut rows);
            (DRAIN_BACKLOG_HEADER, rows)
        }
        _ => return Ok(None),
    };

    Ok(Some(CsvDocument {
        header: append_annotation_header(header, &annotation_columns),
        rows: append_annotation_values(rows, &annotation_columns),
    }))
}

fn annotation_columns(raw: &serde_json::Value) -> Vec<(String, String)> {
    let mut columns = Vec::new();
    let Some(metadata) = raw.get("metadata").and_then(|v| v.as_object()) else {
        return columns;
    };
    for (key, value) in metadata {
        if is_builtin_metadata_key(key) || key == "extraDimensions" {
            continue;
        }
        columns.push((format!("metadata_{}", key), json_value_to_cell(value)));
    }
    if let Some(dimensions) = metadata.get("extraDimensions").and_then(|v| v.as_object()) {
        for (key, value) in dimensions {
            columns.push((
                format!("extra_dimension_{}", key),
                json_value_to_cell(value),
            ));
        }
    }
    columns.sort_by(|a, b| a.0.cmp(&b.0));
    columns
}

fn append_annotation_header(base: &str, columns: &[(String, String)]) -> String {
    if columns.is_empty() {
        return base.to_string();
    }
    let suffix = columns
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(",");
    format!("{},{}", base, suffix)
}

fn append_annotation_values(rows: Vec<String>, columns: &[(String, String)]) -> Vec<String> {
    if columns.is_empty() {
        return rows;
    }
    let suffix = columns
        .iter()
        .map(|(_, value)| csv_escape(value))
        .collect::<Vec<_>>()
        .join(",");
    rows.into_iter()
        .map(|row| format!("{},{}", row, suffix))
        .collect()
}

fn is_builtin_metadata_key(key: &str) -> bool {
    matches!(
        key,
        "dimsterVersion"
            | "name"
            | "kafkaConfigName"
            | "environment"
            | "system"
            | "hardware"
            | "beginTime"
            | "endTime"
    )
}

fn json_value_to_cell(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(v) => v.to_string(),
        serde_json::Value::Number(v) => v.to_string(),
        serde_json::Value::String(v) => v.clone(),
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

// --- Explore conversion ---

fn convert_explore(result: &ExploreResult, rows: &mut Vec<String>) {
    for scenario in &result.scenario_results {
        for test in &scenario.tests {
            let row = format!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.2},{:.6},{:.2},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
                csv_escape(result.effective_name()),
                csv_escape(result.effective_environment()),
                csv_escape(result.effective_system()),
                csv_escape(result.effective_begin_time()),
                csv_escape(result.effective_end_time()),
                csv_escape(&scenario.name),
                csv_escape(test.label.as_deref().unwrap_or("")),
                test.topics,
                test.partitions_per_topic,
                test.producers_per_topic,
                test.consumer_groups_per_topic,
                test.consumers_per_group,
                test.message_size,
                csv_escape(test.key_distributor.as_deref().unwrap_or("")),
                csv_escape(test.consumer_type.as_deref().unwrap_or("")),
                test.peak_sustainable_rate,
                test.peak_sustainable_throughput_mbs,
                test.found_rate_upper,
                test.converged,
                csv_escape(test.reason.as_deref().unwrap_or("")),
                test.publish_latency.p50,
                test.publish_latency.p75,
                test.publish_latency.p99,
                test.publish_latency.p999,
                test.publish_latency.p9999,
                test.end_to_end_latency.p50,
                test.end_to_end_latency.p75,
                test.end_to_end_latency.p99,
                test.end_to_end_latency.p999,
                test.end_to_end_latency.p9999,
            );
            rows.push(row);
        }
    }
}

// --- Run conversion ---

fn convert_run(result: &RunResult, rows: &mut Vec<String>) {
    let m = &result.metadata;

    for scenario in &result.scenario_results {
        for test in &scenario.tests {
            let wc = &test.workload_config;
            let r = &test.results;

            let (prod_min, prod_avg, prod_max) = rate_stats(&r.publish_rate);
            let (con_min, con_avg, con_max) = rate_stats(&r.consume_rate);
            let prod_mbs = avg_mbs(&r.publish_bytes_rate, prod_avg, wc.message_size);
            let con_mbs = avg_mbs(&r.consume_bytes_rate, con_avg, wc.message_size);

            let row = format!(
                "{},{},{},{},{},{},{},{},\
                 {},{},{},{},{},\
                 {},{},{},{},{},\
                 {},{},\
                 {},{},{},\
                 {:.2},{:.2},{:.2},{:.2},{:.2},{:.2},\
                 {:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},\
                 {:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},\
                 {:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},\
                 {},{},{:.1},{:.3},{:.3}",
                csv_escape(m.name.as_deref().unwrap_or("")),
                csv_escape(m.kafka_config_name.as_deref().unwrap_or("")),
                csv_escape(m.environment.as_deref().unwrap_or("")),
                csv_escape(m.system.as_deref().unwrap_or("")),
                csv_escape(test.label.as_deref().unwrap_or("")),
                csv_escape(&scenario.name),
                csv_escape(m.begin_time.as_deref().unwrap_or("")),
                csv_escape(m.end_time.as_deref().unwrap_or("")),
                wc.topics,
                wc.partitions_per_topic,
                wc.producers_per_topic,
                wc.consumer_groups_per_topic,
                wc.consumers_per_group,
                wc.message_size,
                wc.producer_rate,
                csv_escape(wc.key_distributor.as_deref().unwrap_or("")),
                wc.key_distributor_skew,
                csv_escape(wc.consumer_type.as_deref().unwrap_or("")),
                wc.consumer_proc_ms_min,
                wc.consumer_proc_ms_max,
                wc.test_duration_minutes,
                wc.warmup_duration_minutes,
                wc.sample_rate_millis,
                prod_avg,
                prod_min,
                prod_max,
                con_avg,
                con_min,
                con_max,
                r.aggregated_publish_latency_avg,
                r.aggregated_publish_latency_p50,
                r.aggregated_publish_latency_p75,
                r.aggregated_publish_latency_p95,
                r.aggregated_publish_latency_p99,
                r.aggregated_publish_latency_p999,
                r.aggregated_publish_latency_p9999,
                r.aggregated_publish_latency_max,
                r.aggregated_publish_delay_latency_avg,
                r.aggregated_publish_delay_latency_p50,
                r.aggregated_publish_delay_latency_p75,
                r.aggregated_publish_delay_latency_p95,
                r.aggregated_publish_delay_latency_p99,
                r.aggregated_publish_delay_latency_p999,
                r.aggregated_publish_delay_latency_p9999,
                r.aggregated_publish_delay_latency_max,
                r.aggregated_end_to_end_latency_avg,
                r.aggregated_end_to_end_latency_p50,
                r.aggregated_end_to_end_latency_p75,
                r.aggregated_end_to_end_latency_p95,
                r.aggregated_end_to_end_latency_p99,
                r.aggregated_end_to_end_latency_p999,
                r.aggregated_end_to_end_latency_p9999,
                r.aggregated_end_to_end_latency_max,
                csv_escape(wc.dataset_preset.as_deref().unwrap_or("")),
                csv_escape(wc.dataset_format.as_deref().unwrap_or("")),
                r.avg_record_bytes,
                prod_mbs,
                con_mbs,
            );
            rows.push(row);
        }
    }
}

// --- Correctness conversion ---

fn convert_correctness(result: &CorrectnessResult, rows: &mut Vec<String>) {
    let m = &result.metadata;

    for scenario in &result.scenario_results {
        for test in &scenario.tests {
            let wc = &test.workload_config;
            let c = &test.correctness;
            let recorded_count = |violation_type: &str| -> i64 {
                c.violations
                    .iter()
                    .filter(|v| v.violation_type == violation_type)
                    .map(|v| v.count)
                    .sum()
            };

            rows.push(format!(
                "{},{},{},{},{},{},{},{},\
                 {},{},{},{},{},\
                 {},{},{},{},\
                 {},{},{},{},{},{},{},\
                 {},{},{}",
                csv_escape(m.name.as_deref().unwrap_or("")),
                csv_escape(m.kafka_config_name.as_deref().unwrap_or("")),
                csv_escape(m.environment.as_deref().unwrap_or("")),
                csv_escape(m.system.as_deref().unwrap_or("")),
                csv_escape(m.begin_time.as_deref().unwrap_or("")),
                csv_escape(m.end_time.as_deref().unwrap_or("")),
                csv_escape(&scenario.name),
                csv_escape(test.label.as_deref().unwrap_or("")),
                wc.topics,
                wc.partitions_per_topic,
                wc.producers_per_topic,
                wc.consumer_groups_per_topic,
                wc.consumers_per_group,
                wc.message_size,
                wc.producer_rate,
                csv_escape(wc.key_distributor.as_deref().unwrap_or("")),
                csv_escape(wc.consumer_type.as_deref().unwrap_or("")),
                c.passed,
                c.total_sent,
                c.total_received,
                c.lost_messages,
                c.duplicate_count,
                c.out_of_order_count,
                c.crc_failure_count,
                recorded_count("DUPLICATE"),
                recorded_count("OUT_OF_ORDER"),
                recorded_count("CRC_FAILURE"),
            ));
        }
    }
}

/// Average MB/s: the measured per-period byte rates when present (exact for
/// variable-size dataset records), otherwise the message rate times messageSize.
fn avg_mbs(bytes_rate: &[f64], rate_avg: f64, message_size: i64) -> f64 {
    if bytes_rate.is_empty() {
        rate_avg * message_size as f64 / 1_000_000.0
    } else {
        bytes_rate.iter().sum::<f64>() / bytes_rate.len() as f64
    }
}

// --- Drain-backlog conversion ---

fn convert_drain_backlog(result: &DrainBacklogResult, rows: &mut Vec<String>) {
    let m = &result.metadata;

    for scenario in &result.scenario_results {
        for test in &scenario.tests {
            let wc = &test.workload_config;
            let d = &test.drain;

            let drain_minutes = d.drain_duration_ms as f64 / 60_000.0;
            // Average MB/s consumed during drain. Guard against div-by-zero
            // for the timeout case where drain_duration_ms is 0.
            let drain_throughput_mbs = if d.drain_duration_ms > 0 {
                (d.backlog_size_bytes as f64 / 1_048_576.0) / (d.drain_duration_ms as f64 / 1000.0)
            } else {
                0.0
            };

            let row = format!(
                "{},{},{},{},{},{},{},{},{},\
                 {},{},{},{},{},\
                 {},{},{},{},\
                 {},{},\
                 {},{},{},\
                 {},{},{:.2},\
                 {},{},\
                 {},{},{},\
                 {:.3},{:.3}",
                csv_escape(m.name.as_deref().unwrap_or("")),
                csv_escape(m.kafka_config_name.as_deref().unwrap_or("")),
                csv_escape(m.environment.as_deref().unwrap_or("")),
                csv_escape(m.system.as_deref().unwrap_or("")),
                csv_escape(m.hardware.as_deref().unwrap_or("")),
                csv_escape(m.begin_time.as_deref().unwrap_or("")),
                csv_escape(m.end_time.as_deref().unwrap_or("")),
                csv_escape(&scenario.name),
                csv_escape(test.label.as_deref().unwrap_or("")),
                wc.topics,
                wc.partitions_per_topic,
                wc.producers_per_topic,
                wc.consumer_groups_per_topic,
                wc.consumers_per_group,
                wc.message_size,
                wc.producer_rate,
                csv_escape(wc.key_distributor.as_deref().unwrap_or("")),
                csv_escape(wc.consumer_type.as_deref().unwrap_or("")),
                wc.consumer_proc_ms_min,
                wc.consumer_proc_ms_max,
                wc.warmup_duration_minutes,
                wc.warmup_producer_rate,
                wc.sample_rate_millis,
                d.backlog_messages,
                d.backlog_size_bytes,
                d.backlog_size_gb,
                d.build_rate,
                d.backlog_producer_rate,
                d.build_duration_ms,
                d.drain_duration_ms,
                d.drain_timeout_minutes,
                drain_minutes,
                drain_throughput_mbs,
            );
            rows.push(row);
        }
    }
}

fn rate_stats(rates: &[f64]) -> (f64, f64, f64) {
    if rates.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let min = rates.iter().copied().reduce(f64::min).unwrap_or(0.0);
    let max = rates.iter().copied().reduce(f64::max).unwrap_or(0.0);
    let avg = rates.iter().sum::<f64>() / rates.len() as f64;
    (min, avg, max)
}

/// Escape a field for CSV: quote if it contains comma, quote, or newline.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_value_skips_typed_helper_files() {
        let run_request = serde_json::json!({
            "fileType": "dimster-run-request",
            "mode": "correctness"
        });

        let converted = convert_value(&run_request, Path::new("run-request.json")).unwrap();

        assert!(converted.is_none());
    }

    #[test]
    fn avg_mbs_prefers_measured_byte_rates() {
        assert_eq!(avg_mbs(&[0.5, 1.5], 9999.0, 1000), 1.0);
    }

    #[test]
    fn avg_mbs_falls_back_to_rate_times_message_size() {
        assert_eq!(avg_mbs(&[], 2000.0, 500), 1.0);
    }

    #[test]
    fn run_rows_have_one_cell_per_header_column() {
        let json = r#"{
            "metadata": {"name": "w"},
            "scenarioResults": [{"name": "s", "tests": [{
                "label": "t",
                "workloadConfig": {"messageSize": 281, "datasetPreset": "clicks", "datasetFormat": "AVRO"},
                "results": {"publishRate": [1000.0], "consumeRate": [1000.0],
                            "publishBytesRate": [0.28], "consumeBytesRate": [0.28],
                            "avgRecordBytes": 281.0}
            }]}]
        }"#;
        let result: RunResult = serde_json::from_str(json).unwrap();
        let mut rows = Vec::new();
        convert_run(&result, &mut rows);
        assert_eq!(rows[0].split(',').count(), RUN_HEADER.split(',').count());
        assert!(
            rows[0].ends_with("clicks,AVRO,281.0,0.280,0.280"),
            "{}",
            rows[0]
        );
    }
}
