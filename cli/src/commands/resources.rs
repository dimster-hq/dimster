use anyhow::{Context, Result, bail};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// YAML deserialization structs (only fields needed for resource calculation)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkloadFile {
    name: String,
    #[serde(default)]
    explore_config: Option<serde_yaml::Value>,
    #[serde(default)]
    drain_backlog_config: Option<serde_yaml::Value>,
    #[serde(default)]
    correctness_config: Option<serde_yaml::Value>,
    base_workload: BaseWorkload,
    #[serde(default)]
    scenarios: Vec<Scenario>,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct BaseWorkload {
    #[serde(default = "default_one")]
    topics: u32,
    #[serde(default = "default_one")]
    partitions_per_topic: u32,
    #[serde(default = "default_1024")]
    message_size: u32,
    #[serde(default)]
    producer_rate: f64,
    #[serde(default = "default_one")]
    consumer_groups_per_topic: u32,
    #[serde(default)]
    kafka_config: Option<EmbeddedKafkaConfig>,
}

fn default_one() -> u32 {
    1
}
fn default_1024() -> u32 {
    1024
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct EmbeddedKafkaConfig {
    #[serde(default)]
    replication_factor: Option<u16>,
    // Other fields ignored — we only need RF for resource calculation.
    // Allow unknown fields to pass through silently.
    #[serde(flatten)]
    _extra: serde_yaml::Mapping,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Scenario {
    name: String,
    #[serde(default)]
    label: Option<ScalarOrVec<String>>,
    #[serde(default)]
    topics: Option<ScalarOrVec<u32>>,
    #[serde(default)]
    partitions_per_topic: Option<ScalarOrVec<u32>>,
    #[serde(default)]
    message_size: Option<ScalarOrVec<u32>>,
    #[serde(default)]
    producer_rate: Option<ScalarOrVec<f64>>,
    #[serde(default)]
    consumer_groups_per_topic: Option<ScalarOrVec<u32>>,
    #[serde(default)]
    kafka_config: Option<Vec<EmbeddedKafkaConfig>>,
    // Allow other workload fields we don't need (consumersPerGroup, etc.)
    #[serde(flatten)]
    _extra: serde_yaml::Mapping,
}

#[derive(Deserialize, Clone)]
#[serde(untagged)]
enum ScalarOrVec<T> {
    Vec(Vec<T>),
    Scalar(T),
}

impl<T: Clone> ScalarOrVec<T> {
    fn expand(&self, count: usize) -> Result<Vec<T>> {
        match self {
            ScalarOrVec::Scalar(v) => Ok(vec![v.clone(); count]),
            ScalarOrVec::Vec(v) => {
                if v.len() != count {
                    bail!("array length {} does not match expected {}", v.len(), count);
                }
                Ok(v.clone())
            }
        }
    }

    fn vec_len(&self) -> Option<usize> {
        match self {
            ScalarOrVec::Scalar(_) => None,
            ScalarOrVec::Vec(v) => Some(v.len()),
        }
    }
}

/// Kafka config file (the -k flag).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct KafkaConfigFile {
    #[serde(default)]
    replication_factor: Option<u16>,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

struct TestPoint {
    scenario_name: String,
    label: String,
    topics: u32,
    partitions_per_topic: u32,
    message_size: u32,
    producer_rate: f64, // 0 = N/A (explore mode)
    consumer_groups_per_topic: u32,
    replication_factor: u16,
}

struct ResourceEstimate {
    test_point: TestPoint,
    producer_ingress: Option<u64>,
    replication_in: Option<u64>,
    replication_out: Option<u64>,
    consumer_egress: Option<u64>,
    total_network_in: Option<u64>,
    total_network_out: Option<u64>,
    total_network: Option<u64>,
    disk_write_rate: Option<u64>,
    pb_ingress: Option<u64>,
    pb_rep_in: Option<u64>,
    pb_rep_out: Option<u64>,
    pb_cons_out: Option<u64>,
    pb_net_in: Option<u64>,
    pb_net_out: Option<u64>,
    pb_net_total: Option<u64>,
    pb_disk_write: Option<u64>,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn run(
    broker_count: u32,
    workload_paths: &[String],
    kafka_config_path: Option<&str>,
) -> Result<()> {
    let kafka_config_rf = load_kafka_config_rf(kafka_config_path)?;

    for wl_path in workload_paths {
        let content = std::fs::read_to_string(wl_path)
            .with_context(|| format!("Cannot read workload: {}", wl_path))?;
        let workload: WorkloadFile = serde_yaml::from_str(&content)
            .with_context(|| format!("Cannot parse workload: {}", wl_path))?;

        let mode = detect_mode(&workload);
        let is_explore = workload.explore_config.is_some();
        let test_points = expand_workload(&workload, kafka_config_rf, is_explore)?;

        let estimates: Vec<ResourceEstimate> = test_points
            .into_iter()
            .map(|tp| calculate_resources(tp, Some(broker_count)))
            .collect();

        print_workload_resources(&workload.name, &mode, &estimates, broker_count);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

fn load_kafka_config_rf(path: Option<&str>) -> Result<Option<u16>> {
    match path {
        Some(p) => {
            let content = std::fs::read_to_string(p)
                .with_context(|| format!("Cannot read kafka config: {}", p))?;
            let kc: KafkaConfigFile = serde_yaml::from_str(&content)
                .with_context(|| format!("Cannot parse kafka config: {}", p))?;
            Ok(kc.replication_factor)
        }
        None => Ok(None),
    }
}

fn detect_mode(workload: &WorkloadFile) -> &'static str {
    if workload.explore_config.is_some() {
        "explore"
    } else if workload.correctness_config.is_some() {
        "correctness"
    } else if workload.drain_backlog_config.is_some() {
        "drain-backlog"
    } else {
        "run"
    }
}

// ---------------------------------------------------------------------------
// Scenario expansion
// ---------------------------------------------------------------------------

fn expand_workload(
    workload: &WorkloadFile,
    kafka_config_rf: Option<u16>,
    is_explore: bool,
) -> Result<Vec<TestPoint>> {
    let base = &workload.base_workload;

    if workload.scenarios.is_empty() {
        let rf = resolve_rf(None, base.kafka_config.as_ref(), kafka_config_rf);
        let rate = if is_explore { 0.0 } else { base.producer_rate };
        return Ok(vec![TestPoint {
            scenario_name: "default".to_string(),
            label: workload.name.clone(),
            topics: base.topics,
            partitions_per_topic: base.partitions_per_topic,
            message_size: base.message_size,
            producer_rate: rate,
            consumer_groups_per_topic: base.consumer_groups_per_topic,
            replication_factor: rf,
        }]);
    }

    let mut test_points = Vec::new();
    for scenario in &workload.scenarios {
        let count = determine_count(scenario)?;
        let labels = expand_labels(scenario, count)?;
        let topics = expand_field(&scenario.topics, count, base.topics)?;
        let partitions = expand_field(
            &scenario.partitions_per_topic,
            count,
            base.partitions_per_topic,
        )?;
        let msg_sizes = expand_field(&scenario.message_size, count, base.message_size)?;
        let rates = expand_field_f64(&scenario.producer_rate, count, base.producer_rate)?;
        let cg = expand_field(
            &scenario.consumer_groups_per_topic,
            count,
            base.consumer_groups_per_topic,
        )?;

        for i in 0..count {
            let scenario_rf = scenario
                .kafka_config
                .as_ref()
                .and_then(|cfgs| cfgs.get(i))
                .and_then(|c| c.replication_factor);
            let rf = resolve_rf(scenario_rf, base.kafka_config.as_ref(), kafka_config_rf);
            let rate = if is_explore { 0.0 } else { rates[i] };

            test_points.push(TestPoint {
                scenario_name: scenario.name.clone(),
                label: labels[i].clone(),
                topics: topics[i],
                partitions_per_topic: partitions[i],
                message_size: msg_sizes[i],
                producer_rate: rate,
                consumer_groups_per_topic: cg[i],
                replication_factor: rf,
            });
        }
    }

    Ok(test_points)
}

fn determine_count(scenario: &Scenario) -> Result<usize> {
    // Collect Vec lengths from all fields to find the test point count.
    let mut vec_len: Option<usize> = None;

    let mut check = |len: Option<usize>, field: &str| -> Result<()> {
        if let Some(l) = len {
            if let Some(existing) = vec_len {
                if l != existing {
                    bail!(
                        "Scenario '{}': {} has {} elements but expected {}",
                        scenario.name,
                        field,
                        l,
                        existing
                    );
                }
            } else {
                vec_len = Some(l);
            }
        }
        Ok(())
    };

    if let Some(ref v) = scenario.label {
        check(v.vec_len(), "label")?;
    }
    if let Some(ref v) = scenario.topics {
        check(v.vec_len(), "topics")?;
    }
    if let Some(ref v) = scenario.partitions_per_topic {
        check(v.vec_len(), "partitionsPerTopic")?;
    }
    if let Some(ref v) = scenario.message_size {
        check(v.vec_len(), "messageSize")?;
    }
    if let Some(ref v) = scenario.producer_rate {
        check(v.vec_len(), "producerRate")?;
    }
    if let Some(ref v) = scenario.consumer_groups_per_topic {
        check(v.vec_len(), "consumerGroupsPerTopic")?;
    }
    if let Some(ref cfgs) = scenario.kafka_config {
        if cfgs.len() > 1 {
            check(Some(cfgs.len()), "kafkaConfig")?;
        }
    }

    Ok(vec_len.unwrap_or(1))
}

fn expand_labels(scenario: &Scenario, count: usize) -> Result<Vec<String>> {
    match &scenario.label {
        Some(sv) => sv.expand(count),
        None => {
            if count == 1 {
                Ok(vec![scenario.name.clone()])
            } else {
                Ok((0..count)
                    .map(|i| format!("{} #{}", scenario.name, i + 1))
                    .collect())
            }
        }
    }
}

fn expand_field(
    field: &Option<ScalarOrVec<u32>>,
    count: usize,
    base_default: u32,
) -> Result<Vec<u32>> {
    match field {
        Some(sv) => sv.expand(count),
        None => Ok(vec![base_default; count]),
    }
}

fn expand_field_f64(
    field: &Option<ScalarOrVec<f64>>,
    count: usize,
    base_default: f64,
) -> Result<Vec<f64>> {
    match field {
        Some(sv) => sv.expand(count),
        None => Ok(vec![base_default; count]),
    }
}

fn resolve_rf(
    scenario_rf: Option<u16>,
    base_kafka_config: Option<&EmbeddedKafkaConfig>,
    kafka_config_file_rf: Option<u16>,
) -> u16 {
    scenario_rf
        .or_else(|| base_kafka_config.and_then(|c| c.replication_factor))
        .or(kafka_config_file_rf)
        .unwrap_or(1)
}

// ---------------------------------------------------------------------------
// Resource calculation
// ---------------------------------------------------------------------------

fn calculate_resources(tp: TestPoint, broker_count: Option<u32>) -> ResourceEstimate {
    if tp.producer_rate == 0.0 {
        return ResourceEstimate {
            test_point: tp,
            producer_ingress: None,
            replication_in: None,
            replication_out: None,
            consumer_egress: None,
            total_network_in: None,
            total_network_out: None,
            total_network: None,
            disk_write_rate: None,
            pb_ingress: None,
            pb_rep_in: None,
            pb_rep_out: None,
            pb_cons_out: None,
            pb_net_in: None,
            pb_net_out: None,
            pb_net_total: None,
            pb_disk_write: None,
        };
    }

    let rate = tp.producer_rate as u64; // truncate to whole msg/s for resource estimation
    // With a generated `dataset` the workload's messageSize does not apply
    // (record sizes follow from the data, typically 100-500 bytes), so this
    // estimate uses messageSize regardless; good enough for sizing.
    let msg = tp.message_size as u64;
    let rf = tp.replication_factor as u64;
    let cg = tp.consumer_groups_per_topic as u64;

    let producer_ingress = rate * msg;
    let replication = rate * msg * rf.saturating_sub(1);
    let consumer_egress = rate * msg * cg;
    let total_in = producer_ingress + replication;
    let total_out = consumer_egress + replication;
    let total_net = total_in + total_out;
    let disk_write = rate * msg * rf;

    let (
        pb_ingress,
        pb_rep_in,
        pb_rep_out,
        pb_cons_out,
        pb_net_in,
        pb_net_out,
        pb_net_total,
        pb_disk,
    ) = match broker_count {
        Some(n) if n > 0 => {
            let n = n as u64;
            (
                Some(producer_ingress / n),
                Some(replication / n),
                Some(replication / n),
                Some(consumer_egress / n),
                Some(total_in / n),
                Some(total_out / n),
                Some(total_net / n),
                Some(disk_write / n),
            )
        }
        _ => (None, None, None, None, None, None, None, None),
    };

    ResourceEstimate {
        test_point: tp,
        producer_ingress: Some(producer_ingress),
        replication_in: Some(replication),
        replication_out: Some(replication),
        consumer_egress: Some(consumer_egress),
        total_network_in: Some(total_in),
        total_network_out: Some(total_out),
        total_network: Some(total_net),
        disk_write_rate: Some(disk_write),
        pb_ingress,
        pb_rep_in,
        pb_rep_out,
        pb_cons_out,
        pb_net_in,
        pb_net_out,
        pb_net_total,
        pb_disk_write: pb_disk,
    }
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

fn format_throughput(bytes_per_sec: u64) -> String {
    const KB: f64 = 1_000.0;
    const MB: f64 = 1_000_000.0;
    const GB: f64 = 1_000_000_000.0;
    let b = bytes_per_sec as f64;
    if b >= GB {
        format!("{:.1} GB/s", b / GB)
    } else if b >= MB {
        format!("{:.1} MB/s", b / MB)
    } else if b >= KB {
        format!("{:.1} KB/s", b / KB)
    } else {
        format!("{} B/s", bytes_per_sec)
    }
}

fn throughput_or_na(val: Option<u64>) -> String {
    match val {
        Some(v) => format_throughput(v),
        None => "N/A".to_string(),
    }
}

fn print_workload_resources(
    name: &str,
    mode: &str,
    estimates: &[ResourceEstimate],
    broker_count: u32,
) {
    println!();
    println!("=== Workload: {} ({}) ===", name, mode);

    if estimates.is_empty() {
        println!("  No test points.");
        return;
    }

    // Group estimates by scenario
    let mut current_scenario = String::new();
    let mut scenario_estimates: Vec<&ResourceEstimate> = Vec::new();

    for est in estimates {
        if est.test_point.scenario_name != current_scenario {
            if !scenario_estimates.is_empty() {
                print_scenario(&current_scenario, &scenario_estimates, broker_count);
                scenario_estimates.clear();
            }
            current_scenario = est.test_point.scenario_name.clone();
        }
        scenario_estimates.push(est);
    }
    if !scenario_estimates.is_empty() {
        print_scenario(&current_scenario, &scenario_estimates, broker_count);
    }

    // Peak resource summary
    let has_rates = estimates.iter().any(|e| e.producer_ingress.is_some());
    if has_rates {
        let max_net_total = estimates
            .iter()
            .filter_map(|e| e.total_network)
            .max()
            .unwrap_or(0);
        let max_disk = estimates
            .iter()
            .filter_map(|e| e.disk_write_rate)
            .max()
            .unwrap_or(0);
        let max_pb_net = estimates
            .iter()
            .filter_map(|e| e.pb_net_total)
            .max()
            .unwrap_or(0);
        let max_pb_disk = estimates
            .iter()
            .filter_map(|e| e.pb_disk_write)
            .max()
            .unwrap_or(0);

        println!();
        println!(
            "  Peak: with {} brokers, each broker must handle up to {} network and {} disk write",
            broker_count,
            format_throughput(max_pb_net),
            format_throughput(max_pb_disk),
        );
        println!(
            "  Peak aggregate: {} network total, {} disk write total",
            format_throughput(max_net_total),
            format_throughput(max_disk),
        );
        println!();
        println!("  Note: per-broker numbers assume uniform load distribution and do not account");
        println!(
            "  for increased load when brokers are offline. This is not a capacity planning tool."
        );
    }
}

fn print_scenario(scenario_name: &str, estimates: &[&ResourceEstimate], broker_count: u32) {
    let rf = estimates[0].test_point.replication_factor;
    let all_same_rf = estimates
        .iter()
        .all(|e| e.test_point.replication_factor == rf);

    println!();
    if all_same_rf {
        println!("  Scenario: {} (RF={})", scenario_name, rf);
    } else {
        println!("  Scenario: {}", scenario_name);
    }

    let is_explore = estimates[0].producer_ingress.is_none();

    if is_explore {
        // For explore mode, just show the test shape
        println!();
        println!(
            "  {:<20} {:>8} {:>8} {:>8} {:>3}",
            "LABEL", "TOPICS", "PARTS", "GROUPS", "RF"
        );
        for est in estimates {
            let tp = &est.test_point;
            println!(
                "  {:<20} {:>8} {:>8} {:>8} {:>3}",
                tp.label,
                tp.topics,
                tp.partitions_per_topic,
                tp.consumer_groups_per_topic,
                tp.replication_factor,
            );
        }
        println!();
        println!("  Throughput: N/A (explore mode -- rate discovered dynamically)");
        return;
    }

    // Aggregate table
    println!();
    print_table_header();
    for est in estimates {
        println!(
            "  {:<20} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
            est.test_point.label,
            throughput_or_na(est.producer_ingress),
            throughput_or_na(est.replication_in),
            throughput_or_na(est.replication_out),
            throughput_or_na(est.consumer_egress),
            throughput_or_na(est.total_network_in),
            throughput_or_na(est.total_network_out),
            throughput_or_na(est.total_network),
            throughput_or_na(est.disk_write_rate),
        );
    }

    // Per-broker table
    if estimates.iter().any(|e| e.pb_net_in.is_some()) {
        println!();
        println!("  Per broker ({} brokers):", broker_count);
        print_table_header();
        for est in estimates {
            println!(
                "  {:<20} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
                est.test_point.label,
                throughput_or_na(est.pb_ingress),
                throughput_or_na(est.pb_rep_in),
                throughput_or_na(est.pb_rep_out),
                throughput_or_na(est.pb_cons_out),
                throughput_or_na(est.pb_net_in),
                throughput_or_na(est.pb_net_out),
                throughput_or_na(est.pb_net_total),
                throughput_or_na(est.pb_disk_write),
            );
        }
    }
}

fn print_table_header() {
    println!(
        "  {:<20} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "LABEL",
        "INGRESS",
        "REP IN",
        "REP OUT",
        "CONS OUT",
        "NET IN",
        "NET OUT",
        "NET TOTAL",
        "DISK WR"
    );
}
