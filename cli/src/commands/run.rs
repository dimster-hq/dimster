use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::Child;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Result, bail};
use serde::Serialize;

use crate::commands::find_coordinator;
use crate::commands::kafka_tls;
use crate::commands::results;
use crate::commands::run_configs;
use crate::config::{ConnectionConfig, KafkaTlsConfig, MetricsOutput};
use crate::kubectl::Kubectl;

const COORDINATOR_LOG_FILE: &str = "/benchmark/results/.dimster-logs/coordinator.log";
const STREAM_STATUS_POLL_INTERVAL: Duration = Duration::from_secs(5);
const STREAM_FILE_SIZE_POLL_INTERVAL: Duration = Duration::from_secs(5);
const STREAM_STALL_GRACE: Duration = Duration::from_secs(60);
const STREAM_RECONNECT_OVERLAP: Duration = Duration::from_secs(10);
const STREAM_TERMINAL_FLUSH: Duration = Duration::from_secs(3);
const STREAM_RECONNECT_BACKOFF: Duration = Duration::from_secs(1);
const STREAM_EVENT_WAIT: Duration = Duration::from_millis(250);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunRequest {
    file_type: &'static str,
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    kafka_config_yaml: Option<String>,
    workloads: HashMap<String, String>,
    workers: Vec<String>,
    output_dir: String,
    #[serde(rename = "topology", skip_serializing_if = "Option::is_none")]
    worker_assignment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker_ratio: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    producer_workers: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consumer_workers: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    environment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hardware: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    metadata: BTreeMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    extra_dimensions: BTreeMap<String, serde_json::Value>,
    // CLI-chosen results subdirectory name (yyyy-MM-dd-HH-mm-ss). When set,
    // the coordinator writes results into {outputDir}/{resultsSubdir} instead
    // of generating its own timestamp. This lets the CLI create the local
    // results directory up front and stage workload/kafka/broker config
    // captures into it before the run starts.
    #[serde(skip_serializing_if = "Option::is_none")]
    results_subdir: Option<String>,
}

/// Wire payload posted to the coordinator's `/modify` endpoint.
///
/// Keeping this type at module scope lets the argument parser be tested without
/// entering the Kubernetes transport path. Every optional field is omitted
/// when unused so older coordinators continue to receive the compact request
/// shape used by existing modify commands.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ModifyRequest {
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    terminate: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    discard_stats: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    skip_phase: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_for_minutes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    restart_consumers_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    restart_producers_count: Option<i64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    workload_overrides: HashMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    kafka_config_overrides: HashMap<String, serde_json::Value>,
}

pub struct RunArgs {
    pub mode: String,
    pub kafka_config: Option<String>,
    pub workloads: Vec<String>,
    pub worker_assignment: Option<String>,
    pub worker_ratio: Option<String>,
    pub producer_workers: Option<u32>,
    pub consumer_workers: Option<u32>,
    pub system: Option<String>,
    pub environment: String,
    pub hardware: Option<String>,
    pub connection: Option<ConnectionConfig>,
    // Path to the broker config that was last deployed for this environment
    // (sourced from env config's `kafka.brokerConfig`). Captured into the
    // results directory so each run is self-describing. None when the env
    // doesn't define one (e.g. external Kafka).
    pub broker_config: Option<String>,
    // Inline broker config overrides (sourced from env config's
    // `kafka.extraConfig`). Merged with `broker_config` to produce the
    // effective broker config captured into the results directory — so the
    // captured file matches what the broker actually ran with, not just the
    // unmerged source file.
    pub extra_config: Option<String>,
    // Resolved TLS settings for the env, present only when `kafka.tls.enabled`
    // is true. When set, the run command scans each workload for an opt-in
    // (`baseWorkload.kafkaConfig.commonConfig.security.protocol: SSL` /
    // `SASL_SSL`) and rewrites that workload to use the SSL bootstrap port
    // and the mounted client keystore/truststore. Workloads without the opt-in
    // line use PLAINTEXT, so a single campaign can mix both.
    pub tls_config: Option<KafkaTlsConfig>,
    pub metadata: Vec<String>,
    pub extra_dimensions: Vec<String>,
    // Metrics outputs to save once the run completes, already resolved from
    // `--metrics-output` and the env's `metricsOutput`.
    pub metrics_output: Vec<MetricsOutput>,
}

pub fn run(
    kubectl: &Kubectl,
    args: RunArgs,
    output_dir: &str,
    script_dir: &Path,
    config_path: &Path,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    // Check Kafka readiness (skip for client-only / external Kafka)
    if args.connection.is_none() {
        println!("==> Checking Kafka readiness...");
        let mut kafka_ready = false;
        for attempt in 0..30 {
            let total_str = kubectl
                .run(&[
                    "get",
                    "statefulset",
                    "kafka",
                    "-o",
                    "jsonpath={.spec.replicas}",
                ])
                .unwrap_or_else(|_| "0".to_string());
            let total: usize = total_str.trim().parse().unwrap_or(0);

            if total == 0 {
                if attempt == 0 {
                    bail!("No Kafka StatefulSet found. Run 'dimster deploy' first.");
                }
                // StatefulSet disappeared mid-check
                break;
            }

            let ready_output = kubectl.run(&[
                "get", "pods", "-l", "app=kafka",
                "-o", "jsonpath={range .items[*]}{.status.conditions[?(@.type==\"Ready\")].status}{\"\\n\"}{end}",
            ]).unwrap_or_default();

            let ready_count = ready_output.lines().filter(|l| l.trim() == "True").count();

            if ready_count == total {
                kafka_ready = true;
                break;
            }
            println!(
                "    Waiting for Kafka pods ({}/{} ready)...",
                ready_count, total
            );
            std::thread::sleep(std::time::Duration::from_secs(10));
            if stop.load(Ordering::Relaxed) {
                bail!("Interrupted while waiting for Kafka readiness");
            }
        }

        if !kafka_ready {
            let _ = kubectl.run_inherit(&["get", "pods", "-l", "app=kafka"]);
            bail!("Kafka pods did not become ready within 5 minutes");
        }
        println!("    Kafka is ready.");
    } else {
        println!("==> Using external Kafka (skipping readiness check)");
    }

    // Check coordinator
    println!("==> Checking coordinator readiness...");
    let coord_pod = find_coordinator(kubectl)?;

    let status = kubectl
        .exec_curl(&coord_pod, "GET", "http://localhost:8080/status", None)
        .unwrap_or_else(|_| "{}".to_string());
    if status.contains("\"RUNNING\"") || status.contains("\"STOPPING\"") {
        bail!(
            "Coordinator is busy ({}). Use 'dimster stop' first.",
            status
        );
    }
    println!("    Coordinator is ready ({}).", coord_pod);

    // Discover workers
    println!("==> Discovering worker pods...");
    let worker_ips = kubectl.run(&[
        "get",
        "pods",
        "-l",
        "app=benchmark-worker",
        "-o",
        "jsonpath={range .items[?(@.status.phase==\"Running\")]}{.status.podIP}{\"\\n\"}{end}",
    ])?;

    let workers: Vec<String> = worker_ips
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|ip| format!("http://{}:8080", ip.trim()))
        .collect();

    if workers.is_empty() {
        bail!("No worker pods found. Run 'dimster deploy' first.");
    }
    println!("    Found {} worker(s)", workers.len());

    // Discover the broker count once. Reused below for both the PLAINTEXT
    // bootstrap list and (when TLS is on) the SSL bootstrap list, so we
    // don't pay for two `kubectl get statefulset` round-trips per run.
    // Returns 0 for external Kafka (we never call kubectl in that branch).
    let kafka_replicas: u32 = if args.connection.is_some() {
        0
    } else {
        let replicas_str = kubectl
            .run(&[
                "get",
                "statefulset",
                "kafka",
                "-o",
                "jsonpath={.spec.replicas}",
            ])
            .unwrap_or_else(|_| "0".to_string());
        replicas_str.trim().parse().unwrap_or(0)
    };

    // Read all input files up-front so we can scan the -k file AND each
    // workload for TLS opt-in BEFORE we choose between PLAINTEXT and SSL
    // bootstrap. A TLS opt-in in the -k file means the AdminClient (and every
    // workload that uses this file) must talk to the SSL listener — we'd
    // otherwise inject a PLAINTEXT bootstrap that the file's
    // security.protocol: SSL would then mismatch.
    let kafka_config_content: Option<String> = match &args.kafka_config {
        Some(path) => Some(
            std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("Cannot read kafka config '{}': {}", path, e))?,
        ),
        None => None,
    };

    let mut raw_workloads: Vec<(String, String)> = Vec::new();
    for wl_path in &args.workloads {
        let path = Path::new(wl_path);
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("workload")
            .to_string();
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Cannot read workload '{}': {}", wl_path, e))?;
        raw_workloads.push((name, content));
    }

    // TLS detection: a security.protocol: SSL/SASL_SSL line in EITHER the -k
    // file or any workload triggers SSL plumbing. The check is cheap (text
    // pre-scan) and intentionally over-broad — the actual injectors re-check
    // via real YAML parses before rewriting anything.
    let kafka_config_uses_tls = kafka_config_content
        .as_deref()
        .map(opts_into_tls)
        .unwrap_or(false);
    let any_workload_uses_tls = raw_workloads
        .iter()
        .any(|(_, content)| opts_into_tls(content));

    // True when the env has TLS enabled AND something in this run actually
    // opts in. Drives both the -k file rewrite and the per-workload rewrite
    // below. The value carries no secret — the unencrypted PEM key is the
    // only "credential" needed at runtime, and that's already mounted into
    // the pod from the kafka-tls-client Secret.
    let tls_active = args.tls_config.is_some() && (kafka_config_uses_tls || any_workload_uses_tls);
    if tls_active {
        let tls = args.tls_config.as_ref().unwrap();
        println!(
            "    Detected TLS opt-in (kafka-config: {}, workloads: {}); using SSL bootstrap on port {}",
            kafka_config_uses_tls,
            any_workload_uses_tls,
            tls.resolved_ssl_port()
        );
    }

    // Build connection properties — from environment config or discover from K8s pods.
    // When TLS is enabled at the env level AND the -k file opts in, we
    // substitute the SSL bootstrap list and add the ssl.* lines so
    // inject_connection lands them in commonConfig (where Kafka's AdminClient
    // and producer/consumer clients all read connection settings).
    let (bootstrap, connection_lines) = if let Some(ref conn) = args.connection {
        let bs = conn.bootstrap_servers()?;
        println!("    Bootstrap: {} (from config)", bs);
        let lines = conn.to_common_config_lines();
        if lines.len() > 1 {
            println!("    Connection properties: {} additional", lines.len() - 1);
        }
        (Some(bs.to_string()), Some(lines))
    } else if kafka_replicas > 0 {
        let ns = kubectl.namespace();
        let use_ssl_for_file = kafka_config_uses_tls && tls_active;
        let port = if use_ssl_for_file {
            args.tls_config.as_ref().unwrap().resolved_ssl_port()
        } else {
            9092
        };
        let bs: String = (0..kafka_replicas)
            .map(|i| {
                format!(
                    "kafka-{}.kafka-headless.{}.svc.cluster.local:{}",
                    i, ns, port
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        println!("    Bootstrap: {}", bs);
        let mut lines = vec![format!("bootstrap.servers: \"{}\"", bs)];
        if use_ssl_for_file {
            // ssl.* lines for the AdminClient + every client that reads the
            // file's commonConfig. Format matches what inject_connection
            // already consumes (`key: "value"`).
            for (key, val) in kafka_tls::client_tls_pairs() {
                lines.push(format!("{}: \"{}\"", key, val));
            }
        }
        (Some(bs.clone()), Some(lines))
    } else {
        println!("    WARNING: No Kafka StatefulSet found. Using configs as-is.");
        (None, None)
    };

    // Process kafka config: inject connection into provided file, or synthesize
    // a minimal config with just connection properties. The Java-side merge
    // (KafkaConfig.merge) ensures these properties reach embedded workload configs.
    let kafka_config_yaml = match kafka_config_content {
        Some(content) => Some(inject_connection(&content, connection_lines.as_deref())),
        None => {
            // No -k flag: synthesize a minimal kafka config with connection properties
            connection_lines.as_deref().map(|lines| {
                let mut yaml = String::from("commonConfig:\n");
                for line in lines {
                    yaml.push_str(&format!("  {}\n", line));
                }
                yaml
            })
        }
    };

    // Per-workload TLS injection — only matters when a workload itself opts
    // in (the -k file path is already covered above). Workloads that don't
    // opt in are passed through verbatim so PLAINTEXT runs are unaffected by
    // YAML round-tripping (which would otherwise drop comments).
    let mut workloads_map = HashMap::new();
    let workload_ssl_bootstrap = if tls_active {
        let tls = args.tls_config.as_ref().unwrap();
        Some(kafka_tls::ssl_bootstrap_servers(
            kafka_replicas,
            kubectl.namespace(),
            tls.resolved_ssl_port(),
        ))
    } else {
        None
    };

    for (name, content) in raw_workloads {
        let final_content = match workload_ssl_bootstrap.as_ref() {
            Some(bs) => inject_workload_tls(&content, bs)?.unwrap_or(content),
            None => content,
        };
        workloads_map.insert(name, final_content);
    }

    // Validate that bootstrap.servers is present somewhere
    let has_bootstrap = bootstrap.is_some()
        || kafka_config_yaml
            .as_ref()
            .map_or(false, |c| c.contains("bootstrap.servers:"))
        || workloads_map
            .values()
            .any(|w| w.contains("bootstrap.servers:"));

    if !has_bootstrap {
        bail!(
            "No bootstrap.servers found. Either set a connection section in the environment config, \
             pass a kafka config with -k, or include bootstrap.servers in the workload's \
             baseWorkload.kafkaConfig.commonConfig."
        );
    }

    // Choose the remote timestamp now (before we send anything to the
    // cluster). The coordinator still writes to /benchmark/results/<timestamp>;
    // metadata.series only changes where the CLI stages/copies the local
    // archive, e.g. results/<series>/<instance>/<timestamp>.
    let timestamp = run_configs::now_timestamp();
    let annotations =
        run_configs::parse_result_annotations(&args.metadata, &args.extra_dimensions, &timestamp)?;
    let local_subdir = run_configs::local_result_subdir(&timestamp, &annotations.metadata)?;
    run_configs::prepare_run_dir(
        output_dir,
        &local_subdir,
        &args.workloads,
        args.kafka_config.as_deref(),
        args.broker_config.as_deref(),
        args.extra_config.as_deref(),
        config_path,
        &args.environment,
    )?;

    // Save environment name before moving into request
    let env_name = args.environment.clone();

    // Build the run request
    let request = RunRequest {
        file_type: "dimster-run-request",
        mode: args.mode,
        kafka_config_yaml,
        workloads: workloads_map,
        workers,
        output_dir: "/benchmark/results".to_string(),
        worker_assignment: args.worker_assignment,
        worker_ratio: args.worker_ratio,
        producer_workers: args.producer_workers,
        consumer_workers: args.consumer_workers,
        system: args.system,
        environment: Some(args.environment),
        hardware: args.hardware,
        metadata: annotations.metadata,
        extra_dimensions: annotations.extra_dimensions,
        results_subdir: Some(timestamp),
    };

    let request_json = serde_json::to_string(&request)?;

    // Write request to temp file, copy to pod, POST it
    let tmp_file = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp_file.path(), &request_json)?;

    println!("==> Starting benchmark...");
    kubectl.cp_to_pod(
        &tmp_file.path().to_string_lossy(),
        &coord_pod,
        "/tmp/run-request.json",
    )?;

    let response = kubectl.exec_curl(
        &coord_pod,
        "POST",
        "http://localhost:8080/run",
        Some("/tmp/run-request.json"),
    )?;

    if response.contains("\"error\"") {
        bail!("Failed to start benchmark: {}", response);
    }
    println!("    {}", response);

    // Stream coordinator logs until benchmark completes or user detaches
    println!("==> Following benchmark output...");
    let detached = stream_and_wait(kubectl, &coord_pod, stop.clone())?;

    if detached {
        println!();
        println!("==> Detached from benchmark (still running on coordinator).");
        println!("    To reattach:  dimster reattach {}", env_name);
        println!("    To stop:      dimster stop {}", env_name);
        return Ok(());
    }

    // Check if the benchmark failed
    let final_status = kubectl
        .exec_curl(&coord_pod, "GET", "http://localhost:8080/status", None)
        .unwrap_or_else(|_| "{}".to_string());

    if final_status.contains("\"FAILED\"") {
        eprintln!("==> Benchmark FAILED. Checkpoint preserved.");
        eprintln!("    To resume:  dimster resume {}", env_name);
        bail!("Benchmark failed — check coordinator logs for details");
    }

    // Collect results, generate charts, and print summary
    results::collect_results(kubectl, output_dir, script_dir, &args.metrics_output, &stop)?;
    Ok(())
}

/// Pre-parser intercept for `dimster run <env> --modify ...`. Returns
/// `Some(result)` if the argv is a modify command (and we've handled it
/// fully); `None` if it isn't (and main() should fall through to clap).
///
/// Why pre-parse instead of going through clap: the modify path has a custom
/// `--set key=value ...` grammar and action-combination rules. We claim the
/// flag handling for this one command and emit a single typed JSON request to
/// the coordinator.
pub fn try_handle_modify(args: &[String]) -> Option<Result<()>> {
    // Match `... run <env> ... --modify ...`. The env appears as the first
    // non-flag positional after "run". --modify can sit anywhere after
    // the env.
    let mut iter = args.iter().enumerate().peekable();
    // Skip program name (args[0]).
    iter.next();
    let mut env: Option<String> = None;
    let mut modify_idx: Option<usize> = None;
    while let Some((i, arg)) = iter.next() {
        if arg == "run" && env.is_none() {
            // Next positional (skipping flags) is env.
            for (_, next) in iter.by_ref() {
                if !next.starts_with('-') {
                    env = Some(next.clone());
                    break;
                }
            }
            continue;
        }
        if arg == "--modify" {
            modify_idx = Some(i);
            break;
        }
    }
    if modify_idx.is_none() {
        return None;
    }
    let env = match env {
        Some(e) => e,
        None => {
            return Some(Err(anyhow::anyhow!(
                "`dimster run --modify` requires an environment name (e.g. `dimster run localBeefy --modify ...`)"
            )));
        }
    };

    Some(execute_modify(env, &args[modify_idx.unwrap() + 1..]))
}

/// Parse the slice of args after `--modify` into a typed request. `--set`
/// accepts workload and Kafka config key/value pairs, while named action flags
/// represent engine operations such as phase changes and client restarts.
///
/// Parsing is deliberately transport-free: tests can assert the exact wire
/// payload and validation behavior without requiring kubectl or a coordinator.
fn parse_modify_args(modify_args: &[String]) -> Result<ModifyRequest> {
    use serde_json::Value;

    let mut terminate = false;
    let mut discard_stats = false;
    let mut skip_phase = false;
    let mut run_for_minutes: Option<i64> = None;
    let mut restart_consumers_count: Option<i64> = None;
    let mut restart_producers_count: Option<i64> = None;
    let mut workload_overrides: HashMap<String, Value> = HashMap::new();
    let mut kafka_overrides: HashMap<String, Value> = HashMap::new();

    let mut iter = modify_args.iter();
    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "--terminate" => {
                terminate = true;
                continue;
            }
            "--discard-stats" => {
                discard_stats = true;
                continue;
            }
            "--skip-phase" => {
                skip_phase = true;
                continue;
            }
            "--run-for-minutes" => {
                let v = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--run-for-minutes requires a value"))?;
                run_for_minutes = Some(v.parse().map_err(|_| {
                    anyhow::anyhow!("--run-for-minutes value must be an integer, got {}", v)
                })?);
                continue;
            }
            "--restart-consumers" => {
                let v = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--restart-consumers requires a value"))?;
                let count = v.parse::<i64>().map_err(|_| {
                    anyhow::anyhow!("--restart-consumers value must be an integer, got {}", v)
                })?;
                if count < 0 {
                    bail!("--restart-consumers must be >= 0, got {}", count);
                }
                restart_consumers_count = Some(count);
                continue;
            }
            "--restart-producers" => {
                let v = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--restart-producers requires a value"))?;
                let count = v.parse::<i64>().map_err(|_| {
                    anyhow::anyhow!("--restart-producers value must be an integer, got {}", v)
                })?;
                if count < 0 {
                    bail!("--restart-producers must be >= 0, got {}", count);
                }
                restart_producers_count = Some(count);
                continue;
            }
            "--set" => {
                // Parse key=value pairs until we hit another flag or end of args
                while let Some(arg) = iter.next() {
                    if arg.starts_with("--") {
                        bail!("--set expects key=value pairs, got flag: {}", arg);
                    }

                    let (key, value_raw) = arg.split_once('=').ok_or_else(|| {
                        anyhow::anyhow!("--set expects key=value format, got: {}", arg)
                    })?;

                    if key.is_empty() {
                        bail!("--set: key cannot be empty in: {}", arg);
                    }

                    let value = coerce_value(value_raw);

                    if let Some(path) = key.strip_prefix("kafkaConfig.") {
                        if path.is_empty() {
                            bail!(
                                "--set kafkaConfig requires a path, e.g. kafkaConfig.producerConfig.batch.size=65536"
                            );
                        }
                        kafka_overrides.insert(path.to_string(), value);
                    } else {
                        // Whitelist of valid workload field names (from TestPoint record)
                        const VALID_WORKLOAD_FIELDS: &[&str] = &[
                            "topics",
                            "partitionsPerTopic",
                            "producersPerTopic",
                            "consumerGroupsPerTopic",
                            "consumersPerGroup",
                            "messageSize",
                            "producerRate",
                            "producerRateSchedule",
                            "messageDistributor",
                            "messageDistributorSkew",
                            "consumerType",
                            "consumerProcMsMin",
                            "consumerProcMsMax",
                        ];

                        if !VALID_WORKLOAD_FIELDS.contains(&key) {
                            bail!(
                                "not a supported workload field: {}. Valid fields: {}",
                                key,
                                VALID_WORKLOAD_FIELDS.join(", ")
                            );
                        }

                        workload_overrides.insert(key.to_string(), value);
                    }
                }
                continue;
            }
            _ => {}
        }

        // If we get here, it's an unrecognized flag
        bail!(
            "unrecognized modify flag: {}. Valid action flags: --terminate, --discard-stats, \
               --skip-phase, --run-for-minutes, --restart-consumers, --restart-producers, --set",
            flag
        );
    }

    // Client-side combo validation — mirrors the coordinator-side rules so
    // the operator gets a fast, descriptive error instead of a 409 round-trip.
    let has_field_overrides = !workload_overrides.is_empty() || !kafka_overrides.is_empty();
    if !has_field_overrides
        && !terminate
        && !discard_stats
        && !skip_phase
        && run_for_minutes.is_none()
        && restart_consumers_count.is_none()
        && restart_producers_count.is_none()
    {
        bail!(
            "--modify requires at least one workload override, --kafkaConfig.* override, \
               or action flag (--terminate / --discard-stats / --run-for-minutes / \
               --skip-phase / --restart-consumers / --restart-producers)"
        );
    }
    if terminate
        && (has_field_overrides || discard_stats || skip_phase || run_for_minutes.is_some())
    {
        bail!("--terminate must be sent alone (no other --modify fields with it)");
    }
    if skip_phase && (has_field_overrides || discard_stats || run_for_minutes.is_some()) {
        // terminate+skip-phase is already caught by the terminate rule above.
        bail!("--skip-phase must be sent alone (no other --modify fields with it)");
    }
    if discard_stats && has_field_overrides {
        bail!(
            "--discard-stats must be sent alone (send field overrides first in a \
               separate --modify call, then --discard-stats to lock in)"
        );
    }
    if let Some(m) = run_for_minutes {
        if m <= 0 {
            bail!("--run-for-minutes must be > 0, got {}", m);
        }
    }
    if restart_consumers_count.is_some() && restart_producers_count.is_some() {
        bail!("--restart-consumers and --restart-producers must be sent separately");
    }
    if restart_consumers_count.is_some() || restart_producers_count.is_some() {
        let restart_flag = if restart_consumers_count.is_some() {
            "--restart-consumers"
        } else {
            "--restart-producers"
        };
        if has_field_overrides
            || discard_stats
            || terminate
            || skip_phase
            || run_for_minutes.is_some()
        {
            bail!(
                "{} must be sent alone (no other --modify fields with it)",
                restart_flag
            );
        }
    }

    Ok(ModifyRequest {
        terminate,
        discard_stats,
        skip_phase,
        run_for_minutes,
        restart_consumers_count,
        restart_producers_count,
        workload_overrides,
        kafka_config_overrides: kafka_overrides,
    })
}

/// Parse and POST one modify request to the active coordinator.
fn execute_modify(env: String, modify_args: &[String]) -> Result<()> {
    let request = parse_modify_args(modify_args)?;
    let json = serde_json::to_string(&request)?;

    // Discover the coordinator and POST. Same kubectl + curl pattern as
    // the existing /run path.
    let dimster_config = crate::config::DimsterConfig::load(&find_config_path()?)?;
    let kubectl = make_kubectl_for_modify(&dimster_config, &env)?;
    let coord_pod = find_coordinator(&kubectl)?;
    println!("==> Sending modify to coordinator pod: {}", coord_pod);
    println!("    {}", json);

    let tmp_file = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp_file.path(), &json)?;
    kubectl.cp_to_pod(
        &tmp_file.path().to_string_lossy(),
        &coord_pod,
        "/tmp/modify-request.json",
    )?;
    let response = kubectl.run(&[
        "exec",
        &coord_pod,
        "--",
        "curl",
        "-s",
        "-X",
        "POST",
        "-H",
        "Content-Type: application/json",
        "-d",
        "@/tmp/modify-request.json",
        "http://localhost:8080/modify",
    ])?;
    println!("    {}", response);
    if response.contains("\"error\"") {
        bail!("Modify rejected by coordinator");
    }
    Ok(())
}

/// Coerce a raw string value to a JSON value. Numbers parse as numbers,
/// `true`/`false` as booleans, everything else as strings. The
/// coordinator's Jackson deserializer accepts string-form numbers too,
/// so this is a best-effort upgrade for cleaner wire payloads.
fn coerce_value(s: &str) -> serde_json::Value {
    let trimmed = s.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') || trimmed == "null" {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
            return v;
        }
    }
    if let Ok(n) = s.parse::<i64>() {
        return serde_json::Value::Number(n.into());
    }
    if let Ok(f) = s.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return serde_json::Value::Number(n);
        }
    }
    if s.eq_ignore_ascii_case("true") {
        return serde_json::Value::Bool(true);
    }
    if s.eq_ignore_ascii_case("false") {
        return serde_json::Value::Bool(false);
    }
    serde_json::Value::String(s.to_string())
}

/// Build a Kubectl for the modify path. The main path threads kubectl
/// down through a long argument list — for modify we just need the
/// env-resolved context + namespace.
fn make_kubectl_for_modify(
    config: &crate::config::DimsterConfig,
    env: &str,
) -> Result<crate::kubectl::Kubectl> {
    config.get_env(env)?;
    let context = config.kubectl_context(env);
    let ns = config.kubectl_namespace(env);
    if let Some(ref ctx) = context {
        // SAFETY: single-threaded at this point; only spawned children read it.
        unsafe { std::env::set_var("DIMSTER_K8S_CONTEXT", ctx) };
    }
    Ok(crate::kubectl::Kubectl::new(context, ns))
}

fn find_config_path() -> Result<std::path::PathBuf> {
    if let Ok(p) = std::env::var("DIMSTER_CONFIG") {
        return Ok(std::path::PathBuf::from(p));
    }
    // Fall back to walking up from the current dir looking for pom.xml,
    // matching the find_script_dir() pattern in main.rs.
    let mut dir = std::env::current_dir()?;
    for _ in 0..5 {
        let candidate = dir.join("dimster-config.yaml");
        if candidate.exists() {
            return Ok(candidate);
        }
        if let Some(p) = dir.parent() {
            dir = p.to_path_buf();
        } else {
            break;
        }
    }
    Ok(std::path::PathBuf::from("dimster-config.yaml"))
}

/// Stream coordinator logs and poll for completion.
/// Returns Ok(true) if detached via Ctrl+C, Ok(false) if completed normally.
pub fn stream_and_wait(kubectl: &Kubectl, coord_pod: &str, stop: Arc<AtomicBool>) -> Result<bool> {
    // The cursor starts at attach time, matching the historical behavior that
    // `dimster run` follows new coordinator output rather than replaying the
    // whole accumulated pod log. Later reconnects move the cursor back by a
    // small overlap so a dead `kubectl logs -f` cannot drop the last few lines.
    let mut log_since = SystemTime::now();
    let mut monitor = LogTailMonitor::new(Instant::now(), STREAM_STALL_GRACE);
    let ns = kubectl.namespace().to_string();
    let ctx = std::env::var("DIMSTER_K8S_CONTEXT").ok();

    loop {
        let since_arg = format!("--since-time={}", format_system_time_utc(log_since));
        let mut log_child = kubectl.spawn(&["logs", "-f", coord_pod, &since_arg])?;
        let log_pid = log_child.id();
        let (log_rx, reader_handle) = spawn_log_reader(&mut log_child)?;

        let mut last_status_poll = Instant::now() - STREAM_STATUS_POLL_INTERVAL;
        let mut last_file_size_poll = Instant::now() - STREAM_FILE_SIZE_POLL_INTERVAL;
        let mut terminal_deadline: Option<Instant> = None;

        loop {
            let wait = terminal_deadline
                .map(|deadline| {
                    std::cmp::min(
                        STREAM_EVENT_WAIT,
                        deadline.saturating_duration_since(Instant::now()),
                    )
                })
                .unwrap_or(STREAM_EVENT_WAIT);

            match log_rx.recv_timeout(wait) {
                Ok(LogEvent::Line(text)) => {
                    println!("{}", text);
                    monitor.record_live_line(Instant::now());
                    log_since = reconnect_since_time(SystemTime::now(), STREAM_RECONNECT_OVERLAP);
                }
                Ok(LogEvent::Ended) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    let state = poll_coordinator_state(ctx.as_deref(), &ns, coord_pod);
                    if state.is_terminal() {
                        return finish_log_child(log_child, reader_handle, log_pid, false);
                    }
                    eprintln!("==> Coordinator log stream ended unexpectedly; reattaching...");
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }

            if terminal_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return finish_log_child(log_child, reader_handle, log_pid, false);
            }

            if stop.load(Ordering::Relaxed) {
                // Ctrl+C means detach from the stream only. The coordinator is
                // left running so the user can reattach or stop it explicitly.
                return finish_log_child(log_child, reader_handle, log_pid, true);
            }

            if terminal_deadline.is_none()
                && last_status_poll.elapsed() >= STREAM_STATUS_POLL_INTERVAL
            {
                last_status_poll = Instant::now();
                if poll_coordinator_state(ctx.as_deref(), &ns, coord_pod).is_terminal() {
                    // Keep draining the reader during the flush window so the
                    // final coordinator lines are still printed before we stop
                    // the `kubectl logs` child.
                    terminal_deadline = Some(Instant::now() + STREAM_TERMINAL_FLUSH);
                }
            }

            if terminal_deadline.is_none()
                && last_file_size_poll.elapsed() >= STREAM_FILE_SIZE_POLL_INTERVAL
            {
                last_file_size_poll = Instant::now();
                if let Some(size) = coordinator_log_size_standalone(ctx.as_deref(), &ns, coord_pod)
                {
                    if monitor.observe_file_size(size, Instant::now()) {
                        eprintln!("==> Coordinator log tail stalled; reattaching...");
                        break;
                    }
                }
            }
        }

        finish_log_child(log_child, reader_handle, log_pid, false)?;
        std::thread::sleep(STREAM_RECONNECT_BACKOFF);
    }
}

enum LogEvent {
    Line(String),
    Ended,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoordinatorState {
    Running,
    Stopping,
    Idle,
    Failed,
    Unknown,
}

impl CoordinatorState {
    fn is_terminal(self) -> bool {
        matches!(self, CoordinatorState::Idle | CoordinatorState::Failed)
    }
}

/// Decides whether the live `kubectl logs -f` stream has fallen behind the
/// persisted coordinator log. Quiet periods alone are not treated as stalls:
/// long benchmarks can legitimately emit no lines for a while, so file growth
/// is the evidence that the tail stream is the part that is stuck.
#[derive(Debug)]
struct LogTailMonitor {
    last_live_line_at: Instant,
    last_observed_file_size: Option<u64>,
    unstreamed_growth_seen_at: Option<Instant>,
    stall_grace: Duration,
}

impl LogTailMonitor {
    fn new(now: Instant, stall_grace: Duration) -> Self {
        Self {
            last_live_line_at: now,
            last_observed_file_size: None,
            unstreamed_growth_seen_at: None,
            stall_grace,
        }
    }

    fn record_live_line(&mut self, now: Instant) {
        self.last_live_line_at = now;
        self.unstreamed_growth_seen_at = None;
    }

    fn observe_file_size(&mut self, size: u64, now: Instant) -> bool {
        if let Some(previous) = self.last_observed_file_size {
            if size > previous && now > self.last_live_line_at {
                self.unstreamed_growth_seen_at.get_or_insert(now);
            }
        }
        self.last_observed_file_size = Some(size);

        self.unstreamed_growth_seen_at.is_some()
            && now.duration_since(self.last_live_line_at) >= self.stall_grace
    }
}

fn spawn_log_reader(
    log_child: &mut Child,
) -> Result<(mpsc::Receiver<LogEvent>, std::thread::JoinHandle<()>)> {
    let stdout = log_child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("kubectl logs stdout was not piped"))?;
    let (tx, rx) = mpsc::channel();

    let handle = std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            match line {
                Ok(text) => {
                    if tx.send(LogEvent::Line(text)).is_err() {
                        return;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = tx.send(LogEvent::Ended);
    });

    Ok((rx, handle))
}

fn finish_log_child(
    mut log_child: Child,
    reader_handle: std::thread::JoinHandle<()>,
    log_pid: u32,
    detached: bool,
) -> Result<bool> {
    kill_process(log_pid);
    let _ = log_child.wait();
    let _ = reader_handle.join();
    Ok(detached)
}

fn poll_coordinator_state(context: Option<&str>, namespace: &str, pod: &str) -> CoordinatorState {
    let status = exec_curl_standalone(
        context,
        namespace,
        pod,
        "GET",
        "http://localhost:8080/status",
    );
    parse_coordinator_state(&status)
}

fn parse_coordinator_state(status: &str) -> CoordinatorState {
    if status.contains("\"RUNNING\"") {
        CoordinatorState::Running
    } else if status.contains("\"STOPPING\"") {
        CoordinatorState::Stopping
    } else if status.contains("\"IDLE\"") {
        CoordinatorState::Idle
    } else if status.contains("\"FAILED\"") {
        CoordinatorState::Failed
    } else {
        CoordinatorState::Unknown
    }
}

fn reconnect_since_time(received_at: SystemTime, overlap: Duration) -> SystemTime {
    match received_at.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(since_epoch) if since_epoch > overlap => {
            SystemTime::UNIX_EPOCH + (since_epoch - overlap)
        }
        _ => SystemTime::UNIX_EPOCH,
    }
}

/// Inject connection properties into a YAML config string's `commonConfig` block.
///
/// Connection lines use YAML map format: `key: "value"`. Replaces existing entries
/// with the same key and appends any that aren't already present.
/// If no `commonConfig` block exists, creates one.
///
/// If connection_lines is None, returns the content unchanged.
fn inject_connection(content: &str, connection_lines: Option<&[String]>) -> String {
    let conn_lines = match connection_lines {
        Some(lines) if !lines.is_empty() => lines,
        _ => return content.to_string(),
    };

    // Build a map of YAML key -> full line from connection config
    let conn_map: HashMap<&str, &str> = conn_lines
        .iter()
        .filter_map(|line| line.split_once(':').map(|(k, _)| (k.trim(), line.as_str())))
        .collect();

    let src_lines: Vec<&str> = content.lines().collect();
    let mut result: Vec<String> = Vec::new();
    let mut injected_keys: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut in_common_config = false;
    let mut common_config_indent = "  ";
    let mut found_common_config = false;

    for (i, line) in src_lines.iter().enumerate() {
        if line.starts_with("commonConfig:") {
            found_common_config = true;
            in_common_config = true;
            result.push(line.to_string());

            // Detect indent from next line
            if i + 1 < src_lines.len() {
                let next = src_lines[i + 1];
                let trimmed = next.trim_start();
                if !trimmed.is_empty() {
                    common_config_indent = &next[..next.len() - trimmed.len()];
                }
            }
            continue;
        }

        if in_common_config {
            let trimmed = line.trim();
            // Still in the block if indented or empty
            if trimmed.is_empty()
                || line.starts_with(common_config_indent)
                || line.starts_with('\t')
            {
                // Check if this line has a YAML key we want to replace
                if let Some((key, _)) = trimmed.split_once(':') {
                    if let Some(replacement) = conn_map.get(key.trim()) {
                        result.push(format!("{}{}", common_config_indent, replacement));
                        injected_keys.insert(key.trim());
                        continue;
                    }
                }
                result.push(line.to_string());
                continue;
            }
            // Exited the block — inject remaining connection properties before this line
            for conn_line in conn_lines {
                if let Some((key, _)) = conn_line.split_once(':') {
                    let key = key.trim();
                    if !injected_keys.contains(key) {
                        result.push(format!("{}{}", common_config_indent, conn_line));
                        injected_keys.insert(key);
                    }
                }
            }
            in_common_config = false;
        }

        result.push(line.to_string());
    }

    // If we were still in commonConfig at EOF, inject remaining
    if in_common_config {
        for conn_line in conn_lines {
            if let Some((key, _)) = conn_line.split_once(':') {
                let key = key.trim();
                if !injected_keys.contains(key) {
                    result.push(format!("{}{}", common_config_indent, conn_line));
                }
            }
        }
    }

    // If no commonConfig block existed, create one as a YAML map
    if !found_common_config {
        result.push("commonConfig:".to_string());
        for conn_line in conn_lines {
            result.push(format!("  {}", conn_line));
        }
    }

    result.join("\n")
}

/// Cheap pre-scan to decide whether a kafka-config or workload file opted in
/// to TLS. Avoids the full YAML parse + Secret roundtrip when nothing in the
/// campaign uses SSL. We accept the false-positive risk of a literal
/// `security.protocol: SSL` appearing inside a comment because the only
/// downside is one extra kubectl call per run, and the actual injection
/// step (`inject_workload_tls`) re-checks via a real YAML parse. The
/// kafka-config file path applies the equivalent re-check by only adding
/// ssl.* lines when `args.tls_config` is also set.
fn opts_into_tls(content: &str) -> bool {
    for raw in content.lines() {
        let line = raw.trim_start();
        if line.starts_with('#') {
            continue;
        }
        if !line.starts_with("security.protocol") {
            continue;
        }
        // Match "security.protocol: SSL" / "SASL_SSL" with optional quotes.
        if let Some((_, value)) = line.split_once(':') {
            let v = value
                .trim()
                .trim_matches(|c| c == '"' || c == '\'')
                .to_uppercase();
            if v == "SSL" || v == "SASL_SSL" {
                return true;
            }
        }
    }
    false
}

/// Rewrite a workload's `baseWorkload.kafkaConfig.commonConfig` to use TLS
/// when the user has set `security.protocol: SSL` (or `SASL_SSL`) in that
/// map. Returns `Ok(None)` when the workload does not opt in (so the caller
/// can keep the original content verbatim, comments and all). Returns
/// `Ok(Some(yaml))` with the rewritten YAML otherwise.
///
/// The rewrite:
///   1. Sets `bootstrap.servers` to the SSL-port broker list (overwrites any
///      existing value — a workload that opts in to TLS must talk to the SSL
///      listener; the file/env-level PLAINTEXT bootstrap would break it).
///   2. Adds `ssl.keystore.*` / `ssl.truststore.*` paths/types from
///      `kafka_tls::client_tls_pairs`. Existing user values are NOT
///      overwritten — operators can pin to a different path if they want to
///      plug their own Secret in later. No `ssl.key.password` is added: the
///      mounted PEM key is unencrypted (see `kafka_tls` file-level comment).
///
/// Comments and blank lines in the workload are lost on the re-serialize
/// (serde_yaml is structural, not source-preserving). Acceptable tradeoff
/// for the MVP — TLS workloads are typically short copies of a base
/// PLAINTEXT workload, and the lost comments live in the un-injected
/// source file the user authored.
fn inject_workload_tls(content: &str, ssl_bootstrap: &str) -> Result<Option<String>> {
    use serde_yaml::Value;

    let mut value: Value = serde_yaml::from_str(content)
        .map_err(|e| anyhow::anyhow!("Failed to parse workload YAML for TLS injection: {}", e))?;

    // Navigate to baseWorkload.kafkaConfig.commonConfig — bail with None
    // (caller keeps original content) if any level is missing or non-map.
    // This is the only place we materialise the path; if the path doesn't
    // exist there's no commonConfig to opt in via, so it's not a TLS
    // workload by definition.
    let common = value
        .get_mut("baseWorkload")
        .and_then(|v| v.get_mut("kafkaConfig"))
        .and_then(|v| v.get_mut("commonConfig"));

    let common = match common {
        Some(Value::Mapping(m)) => m,
        _ => return Ok(None),
    };

    // Look for the opt-in trigger. Re-checks here defensively (the cheap
    // pre-scan in opts_into_tls() is intentionally over-broad) so we never
    // inject SSL config into a workload that didn't ask for it.
    let trigger = common
        .get(Value::String("security.protocol".to_string()))
        .and_then(|v| v.as_str())
        .map(|s| s.to_uppercase());
    let opted_in = matches!(trigger.as_deref(), Some("SSL") | Some("SASL_SSL"));
    if !opted_in {
        return Ok(None);
    }

    // bootstrap.servers ALWAYS gets the SSL list — see the function-doc.
    common.insert(
        Value::String("bootstrap.servers".to_string()),
        Value::String(ssl_bootstrap.to_string()),
    );

    // Other ssl.* keys are inserted only when absent — respects user overrides.
    for (key, val) in kafka_tls::client_tls_pairs() {
        let k = Value::String(key.to_string());
        if !common.contains_key(&k) {
            common.insert(k, Value::String(val));
        }
    }

    let serialized = serde_yaml::to_string(&value).map_err(|e| {
        anyhow::anyhow!(
            "Failed to re-serialize workload YAML after TLS injection: {}",
            e
        )
    })?;
    Ok(Some(serialized))
}

fn format_system_time_utc(system_time: SystemTime) -> String {
    let utc: time::OffsetDateTime = system_time.into();
    utc.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "2000-01-01T00:00:00Z".to_string())
}

/// Send SIGTERM to a process by PID (used to stop a stuck `kubectl logs` tail).
pub(crate) fn kill_process(pid: u32) {
    let _ = std::process::Command::new("kill")
        .arg(pid.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

/// Standalone curl execution for the poller thread (can't share Kubectl across threads).
pub(crate) fn exec_curl_standalone(
    context: Option<&str>,
    namespace: &str,
    pod: &str,
    method: &str,
    url: &str,
) -> String {
    let mut cmd = std::process::Command::new("kubectl");
    if let Some(ctx) = context {
        cmd.args(["--context", ctx]);
    }
    cmd.arg("--request-timeout=10s");
    cmd.args([
        "-n",
        namespace,
        "exec",
        pod,
        "--",
        "curl",
        "-sf",
        "--max-time",
        "5",
        "-X",
        method,
        url,
    ]);
    cmd.stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());

    match cmd.output() {
        Ok(output) => String::from_utf8_lossy(&output.stdout).trim().to_string(),
        Err(_) => "{}".to_string(),
    }
}

fn coordinator_log_size_standalone(
    context: Option<&str>,
    namespace: &str,
    pod: &str,
) -> Option<u64> {
    let mut cmd = std::process::Command::new("kubectl");
    if let Some(ctx) = context {
        cmd.args(["--context", ctx]);
    }
    cmd.arg("--request-timeout=10s");

    // `kubectl logs -f` can stop delivering lines even while the coordinator's
    // RollingFile appender keeps writing to the PVC. Reading the byte count is
    // a cheap health signal for the tail stream without replaying the file.
    let script = format!(
        "if [ -f {0} ]; then wc -c < {0}; else echo 0; fi",
        COORDINATOR_LOG_FILE
    );
    cmd.args(["-n", namespace, "exec", pod, "--", "sh", "-c", &script]);
    cmd.stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());

    match cmd.output() {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u64>()
            .ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn modify_args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn parse_restart_consumers_emits_only_consumer_count() {
        let request = parse_modify_args(&modify_args(&["--restart-consumers", "10"])).unwrap();
        let json = serde_json::to_value(request).unwrap();

        assert_eq!(json, serde_json::json!({"restartConsumersCount": 10}));
    }

    #[test]
    fn parse_restart_producers_emits_only_producer_count() {
        let request = parse_modify_args(&modify_args(&["--restart-producers", "7"])).unwrap();
        let json = serde_json::to_value(request).unwrap();

        assert_eq!(json, serde_json::json!({"restartProducersCount": 7}));
    }

    #[test]
    fn parse_restart_allows_zero_as_an_explicit_noop() {
        let request = parse_modify_args(&modify_args(&["--restart-producers", "0"])).unwrap();

        assert_eq!(request.restart_producers_count, Some(0));
    }

    #[test]
    fn parse_restart_rejects_missing_invalid_and_negative_counts() {
        assert!(parse_modify_args(&modify_args(&["--restart-producers"])).is_err());
        assert!(parse_modify_args(&modify_args(&["--restart-producers", "many"])).is_err());
        assert!(parse_modify_args(&modify_args(&["--restart-consumers", "-1"])).is_err());
    }

    #[test]
    fn parse_restart_actions_must_be_separate() {
        let both = parse_modify_args(&modify_args(&[
            "--restart-consumers",
            "2",
            "--restart-producers",
            "3",
        ]))
        .unwrap_err();
        assert!(both.to_string().contains("sent separately"));

        let with_override = parse_modify_args(&modify_args(&[
            "--restart-producers",
            "2",
            "--set",
            "producerRate=1000",
        ]))
        .unwrap_err();
        assert!(with_override.to_string().contains("must be sent alone"));
    }

    #[test]
    fn log_tail_monitor_does_not_reconnect_for_quiet_stream_without_file_growth() {
        let start = Instant::now();
        let mut monitor = LogTailMonitor::new(start, Duration::from_secs(60));

        assert!(!monitor.observe_file_size(100, start + Duration::from_secs(5)));
        assert!(!monitor.observe_file_size(100, start + Duration::from_secs(65)));
    }

    #[test]
    fn log_tail_monitor_reconnects_when_file_grows_but_stream_stays_quiet() {
        let start = Instant::now();
        let mut monitor = LogTailMonitor::new(start, Duration::from_secs(60));

        assert!(!monitor.observe_file_size(100, start + Duration::from_secs(5)));
        assert!(!monitor.observe_file_size(120, start + Duration::from_secs(10)));
        assert!(monitor.observe_file_size(120, start + Duration::from_secs(60)));
    }

    #[test]
    fn log_tail_monitor_live_line_clears_unstreamed_growth() {
        let start = Instant::now();
        let mut monitor = LogTailMonitor::new(start, Duration::from_secs(60));

        assert!(!monitor.observe_file_size(100, start + Duration::from_secs(5)));
        assert!(!monitor.observe_file_size(120, start + Duration::from_secs(10)));
        monitor.record_live_line(start + Duration::from_secs(20));

        assert!(!monitor.observe_file_size(120, start + Duration::from_secs(85)));
    }

    #[test]
    fn reconnect_since_time_applies_overlap_and_saturates_at_epoch() {
        let received = SystemTime::UNIX_EPOCH + Duration::from_secs(100);

        assert_eq!(
            reconnect_since_time(received, Duration::from_secs(10)),
            SystemTime::UNIX_EPOCH + Duration::from_secs(90)
        );
        assert_eq!(
            reconnect_since_time(received, Duration::from_secs(101)),
            SystemTime::UNIX_EPOCH
        );
    }

    #[test]
    fn parse_coordinator_state_handles_known_states() {
        assert_eq!(
            parse_coordinator_state("{\"state\":\"RUNNING\"}"),
            CoordinatorState::Running
        );
        assert_eq!(
            parse_coordinator_state("{\"state\":\"STOPPING\"}"),
            CoordinatorState::Stopping
        );
        assert_eq!(
            parse_coordinator_state("{\"state\":\"IDLE\"}"),
            CoordinatorState::Idle
        );
        assert_eq!(
            parse_coordinator_state("{\"state\":\"FAILED\"}"),
            CoordinatorState::Failed
        );
        assert_eq!(parse_coordinator_state("{}"), CoordinatorState::Unknown);
    }

    // Sample workload that opts in to TLS via the documented surface.
    // Mirrors a real workload's nesting: scenarios array, scalar config maps,
    // string/list values — keeps the round-trip honest.
    const TLS_WORKLOAD: &str = r#"name: tls-test
baseWorkload:
  topics: 1
  partitionsPerTopic: 4
  kafkaConfig:
    replicationFactor: 3
    commonConfig:
      security.protocol: SSL
    producerConfig:
      acks: all
scenarios:
  - name: warmup
    repeat: 1
"#;

    const PLAINTEXT_WORKLOAD: &str = r#"name: plaintext-test
baseWorkload:
  topics: 1
  kafkaConfig:
    commonConfig:
      acks: all
"#;

    const NO_COMMON_CONFIG_WORKLOAD: &str = r#"name: no-common
baseWorkload:
  topics: 1
  kafkaConfig:
    replicationFactor: 3
"#;

    #[test]
    fn opts_into_tls_detects_ssl_value() {
        assert!(opts_into_tls(TLS_WORKLOAD));
        assert!(opts_into_tls("security.protocol: SASL_SSL"));
        assert!(opts_into_tls("  security.protocol: \"SSL\""));
    }

    #[test]
    fn opts_into_tls_ignores_plaintext_and_comments() {
        assert!(!opts_into_tls(PLAINTEXT_WORKLOAD));
        // A commented-out trigger must not match.
        assert!(!opts_into_tls("# security.protocol: SSL"));
        // A different protocol is a no-op (PLAINTEXT, SASL_PLAINTEXT, etc.).
        assert!(!opts_into_tls("security.protocol: PLAINTEXT"));
    }

    // The kafka-config file shape is different from a workload (no
    // baseWorkload nesting) — a TLS opt-in there must still be detected by
    // the same pre-scan so the run command knows to wire SSL into the
    // file's commonConfig (which is where the AdminClient reads from).
    #[test]
    fn opts_into_tls_detects_kafka_config_file_shape() {
        let kafka_cfg = r#"name: kafka-safe-tls
replicationFactor: 3
commonConfig:
  request.timeout.ms: 120000
  security.protocol: SSL
topicConfig:
  min.insync.replicas: 2
"#;
        assert!(opts_into_tls(kafka_cfg));
    }

    #[test]
    fn inject_workload_tls_returns_none_for_plaintext() {
        let out = inject_workload_tls(PLAINTEXT_WORKLOAD, "kafka-0:9094").unwrap();
        assert!(out.is_none(), "PLAINTEXT workload must not be rewritten");
    }

    #[test]
    fn inject_workload_tls_returns_none_when_no_common_config() {
        // The opt-in lives in commonConfig; no commonConfig => no opt-in
        // path regardless of any other content.
        let out = inject_workload_tls(NO_COMMON_CONFIG_WORKLOAD, "kafka-0:9094").unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn inject_workload_tls_rewrites_bootstrap_and_adds_ssl_props() {
        let bootstrap = "kafka-0.kafka-headless.dimster.svc.cluster.local:9094";
        let out = inject_workload_tls(TLS_WORKLOAD, bootstrap).unwrap();
        let yaml = out.expect("TLS workload should be rewritten");

        // Re-parse to verify structure (string contains is too brittle for
        // YAML's flexible quoting / ordering rules)
        let v: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        let cc = v
            .get("baseWorkload")
            .unwrap()
            .get("kafkaConfig")
            .unwrap()
            .get("commonConfig")
            .unwrap();

        assert_eq!(
            cc.get("bootstrap.servers").and_then(|v| v.as_str()),
            Some(bootstrap),
            "bootstrap.servers must point at the SSL listener"
        );
        assert_eq!(
            cc.get("ssl.keystore.type").and_then(|v| v.as_str()),
            Some("PEM")
        );
        // Regression: ssl.key.password must NOT be injected — the mounted
        // PEM key is unencrypted and Kafka would try (and fail) to decrypt it.
        assert!(cc.get("ssl.key.password").is_none());
        // Original opt-in trigger is preserved
        assert_eq!(
            cc.get("security.protocol").and_then(|v| v.as_str()),
            Some("SSL")
        );
        // Other parts of the workload are untouched
        assert_eq!(
            v.get("baseWorkload")
                .unwrap()
                .get("kafkaConfig")
                .unwrap()
                .get("producerConfig")
                .unwrap()
                .get("acks")
                .and_then(|v| v.as_str()),
            Some("all")
        );
    }

    #[test]
    fn inject_workload_tls_respects_existing_ssl_overrides() {
        // User-pinned ssl.keystore.location should not be clobbered by the
        // injector — operator may have plugged in their own Secret/path.
        let custom = r#"name: custom-tls
baseWorkload:
  topics: 1
  kafkaConfig:
    commonConfig:
      security.protocol: SSL
      ssl.keystore.location: /custom/keystore.pem
"#;
        let yaml = inject_workload_tls(custom, "kafka-0:9094")
            .unwrap()
            .unwrap();
        let v: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        let cc = v
            .get("baseWorkload")
            .unwrap()
            .get("kafkaConfig")
            .unwrap()
            .get("commonConfig")
            .unwrap();
        assert_eq!(
            cc.get("ssl.keystore.location").and_then(|v| v.as_str()),
            Some("/custom/keystore.pem"),
            "user-supplied ssl.keystore.location must win"
        );
        // bootstrap.servers, by contrast, is always rewritten — opting in
        // to TLS implies talking to the SSL listener.
        assert_eq!(
            cc.get("bootstrap.servers").and_then(|v| v.as_str()),
            Some("kafka-0:9094")
        );
    }
}
