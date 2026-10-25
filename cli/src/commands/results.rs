use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, bail};

use crate::commands::chart;
use crate::commands::csv;
use crate::commands::run_configs;
use crate::config::MetricsOutput;
use crate::kubectl::Kubectl;

/// Result-dir subdirectory holding the Prometheus snapshot (gzipped
/// OpenMetrics plus the dashboard JSON it was taken for). Shared with the
/// local snapshot viewer, which reads it back.
pub const SNAPSHOT_DIR: &str = "prometheus-snapshot";
/// Snapshot data file inside `SNAPSHOT_DIR`.
pub const SNAPSHOT_FILE: &str = "metrics.om.gz";

// Shell snippet that prints the basename of the most-recently-modified
// *directory* under /results. The trailing-slash glob (`*/`) is the POSIX
// idiom for "match directories only" — important because stray files like
// checkpoint.json at the PVC root would otherwise be picked as a "subdir".
const LATEST_PVC_SUBDIR_CMD: &str =
    "cd /results 2>/dev/null && ls -1td */ 2>/dev/null | head -1 | sed 's|/$||'";

/// Controls how much of each pod's persisted log file is written to disk.
///
/// Why this exists: log4j2 writes a long-lived file per pod that accumulates
/// across multiple runs (see `save_pod_log_file`). The end-of-run pipeline
/// only wants the slice belonging to the current run, but a user debugging
/// post-run state (e.g. a Kafka client that kept spewing after `end_ts`)
/// needs the rest of the file too. Modelling that as an enum keeps the two
/// intents explicit at every call site instead of hiding behind an
/// `Option<(begin, end)>` whose `None` would otherwise mean "no metadata".
#[derive(Clone, Copy, Debug)]
pub enum LogCaptureMode {
    /// Keep only lines whose log4j2 timestamp falls within
    /// [begin − BUFFER_SECS, end + BUFFER_SECS]. The historical default
    /// used by the end-of-run pipeline.
    Filtered { begin: i64, end: i64 },
    /// Keep the entire concatenation of rolled .gz backups + active file
    /// with no timestamp filter, and skip `--since-time` on any kubectl-
    /// logs fallback. Opt-in via `dimster results --full`.
    Full,
}

/// Selectively run result collection steps based on flags.
/// When no step flag is set, all steps run (matching the full end-of-run pipeline).
/// When any step flag is set, only those steps run. `full` is a modifier on the
/// log-capture step (see `LogCaptureMode`); it's silently inert if the log step
/// isn't running.
pub fn run(
    kubectl: &Kubectl,
    output_dir: &str,
    script_dir: &Path,
    data: bool,
    logs: bool,
    metrics: bool,
    metrics_outputs: &[MetricsOutput],
    charts: bool,
    full: bool,
    stop: &AtomicBool,
) -> Result<()> {
    // No flags = run everything. `metrics_outputs` is already resolved
    // (flag > env > default) so it applies whether or not `metrics` was set.
    let all = !data && !logs && !metrics && !charts;
    let do_data = data || all;
    let do_logs = logs || all;
    let do_metrics = metrics || all;
    let do_charts = charts || all;

    // Resolve the result directory. When downloading data, copy_results_from_pvc
    // determines it. Otherwise, look up the latest subdir from the PVC (if cluster
    // steps are needed) or from the local filesystem (charts-only).
    let result_dir = if do_data {
        copy_results_from_pvc(kubectl, output_dir, stop)?
    } else {
        let needs_cluster = do_logs || do_metrics;
        let subdir = if needs_cluster {
            find_latest_pvc_subdir(kubectl, stop)?
        } else {
            find_latest_local_subdir(output_dir)?
        };
        let dir = format!("{}/{}", output_dir, subdir);
        if !Path::new(&dir).is_dir() {
            std::fs::create_dir_all(&dir)?;
        }
        Some(dir)
    };

    if let Some(ref dir) = result_dir {
        if do_logs && !stop.load(Ordering::Relaxed) {
            save_logs(kubectl, dir, full)?;
        }
        if do_metrics && !stop.load(Ordering::Relaxed) {
            if let Err(e) = export_metrics(kubectl, dir, metrics_outputs, stop) {
                eprintln!("    Warning: metrics export failed: {}", e);
            }
        }
        if do_charts && !stop.load(Ordering::Relaxed) {
            if let Err(e) = chart::run(dir, script_dir) {
                eprintln!("    Warning: chart generation failed: {}", e);
            }
        }
        // CSV rides on the same gate as charts: both transform JSON
        // result files into a viewable artifact.
        if do_charts && !stop.load(Ordering::Relaxed) {
            if let Err(e) = csv::run(dir, None) {
                eprintln!("    Warning: CSV generation failed: {}", e);
            }
        }
        print_result_summary(dir);
    }

    Ok(())
}

/// Look up the latest timestamped subdirectory on the results PVC by spinning
/// up a short-lived busybox pod. The local result directory uses the same name
/// so that export_metrics can derive the PVC path from the local path.
fn find_latest_pvc_subdir(kubectl: &Kubectl, stop: &AtomicBool) -> Result<String> {
    println!("==> Looking up latest result directory on PVC...");

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let helper_name = format!("pvc-lookup-{}", timestamp);
    let ns = kubectl.namespace();

    // Pin to the coordinator node (PVC is ReadWriteOnce / EBS)
    let coord_node = find_coordinator_node(kubectl);
    let node_name_field = if !coord_node.is_empty() {
        format!("  nodeName: {}", coord_node)
    } else {
        String::new()
    };

    let manifest = format!(
        r#"apiVersion: v1
kind: Pod
metadata:
  name: {helper_name}
  namespace: {ns}
  labels:
    app: pvc-lookup
spec:
  restartPolicy: Never
{node_name_field}
  containers:
    - name: lookup
      image: busybox:latest
      command: ["sleep", "60"]
      volumeMounts:
        - name: results
          mountPath: /results
          readOnly: true
  volumes:
    - name: results
      persistentVolumeClaim:
        claimName: benchmark-results"#
    );

    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), &manifest)?;
    kubectl.run_no_ns(&["-n", ns, "apply", "-f", &tmp.path().to_string_lossy()])?;

    let started = wait_for_pod(kubectl, &helper_name, 30, stop);
    if !started {
        let _ = kubectl.run(&["delete", "pod", &helper_name, "--ignore-not-found"]);
        if stop.load(Ordering::Relaxed) {
            bail!("Interrupted");
        }
        bail!("PVC lookup pod did not start");
    }

    let subdir = kubectl
        .run(&[
            "exec",
            &helper_name,
            "--",
            "sh",
            "-c",
            LATEST_PVC_SUBDIR_CMD,
        ])
        .unwrap_or_default();
    let subdir = subdir.trim().to_string();

    let _ = kubectl.run(&["delete", "pod", &helper_name, "--ignore-not-found"]);

    if subdir.is_empty() {
        bail!("No result directories found on PVC");
    }

    println!("    Found: {}", subdir);
    Ok(subdir)
}

/// Find the latest timestamped subdirectory under the local output directory.
/// Subdirs are sorted by name descending (newest first) since they use
/// a "yyyy-MM-dd-HH-mm-ss" naming convention.
fn find_latest_local_subdir(output_dir: &str) -> Result<String> {
    let dir = Path::new(output_dir);
    if !dir.is_dir() {
        bail!("Output directory does not exist: {}", output_dir);
    }

    let mut subdirs: Vec<String> = std::fs::read_dir(dir)?
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| e.file_name().to_str().map(|s| s.to_string()))
        .collect();
    subdirs.sort();
    subdirs.reverse();

    match subdirs.first() {
        Some(name) => {
            println!(
                "==> Using latest local result directory: {}/{}",
                output_dir, name
            );
            Ok(name.clone())
        }
        None => bail!("No result subdirectories found in {}", output_dir),
    }
}

/// Find the node that the coordinator pod is running on. Returns empty string
/// if not found (caller decides whether to pin or not).
fn find_coordinator_node(kubectl: &Kubectl) -> String {
    kubectl
        .run(&[
            "get",
            "pods",
            "-l",
            "app=benchmark-coordinator",
            "-o",
            "jsonpath={.items[0].spec.nodeName}",
        ])
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// Wait for a pod to enter the Running phase. Returns true if it started,
/// false if it timed out after `max_attempts * 2` seconds.
fn wait_for_pod(kubectl: &Kubectl, pod_name: &str, max_attempts: usize, stop: &AtomicBool) -> bool {
    for _ in 0..max_attempts {
        let phase = kubectl
            .run(&["get", "pod", pod_name, "-o", "jsonpath={.status.phase}"])
            .unwrap_or_default();
        if phase == "Running" {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
        if stop.load(Ordering::Relaxed) {
            return false;
        }
    }
    false
}

/// Copy results from PVC via a temporary busybox pod.
/// Returns the local directory where results were saved, if any.
pub fn copy_results_from_pvc(
    kubectl: &Kubectl,
    output_dir: &str,
    stop: &AtomicBool,
) -> Result<Option<String>> {
    println!("==> Copying results from PVC...");

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let helper_name = format!("results-copy-{}", timestamp);
    let ns = kubectl.namespace();

    // Pin to the coordinator node (PVC is ReadWriteOnce / EBS)
    let coord_node = find_coordinator_node(kubectl);
    let node_name_field = if !coord_node.is_empty() {
        format!("  nodeName: {}", coord_node)
    } else {
        String::new()
    };

    // Create helper pod manifest
    let manifest = format!(
        r#"apiVersion: v1
kind: Pod
metadata:
  name: {helper_name}
  namespace: {ns}
  labels:
    app: benchmark-results-copy
spec:
  restartPolicy: Never
{node_name_field}
  containers:
    - name: copy
      image: busybox:latest
      command: ["sleep", "300"]
      volumeMounts:
        - name: results
          mountPath: /results
          readOnly: true
  volumes:
    - name: results
      persistentVolumeClaim:
        claimName: benchmark-results"#
    );

    // Apply via stdin
    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), &manifest)?;
    kubectl.run_no_ns(&["-n", ns, "apply", "-f", &tmp.path().to_string_lossy()])?;

    if !wait_for_pod(kubectl, &helper_name, 30, stop) {
        if !stop.load(Ordering::Relaxed) {
            eprintln!("    Helper pod failed to start. Pod status:");
            let _ = kubectl.run_inherit(&["describe", "pod", &helper_name]);
        }
        let _ = kubectl.run(&["delete", "pod", &helper_name, "--ignore-not-found"]);
        if stop.load(Ordering::Relaxed) {
            bail!("Interrupted");
        }
        bail!("Results copy pod did not start");
    }

    // Find the latest timestamped subdir on the PVC
    let latest_subdir = kubectl
        .run(&[
            "exec",
            &helper_name,
            "--",
            "sh",
            "-c",
            LATEST_PVC_SUBDIR_CMD,
        ])
        .unwrap_or_default();
    let latest_subdir = latest_subdir.trim().to_string();

    let result_dir = if latest_subdir.is_empty() {
        println!("    No result directories found on PVC.");
        None
    } else {
        // Count files in the latest subdir only
        let count_str = kubectl
            .run(&[
                "exec",
                &helper_name,
                "--",
                "sh",
                "-c",
                &format!(
                    "find /results/{} -name '*.json' -type f 2>/dev/null | wc -l",
                    latest_subdir
                ),
            ])
            .unwrap_or_else(|_| "0".to_string());
        let count: usize = count_str.trim().parse().unwrap_or(0);

        if count == 0 {
            println!(
                "    No result files found in latest directory ({}).",
                latest_subdir
            );
            None
        } else {
            let local_subdir =
                local_result_subdir_from_pvc_request(kubectl, &helper_name, &latest_subdir);
            let local_dir = Path::new(output_dir)
                .join(local_subdir)
                .to_string_lossy()
                .to_string();
            std::fs::create_dir_all(&local_dir)?;

            // List individual files and copy each one via kubectl cp
            let file_list = kubectl
                .run(&[
                    "exec",
                    &helper_name,
                    "--",
                    "sh",
                    "-c",
                    &format!(
                        "find /results/{} -name '*.json' -type f 2>/dev/null",
                        latest_subdir
                    ),
                ])
                .unwrap_or_default();

            for remote_path in file_list.lines().filter(|l| !l.trim().is_empty()) {
                let remote_path = remote_path.trim();
                let filename = Path::new(remote_path)
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy();
                let local_path = format!("{}/{}", local_dir, filename);

                if !kubectl.copy_file_from_pod(&helper_name, remote_path, &local_path) {
                    eprintln!("    Warning: failed to copy {}", filename);
                }
            }

            // Count local results
            let local_count = count_json_files(Path::new(&local_dir));
            println!("    {} result file(s) saved to {}/", local_count, local_dir);
            Some(local_dir)
        }
    };

    // Cleanup helper pod
    let _ = kubectl.run(&["delete", "pod", &helper_name, "--ignore-not-found"]);
    Ok(result_dir)
}

fn local_result_subdir_from_pvc_request(
    kubectl: &Kubectl,
    helper_name: &str,
    pvc_subdir: &str,
) -> std::path::PathBuf {
    let request_json = kubectl
        .run(&[
            "exec",
            helper_name,
            "--",
            "sh",
            "-c",
            &format!("cat /results/{}/run-request.json 2>/dev/null", pvc_subdir),
        ])
        .unwrap_or_default();
    run_configs::local_result_subdir_from_request_json(pvc_subdir, &request_json)
}

/// Save coordinator and worker logs to a logs/ subdirectory within the result directory.
///
/// For coordinator and worker pods we read the persistent log file written by
/// log4j2's RollingFile appender (path supplied via the DIMSTER_LOG_FILE env
/// var on each pod) and filter it down to this run's begin/end window. That
/// file lives outside the per-run timestamp subdir so it survives container
/// restarts — which is exactly the case where `kubectl logs` would otherwise
/// drop history. We still capture `kubectl logs --previous` as a fallback for
/// the scenario where the pod restarted *during* the run (the in-memory log
/// file from the killed container is gone, but k8s retained the prior
/// container's stdout). For Kafka pods we keep the existing /opt/kafka/logs
/// file copy.
pub fn save_logs(kubectl: &Kubectl, result_dir: &str, full: bool) -> Result<()> {
    let logs_dir = format!("{}/logs", result_dir);
    std::fs::create_dir_all(&logs_dir)?;

    println!("==> Saving pod logs to {}/", logs_dir);

    // Resolve capture intent. `full=true` always wins, even if result metadata
    // exists. Otherwise we use Filtered when we have a begin/end window, and
    // fall back to Full when we don't (no metadata = no window to filter on,
    // so we may as well keep everything we can read from the pod).
    let time_range = extract_time_range(result_dir).ok();
    let mode = if full {
        LogCaptureMode::Full
    } else {
        match time_range {
            Some((begin, end)) => LogCaptureMode::Filtered { begin, end },
            None => LogCaptureMode::Full,
        }
    };

    // `--since-time` for the kubectl-logs fallback path. In Full mode we drop
    // it so we get whatever the kubelet has retained for the container.
    let since_time = match mode {
        LogCaptureMode::Filtered { begin, .. } => Some(format_rfc3339(begin)),
        LogCaptureMode::Full => None,
    };

    match mode {
        LogCaptureMode::Filtered { begin, end } => {
            println!(
                "    Filtering pod log files to [{} ± 15s, {} ± 15s]",
                format_log_timestamp(begin),
                format_log_timestamp(end)
            );
        }
        LogCaptureMode::Full => {
            if full {
                println!(
                    "    Capturing full coordinator/worker log files (--full: no time filter)"
                );
            } else {
                println!("    Warning: no result metadata yet — capturing full pod log files");
            }
        }
    }

    // Save coordinator logs
    let coord_pods = kubectl
        .run(&[
            "get",
            "pods",
            "-l",
            "app=benchmark-coordinator",
            "-o",
            "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
        ])
        .unwrap_or_default();

    for pod in coord_pods.lines().filter(|l| !l.trim().is_empty()) {
        let pod = pod.trim();
        save_pod_persisted_or_kubectl_logs(
            kubectl,
            pod,
            "/benchmark/results/.dimster-logs/coordinator.log",
            &logs_dir,
            mode,
            since_time.as_deref(),
        );
    }

    // Save worker logs
    let worker_pods = kubectl
        .run(&[
            "get",
            "pods",
            "-l",
            "app=benchmark-worker",
            "-o",
            "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
        ])
        .unwrap_or_default();

    for pod in worker_pods.lines().filter(|l| !l.trim().is_empty()) {
        let pod = pod.trim();
        save_pod_persisted_or_kubectl_logs(
            kubectl,
            pod,
            "/dimster/logs/worker.log",
            &logs_dir,
            mode,
            since_time.as_deref(),
        );
    }

    // Save Kafka logs (file-based for reliability — kubectl logs truncates on long campaigns)
    let kafka_pods = kubectl
        .run(&[
            "get",
            "pods",
            "-l",
            "app=kafka",
            "-o",
            "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
        ])
        .unwrap_or_default();

    for pod in kafka_pods.lines().filter(|l| !l.trim().is_empty()) {
        let pod = pod.trim();
        if !save_kafka_file_logs(kubectl, pod, &logs_dir) {
            eprintln!(
                "    Warning: file-based log download failed for {}, falling back to kubectl logs",
                pod
            );
            save_pod_logs(kubectl, pod, &logs_dir, since_time.as_deref());
        }
    }

    Ok(())
}

/// Try the persistent (PVC- or emptyDir-backed) log file first, applying the
/// requested capture mode (filtered or full). If the file isn't on the pod,
/// fall back to `kubectl logs` for the live stdout of the current container.
/// Always also capture `kubectl logs --previous` so that if the pod restarted
/// during the run we still get the killed container's stdout.
fn save_pod_persisted_or_kubectl_logs(
    kubectl: &Kubectl,
    pod: &str,
    remote_log_path: &str,
    logs_dir: &str,
    mode: LogCaptureMode,
    since_time: Option<&str>,
) {
    let captured = save_pod_log_file(kubectl, pod, remote_log_path, logs_dir, mode);

    if !captured {
        // Persisted file missing — fall back to kubectl logs for the active
        // container. This keeps things working on first run after upgrading
        // from an older pod image.
        save_pod_logs(kubectl, pod, logs_dir, since_time);
    } else {
        save_pod_logs_previous(kubectl, pod, logs_dir, since_time);
    }
}

/// Save only the previous container's stdout (if the pod restarted during the
/// run). Split out from save_pod_logs because the persisted-file path doesn't
/// need the current container's `kubectl logs` capture.
fn save_pod_logs_previous(kubectl: &Kubectl, pod: &str, logs_dir: &str, since_time: Option<&str>) {
    let mut prev_args: Vec<&str> = vec!["logs", pod, "--previous"];
    let prev_since_arg;
    if let Some(ts) = since_time {
        prev_since_arg = ts.to_string();
        prev_args.push("--since-time");
        prev_args.push(&prev_since_arg);
    }
    if let Ok(logs) = kubectl.run(&prev_args) {
        if !logs.is_empty() {
            let path = format!("{}/{}.previous.log", logs_dir, pod);
            if let Err(e) = std::fs::write(&path, &logs) {
                eprintln!(
                    "    Warning: failed to write previous logs for {}: {}",
                    pod, e
                );
            } else {
                println!("    Saved {} (previous container)", path);
            }
        }
    }
}

fn save_pod_logs(kubectl: &Kubectl, pod: &str, logs_dir: &str, since_time: Option<&str>) {
    // Current container logs
    let mut args: Vec<&str> = vec!["logs", pod];
    let since_arg;
    if let Some(ts) = since_time {
        since_arg = ts.to_string();
        args.push("--since-time");
        args.push(&since_arg);
    }
    match kubectl.run(&args) {
        Ok(logs) => {
            let path = format!("{}/{}.log", logs_dir, pod);
            if let Err(e) = std::fs::write(&path, &logs) {
                eprintln!("    Warning: failed to write logs for {}: {}", pod, e);
            } else {
                println!("    Saved {}", path);
            }
        }
        Err(e) => eprintln!("    Warning: failed to get logs for {}: {}", pod, e),
    }

    // Previous container logs (if pod was restarted during the test)
    let mut prev_args: Vec<&str> = vec!["logs", pod, "--previous"];
    let prev_since_arg;
    if let Some(ts) = since_time {
        prev_since_arg = ts.to_string();
        prev_args.push("--since-time");
        prev_args.push(&prev_since_arg);
    }
    if let Ok(logs) = kubectl.run(&prev_args) {
        if !logs.is_empty() {
            let path = format!("{}/{}.previous.log", logs_dir, pod);
            if let Err(e) = std::fs::write(&path, &logs) {
                eprintln!(
                    "    Warning: failed to write previous logs for {}: {}",
                    pod, e
                );
            } else {
                println!("    Saved {} (previous container)", path);
            }
        }
    }
}

/// Download Kafka log files from /opt/kafka/logs on the pod.
/// Returns true if at least one file was copied successfully.
fn save_kafka_file_logs(kubectl: &Kubectl, pod: &str, logs_dir: &str) -> bool {
    let pod_dir = format!("{}/kafka/{}", logs_dir, pod);
    if let Err(e) = std::fs::create_dir_all(&pod_dir) {
        eprintln!("    Warning: could not create directory {}: {}", pod_dir, e);
        return false;
    }

    // List log files on the pod
    let file_list = kubectl.run(&[
        "exec",
        pod,
        "--",
        "find",
        "/opt/kafka/logs",
        "-type",
        "f",
        "(",
        "-name",
        "*.log",
        "-o",
        "-name",
        "*.log.*",
        ")",
    ]);

    let files: Vec<&str> = match &file_list {
        Ok(output) => output
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| l.trim())
            .collect(),
        Err(e) => {
            eprintln!("    Warning: could not list log files on {}: {}", pod, e);
            return false;
        }
    };

    if files.is_empty() {
        println!("    {}: no log files found in /opt/kafka/logs", pod);
        return false;
    }

    println!("    {}: downloading {} log file(s)...", pod, files.len());

    let mut copied = 0;
    for remote_path in &files {
        let filename = Path::new(remote_path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy();
        let local_path = format!("{}/{}", pod_dir, filename);

        if kubectl.copy_file_from_pod(pod, remote_path, &local_path) {
            copied += 1;
        } else {
            eprintln!(
                "    Warning: failed to download {} from {}",
                remote_path, pod
            );
        }
    }

    if copied > 0 {
        println!("    {}: saved {} file(s) to {}/", pod, copied, pod_dir);
    }
    copied > 0
}

/// Save the persisted log file from a pod to `{logs_dir}/{pod}.log`, filtered
/// to lines whose timestamp falls within [begin_ts - BUFFER, end_ts + BUFFER].
///
/// Why this exists: log4j2 writes one long-lived file per pod that accumulates
/// across multiple `dimster run` invocations (the file lives outside the
/// per-run timestamped subdirectory so it survives container restarts on the
/// coordinator's PVC). To attribute lines to a specific run we filter by the
/// leading `yyyy-MM-dd HH:mm:ss.SSS` timestamp. The pod-side shell does only
/// concatenation (zcat for rolled .gz backups, cat for the active file) so
/// that the filtering itself stays in Rust where it's unit-testable.
///
/// Returns true if the file existed on the pod and we wrote a local file
/// (which may have zero bytes if no lines fell within the run window).
fn save_pod_log_file(
    kubectl: &Kubectl,
    pod: &str,
    remote_log_path: &str,
    logs_dir: &str,
    mode: LogCaptureMode,
) -> bool {
    // Pod-side script: concatenate rolled backups (oldest → newest by numeric
    // suffix; with log4j2's default DefaultRolloverStrategy, .log.1.gz is the
    // most-recent backup and .log.N.gz is the oldest, so we sort suffix
    // descending to emit oldest first), then the active file. Exit 7 when
    // nothing exists so the caller can fall back to `kubectl logs`.
    let script = format!(
        r#"set -e
LOG="{remote}"
if [ ! -f "$LOG" ] && ! ls "$LOG".*.gz >/dev/null 2>&1; then
  exit 7
fi
ls -1 "$LOG".*.gz 2>/dev/null | awk -F. '{{print $(NF-1)" "$0}}' | sort -k1,1 -nr | cut -d' ' -f2- | while read -r f; do
  zcat -- "$f"
done
[ -f "$LOG" ] && cat -- "$LOG"
exit 0
"#,
        remote = remote_log_path,
    );

    let raw = match kubectl.run(&["exec", pod, "--", "sh", "-c", &script]) {
        Ok(content) => content,
        Err(e) => {
            eprintln!(
                "    Warning: could not read {} on {}: {}",
                remote_log_path, pod, e
            );
            return false;
        }
    };

    let payload = render_persisted_log(&raw, mode);
    let local_path = format!("{}/{}.log", logs_dir, pod);
    if let Err(e) = std::fs::write(&local_path, payload.as_bytes()) {
        eprintln!("    Warning: failed to write {}: {}", local_path, e);
        return false;
    }
    println!("    Saved {} ({} bytes)", local_path, payload.len());
    true
}

/// Pure helper: given the raw concatenation produced by the pod-side script,
/// return the bytes that should land on disk for the requested capture mode.
/// Split out from `save_pod_log_file` so it's testable without a live cluster.
fn render_persisted_log(raw: &str, mode: LogCaptureMode) -> String {
    match mode {
        LogCaptureMode::Filtered { begin, end } => {
            filter_log_by_timestamp(raw, begin, end, BUFFER_SECS)
        }
        LogCaptureMode::Full => raw.to_string(),
    }
}

/// Buffer applied to the begin/end timestamps so the very first and very last
/// lines of a run aren't truncated due to second-resolution rounding or clock
/// drift between coordinator and worker pods.
const BUFFER_SECS: i64 = 15;

/// Filter a raw log4j2 stream down to lines whose timestamp falls within
/// `[begin_ts - buffer, end_ts + buffer]` (inclusive). Lines that don't begin
/// with a parseable `yyyy-MM-dd HH:mm:ss.SSS` timestamp inherit the previous
/// timestamped line's keep/drop decision so multi-line entries (stack traces,
/// continuations) stay intact.
///
/// Lexicographic comparison of the fixed-width ISO prefix is equivalent to
/// chronological comparison, which lets us avoid parsing the millisecond
/// portion. Lines before any timestamped line are dropped.
fn filter_log_by_timestamp(raw: &str, begin_ts: i64, end_ts: i64, buffer: i64) -> String {
    let start = format_log_timestamp(begin_ts - buffer);
    let end = format_log_timestamp(end_ts + buffer);
    let mut out = String::with_capacity(raw.len() / 4);
    let mut keep = false;
    for line in raw.split_inclusive('\n') {
        if let Some(ts) = leading_log_timestamp(line) {
            keep = ts >= start.as_str() && ts <= end.as_str();
        }
        if keep {
            out.push_str(line);
        }
    }
    out
}

/// Return the leading 23-byte timestamp slice ("yyyy-MM-dd HH:mm:ss.SSS") if
/// the line starts with one, else None. Strict on shape so non-log lines and
/// stack-trace continuations (which start with whitespace or "at ...") don't
/// match.
///
/// Operates on raw bytes throughout because log4j wraps multi-line log
/// messages — continuation lines often begin with box-drawing characters
/// (e.g. `── Throughput ──`, 3-byte UTF-8) and slicing `&line[..23]` mid-char
/// would panic. We only return a `&str` slice once we've verified all 23
/// leading bytes are ASCII (which guarantees a valid char boundary).
fn leading_log_timestamp(line: &str) -> Option<&str> {
    let b = line.as_bytes();
    if b.len() < 23 {
        return None;
    }
    let is_digit = |i: usize| b[i].is_ascii_digit();
    if is_digit(0)
        && is_digit(1)
        && is_digit(2)
        && is_digit(3)
        && b[4] == b'-'
        && is_digit(5)
        && is_digit(6)
        && b[7] == b'-'
        && is_digit(8)
        && is_digit(9)
        && b[10] == b' '
        && is_digit(11)
        && is_digit(12)
        && b[13] == b':'
        && is_digit(14)
        && is_digit(15)
        && b[16] == b':'
        && is_digit(17)
        && is_digit(18)
        && b[19] == b'.'
        && is_digit(20)
        && is_digit(21)
        && is_digit(22)
    {
        // All 23 leading bytes are ASCII, so the slice is on a char boundary.
        Some(&line[..23])
    } else {
        None
    }
}

/// Format a Unix timestamp as "yyyy-MM-dd HH:mm:ss.SSS" matching the log4j2
/// File appender pattern. Sub-second precision is always .000 because we only
/// have second-resolution input — the buffer (±15 s) absorbs the rounding.
fn format_log_timestamp(unix_ts: i64) -> String {
    let secs_per_day: i64 = 86400;
    let mut remaining = unix_ts;
    let mut year = 1970i64;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        let secs_in_year = days_in_year * secs_per_day;
        if remaining < secs_in_year {
            break;
        }
        remaining -= secs_in_year;
        year += 1;
    }
    let month_days = [
        31,
        28 + if is_leap_year(year) { 1 } else { 0 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1;
    for &md in &month_days {
        if remaining < md * secs_per_day {
            break;
        }
        remaining -= md * secs_per_day;
        month += 1;
    }
    let day = remaining / secs_per_day + 1;
    remaining %= secs_per_day;
    let hour = remaining / 3600;
    remaining %= 3600;
    let minute = remaining / 60;
    let second = remaining % 60;
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.000",
        year, month, day, hour, minute, second
    )
}

/// Format a Unix timestamp as RFC3339 for kubectl --since-time.
fn format_rfc3339(unix_ts: i64) -> String {
    let secs_per_day: i64 = 86400;
    let mut remaining = unix_ts;
    let mut year = 1970i64;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        let secs_in_year = days_in_year * secs_per_day;
        if remaining < secs_in_year {
            break;
        }
        remaining -= secs_in_year;
        year += 1;
    }
    let month_days = [
        31,
        28 + if is_leap_year(year) { 1 } else { 0 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1;
    for &md in &month_days {
        if remaining < md * secs_per_day {
            break;
        }
        remaining -= md * secs_per_day;
        month += 1;
    }
    let day = remaining / secs_per_day + 1;
    remaining %= secs_per_day;
    let hour = remaining / 3600;
    remaining %= 3600;
    let minute = remaining / 60;
    let second = remaining % 60;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hour, minute, second
    )
}

/// Save the run's Prometheus metrics in each requested output format by
/// running a short-lived pod that queries Prometheus with export.py: `html`
/// renders the Grafana dashboards as static Chart.js pages, `snapshot` dumps
/// the raw samples behind them (see `SNAPSHOT_DIR`). An empty list skips the
/// export entirely, without starting the pod.
pub fn export_metrics(
    kubectl: &Kubectl,
    result_dir: &str,
    outputs: &[MetricsOutput],
    stop: &AtomicBool,
) -> Result<()> {
    if outputs.is_empty() {
        println!("==> Metrics output disabled (none selected)");
        return Ok(());
    }
    let outputs_arg = outputs_arg(outputs);
    println!("==> Exporting metrics ({})...", outputs_arg);

    // Extract begin/end timestamps from the result JSON. Buffering and
    // window splitting are both handled by export.py (see --window-hours
    // and --window-buffer-minutes below), so we pass the raw range through.
    let (raw_start, raw_end) = extract_time_range(result_dir)?;
    println!(
        "    Time range: {} -> {} ({}s)",
        raw_start,
        raw_end,
        raw_end - raw_start
    );

    // Per-scenario time ranges, used by export.py to align HTML-window cuts
    // with scenario boundaries so a scenario's test points stay together in
    // a single file. Empty when the result JSON predates per-scenario
    // timestamps — export.py then falls back to its even-split algorithm.
    let scenario_boundaries = extract_scenario_boundaries(result_dir).unwrap_or_default();
    if !scenario_boundaries.is_empty() {
        println!(
            "    Discovered {} scenario boundary range(s) for window alignment",
            scenario_boundaries.len()
        );
    }

    // Derive PVC subdir name from the local result_dir path
    let pvc_subdir = Path::new(result_dir)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let helper_name = format!("dashboard-export-{}", timestamp);
    let ns = kubectl.namespace();

    // Pin to coordinator node (PVC is ReadWriteOnce / EBS)
    let coord_node = find_coordinator_node(kubectl);
    let node_name_field = if !coord_node.is_empty() {
        format!("  nodeName: {}", coord_node)
    } else {
        String::new()
    };

    let manifest = format!(
        r#"apiVersion: v1
kind: Pod
metadata:
  name: {helper_name}
  namespace: {ns}
  labels:
    app: dashboard-export
spec:
  restartPolicy: Never
{node_name_field}
  containers:
    - name: export
      image: python:3-slim
      command: ["sleep", "600"]
      volumeMounts:
        - name: results
          mountPath: /results
        - name: export-script
          mountPath: /scripts
        - name: dashboards-main
          mountPath: /dashboards/main
        - name: dashboards-system
          mountPath: /dashboards/system
  volumes:
    - name: results
      persistentVolumeClaim:
        claimName: benchmark-results
    - name: export-script
      configMap:
        name: dashboard-export-script
    - name: dashboards-main
      configMap:
        name: grafana-dashboards
    - name: dashboards-system
      configMap:
        name: grafana-dashboards-system"#
    );

    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), &manifest)?;
    kubectl.run_no_ns(&["-n", ns, "apply", "-f", &tmp.path().to_string_lossy()])?;

    if !wait_for_pod(kubectl, &helper_name, 60, stop) {
        if !stop.load(Ordering::Relaxed) {
            eprintln!("    Dashboard export pod failed to start:");
            let _ = kubectl.run_inherit(&["describe", "pod", &helper_name]);
        }
        let _ = kubectl.run(&["delete", "pod", &helper_name, "--ignore-not-found"]);
        if stop.load(Ordering::Relaxed) {
            bail!("Interrupted");
        }
        bail!("Dashboard export pod did not start");
    }

    // Run the export script. For long campaigns, --window-hours splits the
    // total range into multiple HTML files per dashboard so individual
    // files stay quick to load in a browser; --window-buffer-minutes adds
    // overlap on each side so transitions at boundaries remain visible.
    // --scenario-boundaries lets export.py align cuts with scenario
    // transitions, keeping a scenario's test points in the same file.
    let output_path = format!("/results/{}/dashboards", pvc_subdir);
    let snapshot_path = format!("/results/{}/{}", pvc_subdir, SNAPSHOT_DIR);
    let boundaries_json =
        serde_json::to_string(&scenario_boundaries).unwrap_or_else(|_| "[]".to_string());
    let raw_start_str = raw_start.to_string();
    let raw_end_str = raw_end.to_string();
    let exec_result = kubectl.run(&[
        "exec",
        &helper_name,
        "--",
        "python3",
        "/scripts/export.py",
        "--prometheus-url",
        "http://prometheus:9090",
        "--start",
        &raw_start_str,
        "--end",
        &raw_end_str,
        "--dashboard-dirs",
        "/dashboards/main,/dashboards/system",
        "--output-dir",
        &output_path,
        "--snapshot-dir",
        &snapshot_path,
        "--outputs",
        &outputs_arg,
        "--window-hours",
        "3",
        "--window-buffer-minutes",
        "10",
        "--scenario-boundaries",
        &boundaries_json,
    ]);

    match &exec_result {
        Ok(output) => println!("{}", output),
        Err(e) => eprintln!("    Warning: export script returned error: {}", e),
    }

    if outputs.contains(&MetricsOutput::Html) {
        copy_html_dashboards(kubectl, &helper_name, &output_path, result_dir)?;
    }
    if outputs.contains(&MetricsOutput::Snapshot) {
        copy_prometheus_snapshot(kubectl, &helper_name, &snapshot_path, result_dir)?;
    }

    // Cleanup
    let _ = kubectl.run(&["delete", "pod", &helper_name, "--ignore-not-found"]);
    Ok(())
}

/// The `--outputs` value export.py expects, e.g. "html,snapshot".
fn outputs_arg(outputs: &[MetricsOutput]) -> String {
    outputs
        .iter()
        .map(|o| o.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

/// Copy the generated dashboard HTML files to `<result_dir>/dashboards/`.
fn copy_html_dashboards(
    kubectl: &Kubectl,
    helper_name: &str,
    remote_dir: &str,
    result_dir: &str,
) -> Result<()> {
    let local_dashboards = format!("{}/dashboards", result_dir);
    let copied = copy_matching_files(
        kubectl,
        helper_name,
        remote_dir,
        "*.html",
        &local_dashboards,
    )?;
    println!(
        "    {} dashboard file(s) saved to {}/",
        copied, local_dashboards
    );
    Ok(())
}

/// Copy the snapshot data file and the dashboard JSON it was taken for to
/// `<result_dir>/prometheus-snapshot/`, keeping the remote layout so the
/// snapshot viewer can find both.
fn copy_prometheus_snapshot(
    kubectl: &Kubectl,
    helper_name: &str,
    remote_dir: &str,
    result_dir: &str,
) -> Result<()> {
    let local_snapshot = format!("{}/{}", result_dir, SNAPSHOT_DIR);
    let data = copy_matching_files(
        kubectl,
        helper_name,
        remote_dir,
        SNAPSHOT_FILE,
        &local_snapshot,
    )?;
    let dashboards = copy_matching_files(
        kubectl,
        helper_name,
        &format!("{}/dashboards", remote_dir),
        "*.json",
        &format!("{}/dashboards", local_snapshot),
    )?;
    if data == 0 {
        eprintln!("    Warning: no Prometheus snapshot was produced");
    } else {
        println!(
            "    Prometheus snapshot ({} dashboard(s)) saved to {}/",
            dashboards, local_snapshot
        );
    }
    Ok(())
}

/// Copy every file directly under `remote_dir` in the helper pod whose name
/// matches the shell glob `pattern` into `local_dir`. Returns the number of
/// files copied; individual copy failures are warned about, not fatal.
fn copy_matching_files(
    kubectl: &Kubectl,
    helper_name: &str,
    remote_dir: &str,
    pattern: &str,
    local_dir: &str,
) -> Result<usize> {
    std::fs::create_dir_all(local_dir)?;
    let file_list = kubectl
        .run(&[
            "exec",
            helper_name,
            "--",
            "sh",
            "-c",
            &format!(
                "find {} -maxdepth 1 -name '{}' -type f 2>/dev/null",
                remote_dir, pattern
            ),
        ])
        .unwrap_or_default();

    let mut copied_count = 0;
    for remote_path in file_list.lines().filter(|l| !l.trim().is_empty()) {
        let remote_path = remote_path.trim();
        let filename = Path::new(remote_path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy();
        let local_path = format!("{}/{}", local_dir, filename);

        if kubectl.copy_file_from_pod(helper_name, remote_path, &local_path) {
            copied_count += 1;
        } else {
            eprintln!("    Warning: failed to copy {}", filename);
        }
    }
    Ok(copied_count)
}

/// Extract begin and end Unix timestamps from the first result JSON in a directory.
/// When the benchmark is still running, endTime may be absent — in that case
/// we fall back to the current wall-clock time so callers can still operate
/// on the "start … now" range.
pub(crate) fn extract_time_range(result_dir: &str) -> Result<(i64, i64)> {
    let dir = std::fs::read_dir(result_dir)?;
    for entry in dir.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "json") {
            let content = std::fs::read_to_string(&path)?;
            let val: serde_json::Value = serde_json::from_str(&content)?;
            if let Some(metadata) = val.get("metadata") {
                let begin = metadata
                    .get("beginTime")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if begin.is_empty() {
                    continue;
                }
                let start_ts = parse_dimster_timestamp(begin)?;
                let end = metadata
                    .get("endTime")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let end_ts = if end.is_empty() {
                    // Benchmark still running — use current time
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64;
                    println!("    endTime not set (benchmark still running), using current time");
                    now
                } else {
                    parse_dimster_timestamp(end)?
                };
                return Ok((start_ts, end_ts));
            }
        }
    }
    bail!(
        "No result JSON with metadata.beginTime found in {}",
        result_dir
    )
}

/// One scenario's wall-clock range as serialized for export.py. The Python
/// side bin-packs these into HTML windows so all test points of a scenario
/// land in the same file.
#[derive(serde::Serialize)]
struct ScenarioBoundary {
    name: String,
    start: i64,
    end: i64,
}

/// Read per-scenario begin/end timestamps from result JSONs in a directory.
/// Returns an empty list if no scenario in any result file has both timestamps
/// — older result files (pre-scenario-timing) and synthetic "(merged)"
/// repeat-aggregates both fall through harmlessly. The caller treats an empty
/// list as "exporter should fall back to even time-based splitting."
fn extract_scenario_boundaries(result_dir: &str) -> Result<Vec<ScenarioBoundary>> {
    let mut out: Vec<ScenarioBoundary> = Vec::new();
    let dir = std::fs::read_dir(result_dir)?;
    for entry in dir.flatten() {
        let path = entry.path();
        if !path.extension().is_some_and(|e| e == "json") {
            continue;
        }
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let val: serde_json::Value = match serde_json::from_str(&content) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(scenarios) = val.get("scenarioResults").and_then(|v| v.as_array()) else {
            continue;
        };
        for sc in scenarios {
            let name = sc
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let begin = sc.get("beginTime").and_then(|v| v.as_str()).unwrap_or("");
            let end = sc.get("endTime").and_then(|v| v.as_str()).unwrap_or("");
            // Synthetic merged scenarios and unfinished scenarios won't have
            // both stamps — silently skip rather than emit malformed bounds.
            if begin.is_empty() || end.is_empty() {
                continue;
            }
            let (Ok(start_ts), Ok(end_ts)) =
                (parse_dimster_timestamp(begin), parse_dimster_timestamp(end))
            else {
                continue;
            };
            out.push(ScenarioBoundary {
                name,
                start: start_ts,
                end: end_ts,
            });
        }
    }
    // Sort by start time so the Python bin-packer can walk in chronological order
    // without re-sorting (it does so defensively anyway, but ordered input keeps
    // the on-the-wire JSON stable for log/diff readability).
    out.sort_by_key(|s| s.start);
    Ok(out)
}

/// Parse a Dimster timestamp "yyyy-MM-dd-HH-mm-ss" into a Unix timestamp (UTC).
fn parse_dimster_timestamp(s: &str) -> Result<i64> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 6 {
        bail!("Invalid timestamp format: {}", s);
    }
    let year: i64 = parts[0].parse()?;
    let month: i64 = parts[1].parse()?;
    let day: i64 = parts[2].parse()?;
    let hour: i64 = parts[3].parse()?;
    let minute: i64 = parts[4].parse()?;
    let second: i64 = parts[5].parse()?;

    // Days from year 1 to year Y (simplified, good for 2000-2100)
    let mut days: i64 = 0;
    for y in 1970..year {
        days += if is_leap_year(y) { 366 } else { 365 };
    }
    let month_days = [
        31,
        28 + if is_leap_year(year) { 1 } else { 0 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    for m in 0..(month - 1) as usize {
        days += month_days[m];
    }
    days += day - 1;

    Ok(days * 86400 + hour * 3600 + minute * 60 + second)
}

fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Count result JSON files under a result dir. The Prometheus snapshot's
/// dashboard JSON is skipped since it isn't a result.
fn count_json_files(dir: &Path) -> usize {
    walkdir(dir, "json", &[SNAPSHOT_DIR])
}

/// Copy results, save logs, export metrics, generate charts, and print summary.
pub fn collect_results(
    kubectl: &Kubectl,
    output_dir: &str,
    script_dir: &Path,
    metrics_outputs: &[MetricsOutput],
    stop: &AtomicBool,
) -> Result<()> {
    let result_dir = copy_results_from_pvc(kubectl, output_dir, stop)?;
    if let Some(ref dir) = result_dir {
        if !stop.load(Ordering::Relaxed) {
            // End-of-run pipeline always uses the per-run time filter — the
            // user-facing escape hatch for full capture lives on the explicit
            // `dimster results --full` command, not on this implicit path.
            save_logs(kubectl, dir, /*full=*/ false)?;
        }
        if !stop.load(Ordering::Relaxed) {
            if let Err(e) = export_metrics(kubectl, dir, metrics_outputs, stop) {
                eprintln!("    Warning: metrics export failed: {}", e);
            }
        }
        if !stop.load(Ordering::Relaxed) {
            if let Err(e) = chart::run(dir, script_dir) {
                eprintln!("    Warning: chart generation failed: {}", e);
            }
        }
        if !stop.load(Ordering::Relaxed) {
            if let Err(e) = csv::run(dir, None) {
                eprintln!("    Warning: CSV generation failed: {}", e);
            }
        }

        print_result_summary(dir);
    }
    if stop.load(Ordering::Relaxed) {
        println!("==> Interrupted during result collection.");
    } else {
        println!("==> Benchmark run complete.");
    }
    Ok(())
}

/// Print a clean summary of all files in the result directory.
fn print_result_summary(result_dir: &str) {
    println!();
    println!("==> Result output: {}/", result_dir);

    let dir = Path::new(result_dir);

    // Result JSON files (top level)
    let mut results = list_files_with_ext(dir, "json");
    results.sort();
    if !results.is_empty() {
        println!("    Results:");
        for f in &results {
            println!("      {}", f);
        }
    }

    // CSV exports written sibling to the JSON results
    let mut csvs = list_files_with_ext(dir, "csv");
    csvs.sort();
    if !csvs.is_empty() {
        println!("    CSVs:");
        for f in &csvs {
            println!("      {}", f);
        }
    }

    // Charts
    let charts_dir = dir.join("charts");
    if charts_dir.is_dir() {
        let mut charts = list_files_with_ext(&charts_dir, "html");
        charts.sort();
        if !charts.is_empty() {
            println!("    Charts:");
            for f in &charts {
                println!("      charts/{}", f);
            }
        }
    }

    // Dashboards
    let dashboards_dir = dir.join("dashboards");
    if dashboards_dir.is_dir() {
        let mut dashboards = list_files_with_ext(&dashboards_dir, "html");
        dashboards.sort();
        if !dashboards.is_empty() {
            println!("    Dashboards:");
            for f in &dashboards {
                println!("      dashboards/{}", f);
            }
        }
    }

    // Prometheus snapshot
    let snapshot_file = dir.join(SNAPSHOT_DIR).join(SNAPSHOT_FILE);
    if snapshot_file.is_file() {
        println!("    Prometheus snapshot:");
        println!("      {}/{}", SNAPSHOT_DIR, SNAPSHOT_FILE);
    }

    // Logs
    let logs_dir = dir.join("logs");
    if logs_dir.is_dir() {
        // Flat log files (coordinator, worker)
        let mut logs = list_files_with_ext(&logs_dir, "log");
        logs.sort();

        // Kafka log files in per-pod subdirectories (logs/kafka/{pod}/)
        let kafka_dir = logs_dir.join("kafka");
        let mut kafka_logs: Vec<String> = Vec::new();
        if kafka_dir.is_dir() {
            if let Ok(pods) = std::fs::read_dir(&kafka_dir) {
                let mut pod_dirs: Vec<_> = pods.flatten().filter(|e| e.path().is_dir()).collect();
                pod_dirs.sort_by_key(|e| e.file_name());
                for pod_entry in pod_dirs {
                    let pod_name = pod_entry.file_name().to_string_lossy().to_string();
                    let mut files = list_files_with_ext(&pod_entry.path(), "log");
                    // Also include rotated logs (e.g. server.log.1)
                    if let Ok(entries) = std::fs::read_dir(pod_entry.path()) {
                        for entry in entries.flatten() {
                            let name = entry.file_name().to_string_lossy().to_string();
                            if name.contains(".log.") && !files.contains(&name) {
                                files.push(name);
                            }
                        }
                    }
                    files.sort();
                    for f in files {
                        kafka_logs.push(format!("kafka/{}/{}", pod_name, f));
                    }
                }
            }
        }

        if !logs.is_empty() || !kafka_logs.is_empty() {
            println!("    Logs:");
            for f in &logs {
                println!("      logs/{}", f);
            }
            for f in &kafka_logs {
                println!("      logs/{}", f);
            }
        }
    }
}

/// List filenames (not full paths) with a given extension in a directory (non-recursive).
fn list_files_with_ext(dir: &Path, ext: &str) -> Vec<String> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|e| e == ext) {
                if let Some(name) = path.file_name() {
                    files.push(name.to_string_lossy().to_string());
                }
            }
        }
    }
    files
}

fn walkdir(dir: &Path, ext: &str, skip_dirs: &[&str]) -> usize {
    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if !skip_dirs.iter().any(|d| entry.file_name() == *d) {
                    count += walkdir(&path, ext, skip_dirs);
                }
            } else if path.extension().is_some_and(|e| e == ext) {
                count += 1;
            }
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    // Anchor: 2026-05-01 12:00:00 UTC = 1777982400 (computed by hand from
    // the formatter we use here, so we never compare against an external
    // timestamp library — we just round-trip our own format function).
    fn ts(date_hms: &str) -> i64 {
        // date_hms = "yyyy-MM-dd HH:mm:ss"
        let bytes = date_hms.as_bytes();
        let yr: i64 = std::str::from_utf8(&bytes[0..4]).unwrap().parse().unwrap();
        let mo: i64 = std::str::from_utf8(&bytes[5..7]).unwrap().parse().unwrap();
        let da: i64 = std::str::from_utf8(&bytes[8..10]).unwrap().parse().unwrap();
        let hr: i64 = std::str::from_utf8(&bytes[11..13])
            .unwrap()
            .parse()
            .unwrap();
        let mi: i64 = std::str::from_utf8(&bytes[14..16])
            .unwrap()
            .parse()
            .unwrap();
        let se: i64 = std::str::from_utf8(&bytes[17..19])
            .unwrap()
            .parse()
            .unwrap();
        // Re-implement the inverse of format_log_timestamp using the same
        // leap-year and month-table logic so the round-trip is consistent.
        let mut days: i64 = 0;
        for y in 1970..yr {
            days += if is_leap_year(y) { 366 } else { 365 };
        }
        let month_days = [
            31,
            28 + if is_leap_year(yr) { 1 } else { 0 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        for m in 0..(mo - 1) as usize {
            days += month_days[m];
        }
        days += da - 1;
        days * 86400 + hr * 3600 + mi * 60 + se
    }

    #[test]
    fn format_log_timestamp_round_trip() {
        // Sanity: format then re-parse to the same Unix timestamp.
        let cases = [
            "2026-05-01 12:00:00",
            "2026-12-31 23:59:59",
            "2024-02-29 00:00:00", // leap day
            "2025-03-01 00:00:00",
        ];
        for c in cases {
            let unix = ts(c);
            let formatted = format_log_timestamp(unix);
            assert!(formatted.starts_with(c), "got {} for {}", formatted, c);
            assert!(formatted.ends_with(".000"));
        }
    }

    #[test]
    fn leading_log_timestamp_strict_match() {
        assert_eq!(
            leading_log_timestamp("2026-05-01 12:00:00.123 INFO: hi\n"),
            Some("2026-05-01 12:00:00.123"),
        );
        // Stack trace continuation
        assert_eq!(
            leading_log_timestamp("\tat com.example.Foo(Bar.java:42)\n"),
            None
        );
        // Wrong width
        assert_eq!(leading_log_timestamp("2026-5-01 12:00:00.123 hi\n"), None);
        // Letters where digits expected
        assert_eq!(leading_log_timestamp("abcd-05-01 12:00:00.123 hi\n"), None);
        // Too short
        assert_eq!(leading_log_timestamp("hi\n"), None);
    }

    #[test]
    fn filter_keeps_only_lines_in_window() {
        let raw = "\
2026-05-01 11:59:00.000 INFO: way before\n\
2026-05-01 11:59:50.000 INFO: just outside lower buffer\n\
2026-05-01 11:59:55.000 INFO: inside lower buffer\n\
2026-05-01 12:00:00.000 INFO: at start\n\
2026-05-01 12:30:00.000 INFO: middle\n\
2026-05-01 13:00:00.000 INFO: at end\n\
2026-05-01 13:00:14.999 INFO: still in upper buffer\n\
2026-05-01 13:01:00.000 INFO: outside upper buffer\n";
        let begin = ts("2026-05-01 12:00:00");
        let end = ts("2026-05-01 13:00:00");
        let out = filter_log_by_timestamp(raw, begin, end, 15);
        assert!(out.contains("inside lower buffer"));
        assert!(out.contains("at start"));
        assert!(out.contains("middle"));
        assert!(out.contains("at end"));
        assert!(out.contains("still in upper buffer"));
        assert!(!out.contains("way before"));
        // 11:59:50.000 < 11:59:45.000 (begin - 15s)? No: begin-15 = 11:59:45,
        // and 11:59:50 > 11:59:45, so this line IS kept. Adjust assertion.
        assert!(out.contains("just outside lower buffer"));
        assert!(!out.contains("outside upper buffer"));
    }

    #[test]
    fn filter_keeps_multiline_continuations_with_their_owning_line() {
        let raw = "\
2026-05-01 11:00:00.000 ERROR: pre-run noise\n\
\tat com.example.Pre(File.java:1)\n\
2026-05-01 12:30:00.000 ERROR: in-run failure\n\
\tat com.example.Inner(File.java:42)\n\
\tat com.example.Outer(File.java:99)\n\
Caused by: java.lang.NullPointerException\n\
2026-05-01 12:30:00.500 INFO: after stack trace\n";
        let begin = ts("2026-05-01 12:00:00");
        let end = ts("2026-05-01 13:00:00");
        let out = filter_log_by_timestamp(raw, begin, end, 15);
        // Pre-run error line + its stack trace are dropped together
        assert!(!out.contains("pre-run noise"));
        assert!(!out.contains("com.example.Pre"));
        // In-run error line + its full stack trace are kept together
        assert!(out.contains("in-run failure"));
        assert!(out.contains("com.example.Inner"));
        assert!(out.contains("com.example.Outer"));
        assert!(out.contains("Caused by"));
        assert!(out.contains("after stack trace"));
    }

    #[test]
    fn filter_drops_orphaned_continuation_lines_before_first_timestamp() {
        // If the very first line of input is a continuation (no leading
        // timestamp), it has no owning line so it must be dropped.
        let raw = "\
\tat com.example.Orphan(File.java:1)\n\
2026-05-01 12:30:00.000 INFO: real entry\n";
        let begin = ts("2026-05-01 12:00:00");
        let end = ts("2026-05-01 13:00:00");
        let out = filter_log_by_timestamp(raw, begin, end, 15);
        assert!(!out.contains("Orphan"));
        assert!(out.contains("real entry"));
    }

    #[test]
    fn filter_handles_empty_input() {
        let out = filter_log_by_timestamp("", 1000, 2000, 15);
        assert_eq!(out, "");
    }

    #[test]
    fn render_persisted_log_full_passes_raw_through_unchanged() {
        // Full mode is the --full escape hatch: every byte the pod-side
        // script produced (including post-end_ts spew that Filtered would
        // strip) must land on disk verbatim.
        let raw = "\
2026-05-01 11:00:00.000 INFO: way before run\n\
2026-05-01 12:30:00.000 INFO: during run\n\
2026-05-01 14:00:00.000 INFO: long after end_ts\n";
        let out = render_persisted_log(raw, LogCaptureMode::Full);
        assert_eq!(out, raw);
    }

    #[test]
    fn render_persisted_log_filtered_applies_window() {
        // Sanity-check that Filtered mode still routes through
        // filter_log_by_timestamp — the post-end_ts line must be dropped.
        let raw = "\
2026-05-01 11:00:00.000 INFO: way before run\n\
2026-05-01 12:30:00.000 INFO: during run\n\
2026-05-01 14:00:00.000 INFO: long after end_ts\n";
        let begin = ts("2026-05-01 12:00:00");
        let end = ts("2026-05-01 13:00:00");
        let out = render_persisted_log(raw, LogCaptureMode::Filtered { begin, end });
        assert!(!out.contains("way before run"));
        assert!(out.contains("during run"));
        assert!(!out.contains("long after end_ts"));
    }

    #[test]
    fn leading_timestamp_does_not_panic_on_multibyte_continuation() {
        // Multi-line log4j messages produce continuation lines that often
        // start with box-drawing chars (3-byte UTF-8). Slicing &line[..23]
        // naïvely would panic mid-character — guard against that.
        let line = "── Throughput ─────────────────────────────────────────────\n";
        assert!(line.as_bytes().len() >= 23);
        assert_eq!(leading_log_timestamp(line), None);
    }

    #[test]
    fn filter_keeps_multibyte_continuation_lines() {
        let raw = "\
2026-05-03 08:08:53.781 INFO:
══ Aggregated Results ═════════════════════════════════════
                                avg        p50        p95
  Publish Latency (ms)         3.00       3.28       5.52
2026-05-03 08:08:53.804 INFO: Per-partition metrics recorded
";
        let begin = ts("2026-05-03 08:00:00");
        let end = ts("2026-05-03 09:00:00");
        let out = filter_log_by_timestamp(raw, begin, end, 15);
        assert!(out.contains("Aggregated Results"));
        assert!(out.contains("Publish Latency"));
        assert!(out.contains("Per-partition metrics recorded"));
    }

    #[test]
    fn outputs_arg_joins_in_order() {
        assert_eq!(outputs_arg(&[MetricsOutput::Html]), "html");
        assert_eq!(
            outputs_arg(&[MetricsOutput::Snapshot, MetricsOutput::Html]),
            "snapshot,html"
        );
    }

    /// The snapshot carries Grafana dashboard JSON; it must not be counted
    /// as a result file, while result JSON in other subdirs still is.
    #[test]
    fn count_json_files_skips_snapshot_dashboards() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join("run-result.json"), "{}").unwrap();
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(root.join("nested/other.json"), "{}").unwrap();
        let snap_dash = root.join(SNAPSHOT_DIR).join("dashboards");
        std::fs::create_dir_all(&snap_dash).unwrap();
        std::fs::write(snap_dash.join("kafka.json"), "{}").unwrap();
        std::fs::write(snap_dash.join("system.json"), "{}").unwrap();
        assert_eq!(count_json_files(root), 2);
    }
}
