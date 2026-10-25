use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Result, bail};
use serde::Serialize;

use crate::commands::find_coordinator;
use crate::commands::results;
use crate::commands::run::stream_and_wait;
use crate::config::MetricsOutput;
use crate::kubectl::Kubectl;

#[derive(Serialize)]
struct ResumeRequest {
    workers: Vec<String>,
}

pub fn run(
    kubectl: &Kubectl,
    output_dir: &str,
    script_dir: &Path,
    stop: Arc<AtomicBool>,
    environment: &str,
    metrics_outputs: &[MetricsOutput],
) -> Result<()> {
    // Check coordinator status
    println!("==> Checking coordinator...");
    let coord_pod = find_coordinator(kubectl)?;

    let status = kubectl
        .exec_curl(&coord_pod, "GET", "http://localhost:8080/status", None)
        .unwrap_or_else(|_| "{}".to_string());

    if status.contains("\"RUNNING\"") || status.contains("\"STOPPING\"") {
        bail!(
            "Coordinator is busy ({}). Use 'dimster reattach {}' to follow it, or 'dimster stop {}' first.",
            status,
            environment,
            environment
        );
    }

    if !status.contains("\"checkpoint\":true") {
        bail!("No checkpoint found. Nothing to resume.");
    }

    println!("    Coordinator has a checkpoint ready to resume.");

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

    // Build resume request (only fresh worker list — everything else from checkpoint)
    let request = ResumeRequest { workers };
    let request_json = serde_json::to_string(&request)?;

    // Send resume request
    let tmp_file = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp_file.path(), &request_json)?;

    println!("==> Resuming benchmark...");
    kubectl.cp_to_pod(
        &tmp_file.path().to_string_lossy(),
        &coord_pod,
        "/tmp/resume-request.json",
    )?;

    let response = kubectl.exec_curl(
        &coord_pod,
        "POST",
        "http://localhost:8080/resume",
        Some("/tmp/resume-request.json"),
    )?;

    if response.contains("\"error\"") {
        bail!("Failed to resume benchmark: {}", response);
    }
    println!("    {}", response);

    // Stream coordinator logs until benchmark completes or user detaches
    println!("==> Following benchmark output...");
    let detached = stream_and_wait(kubectl, &coord_pod, stop.clone())?;

    if detached {
        println!();
        println!("==> Detached from benchmark (still running on coordinator).");
        println!("    To reattach:  dimster reattach {}", environment);
        println!("    To stop:      dimster stop {}", environment);
        return Ok(());
    }

    // Check if the benchmark failed again
    let final_status = kubectl
        .exec_curl(&coord_pod, "GET", "http://localhost:8080/status", None)
        .unwrap_or_else(|_| "{}".to_string());

    if final_status.contains("\"FAILED\"") {
        eprintln!("==> Benchmark FAILED. Checkpoint preserved.");
        eprintln!("    To resume:  dimster resume {}", environment);
        bail!("Benchmark failed — check coordinator logs for details");
    }

    // Collect results
    results::collect_results(kubectl, output_dir, script_dir, metrics_outputs, &stop)?;
    Ok(())
}
