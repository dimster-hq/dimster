use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::Result;

use crate::commands::find_coordinator;
use crate::commands::results;
use crate::commands::run::stream_and_wait;
use crate::config::MetricsOutput;
use crate::kubectl::Kubectl;

pub fn run(
    kubectl: &Kubectl,
    output_dir: &str,
    script_dir: &Path,
    stop: Arc<AtomicBool>,
    environment: &str,
    metrics_outputs: &[MetricsOutput],
) -> Result<()> {
    let coord_pod = find_coordinator(kubectl)?;

    let status = kubectl
        .exec_curl(&coord_pod, "GET", "http://localhost:8080/status", None)
        .unwrap_or_else(|_| "{}".to_string());

    if status.contains("\"RUNNING\"") || status.contains("\"STOPPING\"") {
        println!("==> Reattaching to running benchmark ({})...", coord_pod);
        let detached = stream_and_wait(kubectl, &coord_pod, stop.clone())?;

        if detached {
            println!();
            println!("==> Detached from benchmark (still running on coordinator).");
            println!("    To reattach:  dimster reattach {}", environment);
            println!("    To stop:      dimster stop {}", environment);
            return Ok(());
        }
    } else {
        println!("==> Coordinator is idle. Copying latest results...");
    }

    results::collect_results(kubectl, output_dir, script_dir, metrics_outputs, &stop)?;
    Ok(())
}
