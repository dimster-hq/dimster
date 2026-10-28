pub mod broker_config;
pub mod build;
pub mod chart;
pub mod compare;
pub mod csv;
pub mod dashboard;
pub mod deploy;
pub mod download_kafka_logs;
pub mod flink;
pub mod health;
pub mod hooks;
pub mod init;
pub mod inspect;
pub mod kafka;
pub mod kafka_tls;
pub mod logs;
pub mod pivot;
pub mod reattach;
pub mod resources;
pub mod results;
pub mod resume;
pub mod run;
pub mod run_configs;
pub mod snapshot_viewer;
pub mod stop;
pub mod teardown;

use anyhow::{Result, bail};

use crate::kubectl::Kubectl;

/// Returns true when the image name looks like a registry URL (contains a dot).
/// Plain names like "dimster" are local; "123.dkr.ecr.us-east-1.amazonaws.com/dimster"
/// is remote. The CLI uses this to pick `imagePullPolicy: Always` for remote
/// images (so freshly-pushed tags are picked up) vs `IfNotPresent` for local
/// images (so we don't try to pull from a registry that doesn't have the image).
/// Three call sites today: deploy, build, and inspect's pod creation.
pub fn is_registry_image(image: &str) -> bool {
    image.contains('.')
}

/// Returns the name of a running benchmark-coordinator pod, or an error
/// if none is found. Shared across stop, resume, and reattach so each
/// command targets the same coordinator without duplicating the
/// label/selector logic.
pub fn find_coordinator(kubectl: &Kubectl) -> Result<String> {
    // Use range-based jsonpath to avoid error when no items exist
    let pods = kubectl
        .run(&[
            "get",
            "pods",
            "-l",
            "app=benchmark-coordinator",
            "--field-selector=status.phase=Running",
            "-o",
            "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
        ])
        .unwrap_or_default();

    let pod = pods.lines().next().unwrap_or("").trim().to_string();
    if pod.is_empty() {
        bail!("No running benchmark coordinator found. Run 'dimster deploy' first.");
    }
    Ok(pod)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_registry_image_detects_dot() {
        assert!(is_registry_image("registry.example.com/dimster"));
        assert!(is_registry_image(
            "123.dkr.ecr.us-west-2.amazonaws.com/dimster"
        ));
        assert!(!is_registry_image("dimster"));
        assert!(!is_registry_image("local-image"));
    }
}
