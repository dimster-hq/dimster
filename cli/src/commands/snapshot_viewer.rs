//! Local viewer for a run's Prometheus snapshot (`dimster dashboard --snapshot`).
//!
//! Exists so a finished run can be explored in real Grafana long after the
//! cluster is gone. The end-of-run export (results.rs `export_metrics`) saves
//! `prometheus-snapshot/` into the result dir: raw samples as gzipped
//! OpenMetrics plus the dashboard JSON they were taken for. This module turns
//! that back into a throwaway Prometheus + Grafana pair in Docker, then removes
//! everything on Ctrl+C. It deliberately never touches the in-cluster
//! monitoring stack (see dashboard.rs for that).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::commands::results::{self, SNAPSHOT_DIR, SNAPSHOT_FILE};

// Same images as the in-cluster stack (run/deploy/kubernetes/base/monitoring/
// prometheus.yaml and grafana.yaml) so dashboards render identically.
const PROMETHEUS_IMAGE: &str = "prom/prometheus:v2.51.0";
const GRAFANA_IMAGE: &str = "grafana/grafana:10.4.0";

/// Padding around the run window in the printed links, matching the buffer
/// export.py adds when taking the snapshot.
const RANGE_BUFFER_SECS: i64 = 600;

/// Preferred host ports; the usual Grafana/Prometheus ports unless they're
/// taken, e.g. by a `dimster dashboard <env>` port-forward running alongside.
const PREFERRED_GRAFANA_PORT: u16 = 3000;
const PREFERRED_PROMETHEUS_PORT: u16 = 9090;

pub fn run(result_dir: &Path, stop: Arc<AtomicBool>) -> Result<()> {
    let (snapshot_file, dashboards_dir) = snapshot_paths(result_dir)?;
    check_docker()?;
    let grafana_port = pick_port(PREFERRED_GRAFANA_PORT)?;
    let prometheus_port = pick_port(PREFERRED_PROMETHEUS_PORT)?;

    let (start, end) = results::extract_time_range(&result_dir.to_string_lossy())?;
    let (from_ms, to_ms) = (
        (start - RANGE_BUFFER_SECS) * 1000,
        (end + RANGE_BUFFER_SECS) * 1000,
    );

    // Everything generated lives in a temp dir that is deleted on drop. It
    // sits next to the snapshot rather than in the system temp dir because
    // VM-based Docker (e.g. Colima) only shares the user's home with
    // containers. The containers read it through bind mounts, so it must be
    // readable by the container users rather than just us (TempDir
    // defaults to 0700).
    let snapshot_dir = snapshot_file
        .parent()
        .context("snapshot file has no parent")?;
    let work = tempfile::Builder::new()
        .prefix(".viewer-")
        .tempdir_in(snapshot_dir)?;
    let work_dir = work.path().to_path_buf();
    set_mode(&work_dir, 0o755)?;
    let user = current_user()?;

    println!(
        "==> Building Prometheus blocks from {}...",
        snapshot_file.display()
    );
    import_snapshot(&snapshot_file, &work_dir, &user)?;

    let suffix = std::process::id();
    let network = format!("dimster-snapshot-{}", suffix);
    let prom_name = format!("dimster-snapshot-prometheus-{}", suffix);
    let grafana_name = format!("dimster-snapshot-grafana-{}", suffix);
    let mut cleanup = DockerCleanup::default();

    docker(&["network", "create", &network])?;
    cleanup.network = Some(network.clone());

    println!("==> Starting Prometheus and Grafana...");
    write_file(&work_dir.join("prometheus.yml"), "scrape_configs: []\n")?;
    docker(&[
        "run",
        "-d",
        "--name",
        &prom_name,
        "--network",
        &network,
        "--user",
        &user,
        "-p",
        &format!("{}:9090", prometheus_port),
        "-v",
        &format!("{}:/prometheus", work_dir.join("data").display()),
        "-v",
        &format!(
            "{}:/etc/prometheus/prometheus.yml:ro",
            work_dir.join("prometheus.yml").display()
        ),
        PROMETHEUS_IMAGE,
        "--config.file=/etc/prometheus/prometheus.yml",
        "--storage.tsdb.path=/prometheus",
        // Backfilled samples are old; never let retention drop them.
        "--storage.tsdb.retention.time=100y",
        // A snapshot has no staleness markers, so with the default 5m
        // lookback a series that ended mid-run would carry on for 5 minutes.
        // 3x the 10s scrape interval (prometheus-config.yaml) ends it about
        // as promptly as live Prometheus while tolerating a missed scrape.
        "--query.lookback-delta=30s",
    ])?;
    cleanup.containers.push(prom_name.clone());

    let provisioning = work_dir.join("provisioning");
    write_file(
        &provisioning.join("datasources/datasource.yaml"),
        &datasource_yaml(&format!("http://{}:9090", prom_name)),
    )?;
    write_file(
        &provisioning.join("dashboards/dashboards.yaml"),
        DASHBOARD_PROVIDER_YAML,
    )?;
    docker(&[
        "run",
        "-d",
        "--name",
        &grafana_name,
        "--network",
        &network,
        "-p",
        &format!("{}:3000", grafana_port),
        "-e",
        "GF_AUTH_ANONYMOUS_ENABLED=true",
        "-e",
        "GF_AUTH_ANONYMOUS_ORG_ROLE=Admin",
        "-e",
        "GF_AUTH_DISABLE_LOGIN_FORM=true",
        "-v",
        &format!("{}:/etc/grafana/provisioning:ro", provisioning.display()),
        "-v",
        &format!(
            "{}:/var/lib/grafana/dashboards:ro",
            dashboards_dir.display()
        ),
        GRAFANA_IMAGE,
    ])?;
    cleanup.containers.push(grafana_name.clone());

    wait_for_grafana(grafana_port, &stop)?;

    println!("==> Snapshot of {}", result_dir.display());
    for (title, url) in dashboard_links(&dashboards_dir, grafana_port, from_ms, to_ms)? {
        println!("    {:<10} {}", title, url);
    }
    println!("    Prometheus http://localhost:{}", prometheus_port);
    println!("    Press Ctrl+C to stop");

    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(500));
    }
    println!("\n==> Removing snapshot containers...");
    // `cleanup` then `work` drop here, removing containers before the temp
    // dir they mount.
    drop(cleanup);
    Ok(())
}

/// Locate the snapshot data file and dashboards dir in a result dir, with an
/// actionable error when the run was saved without a snapshot.
fn snapshot_paths(result_dir: &Path) -> Result<(PathBuf, PathBuf)> {
    let snapshot_dir = result_dir.join(SNAPSHOT_DIR);
    let data = snapshot_dir.join(SNAPSHOT_FILE);
    let dashboards = snapshot_dir.join("dashboards");
    if !data.is_file() {
        bail!(
            "No Prometheus snapshot at {}. Save one with `metricsOutput: [snapshot]` \
             in the environment, `--metrics-output snapshot` on the run, or \
             `dimster results <env> --metrics-output snapshot` while the cluster's \
             Prometheus still holds the run.",
            data.display()
        );
    }
    if !dashboards.is_dir() {
        bail!(
            "Snapshot is missing its dashboards: {}",
            dashboards.display()
        );
    }
    // Absolute paths, since they become docker bind mounts.
    Ok((data.canonicalize()?, dashboards.canonicalize()?))
}

/// Decompress the snapshot and turn it into TSDB blocks with promtool, both
/// inside the Prometheus image so the host needs nothing beyond Docker.
fn import_snapshot(snapshot_file: &Path, work_dir: &Path, user: &str) -> Result<()> {
    let snapshot_dir = snapshot_file
        .parent()
        .context("snapshot file has no parent")?;
    docker(&[
        "run",
        "--rm",
        "--user",
        user,
        "-v",
        &format!("{}:/snapshot:ro", snapshot_dir.display()),
        "-v",
        &format!("{}:/work", work_dir.display()),
        "--entrypoint",
        "sh",
        PROMETHEUS_IMAGE,
        "-c",
        &format!(
            "gunzip -c /snapshot/{} > /work/metrics.om && \
             promtool tsdb create-blocks-from openmetrics /work/metrics.om /work/data && \
             rm /work/metrics.om",
            SNAPSHOT_FILE
        ),
    ])?;
    Ok(())
}

/// Grafana datasource provisioning. It's the default datasource, which is
/// what the dashboards' `${DS_PROMETHEUS}` variable resolves to.
fn datasource_yaml(prometheus_url: &str) -> String {
    format!(
        r#"apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: {}
    isDefault: true
    editable: true
    jsonData:
      timeInterval: 10s
"#,
        prometheus_url
    )
}

const DASHBOARD_PROVIDER_YAML: &str = r#"apiVersion: 1
providers:
  - name: 'dimster-snapshot'
    orgId: 1
    folder: 'Dimster'
    type: file
    options:
      path: /var/lib/grafana/dashboards
"#;

/// One (title, URL) per dashboard JSON, each URL opening that dashboard on
/// the run's time range. Sorted by title for stable output.
fn dashboard_links(
    dashboards_dir: &Path,
    grafana_port: u16,
    from_ms: i64,
    to_ms: i64,
) -> Result<Vec<(String, String)>> {
    let mut links = Vec::new();
    for entry in std::fs::read_dir(dashboards_dir)?.flatten() {
        let path = entry.path();
        if !path.extension().is_some_and(|e| e == "json") {
            continue;
        }
        let dashboard: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path)?)
                .with_context(|| format!("Invalid dashboard JSON: {}", path.display()))?;
        let Some(uid) = dashboard.get("uid").and_then(|v| v.as_str()) else {
            continue;
        };
        let title = path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        links.push((
            title,
            format!(
                "http://localhost:{}/d/{}?from={}&to={}",
                grafana_port, uid, from_ms, to_ms
            ),
        ));
    }
    links.sort();
    Ok(links)
}

/// Poll Grafana's health endpoint until it answers 200, so the printed links
/// work as soon as they appear.
fn wait_for_grafana(port: u16, stop: &AtomicBool) -> Result<()> {
    for _ in 0..120 {
        if stop.load(Ordering::Relaxed) {
            bail!("Interrupted");
        }
        if grafana_healthy(port) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    bail!("Grafana did not become healthy on port {}", port)
}

fn grafana_healthy(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    if stream
        .write_all(b"GET /api/health HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200")
}

/// `preferred` if it's free on localhost, otherwise any free port.
fn pick_port(preferred: u16) -> Result<u16> {
    if TcpListener::bind(("127.0.0.1", preferred)).is_ok() {
        return Ok(preferred);
    }
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

fn check_docker() -> Result<()> {
    let ok = Command::new("docker")
        .args(["info"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    if !ok {
        bail!(
            "Docker is required to view a snapshot, but `docker info` failed. Is Docker running?"
        );
    }
    Ok(())
}

/// Run a docker command, returning stdout or failing with its stderr.
fn docker(args: &[&str]) -> Result<String> {
    let output = Command::new("docker").args(args).output()?;
    if !output.status.success() {
        bail!(
            "docker {} failed: {}",
            args.first().unwrap_or(&""),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// "uid:gid" of the current user, so files the containers write into the
/// temp dir stay removable by us.
fn current_user() -> Result<String> {
    let id = |flag: &str| -> Result<String> {
        let out = Command::new("id").arg(flag).output()?;
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    };
    Ok(format!("{}:{}", id("-u")?, id("-g")?))
}

fn write_file(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        set_mode(parent, 0o755)?;
    }
    std::fs::write(path, content)?;
    set_mode(path, 0o644)
}

fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    Ok(())
}

/// Removes the viewer's containers and network when dropped, so an error or
/// Ctrl+C part-way through never leaves them running.
#[derive(Default)]
struct DockerCleanup {
    containers: Vec<String>,
    network: Option<String>,
}

impl Drop for DockerCleanup {
    fn drop(&mut self) {
        for name in &self.containers {
            let _ = docker(&["rm", "-f", name]);
        }
        if let Some(network) = &self.network {
            let _ = docker(&["network", "rm", network]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_port_falls_back_when_preferred_is_taken() {
        let held = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let taken = held.local_addr().unwrap().port();
        let picked = pick_port(taken).unwrap();
        assert_ne!(picked, taken);
        drop(held);
        assert_eq!(pick_port(taken).unwrap(), taken);
    }

    #[test]
    fn missing_snapshot_explains_how_to_create_one() {
        let tmp = tempfile::tempdir().unwrap();
        let err = snapshot_paths(tmp.path()).unwrap_err().to_string();
        assert!(err.contains("No Prometheus snapshot"));
        assert!(err.contains("--metrics-output snapshot"));
    }

    #[test]
    fn snapshot_without_dashboards_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join(SNAPSHOT_DIR);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(SNAPSHOT_FILE), b"").unwrap();
        let err = snapshot_paths(tmp.path()).unwrap_err().to_string();
        assert!(err.contains("missing its dashboards"));
    }

    #[test]
    fn datasource_is_default_and_points_at_container() {
        let yaml = datasource_yaml("http://prom-1:9090");
        let parsed: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        let ds = &parsed["datasources"][0];
        assert_eq!(ds["url"].as_str(), Some("http://prom-1:9090"));
        assert_eq!(ds["isDefault"].as_bool(), Some(true));
        assert_eq!(ds["type"].as_str(), Some("prometheus"));
    }

    #[test]
    fn provider_yaml_points_at_mounted_dashboards() {
        let parsed: serde_yaml::Value = serde_yaml::from_str(DASHBOARD_PROVIDER_YAML).unwrap();
        assert_eq!(
            parsed["providers"][0]["options"]["path"].as_str(),
            Some("/var/lib/grafana/dashboards")
        );
    }

    #[test]
    fn dashboard_links_use_uid_and_range() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("kafka.json"), r#"{"uid": "dimster-kafka"}"#).unwrap();
        std::fs::write(
            tmp.path().join("clients.json"),
            r#"{"uid": "dimster-clients"}"#,
        )
        .unwrap();
        // No uid: can't be linked, so it's skipped rather than failing.
        std::fs::write(tmp.path().join("other.json"), r#"{"title": "x"}"#).unwrap();
        std::fs::write(tmp.path().join("notes.txt"), "ignored").unwrap();

        let links = dashboard_links(tmp.path(), 3001, 1000, 2000).unwrap();
        assert_eq!(
            links,
            vec![
                (
                    "clients".to_string(),
                    "http://localhost:3001/d/dimster-clients?from=1000&to=2000".to_string()
                ),
                (
                    "kafka".to_string(),
                    "http://localhost:3001/d/dimster-kafka?from=1000&to=2000".to_string()
                ),
            ]
        );
    }
}
