//! Opt-in per-container resource metrics (`monitoring.cgroupMetrics`).
//!
//! node-exporter reports per node, which can't split CPU, disk and network
//! between services sharing a node (always the case on a single-machine
//! cluster). Every kubelet embeds cAdvisor, which reads each container's
//! cgroup counters; this module adds the Prometheus scrape job for it and
//! the RBAC that job needs. The resources report (export.py and
//! charts/generate_resource_charts.py) uses the metrics when present.
//!
//! Prometheus reaches each kubelet through the API server's node proxy, so
//! it needs a ClusterRole on nodes: cluster-scoped, which is why this is
//! off by default. The role and binding carry the namespace in their names
//! so Dimster deploys in sibling namespaces don't collide.

use std::path::Path;

use anyhow::Result;

/// The generated RBAC manifest, written at the root of the temporary
/// kustomize overlay by deploy and teardown.
pub(crate) const RBAC_FILE: &str = "prometheus-cadvisor-rbac.yaml";

/// The only cAdvisor metrics kept: those the resources report and the
/// Containers dashboard use. cAdvisor emits dozens per container.
const KEPT_METRICS: &[&str] = &[
    "container_cpu_usage_seconds_total",
    "container_cpu_cfs_periods_total",
    "container_cpu_cfs_throttled_periods_total",
    "container_spec_cpu_quota",
    "container_spec_cpu_period",
    "container_fs_reads_bytes_total",
    "container_fs_writes_bytes_total",
    "container_fs_reads_total",
    "container_fs_writes_total",
    "container_network_receive_bytes_total",
    "container_network_transmit_bytes_total",
    "container_memory_cache",
    "container_memory_working_set_bytes",
];

/// Name of the ClusterRole and ClusterRoleBinding for a namespace.
fn rbac_name(ns: &str) -> String {
    format!("dimster-prometheus-{ns}")
}

/// ClusterRole and ClusterRoleBinding letting the namespace's `prometheus`
/// ServiceAccount (base/monitoring/prometheus.yaml) discover nodes and read
/// their kubelet metrics through the API server proxy.
pub(crate) fn rbac_yaml(ns: &str) -> String {
    let name = rbac_name(ns);
    format!(
        r#"apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {name}
rules:
  - apiGroups: [""]
    resources: ["nodes", "nodes/proxy", "nodes/metrics"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {name}
subjects:
  - kind: ServiceAccount
    name: prometheus
    namespace: {ns}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {name}
"#
    )
}

/// Writes the RBAC manifest into the overlay directory and returns its
/// kustomize `resources:` entry.
pub(crate) fn write_rbac(overlay_dir: &Path, ns: &str) -> Result<String> {
    std::fs::write(overlay_dir.join(RBAC_FILE), rbac_yaml(ns))?;
    Ok(format!("  - {RBAC_FILE}"))
}

/// Prometheus scrape job for every node's cAdvisor, appended to the
/// `scrape_configs` list (the last key of prometheus.yml). Only the
/// namespace's containers and `KEPT_METRICS` are stored. The node name is
/// kept as a `node` label so the report can place pods on nodes.
pub(crate) fn scrape_job(ns: &str) -> String {
    let metrics = KEPT_METRICS.join("|");
    format!(
        r#"
      - job_name: 'cadvisor'
        scheme: https
        tls_config:
          ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
        authorization:
          credentials_file: /var/run/secrets/kubernetes.io/serviceaccount/token
        kubernetes_sd_configs:
          - role: node
        relabel_configs:
          - target_label: __address__
            replacement: kubernetes.default.svc:443
          - source_labels: [__meta_kubernetes_node_name]
            target_label: node
          - source_labels: [__meta_kubernetes_node_name]
            regex: (.+)
            target_label: __metrics_path__
            replacement: /api/v1/nodes/$1/proxy/metrics/cadvisor
        metric_relabel_configs:
          - source_labels: [namespace]
            regex: '{ns}'
            action: keep
          - source_labels: [__name__]
            regex: '{metrics}'
            action: keep
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[test]
    fn rbac_is_named_and_bound_per_namespace() {
        let yaml = rbac_yaml("bench2");
        let docs: Vec<serde_yaml::Value> = serde_yaml::Deserializer::from_str(&yaml)
            .map(|d| serde_yaml::Value::deserialize(d).unwrap())
            .collect();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0]["kind"], "ClusterRole");
        assert_eq!(docs[0]["metadata"]["name"], "dimster-prometheus-bench2");
        assert_eq!(docs[1]["kind"], "ClusterRoleBinding");
        assert_eq!(docs[1]["metadata"]["name"], "dimster-prometheus-bench2");
        assert_eq!(docs[1]["subjects"][0]["name"], "prometheus");
        assert_eq!(docs[1]["subjects"][0]["namespace"], "bench2");
        assert_eq!(docs[1]["roleRef"]["name"], "dimster-prometheus-bench2");
    }

    #[test]
    fn scrape_job_keeps_only_the_namespace_and_report_metrics() {
        // Parsed as a scrape_configs entry, the way prometheus.yml holds it.
        let job: Vec<serde_yaml::Value> =
            serde_yaml::from_str(&textwrap_dedent(&scrape_job("bench2"))).unwrap();
        let job = &job[0];
        assert_eq!(job["job_name"], "cadvisor");
        assert_eq!(job["kubernetes_sd_configs"][0]["role"], "node");
        let keeps = job["metric_relabel_configs"].as_sequence().unwrap();
        assert_eq!(keeps[0]["source_labels"][0], "namespace");
        assert_eq!(keeps[0]["regex"], "bench2");
        let names = keeps[1]["regex"].as_str().unwrap();
        assert!(
            names
                .split('|')
                .any(|m| m == "container_cpu_usage_seconds_total")
        );
        assert!(
            names
                .split('|')
                .any(|m| m == "container_fs_reads_bytes_total")
        );
        assert!(names.split('|').all(|m| m.starts_with("container_")));
    }

    #[test]
    fn write_rbac_returns_its_resource_entry() {
        let dir = tempfile::tempdir().unwrap();
        let entry = write_rbac(dir.path(), "dimster").unwrap();
        assert_eq!(entry, format!("  - {RBAC_FILE}"));
        let written = std::fs::read_to_string(dir.path().join(RBAC_FILE)).unwrap();
        assert!(written.contains("dimster-prometheus-dimster"));
    }

    /// The job is indented to sit under prometheus.yml's `scrape_configs:`
    /// inside the ConfigMap; strip the common indent to parse it alone.
    fn textwrap_dedent(s: &str) -> String {
        let indent = s
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| l.len() - l.trim_start().len())
            .min()
            .unwrap_or(0);
        s.lines()
            .map(|l| {
                if l.len() >= indent {
                    &l[indent..]
                } else {
                    l.trim_start()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}
