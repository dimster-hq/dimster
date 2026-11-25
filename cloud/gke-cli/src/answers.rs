//! GKE Setup answers schema — serialized to/from `gke-answers-{name}.yaml` for replay.
//!
//! Captures every GCP cluster, node-pool, storage, and Dimster sizing decision
//! so clusters can be recreated or deleted deterministically.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Answers {
    #[serde(default = "default_env_name")]
    pub env_name: String,
    pub gcp: GcpAnswers,
    #[serde(default = "default_deploy_mode")]
    pub deploy_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_servers: Option<String>,
    /// Optional explicit kafka-clients version override for `dimster build`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_client_version: Option<String>,
    /// Configure kubelet `cpuManagerPolicy: static` on Kafka & worker node pools
    /// and emit `cpuManager: { policy: static }` in `dimster-config.yaml`.
    #[serde(default)]
    pub cpu_manager_static: bool,
    pub kafka: KafkaAnswers,
    pub storage: StorageAnswers,
    pub workers: WorkerAnswers,
    pub coordinator: CoordinatorAnswers,
    #[serde(default = "default_monitoring")]
    pub monitoring: MonitoringAnswers,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcpAnswers {
    pub project: String,
    pub dimster_name: String,
    pub region: String,
    pub zone: String,
    /// GKE release channel: "regular", "stable", "rapid", or "none".
    #[serde(default = "default_release_channel")]
    pub release_channel: String,
    /// Explicit GKE version (e.g. "1.32"), optional; defaults to channel version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gke_version: Option<String>,
    /// Optional pre-existing image repository URL (skips Artifact Registry create/delete).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_image: Option<String>,
    /// Enable GKE Dataplane V2 (eBPF) for lower-overhead networking.
    #[serde(default = "default_true")]
    pub dataplane_v2: bool,
    /// Provision GKE nodes without external IPs (`--enable-ip-alias --enable-private-nodes`)
    /// and configure a regional Cloud Router + Cloud NAT for outbound internet access.
    #[serde(default = "default_true")]
    pub private_nodes: bool,
    /// Optional `/28` CIDR for legacy VPC-peered private control planes (`--master-ipv4-cidr`).
    /// Omitted by default so GKE uses Private Service Connect without CIDR collisions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub master_ipv4_cidr: Option<String>,
    /// Optional custom VPC network (useful for VPC peering to external Kafka).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,
    /// Optional custom VPC subnetwork.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subnetwork: Option<String>,
    /// Optional custom service account email for GKE node VMs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_service_account: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaAnswers {
    pub version: String,
    pub instance_type: String,
    pub count: u32,
    pub heap: String,
    pub cpu: String,
    pub memory: String,
    #[serde(default)]
    pub tier1_networking: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_config: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra_config: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageAnswers {
    /// "hyperdisk-balanced" | "hyperdisk-extreme" | "hyperdisk-throughput"
    /// | "pd-ssd" | "pd-balanced" | "local-ssd"
    #[serde(rename = "type")]
    pub storage_type: String,
    pub size: String,
    /// "basic" or "provisioned" (used by hyperdisk-balanced)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub performance: Option<String>,
    /// Provisioned IOPS (hyperdisk-balanced provisioned, hyperdisk-extreme)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iops: Option<u32>,
    /// Provisioned throughput in MiB/s (hyperdisk-balanced provisioned, hyperdisk-throughput)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throughput_mibs: Option<u32>,
    /// Number of 375 GiB Local NVMe SSDs (for local-ssd)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_ssd_count: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerAnswers {
    pub instance_type: String,
    pub count: u32,
    pub min: u32,
    pub max: u32,
    pub heap: String,
    pub cpu: String,
    pub memory: String,
    #[serde(default)]
    pub tier1_networking: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinatorAnswers {
    #[serde(default = "default_infra_instance_type")]
    pub infra_instance_type: String,
    #[serde(default = "default_coord_node_label")]
    pub node_label: String,
    pub heap: String,
    pub cpu: String,
    pub memory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MonitoringAnswers {
    pub prometheus_cpu: String,
    pub prometheus_memory: String,
    pub grafana_cpu: String,
    pub grafana_memory: String,
}

fn default_monitoring() -> MonitoringAnswers {
    MonitoringAnswers {
        prometheus_cpu: "500m".to_string(),
        prometheus_memory: "1Gi".to_string(),
        grafana_cpu: "500m".to_string(),
        grafana_memory: "512Mi".to_string(),
    }
}

fn default_infra_instance_type() -> String {
    "e2-standard-2".to_string()
}

fn default_coord_node_label() -> String {
    "role=infra".to_string()
}

fn default_env_name() -> String {
    "gke".to_string()
}

fn default_deploy_mode() -> String {
    "full".to_string()
}

fn default_release_channel() -> String {
    "regular".to_string()
}

fn default_true() -> bool {
    true
}

/// Validate a resource prefix so it satisfies GCP cluster name, Artifact
/// Registry repository ID, and GCE disk label rules (`[a-z][a-z0-9-]{0,29}`).
pub fn validate_gcp_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 30 {
        bail!(
            "Resource name '{}' must be 1..=30 characters (leaves room for '-benchmark' suffix)",
            name
        );
    }
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_lowercase() {
        bail!(
            "Resource name '{}' must start with a lowercase letter (GCP naming rule)",
            name
        );
    }
    if bytes[bytes.len() - 1] == b'-' {
        bail!("Resource name '{}' cannot end with '-'", name);
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        bail!(
            "Resource name '{}' may only contain lowercase letters, digits, and '-'",
            name
        );
    }
    Ok(())
}

impl Answers {
    pub fn is_external(&self) -> bool {
        self.deploy_mode == "external"
    }

    pub fn cluster_name(&self) -> String {
        format!("{}-benchmark", self.gcp.dimster_name)
    }

    /// Standard kubeconfig context name written by `gcloud container clusters get-credentials`.
    pub fn kubectl_context(&self) -> String {
        gke_kubectl_context(&self.gcp.project, &self.gcp.zone, &self.cluster_name())
    }

    /// Container image repository URL (excluding `:tag`).
    pub fn image_repo(&self) -> String {
        if let Some(ref existing) = self.gcp.existing_image {
            existing.clone()
        } else {
            format!(
                "{}-docker.pkg.dev/{}/{}/dimster",
                self.gcp.region, self.gcp.project, self.gcp.dimster_name
            )
        }
    }

    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let answers: Answers = serde_yaml::from_str(&content)?;
        validate_gcp_name(&answers.gcp.dimster_name)?;
        Ok(answers)
    }

    pub fn save(&self, path: &str) -> Result<()> {
        let content = serde_yaml::to_string(self)?;
        let header = "# GKE Setup answers — generated by gke-setup\n# Re-run with: gke-setup create --answers this-file.yaml\n";
        std::fs::write(path, format!("{}{}", header, content))?;
        Ok(())
    }
}

/// Deterministic kubeconfig context name produced by `gcloud container clusters get-credentials`.
pub fn gke_kubectl_context(project: &str, zone: &str, cluster_name: &str) -> String {
    format!("gke_{}_{}_{}", project, zone, cluster_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_gcp_name_enforces_gcp_constraints() {
        assert!(validate_gcp_name("my-dimster").is_ok());
        assert!(validate_gcp_name("bench01").is_ok());
        assert!(validate_gcp_name("").is_err());
        assert!(validate_gcp_name("MyDimster").is_err());
        assert!(validate_gcp_name("1bench").is_err());
        assert!(validate_gcp_name("bench-").is_err());
        assert!(validate_gcp_name("my_dimster").is_err());
        assert!(validate_gcp_name(&"a".repeat(31)).is_err());
    }

    #[test]
    fn image_repo_uses_artifact_registry_or_existing_override() {
        let mut gcp = GcpAnswers {
            project: "demo-proj".into(),
            dimster_name: "bench".into(),
            region: "europe-west1".into(),
            zone: "europe-west1-b".into(),
            release_channel: "regular".into(),
            gke_version: None,
            existing_image: None,
            dataplane_v2: true,
            private_nodes: true,
            master_ipv4_cidr: None,
            network: None,
            subnetwork: None,
            node_service_account: None,
        };
        let mut answers = Answers {
            env_name: "gke".into(),
            gcp: gcp.clone(),
            deploy_mode: "full".into(),
            bootstrap_servers: None,
            kafka_client_version: None,
            cpu_manager_static: false,
            kafka: KafkaAnswers {
                version: "4.2.0".into(),
                instance_type: "c3-standard-8".into(),
                count: 3,
                heap: "-Xms8G -Xmx8G".into(),
                cpu: "6".into(),
                memory: "26Gi".into(),
                tier1_networking: false,
                broker_config: None,
                extra_config: None,
            },
            storage: StorageAnswers {
                storage_type: "hyperdisk-balanced".into(),
                size: "500Gi".into(),
                performance: Some("basic".into()),
                iops: None,
                throughput_mibs: None,
                local_ssd_count: None,
            },
            workers: WorkerAnswers {
                instance_type: "c3-standard-4".into(),
                count: 2,
                min: 1,
                max: 4,
                heap: "-Xms2G -Xmx8G".into(),
                cpu: "3".into(),
                memory: "11Gi".into(),
                tier1_networking: false,
            },
            coordinator: CoordinatorAnswers {
                infra_instance_type: "e2-standard-2".into(),
                node_label: "role=infra".into(),
                heap: "-Xms512M -Xmx512M".into(),
                cpu: "500m".into(),
                memory: "1Gi".into(),
            },
            monitoring: default_monitoring(),
        };

        assert_eq!(
            answers.image_repo(),
            "europe-west1-docker.pkg.dev/demo-proj/bench/dimster"
        );
        assert_eq!(
            answers.kubectl_context(),
            "gke_demo-proj_europe-west1-b_bench-benchmark"
        );

        gcp.existing_image = Some("ghcr.io/org/dimster".into());
        answers.gcp = gcp;
        assert_eq!(answers.image_repo(), "ghcr.io/org/dimster");
    }
}
