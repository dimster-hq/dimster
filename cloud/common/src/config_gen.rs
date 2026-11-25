//! Single source of truth for generating Dimster environment configurations.
//!
//! Previously `eks-setup` maintained three divergent copies of the env YAML
//! (standalone file, `--generate-script`, and `serde_yaml::Value` insertion),
//! which caused schema drift (`storage:` instead of `pvcSize:`, dropped
//! `brokerConfig`/`extraConfig`/`hardware`, unused `coordinator.nodeLabel`).
//! Both `eks-setup` and `gke-setup` now construct one `DimsterEnvSpec` and
//! derive all three outputs from it.

use std::path::Path;

use anyhow::Result;
use serde_yaml::{Mapping, Value};

use crate::exec::{Mode, RunOptions};

/// Optional kubelet static CPU Manager expectation for `dimster deploy`.
#[derive(Debug, Clone)]
pub struct CpuManagerSpec {
    pub policy: String,
    pub full_pcpus_only: bool,
}

/// Kafka broker configuration rendered under `environments.<env>.kafka`.
#[derive(Debug, Clone)]
pub struct KafkaRenderSpec {
    pub replicas: u32,
    pub version: String,
    pub pvc_size: String,
    pub pvc_storage_class: String,
    pub heap: String,
    pub cpu: String,
    pub memory: String,
    pub broker_config: Option<String>,
    pub extra_config: Option<String>,
}

/// Worker pool configuration rendered under `environments.<env>.workers`.
#[derive(Debug, Clone)]
pub struct WorkersRenderSpec {
    pub replicas: u32,
    pub heap: String,
    pub cpu: String,
    pub memory: String,
}

/// Coordinator configuration rendered under `environments.<env>.coordinator`.
#[derive(Debug, Clone)]
pub struct CoordinatorRenderSpec {
    pub heap: String,
    pub cpu: String,
    pub memory: String,
    /// Optional explicit `nodeLabel` (`role=infra` or `role=worker`).
    pub node_label: Option<String>,
}

/// Monitoring configuration rendered under `environments.<env>.monitoring`.
#[derive(Debug, Clone)]
pub struct MonitoringRenderSpec {
    pub cgroup_metrics: bool,
    pub prometheus_cpu: String,
    pub prometheus_memory: String,
    pub grafana_cpu: String,
    pub grafana_memory: String,
}

/// Complete Dimster environment specification matching `cli/src/config.rs`.
#[derive(Debug, Clone)]
pub struct DimsterEnvSpec {
    pub env_name: String,
    pub image: String,
    pub context: String,
    pub hardware: String,
    pub system: Option<String>,
    /// Emitted as snake_case `kafka_client_version` to match `cli/src/config.rs`.
    pub kafka_client_version: Option<String>,
    /// `"full"` or `"external"`.
    pub deploy_mode: String,
    pub bootstrap_servers: Option<String>,
    pub cpu_manager: Option<CpuManagerSpec>,
    pub kafka: Option<KafkaRenderSpec>,
    pub workers: WorkersRenderSpec,
    pub coordinator: CoordinatorRenderSpec,
    pub monitoring: MonitoringRenderSpec,
}

impl DimsterEnvSpec {
    pub fn is_external(&self) -> bool {
        self.deploy_mode == "external"
    }

    /// Convert the environment block to a `serde_yaml::Value` for inserting
    /// into an existing `dimster-config.yaml`.
    pub fn to_yaml_value(&self) -> Value {
        let mut env = Mapping::new();
        env.insert(Value::String("image".into()), Value::String(self.image.clone()));
        env.insert(Value::String("context".into()), Value::String(self.context.clone()));
        env.insert(Value::String("hardware".into()), Value::String(self.hardware.clone()));
        if let Some(ref sys) = self.system {
            env.insert(Value::String("system".into()), Value::String(sys.clone()));
        }
        if let Some(ref kcv) = self.kafka_client_version {
            env.insert(
                Value::String("kafka_client_version".into()),
                Value::String(kcv.clone()),
            );
        }

        if self.is_external() {
            env.insert(Value::String("deploy".into()), Value::String("external".into()));
            let mut connection = Mapping::new();
            let bootstrap = self
                .bootstrap_servers
                .as_deref()
                .unwrap_or("CHANGE_ME:9092");
            connection.insert(
                Value::String("bootstrap.servers".into()),
                Value::String(bootstrap.into()),
            );
            env.insert(Value::String("connection".into()), Value::Mapping(connection));
        } else {
            env.insert(Value::String("deploy".into()), Value::String("full".into()));
        }

        if let Some(ref cm) = self.cpu_manager {
            let mut cm_map = Mapping::new();
            cm_map.insert(Value::String("policy".into()), Value::String(cm.policy.clone()));
            if cm.full_pcpus_only {
                cm_map.insert(Value::String("fullPcpusOnly".into()), Value::Bool(true));
            }
            env.insert(Value::String("cpuManager".into()), Value::Mapping(cm_map));
        }

        if !self.is_external()
            && let Some(ref k) = self.kafka
        {
            let mut kafka = Mapping::new();
            kafka.insert(Value::String("replicas".into()), Value::Number(k.replicas.into()));
            kafka.insert(Value::String("version".into()), Value::String(k.version.clone()));
            kafka.insert(Value::String("pvcSize".into()), Value::String(k.pvc_size.clone()));
            kafka.insert(
                Value::String("pvcStorageClass".into()),
                Value::String(k.pvc_storage_class.clone()),
            );
            kafka.insert(Value::String("heap".into()), Value::String(k.heap.clone()));
            let mut reqs = Mapping::new();
            reqs.insert(Value::String("cpu".into()), Value::String(k.cpu.clone()));
            reqs.insert(Value::String("memory".into()), Value::String(k.memory.clone()));
            kafka.insert(Value::String("requests".into()), Value::Mapping(reqs));
            kafka.insert(Value::String("nodeLabel".into()), Value::String("role=kafka".into()));
            if let Some(ref bc) = k.broker_config {
                kafka.insert(Value::String("brokerConfig".into()), Value::String(bc.clone()));
            }
            if let Some(ref ec) = k.extra_config {
                kafka.insert(Value::String("extraConfig".into()), Value::String(ec.clone()));
            }
            env.insert(Value::String("kafka".into()), Value::Mapping(kafka));
        }

        let mut workers = Mapping::new();
        workers.insert(
            Value::String("replicas".into()),
            Value::Number(self.workers.replicas.into()),
        );
        workers.insert(
            Value::String("heap".into()),
            Value::String(self.workers.heap.clone()),
        );
        let mut worker_reqs = Mapping::new();
        worker_reqs.insert(
            Value::String("cpu".into()),
            Value::String(self.workers.cpu.clone()),
        );
        worker_reqs.insert(
            Value::String("memory".into()),
            Value::String(self.workers.memory.clone()),
        );
        workers.insert(Value::String("requests".into()), Value::Mapping(worker_reqs));
        workers.insert(
            Value::String("nodeLabel".into()),
            Value::String("role=worker".into()),
        );
        env.insert(Value::String("workers".into()), Value::Mapping(workers));

        let mut coordinator = Mapping::new();
        coordinator.insert(
            Value::String("heap".into()),
            Value::String(self.coordinator.heap.clone()),
        );
        let mut coord_reqs = Mapping::new();
        coord_reqs.insert(
            Value::String("cpu".into()),
            Value::String(self.coordinator.cpu.clone()),
        );
        coord_reqs.insert(
            Value::String("memory".into()),
            Value::String(self.coordinator.memory.clone()),
        );
        coordinator.insert(Value::String("requests".into()), Value::Mapping(coord_reqs));
        if let Some(ref label) = self.coordinator.node_label {
            coordinator.insert(Value::String("nodeLabel".into()), Value::String(label.clone()));
        }
        env.insert(Value::String("coordinator".into()), Value::Mapping(coordinator));

        let mut monitoring = Mapping::new();
        monitoring.insert(
            Value::String("nodeLabel".into()),
            Value::String("role=infra".into()),
        );
        if self.monitoring.cgroup_metrics {
            monitoring.insert(Value::String("cgroupMetrics".into()), Value::Bool(true));
        }

        let mut prometheus = Mapping::new();
        let mut prom_reqs = Mapping::new();
        prom_reqs.insert(
            Value::String("cpu".into()),
            Value::String(self.monitoring.prometheus_cpu.clone()),
        );
        prom_reqs.insert(
            Value::String("memory".into()),
            Value::String(self.monitoring.prometheus_memory.clone()),
        );
        prometheus.insert(Value::String("requests".into()), Value::Mapping(prom_reqs));
        monitoring.insert(Value::String("prometheus".into()), Value::Mapping(prometheus));

        let mut grafana = Mapping::new();
        let mut grafana_reqs = Mapping::new();
        grafana_reqs.insert(
            Value::String("cpu".into()),
            Value::String(self.monitoring.grafana_cpu.clone()),
        );
        grafana_reqs.insert(
            Value::String("memory".into()),
            Value::String(self.monitoring.grafana_memory.clone()),
        );
        grafana.insert(Value::String("requests".into()), Value::Mapping(grafana_reqs));
        monitoring.insert(Value::String("grafana".into()), Value::Mapping(grafana));

        env.insert(Value::String("monitoring".into()), Value::Mapping(monitoring));

        Value::Mapping(env)
    }

    /// Render a complete standalone `dimster-config.yaml` file from `to_yaml_value`.
    ///
    /// Deriving the standalone YAML from `to_yaml_value` guarantees that standalone
    /// configs, `--generate-script` configs, and in-place `dimster-config.yaml`
    /// insertions never drift apart.
    pub fn render_standalone_config(&self, generator: &str, resource_name: &str) -> Result<String> {
        let mode_suffix = if self.is_external() { " (external)" } else { "" };
        let env_yaml = serde_yaml::to_string(&self.to_yaml_value())?;
        let mut indented_env = String::new();
        for line in env_yaml.lines() {
            indented_env.push_str("    ");
            indented_env.push_str(line);
            indented_env.push('\n');
        }

        Ok(format!(
            "# Dimster configuration — generated by {generator} for '{resource_name}'{mode_suffix}\n\
             kubernetes:\n\
             \x20\x20namespace: dimster\n\
             \n\
             # No `image:` block: the dimster CLI defaults the tag to its own version,\n\
             # so generated configs stay aligned with whichever CLI binary you run.\n\
             # Every `dimster build` also produces a :latest-kafka<kver> alias, so\n\
             # `tag: latest` is a valid alternative if you prefer not to pin. Override\n\
             # only if you need a custom image tag:\n\
             #   image:\n\
             #     tag: 0.1.0       # pin to a specific version\n\
             #     # tag: latest    # always resolve to the most recently built image\n\
             \n\
             environments:\n\
             \x20\x20local:\n\
             \x20\x20\x20\x20image: dimster\n\
             \x20\x20{env_name}:\n\
             {indented_env}\n\
             results:\n\
             \x20\x20dir: ./results\n",
            generator = generator,
            resource_name = resource_name,
            mode_suffix = mode_suffix,
            env_name = self.env_name,
            indented_env = indented_env,
        ))
    }
}

/// Insert or replace an environment entry in an existing `dimster-config.yaml`.
pub fn insert_env_into_config(
    config_path: &Path,
    env_name: &str,
    env_value: Value,
) -> Result<()> {
    let content = std::fs::read_to_string(config_path)?;
    let mut doc: Value = serde_yaml::from_str(&content)?;

    let environments = doc
        .as_mapping_mut()
        .ok_or_else(|| anyhow::anyhow!("{} is not a YAML mapping", config_path.display()))?
        .entry(Value::String("environments".into()))
        .or_insert_with(|| Value::Mapping(Mapping::new()));

    let envs_map = environments
        .as_mapping_mut()
        .ok_or_else(|| anyhow::anyhow!("'environments' is not a YAML mapping"))?;

    envs_map.insert(Value::String(env_name.into()), env_value);

    let output = serde_yaml::to_string(&doc)?;
    std::fs::write(config_path, output)?;
    Ok(())
}

/// Prompt the user to either insert/replace the environment in `dimster-config.yaml`
/// or write a standalone `dimster-config-{name}.yaml`.
///
/// Returns `true` when inserted into `dimster-config.yaml`, `false` otherwise.
pub fn save_or_insert_config(
    spec: &DimsterEnvSpec,
    generator: &str,
    resource_name: &str,
    answers_path: &str,
    opts: RunOptions,
) -> Result<bool> {
    let config = spec.render_standalone_config(generator, resource_name)?;
    let separate_config_path = format!("dimster-config-{}.yaml", resource_name);

    if opts.mode == Mode::DryRun {
        println!("  [dry-run] Would write {}:", separate_config_path);
        println!();
        for line in config.lines() {
            println!("    {}", line);
        }
        println!("\n  [dry-run] Would write {}", answers_path);
        return Ok(false);
    }

    let save_options = [
        "Insert/replace environment in dimster-config.yaml",
        &format!("Write to {} (manual copy needed)", separate_config_path),
    ];
    let save_selection = if opts.auto_accept {
        0
    } else {
        dialoguer::Select::new()
            .with_prompt("How should the configuration be saved?")
            .items(&save_options)
            .default(0)
            .interact()?
    };

    if save_selection == 0 {
        let dimster_config_path = Path::new("dimster-config.yaml");
        if dimster_config_path.exists() {
            insert_env_into_config(dimster_config_path, &spec.env_name, spec.to_yaml_value())?;
            println!(
                "  ✓ Updated environment '{}' in {}",
                spec.env_name,
                dimster_config_path.display()
            );
        } else {
            std::fs::write(dimster_config_path, &config)?;
            println!("  ✓ Wrote {}", dimster_config_path.display());
        }
        Ok(true)
    } else {
        std::fs::write(&separate_config_path, &config)?;
        println!("  ✓ Wrote {}", separate_config_path);
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_full_spec() -> DimsterEnvSpec {
        DimsterEnvSpec {
            env_name: "gke".into(),
            image: "us-central1-docker.pkg.dev/my-proj/my-dimster/dimster".into(),
            context: "gke_my-proj_us-central1-a_my-dimster-benchmark".into(),
            hardware: "kafka=3xc3-standard-8, workers=2xc3-standard-4, storage=hyperdisk-balanced/500Gi/16000iops/800MiBs".into(),
            system: Some("Apache Kafka 4.2.0".into()),
            kafka_client_version: Some("4.2.0".into()),
            deploy_mode: "full".into(),
            bootstrap_servers: None,
            cpu_manager: Some(CpuManagerSpec {
                policy: "static".into(),
                full_pcpus_only: false,
            }),
            kafka: Some(KafkaRenderSpec {
                replicas: 3,
                version: "4.2.0".into(),
                pvc_size: "500Gi".into(),
                pvc_storage_class: "dimster-hyperdisk-balanced-fast".into(),
                heap: "-Xms8G -Xmx8G".into(),
                cpu: "6".into(),
                memory: "26Gi".into(),
                broker_config: Some("run/broker-configs/default.yaml".into()),
                extra_config: Some("num.network.threads=8,num.io.threads=16".into()),
            }),
            workers: WorkersRenderSpec {
                replicas: 2,
                heap: "-Xms2G -Xmx8G".into(),
                cpu: "3".into(),
                memory: "11Gi".into(),
            },
            coordinator: CoordinatorRenderSpec {
                heap: "-Xms512M -Xmx512M".into(),
                cpu: "500m".into(),
                memory: "1Gi".into(),
                node_label: Some("role=infra".into()),
            },
            monitoring: MonitoringRenderSpec {
                cgroup_metrics: true,
                prometheus_cpu: "750m".into(),
                prometheus_memory: "2Gi".into(),
                grafana_cpu: "750m".into(),
                grafana_memory: "1Gi".into(),
            },
        }
    }

    #[test]
    fn standalone_config_matches_dimster_schema() {
        let spec = sample_full_spec();
        let yaml = spec
            .render_standalone_config("gke-setup", "my-dimster")
            .unwrap();
        let parsed: Value = serde_yaml::from_str(&yaml).unwrap();
        let env = &parsed["environments"]["gke"];

        assert_eq!(env["deploy"], "full");
        assert_eq!(env["kafka_client_version"], "4.2.0");
        assert_eq!(env["system"], "Apache Kafka 4.2.0");
        assert_eq!(env["cpuManager"]["policy"], "static");
        // Verify pvcSize (never the stale `storage` key)
        assert_eq!(env["kafka"]["pvcSize"], "500Gi");
        assert!(env["kafka"].get("storage").is_none());
        assert_eq!(
            env["kafka"]["pvcStorageClass"],
            "dimster-hyperdisk-balanced-fast"
        );
        assert_eq!(
            env["kafka"]["brokerConfig"],
            "run/broker-configs/default.yaml"
        );
        assert_eq!(
            env["kafka"]["extraConfig"],
            "num.network.threads=8,num.io.threads=16"
        );
        assert_eq!(env["kafka"]["nodeLabel"], "role=kafka");
        assert_eq!(env["workers"]["nodeLabel"], "role=worker");
        assert_eq!(env["coordinator"]["nodeLabel"], "role=infra");
        assert_eq!(env["monitoring"]["nodeLabel"], "role=infra");
        assert_eq!(env["monitoring"]["cgroupMetrics"], true);

        let mut worker_coord = sample_full_spec();
        worker_coord.coordinator.node_label = Some("role=worker".into());
        assert_eq!(
            worker_coord.to_yaml_value()["coordinator"]["nodeLabel"],
            "role=worker"
        );
    }

    #[test]
    fn external_mode_omits_kafka_and_includes_connection() {
        let mut spec = sample_full_spec();
        spec.deploy_mode = "external".into();
        spec.bootstrap_servers = Some("pkc-abc.us-central1.gcp.confluent.cloud:9092".into());
        spec.kafka = None;

        let val = spec.to_yaml_value();
        assert_eq!(val["deploy"], "external");
        assert!(val.get("kafka").is_none());
        assert_eq!(
            val["connection"]["bootstrap.servers"],
            "pkc-abc.us-central1.gcp.confluent.cloud:9092"
        );
    }

    #[test]
    fn insert_env_preserves_existing_environments() {
        let tmp = std::env::temp_dir().join(format!(
            "dimster-cloud-common-test-{}.yaml",
            std::process::id()
        ));
        std::fs::write(
            &tmp,
            "kubernetes:\n  namespace: dimster\nenvironments:\n  local:\n    image: dimster\n",
        )
        .unwrap();

        let spec = sample_full_spec();
        insert_env_into_config(&tmp, "gke", spec.to_yaml_value()).unwrap();

        let content = std::fs::read_to_string(&tmp).unwrap();
        let _ = std::fs::remove_file(&tmp);
        let parsed: Value = serde_yaml::from_str(&content).unwrap();
        assert_eq!(parsed["environments"]["local"]["image"], "dimster");
        assert_eq!(parsed["environments"]["gke"]["kafka"]["pvcSize"], "500Gi");
    }
}
