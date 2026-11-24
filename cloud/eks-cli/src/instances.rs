/// Instance type specifications for AWS EC2 instances.
/// Loaded from instance-types.yaml at runtime.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arch {
    X86_64,
    Arm64,
}

#[derive(Debug, Clone)]
pub struct InstanceSpec {
    pub vcpu: u32,
    pub memory_gib: u32,
    pub nvme_count: u32,
    pub nvme_size_gb: u32,
    pub arch: Arch,
    pub ebs_bandwidth_mbs: u32,
    pub network_baseline_gbps: f64,
}

impl InstanceSpec {
    pub fn has_instance_store(&self) -> bool {
        self.nvme_count > 0
    }

    pub fn total_nvme_gb(&self) -> u32 {
        self.nvme_count * self.nvme_size_gb
    }
}

impl std::fmt::Display for InstanceSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let arch = match self.arch {
            Arch::X86_64 => "x86_64",
            Arch::Arm64 => "arm64",
        };
        let net_mbs = (self.network_baseline_gbps * 1000.0 / 8.0) as u32;
        if self.has_instance_store() {
            write!(
                f,
                "{} vCPU, {} GiB, {}x {} GB NVMe, EBS {} MB/s, net {} MB/s ({:.1} Gbps), {}",
                self.vcpu, self.memory_gib, self.nvme_count, self.nvme_size_gb, self.ebs_bandwidth_mbs, net_mbs, self.network_baseline_gbps, arch
            )
        } else {
            write!(
                f,
                "{} vCPU, {} GiB, EBS {} MB/s, net {} MB/s ({:.1} Gbps), {}",
                self.vcpu, self.memory_gib, self.ebs_bandwidth_mbs, net_mbs, self.network_baseline_gbps, arch
            )
        }
    }
}

/// YAML deserialization format for an instance type entry.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct YamlSpec {
    vcpu: u32,
    memory_gib: u32,
    arch: String,
    #[serde(default)]
    nvme_count: u32,
    #[serde(default)]
    nvme_size_gb: u32,
    #[serde(default)]
    ebs_bandwidth_m_bs: u32,
    #[serde(default)]
    network_baseline_gbps: f64,
}

/// Instance type registry loaded from YAML.
pub struct InstanceTypes {
    specs: HashMap<String, InstanceSpec>,
}

impl InstanceTypes {
    /// Load instance types from a YAML file.
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let raw: HashMap<String, YamlSpec> = serde_yaml::from_str(&content)?;

        let specs = raw
            .into_iter()
            .map(|(name, y)| {
                let arch = match y.arch.as_str() {
                    "arm64" => Arch::Arm64,
                    _ => Arch::X86_64,
                };
                let spec = InstanceSpec {
                    vcpu: y.vcpu,
                    memory_gib: y.memory_gib,
                    nvme_count: y.nvme_count,
                    nvme_size_gb: y.nvme_size_gb,
                    arch,
                    ebs_bandwidth_mbs: y.ebs_bandwidth_m_bs,
                    network_baseline_gbps: y.network_baseline_gbps,
                };
                (name, spec)
            })
            .collect();

        Ok(InstanceTypes { specs })
    }

    /// Look up an instance type by name. Returns None for unknown types.
    pub fn lookup(&self, instance_type: &str) -> Option<&InstanceSpec> {
        self.specs.get(instance_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_instance_types_yaml_loads_cleanly() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("instance-types.yaml");
        let types = InstanceTypes::load(&path).expect("failed to load eks instance-types.yaml");
        let m6i = types.lookup("m6i.2xlarge").expect("missing m6i.2xlarge");
        assert_eq!(m6i.vcpu, 8);
        assert_eq!(m6i.memory_gib, 32);
        let i4i = types.lookup("i4i.2xlarge").expect("missing i4i.2xlarge");
        assert!(i4i.has_instance_store());
    }
}
