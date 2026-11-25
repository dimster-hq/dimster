//! GCP Compute Engine machine type specifications loaded from `instance-types.yaml`.
//!
//! Drives interactive resource suggestions, GKE node-pool boot-disk selection,
//! Kafka disk-type compatibility checks, and VM I/O ceiling warnings.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Result, bail};
use serde::Deserialize;

/// Size of a single GCP Local NVMe SSD partition in GiB.
pub const LOCAL_SSD_SIZE_GIB: u32 = 375;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arch {
    X86_64,
    Arm64,
}

impl Arch {
    pub fn as_str(self) -> &'static str {
        match self {
            Arch::X86_64 => "x86_64",
            Arch::Arm64 => "arm64",
        }
    }
}

/// How Local NVMe SSDs behave on a GCP machine type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalSsdMode {
    /// Machine type cannot attach Local NVMe SSDs (e.g. e2, n4, non-lssd c3/c4).
    None,
    /// Fixed number of bundled 375 GiB Local NVMe SSDs (`-lssd` machine shapes).
    Fixed,
    /// User chooses count (1..=max) via GKE `--local-nvme-ssd-block=count=N` (n2/n2d).
    Attachable,
}

/// Kafka data disk types supported by `gke-setup`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskType {
    HyperdiskBalanced,
    HyperdiskExtreme,
    HyperdiskThroughput,
    PdSsd,
    PdBalanced,
    LocalSsd,
}

impl DiskType {
    pub fn as_str(self) -> &'static str {
        match self {
            DiskType::HyperdiskBalanced => "hyperdisk-balanced",
            DiskType::HyperdiskExtreme => "hyperdisk-extreme",
            DiskType::HyperdiskThroughput => "hyperdisk-throughput",
            DiskType::PdSsd => "pd-ssd",
            DiskType::PdBalanced => "pd-balanced",
            DiskType::LocalSsd => "local-ssd",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "hyperdisk-balanced" => Ok(DiskType::HyperdiskBalanced),
            "hyperdisk-extreme" => Ok(DiskType::HyperdiskExtreme),
            "hyperdisk-throughput" => Ok(DiskType::HyperdiskThroughput),
            "pd-ssd" => Ok(DiskType::PdSsd),
            "pd-balanced" => Ok(DiskType::PdBalanced),
            "local-ssd" => Ok(DiskType::LocalSsd),
            other => bail!(
                "Unknown GCP disk type '{}' (expected hyperdisk-balanced, hyperdisk-extreme, \
                 hyperdisk-throughput, pd-ssd, pd-balanced, or local-ssd)",
                other
            ),
        }
    }
}

/// Parsed specification for a single GCP machine type.
#[derive(Debug, Clone)]
pub struct InstanceSpec {
    pub vcpu: u32,
    pub memory_gib: u32,
    pub arch: Arch,
    /// Baseline `us-central1` on-demand USD/hr (includes bundled Local SSDs on `-lssd`).
    pub hourly_usd: Option<f64>,
    pub network_gbps: f64,
    pub tier1_network_gbps: Option<f64>,
    pub boot_disk_type: String,
    pub supported_disk_types: Vec<DiskType>,
    pub disk_bandwidth_mibs: u32,
    pub disk_max_iops: u32,
    pub local_ssd_mode: LocalSsdMode,
    pub local_ssd_count: u32,
    pub max_local_ssd_count: u32,
}

impl InstanceSpec {
    pub fn supports_local_ssd(&self) -> bool {
        !matches!(self.local_ssd_mode, LocalSsdMode::None)
    }

    pub fn is_hyperdisk_only(&self) -> bool {
        self.boot_disk_type == "hyperdisk-balanced"
            && !self.supported_disk_types.contains(&DiskType::PdBalanced)
            && !self.supported_disk_types.contains(&DiskType::PdSsd)
    }

    pub fn supports_disk_type(&self, disk_type: DiskType) -> bool {
        match disk_type {
            DiskType::LocalSsd => self.supports_local_ssd(),
            other => self.supported_disk_types.contains(&other),
        }
    }

    pub fn fixed_local_ssd_gib(&self) -> u32 {
        self.local_ssd_count * LOCAL_SSD_SIZE_GIB
    }
}

impl std::fmt::Display for InstanceSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let net_mbs = (self.network_gbps * 1000.0 / 8.0) as u32;
        let tier1 = match self.tier1_network_gbps {
            Some(g) => format!(" (Tier_1: {:.0} Gbps)", g),
            None => String::new(),
        };
        let ssd = match self.local_ssd_mode {
            LocalSsdMode::Fixed => format!(
                ", {}x {} GiB Local NVMe",
                self.local_ssd_count, LOCAL_SSD_SIZE_GIB
            ),
            LocalSsdMode::Attachable => format!(
                ", up to {}x {} GiB attachable Local NVMe",
                self.max_local_ssd_count, LOCAL_SSD_SIZE_GIB
            ),
            LocalSsdMode::None => String::new(),
        };
        write!(
            f,
            "{} vCPU, {} GiB{}, disk up to {} MiB/s / {} IOPS, net {} MB/s ({:.0} Gbps{}), {}",
            self.vcpu,
            self.memory_gib,
            ssd,
            self.disk_bandwidth_mibs,
            self.disk_max_iops,
            net_mbs,
            self.network_gbps,
            tier1,
            self.arch.as_str(),
        )
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct YamlSpec {
    vcpu: u32,
    memory_gib: u32,
    arch: String,
    #[serde(default)]
    hourly_usd: Option<f64>,
    #[serde(default)]
    network_gbps: f64,
    #[serde(default)]
    tier1_network_gbps: Option<f64>,
    #[serde(default = "default_boot_disk")]
    boot_disk_type: String,
    #[serde(default)]
    supported_disk_types: Vec<String>,
    #[serde(default)]
    disk_bandwidth_mi_bs: u32,
    #[serde(default)]
    disk_max_iops: u32,
    #[serde(default = "default_local_ssd_mode")]
    local_ssd_mode: String,
    #[serde(default)]
    local_ssd_count: u32,
    #[serde(default)]
    max_local_ssd_count: u32,
}

fn default_boot_disk() -> String {
    "pd-balanced".to_string()
}

fn default_local_ssd_mode() -> String {
    "none".to_string()
}

/// Registry of GCP machine types loaded from `instance-types.yaml`.
pub struct InstanceTypes {
    specs: HashMap<String, InstanceSpec>,
}

impl InstanceTypes {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::parse(&content)
    }

    pub fn parse(content: &str) -> Result<Self> {
        let raw: HashMap<String, YamlSpec> = serde_yaml::from_str(content)?;
        let mut specs = HashMap::with_capacity(raw.len());

        for (name, y) in raw {
            let arch = match y.arch.as_str() {
                "arm64" => Arch::Arm64,
                "x86_64" => Arch::X86_64,
                other => bail!("Invalid arch '{}' for machine type '{}'", other, name),
            };
            let local_ssd_mode = match y.local_ssd_mode.as_str() {
                "none" => LocalSsdMode::None,
                "fixed" => LocalSsdMode::Fixed,
                "attachable" => LocalSsdMode::Attachable,
                other => bail!(
                    "Invalid localSsdMode '{}' for machine type '{}'",
                    other,
                    name
                ),
            };
            let mut supported = Vec::with_capacity(y.supported_disk_types.len());
            for dt in &y.supported_disk_types {
                supported.push(DiskType::parse(dt)?);
            }
            specs.insert(
                name,
                InstanceSpec {
                    vcpu: y.vcpu,
                    memory_gib: y.memory_gib,
                    arch,
                    hourly_usd: y.hourly_usd,
                    network_gbps: y.network_gbps,
                    tier1_network_gbps: y.tier1_network_gbps,
                    boot_disk_type: y.boot_disk_type,
                    supported_disk_types: supported,
                    disk_bandwidth_mibs: y.disk_bandwidth_mi_bs,
                    disk_max_iops: y.disk_max_iops,
                    local_ssd_mode,
                    local_ssd_count: y.local_ssd_count,
                    max_local_ssd_count: y.max_local_ssd_count,
                },
            );
        }

        Ok(Self { specs })
    }

    pub fn lookup(&self, machine_type: &str) -> Option<&InstanceSpec> {
        self.specs.get(machine_type)
    }

    /// Infer the required GKE node boot disk type for a machine type, falling
    /// back to prefix heuristics when the machine type is not in the catalog.
    pub fn boot_disk_for(&self, machine_type: &str) -> &str {
        if let Some(spec) = self.specs.get(machine_type) {
            return &spec.boot_disk_type;
        }
        // Hyperdisk-only 4th-gen families require hyperdisk-balanced boot disks.
        if machine_type.starts_with("n4-")
            || machine_type.starts_with("c4-")
            || machine_type.starts_with("c4a-")
            || machine_type.starts_with("c4d-")
        {
            "hyperdisk-balanced"
        } else {
            "pd-balanced"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_instance_types_yaml_is_valid_and_consistent() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("instance-types.yaml");
        let types = InstanceTypes::load(&path).expect("failed to load instance-types.yaml");
        assert!(types.specs.len() >= 40, "expected curated GCP catalog");

        for (name, spec) in &types.specs {
            assert!(spec.vcpu > 0, "{name}: vcpu must be positive");
            assert!(spec.memory_gib > 0, "{name}: memoryGib must be positive");
            assert!(
                spec.hourly_usd.is_some_and(|p| p > 0.0),
                "{name}: hourlyUsd must be present and positive"
            );
            if let Some(base_name) = name.strip_suffix("-lssd")
                && let Some(base) = types.lookup(base_name)
            {
                assert!(
                    spec.hourly_usd.unwrap() > base.hourly_usd.unwrap(),
                    "{name}: -lssd hourlyUsd must exceed base {base_name}"
                );
            }
            assert!(
                spec.network_gbps > 0.0,
                "{name}: networkGbps must be positive"
            );
            assert!(
                spec.disk_bandwidth_mibs > 0 && spec.disk_max_iops > 0,
                "{name}: disk limits must be positive"
            );
            assert!(
                !spec.supported_disk_types.is_empty(),
                "{name}: supportedDiskTypes must not be empty"
            );
            let boot = DiskType::parse(&spec.boot_disk_type)
                .unwrap_or_else(|e| panic!("{name}: invalid bootDiskType: {e}"));
            assert!(
                spec.supported_disk_types.contains(&boot),
                "{name}: bootDiskType {} must be in supportedDiskTypes",
                spec.boot_disk_type
            );
            match spec.local_ssd_mode {
                LocalSsdMode::Fixed => assert!(
                    spec.local_ssd_count > 0 && name.ends_with("-lssd"),
                    "{name}: fixed Local SSD requires count > 0 and -lssd suffix"
                ),
                LocalSsdMode::Attachable => assert!(
                    spec.max_local_ssd_count > 0,
                    "{name}: attachable Local SSD requires maxLocalSsdCount > 0"
                ),
                LocalSsdMode::None => assert_eq!(
                    spec.local_ssd_count, 0,
                    "{name}: none Local SSD mode must have count 0"
                ),
            }
        }

        // Spot-check critical GCP hardware facts
        let c4 = types.lookup("c4-standard-8").unwrap();
        assert_eq!(c4.memory_gib, 30);
        assert!(c4.is_hyperdisk_only());
        assert!(!c4.supports_disk_type(DiskType::PdSsd));
        assert!(c4.supports_disk_type(DiskType::HyperdiskBalanced));

        let c3_lssd = types.lookup("c3-standard-8-lssd").unwrap();
        assert_eq!(c3_lssd.local_ssd_mode, LocalSsdMode::Fixed);
        assert_eq!(c3_lssd.local_ssd_count, 2);
        assert_eq!(c3_lssd.fixed_local_ssd_gib(), 750);

        let n2 = types.lookup("n2-standard-8").unwrap();
        assert_eq!(n2.local_ssd_mode, LocalSsdMode::Attachable);
        assert!(n2.supports_disk_type(DiskType::LocalSsd));
        assert!(!n2.supports_disk_type(DiskType::HyperdiskBalanced));

        let c4a = types.lookup("c4a-standard-8").unwrap();
        assert_eq!(c4a.arch, Arch::Arm64);
        assert!(c4a.is_hyperdisk_only());
    }

    #[test]
    fn boot_disk_heuristic_handles_unknown_machine_types() {
        let types = InstanceTypes::parse("{}").unwrap();
        assert_eq!(types.boot_disk_for("c4-highmem-8"), "hyperdisk-balanced");
        assert_eq!(types.boot_disk_for("n4-highcpu-16"), "hyperdisk-balanced");
        assert_eq!(types.boot_disk_for("c4a-highmem-4"), "hyperdisk-balanced");
        assert_eq!(types.boot_disk_for("n2-highmem-8"), "pd-balanced");
    }
}
