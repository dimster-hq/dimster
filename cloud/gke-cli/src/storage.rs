//! GKE storage validation, performance estimation, and Kubernetes manifest generation.
//!
//! Encapsulates GCP disk rules (Hyperdisk Balanced IOPS/throughput ratios,
//! Hyperdisk Extreme & Throughput scaling, Persistent Disk size-scaling, and
//! GKE Local NVMe SSD block device formatting) so both interactive prompts and
//! answers-file execution share one authoritative source of truth.

use anyhow::{Result, bail};
use cloud_common::sizing::parse_storage_gib;

use crate::answers::StorageAnswers;
use crate::instances::{DiskType, InstanceSpec, LOCAL_SSD_SIZE_GIB, LocalSsdMode};

// Hyperdisk Balanced volume bounds (GCP Compute Engine limits).
pub const HDB_MIN_GIB: u32 = 6; // 4 GiB is hard min, 6 GiB avoids baseline IOPS/resize trap
pub const HDB_MAX_GIB: u32 = 65_536; // 64 TiB
pub const HDB_BASE_IOPS: u32 = 3_000;
pub const HDB_MAX_IOPS: u32 = 160_000;
pub const HDB_MAX_IOPS_PER_GIB: u32 = 500;
pub const HDB_BASE_THROUGHPUT_MIBS: u32 = 140;
pub const HDB_MAX_THROUGHPUT_MIBS: u32 = 2_400;

// Hyperdisk Extreme volume bounds.
pub const HDE_MIN_GIB: u32 = 64;
pub const HDE_MAX_GIB: u32 = 65_536;
pub const HDE_MIN_IOPS: u32 = 2_000;
pub const HDE_MAX_IOPS: u32 = 350_000;
pub const HDE_MAX_IOPS_PER_GIB: u32 = 1_000;
pub const HDE_MAX_THROUGHPUT_MIBS: u32 = 5_000;

// Hyperdisk Throughput volume bounds.
pub const HDT_MIN_GIB: u32 = 2_048; // 2 TiB
pub const HDT_MAX_GIB: u32 = 32_768; // 32 TiB
pub const HDT_MIN_THROUGHPUT_MIBS: u32 = 20;
pub const HDT_MAX_THROUGHPUT_MIBS: u32 = 2_400;

/// Estimated performance summary displayed to users during disk selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskPerformanceEstimate {
    pub read_iops: u32,
    pub write_iops: u32,
    pub read_mibs: u32,
    pub write_mibs: u32,
}

/// Ordered list of disk types compatible with `spec` (or all types if unknown).
pub fn available_disk_types(spec: Option<&InstanceSpec>) -> Vec<DiskType> {
    let preferred_order = [
        DiskType::HyperdiskBalanced,
        DiskType::LocalSsd,
        DiskType::PdSsd,
        DiskType::PdBalanced,
        DiskType::HyperdiskExtreme,
        DiskType::HyperdiskThroughput,
    ];
    match spec {
        Some(s) => preferred_order
            .into_iter()
            .filter(|&dt| s.supports_disk_type(dt))
            .collect(),
        None => preferred_order.to_vec(),
    }
}

/// Human-readable menu label explaining latency, throughput, and IOPS profile.
pub fn disk_type_menu_label(disk_type: DiskType, spec: Option<&InstanceSpec>) -> String {
    match disk_type {
        DiskType::HyperdiskBalanced => {
            "hyperdisk-balanced   (sub-ms SSD; tune IOPS 3k-160k & throughput 140-2400 MiB/s)"
                .into()
        }
        DiskType::LocalSsd => match spec.map(|s| s.local_ssd_mode) {
            Some(LocalSsdMode::Fixed) => {
                let s = spec.unwrap();
                format!(
                    "local-ssd            (lowest latency ~100-200us; {}x {} GiB bundled NVMe, ephemeral)",
                    s.local_ssd_count, LOCAL_SSD_SIZE_GIB
                )
            }
            Some(LocalSsdMode::Attachable) => {
                let s = spec.unwrap();
                format!(
                    "local-ssd            (lowest latency ~100-200us; attach 1-{}x {} GiB NVMe, ephemeral)",
                    s.max_local_ssd_count, LOCAL_SSD_SIZE_GIB
                )
            }
            _ => "local-ssd            (lowest latency ~100-200us; Local NVMe SSDs, ephemeral)".into(),
        },
        DiskType::PdSsd => {
            "pd-ssd               (sub-ms SSD PD; scales with size: 30 IOPS/GiB, 0.48 MiB/s/GiB)"
                .into()
        }
        DiskType::PdBalanced => {
            "pd-balanced          (balanced SSD PD; scales with size: 6 IOPS/GiB, 0.24 MiB/s/GiB)"
                .into()
        }
        DiskType::HyperdiskExtreme => {
            "hyperdisk-extreme    (lowest durable latency; IOPS 2k-350k, 250 MiB/s per 1k IOPS)"
                .into()
        }
        DiskType::HyperdiskThroughput => {
            "hyperdisk-throughput (HDD-backed sequential; min 2TiB, 20-2400 MiB/s, 4 IOPS/MiB/s)"
                .into()
        }
    }
}

/// Allowed throughput bounds (min, max) in MiB/s for Hyperdisk Balanced given IOPS.
pub fn hyperdisk_balanced_throughput_bounds(iops: u32) -> (u32, u32) {
    let min_t = HDB_BASE_THROUGHPUT_MIBS.max(iops.div_ceil(256));
    let max_t = HDB_MAX_THROUGHPUT_MIBS.min(iops / 4).max(min_t);
    (min_t, max_t)
}

/// Max allowed IOPS for Hyperdisk Balanced given volume size in GiB.
pub fn hyperdisk_balanced_max_iops(size_gib: u32) -> u32 {
    HDB_MAX_IOPS.min(size_gib.saturating_mul(HDB_MAX_IOPS_PER_GIB))
}

/// Validate Hyperdisk Balanced settings against GCP API limits.
/// Returns non-blocking warnings when the disk exceeds the VM's ceiling.
pub fn validate_hyperdisk_balanced(
    size_gib: u32,
    iops: Option<u32>,
    throughput_mibs: Option<u32>,
    spec: Option<&InstanceSpec>,
) -> Result<Vec<String>> {
    if !(HDB_MIN_GIB..=HDB_MAX_GIB).contains(&size_gib) {
        bail!(
            "hyperdisk-balanced size must be {}..={} GiB (got {} GiB)",
            HDB_MIN_GIB,
            HDB_MAX_GIB,
            size_gib
        );
    }

    let mut warnings = Vec::new();
    let effective_iops = iops.unwrap_or(HDB_BASE_IOPS);
    let effective_tp = throughput_mibs.unwrap_or(HDB_BASE_THROUGHPUT_MIBS);

    if let Some(iops_val) = iops {
        let max_iops = hyperdisk_balanced_max_iops(size_gib);
        if iops_val < HDB_BASE_IOPS || iops_val > max_iops {
            bail!(
                "hyperdisk-balanced IOPS must be {}..={} for a {} GiB disk (got {})",
                HDB_BASE_IOPS,
                max_iops,
                size_gib,
                iops_val
            );
        }
    }

    if let Some(tp_val) = throughput_mibs {
        let (min_tp, max_tp) = hyperdisk_balanced_throughput_bounds(effective_iops);
        if tp_val < min_tp || tp_val > max_tp {
            bail!(
                "hyperdisk-balanced throughput must be {}..={} MiB/s for {} IOPS (got {} MiB/s)",
                min_tp,
                max_tp,
                effective_iops,
                tp_val
            );
        }
    }

    if let Some(s) = spec {
        if effective_iops > s.disk_max_iops {
            warnings.push(format!(
                "Provisioned {} IOPS exceeds VM ceiling ({} IOPS); effective IOPS will cap at VM limit",
                effective_iops, s.disk_max_iops
            ));
        }
        if effective_tp > s.disk_bandwidth_mibs {
            warnings.push(format!(
                "Provisioned {} MiB/s exceeds VM ceiling ({} MiB/s); effective throughput will cap at VM limit",
                effective_tp, s.disk_bandwidth_mibs
            ));
        }
    }

    Ok(warnings)
}

/// Validate Hyperdisk Extreme settings. Throughput couples automatically at 250 MiB/s per 1,000 IOPS.
pub fn validate_hyperdisk_extreme(
    size_gib: u32,
    iops: u32,
    spec: Option<&InstanceSpec>,
) -> Result<(u32, Vec<String>)> {
    if !(HDE_MIN_GIB..=HDE_MAX_GIB).contains(&size_gib) {
        bail!(
            "hyperdisk-extreme size must be {}..={} GiB (got {} GiB)",
            HDE_MIN_GIB,
            HDE_MAX_GIB,
            size_gib
        );
    }
    let max_iops = HDE_MAX_IOPS.min(size_gib.saturating_mul(HDE_MAX_IOPS_PER_GIB));
    if iops < HDE_MIN_IOPS || iops > max_iops {
        bail!(
            "hyperdisk-extreme IOPS must be {}..={} for a {} GiB disk (got {})",
            HDE_MIN_IOPS,
            max_iops,
            size_gib,
            iops
        );
    }
    let coupled_tp = HDE_MAX_THROUGHPUT_MIBS.min(iops / 4);
    let mut warnings = Vec::new();
    if let Some(s) = spec {
        if iops > s.disk_max_iops {
            warnings.push(format!(
                "Provisioned {} IOPS exceeds VM ceiling ({} IOPS)",
                iops, s.disk_max_iops
            ));
        }
        if coupled_tp > s.disk_bandwidth_mibs {
            warnings.push(format!(
                "Auto-coupled {} MiB/s exceeds VM ceiling ({} MiB/s)",
                coupled_tp, s.disk_bandwidth_mibs
            ));
        }
    }
    Ok((coupled_tp, warnings))
}

/// Allowed throughput bounds (min, max) in MiB/s for Hyperdisk Throughput given size in GiB.
pub fn hyperdisk_throughput_bounds(size_gib: u32) -> (u32, u32) {
    let size_tib = size_gib.div_ceil(1024);
    let min_tp = HDT_MIN_THROUGHPUT_MIBS.max(size_tib.saturating_mul(5));
    let max_tp = HDT_MAX_THROUGHPUT_MIBS.min(size_tib.saturating_mul(90));
    (min_tp, max_tp.max(min_tp))
}

/// Validate Hyperdisk Throughput settings. IOPS couple automatically at 4 IOPS per MiB/s.
pub fn validate_hyperdisk_throughput(
    size_gib: u32,
    throughput_mibs: u32,
    spec: Option<&InstanceSpec>,
) -> Result<(u32, Vec<String>)> {
    if !(HDT_MIN_GIB..=HDT_MAX_GIB).contains(&size_gib) {
        bail!(
            "hyperdisk-throughput size must be {}..={} GiB (2..=32 TiB, got {} GiB)",
            HDT_MIN_GIB,
            HDT_MAX_GIB,
            size_gib
        );
    }
    let (min_tp, max_tp) = hyperdisk_throughput_bounds(size_gib);
    if throughput_mibs < min_tp || throughput_mibs > max_tp {
        bail!(
            "hyperdisk-throughput must be {}..={} MiB/s for {} GiB (got {} MiB/s)",
            min_tp,
            max_tp,
            size_gib,
            throughput_mibs
        );
    }
    let coupled_iops = throughput_mibs.saturating_mul(4);
    let mut warnings = Vec::new();
    if let Some(s) = spec
        && throughput_mibs > s.disk_bandwidth_mibs
    {
        warnings.push(format!(
            "Provisioned {} MiB/s exceeds VM ceiling ({} MiB/s)",
            throughput_mibs, s.disk_bandwidth_mibs
        ));
    }
    Ok((coupled_iops, warnings))
}

/// Estimate effective Persistent Disk (`pd-ssd` or `pd-balanced`) performance
/// from disk size in GiB, bounded by per-disk and per-VM limits.
pub fn estimate_pd_performance(
    disk_type: DiskType,
    size_gib: u32,
    spec: Option<&InstanceSpec>,
) -> DiskPerformanceEstimate {
    let (disk_iops, disk_mibs) = match disk_type {
        DiskType::PdSsd => (
            (3_000 + size_gib.saturating_mul(30)).min(100_000),
            ((0.28 + size_gib as f64 * 0.48) as u32).min(1_200),
        ),
        DiskType::PdBalanced => (
            (3_000 + size_gib.saturating_mul(6)).min(80_000),
            ((0.14 + size_gib as f64 * 0.24) as u32).min(1_200),
        ),
        _ => (0, 0),
    };

    let (eff_iops, eff_mibs) = match spec {
        Some(s) => (
            disk_iops.min(s.disk_max_iops),
            disk_mibs.min(s.disk_bandwidth_mibs),
        ),
        None => (disk_iops, disk_mibs),
    };

    DiskPerformanceEstimate {
        read_iops: eff_iops,
        write_iops: eff_iops,
        read_mibs: eff_mibs,
        write_mibs: eff_mibs,
    }
}

/// Estimate aggregate Local NVMe SSD read/write IOPS and MiB/s for `count` 375 GiB drives.
///
/// Reflects GCP's published linear scaling up to 4 disks (680k/360k IOPS, 2650/1400 MiB/s)
/// on standard/gen-2 shapes; Titanium `-lssd` shapes with >4 disks can exceed these figures.
pub fn estimate_local_ssd_performance(count: u32) -> DiskPerformanceEstimate {
    let effective = count.clamp(1, 4);
    let (read_iops, write_iops, read_mibs, write_mibs) = match effective {
        1 => (170_000, 90_000, 660, 350),
        2 => (340_000, 180_000, 1_320, 700),
        3 => (510_000, 270_000, 1_980, 1_050),
        _ => (680_000, 360_000, 2_650, 1_400),
    };
    DiskPerformanceEstimate {
        read_iops,
        write_iops,
        read_mibs,
        write_mibs,
    }
}

/// Validate a full `StorageAnswers` block against the selected Kafka `InstanceSpec`.
pub fn validate_storage_answers(
    storage: &StorageAnswers,
    spec: Option<&InstanceSpec>,
) -> Result<Vec<String>> {
    let disk_type = DiskType::parse(&storage.storage_type)?;
    if let Some(s) = spec
        && !s.supports_disk_type(disk_type)
    {
        bail!(
            "Storage type '{}' is not supported on this machine type (supported: {})",
            disk_type.as_str(),
            available_disk_types(Some(s))
                .iter()
                .map(|d| d.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let size_gib = parse_storage_gib(&storage.size)?;
    match disk_type {
        DiskType::HyperdiskBalanced => {
            if storage.performance.as_deref() == Some("provisioned") {
                validate_hyperdisk_balanced(size_gib, storage.iops, storage.throughput_mibs, spec)
            } else {
                validate_hyperdisk_balanced(size_gib, None, None, spec)
            }
        }
        DiskType::HyperdiskExtreme => {
            let iops = storage.iops.unwrap_or(HDE_MIN_IOPS);
            let (_tp, warnings) = validate_hyperdisk_extreme(size_gib, iops, spec)?;
            Ok(warnings)
        }
        DiskType::HyperdiskThroughput => {
            let tp = storage.throughput_mibs.unwrap_or(HDT_MIN_THROUGHPUT_MIBS);
            let (_iops, warnings) = validate_hyperdisk_throughput(size_gib, tp, spec)?;
            Ok(warnings)
        }
        DiskType::PdSsd | DiskType::PdBalanced => {
            if size_gib < 10 || size_gib > 65_536 {
                bail!(
                    "{} size must be 10..=65536 GiB (got {} GiB)",
                    disk_type.as_str(),
                    size_gib
                );
            }
            Ok(Vec::new())
        }
        DiskType::LocalSsd => {
            if let Some(s) = spec
                && s.local_ssd_mode == LocalSsdMode::Attachable
            {
                let count = storage.local_ssd_count.unwrap_or(1);
                if count == 0 || count > s.max_local_ssd_count {
                    bail!(
                        "localSsdCount must be 1..={} for attachable Local SSDs (got {})",
                        s.max_local_ssd_count,
                        count
                    );
                }
            }
            Ok(Vec::new())
        }
    }
}

/// Resolve the Kubernetes `StorageClass` name for a Kafka storage configuration.
///
/// Local NVMe SSDs MUST use the exact name `local-storage` so `dimster deploy`
/// and `dimster teardown` automatically clear stale `claimRef`s on redeploy.
pub fn storage_class_name(storage: &StorageAnswers) -> &'static str {
    match storage.storage_type.as_str() {
        "local-ssd" => "local-storage",
        "hyperdisk-balanced" => {
            if storage.performance.as_deref() == Some("provisioned") {
                "dimster-hyperdisk-balanced-fast"
            } else {
                "dimster-hyperdisk-balanced"
            }
        }
        "hyperdisk-extreme" => "dimster-hyperdisk-extreme",
        "hyperdisk-throughput" => "dimster-hyperdisk-throughput",
        "pd-ssd" => "dimster-pd-ssd",
        "pd-balanced" => "dimster-pd-balanced",
        _ => "dimster-hyperdisk-balanced",
    }
}

/// Compute the GKE `--local-nvme-ssd-block` CLI flag when Kafka uses Local NVMe SSDs.
///
/// For 3rd/4th-gen fixed `-lssd` machine types (C3, C3D, C4, C4A, C4D), GKE
/// automatically maps the bundled drive count when `count=` is omitted.
/// For 2nd-gen attachable machine types (N2, N2D), `count=N` is required.
pub fn local_ssd_node_pool_flag(
    storage: &StorageAnswers,
    spec: Option<&InstanceSpec>,
) -> Option<String> {
    if storage.storage_type != "local-ssd" {
        return None;
    }
    match spec.map(|s| s.local_ssd_mode) {
        Some(LocalSsdMode::Fixed) => Some("--local-nvme-ssd-block".to_string()),
        Some(LocalSsdMode::Attachable) => {
            let count = storage.local_ssd_count.unwrap_or(1);
            Some(format!("--local-nvme-ssd-block=count={}", count))
        }
        _ => match storage.local_ssd_count {
            Some(c) if c > 0 => Some(format!("--local-nvme-ssd-block=count={}", c)),
            _ => Some("--local-nvme-ssd-block".to_string()),
        },
    }
}

/// Render a GCE Persistent Disk / Hyperdisk CSI `StorageClass` YAML manifest.
///
/// Labels every dynamically provisioned disk with `dimster-cluster` and
/// `dimster-name` so `gke-setup delete` can safely sweep any orphaned disks.
pub fn render_dynamic_storage_class(
    storage: &StorageAnswers,
    cluster_name: &str,
    dimster_name: &str,
) -> Result<String> {
    let sc_name = storage_class_name(storage);
    let mut params = vec![
        format!("  type: {}", storage.storage_type),
        "  csi.storage.k8s.io/fstype: xfs".to_string(),
    ];

    match storage.storage_type.as_str() {
        "hyperdisk-balanced" => {
            if storage.performance.as_deref() == Some("provisioned") {
                if let Some(iops) = storage.iops {
                    params.push(format!("  provisioned-iops-on-create: \"{}\"", iops));
                }
                if let Some(tp) = storage.throughput_mibs {
                    params.push(format!("  provisioned-throughput-on-create: \"{}Mi\"", tp));
                }
            }
        }
        "hyperdisk-extreme" => {
            let iops = storage.iops.unwrap_or(HDE_MIN_IOPS);
            params.push(format!("  provisioned-iops-on-create: \"{}\"", iops));
        }
        "hyperdisk-throughput" => {
            let tp = storage.throughput_mibs.unwrap_or(HDT_MIN_THROUGHPUT_MIBS);
            params.push(format!("  provisioned-throughput-on-create: \"{}Mi\"", tp));
        }
        "pd-ssd" | "pd-balanced" => {}
        other => bail!("Cannot render dynamic StorageClass for '{}'", other),
    }

    params.push(format!(
        "  labels: \"dimster-cluster={},dimster-name={}\"",
        cluster_name, dimster_name
    ));

    Ok(format!(
        "apiVersion: storage.k8s.io/v1\n\
         kind: StorageClass\n\
         metadata:\n\
         \x20\x20name: {sc_name}\n\
         provisioner: pd.csi.storage.gke.io\n\
         parameters:\n\
         {params}\n\
         reclaimPolicy: Delete\n\
         volumeBindingMode: WaitForFirstConsumer\n\
         allowVolumeExpansion: true\n",
        sc_name = sc_name,
        params = params.join("\n"),
    ))
}

/// Render the `local-storage` `StorageClass` for GKE Local NVMe SSD static PVs.
pub fn render_local_storage_class() -> &'static str {
    "apiVersion: storage.k8s.io/v1\n\
     kind: StorageClass\n\
     metadata:\n\
     \x20\x20name: local-storage\n\
     provisioner: kubernetes.io/no-provisioner\n\
     volumeBindingMode: WaitForFirstConsumer\n"
}

/// Render the privileged multi-arch DaemonSet that discovers GKE Local NVMe SSDs
/// via `/dev/disk/by-id/google-local-nvme-ssd-*`, stripes RAID-0 if multiple,
/// formats XFS, and mounts at `/mnt/disks/nvme0` (writable on GKE COS).
///
/// Matching `/dev/disk/by-id/google-local-nvme-ssd-*` is critical on GCP: modern
/// C3/C4/N4 boot disks and Hyperdisk volumes also appear as `/dev/nvme*n1`, so
/// wildcarding `/dev/nvme*` would risk touching non-local disks.
pub fn render_nvme_format_daemonset() -> String {
    format!(
        r#"apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: nvme-format
  namespace: kube-system
spec:
  selector:
    matchLabels:
      app: nvme-format
  template:
    metadata:
      labels:
        app: nvme-format
    spec:
      nodeSelector:
        role: kafka
      containers:
        - name: format
          image: debian:bookworm-slim
          securityContext:
            privileged: true
          command:
            - /bin/bash
            - -c
            - |
{script}
          volumeMounts:
            - name: dev
              mountPath: /dev
            - name: mnt-disks
              mountPath: /mnt/disks
              mountPropagation: Bidirectional
      volumes:
        - name: dev
          hostPath:
            path: /dev
        - name: mnt-disks
          hostPath:
            path: /mnt/disks
            type: DirectoryOrCreate
      tolerations:
        - operator: Exists
"#,
        script = gke_nvme_setup_script()
    )
}

/// Render a static `PersistentVolume` bound to `/mnt/disks/nvme0` on a Kafka node.
///
/// Uses `Retain` reclaim policy: static local volumes have no dynamic provisioner
/// to satisfy `Delete`, and Dimster's `reset_local_pvs` clears stale `claimRef`s
/// on `Released` PVs cleanly between teardown/deploy cycles.
pub fn render_local_pv(index: usize, size: &str, node_name: &str) -> String {
    format!(
        "apiVersion: v1\n\
         kind: PersistentVolume\n\
         metadata:\n\
         \x20\x20name: kafka-nvme-{index}\n\
         spec:\n\
         \x20\x20capacity:\n\
         \x20\x20\x20\x20storage: {size}\n\
         \x20\x20accessModes: [\"ReadWriteOnce\"]\n\
         \x20\x20persistentVolumeReclaimPolicy: Retain\n\
         \x20\x20storageClassName: local-storage\n\
         \x20\x20local:\n\
         \x20\x20\x20\x20path: /mnt/disks/nvme0\n\
         \x20\x20nodeAffinity:\n\
         \x20\x20\x20\x20required:\n\
         \x20\x20\x20\x20\x20\x20nodeSelectorTerms:\n\
         \x20\x20\x20\x20\x20\x20\x20\x20- matchExpressions:\n\
         \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20- key: kubernetes.io/hostname\n\
         \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20operator: In\n\
         \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20values: [\"{node_name}\"]\n"
    )
}

/// Compact hardware-description fragment for `dimster-config.yaml`'s `hardware:` field.
pub fn hardware_storage_summary(storage: &StorageAnswers) -> String {
    match storage.storage_type.as_str() {
        "hyperdisk-balanced" if storage.performance.as_deref() == Some("provisioned") => {
            format!(
                "hyperdisk-balanced/{}/provisioned/{}iops/{}MiBs",
                storage.size,
                storage.iops.unwrap_or(HDB_BASE_IOPS),
                storage.throughput_mibs.unwrap_or(HDB_BASE_THROUGHPUT_MIBS)
            )
        }
        "hyperdisk-extreme" => format!(
            "hyperdisk-extreme/{}/{}iops",
            storage.size,
            storage.iops.unwrap_or(HDE_MIN_IOPS)
        ),
        "hyperdisk-throughput" => format!(
            "hyperdisk-throughput/{}/{}MiBs",
            storage.size,
            storage.throughput_mibs.unwrap_or(HDT_MIN_THROUGHPUT_MIBS)
        ),
        "local-ssd" => match storage.local_ssd_count {
            Some(c) => format!("local-ssd/{}x375Gi/{}", c, storage.size),
            None => format!("local-ssd/{}", storage.size),
        },
        other => format!("{}/{}", other, storage.size),
    }
}

fn gke_nvme_setup_script() -> &'static str {
    "              set -euo pipefail
              MOUNT=/mnt/disks/nvme0
              mkdir -p \"$MOUNT\"
              if mountpoint -q \"$MOUNT\"; then
                echo \"$MOUNT already mounted\"
                sleep infinity
              fi
              export DEBIAN_FRONTEND=noninteractive
              apt-get update -qq > /dev/null
              apt-get install -y -qq xfsprogs mdadm > /dev/null
              # Discover ONLY GKE Local NVMe SSDs via persistent Google symlinks
              # (never touch NVMe boot disks or Hyperdisk volumes).
              DEVICES=()
              for link in /dev/disk/by-id/google-local-nvme-ssd-*; do
                [ -e \"$link\" ] || continue
                dev=$(readlink -f \"$link\")
                [ -b \"$dev\" ] && DEVICES+=(\"$dev\")
              done
              if [ ${#DEVICES[@]} -eq 0 ]; then
                echo \"No GKE Local NVMe SSD devices found under /dev/disk/by-id/google-local-nvme-ssd-*\"
                exit 1
              fi
              echo \"Found ${#DEVICES[@]} Local NVMe SSD(s): ${DEVICES[*]}\"
              if [ ${#DEVICES[@]} -eq 1 ]; then
                TARGET=${DEVICES[0]}
                echo \"Single Local SSD: formatting $TARGET as xfs...\"
                mkfs.xfs -f \"$TARGET\"
              else
                TARGET=/dev/md0
                echo \"Multiple Local SSDs: assembling RAID-0 $TARGET...\"
                mdadm --create \"$TARGET\" --level=0 --raid-devices=${#DEVICES[@]} ${DEVICES[*]} --force
                mkfs.xfs -f \"$TARGET\"
              fi
              mount -o discard,defaults,noatime \"$TARGET\" \"$MOUNT\"
              chmod 777 \"$MOUNT\"
              echo \"$MOUNT mounted successfully\"
              sleep infinity"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instances::InstanceTypes;
    use std::path::Path;

    fn load_catalog() -> InstanceTypes {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("instance-types.yaml");
        InstanceTypes::load(&path).unwrap()
    }

    #[test]
    fn disk_compatibility_filters_by_machine_family() {
        let types = load_catalog();
        let c4 = types.lookup("c4-standard-8").unwrap();
        let avail_c4 = available_disk_types(Some(c4));
        assert!(avail_c4.contains(&DiskType::HyperdiskBalanced));
        assert!(!avail_c4.contains(&DiskType::PdSsd));
        assert!(!avail_c4.contains(&DiskType::LocalSsd));
        assert!(!avail_c4.contains(&DiskType::HyperdiskExtreme));

        let n2 = types.lookup("n2-standard-8").unwrap();
        let avail_n2 = available_disk_types(Some(n2));
        assert!(avail_n2.contains(&DiskType::PdSsd));
        assert!(avail_n2.contains(&DiskType::LocalSsd));
        assert!(!avail_n2.contains(&DiskType::HyperdiskBalanced));

        let c3_lssd = types.lookup("c3-standard-8-lssd").unwrap();
        let avail_c3_lssd = available_disk_types(Some(c3_lssd));
        assert!(avail_c3_lssd.contains(&DiskType::HyperdiskBalanced));
        assert!(avail_c3_lssd.contains(&DiskType::LocalSsd));
        assert!(avail_c3_lssd.contains(&DiskType::PdSsd));
    }

    #[test]
    fn hyperdisk_balanced_validates_ratios_and_warns_on_vm_ceiling() {
        let types = load_catalog();
        let c4_8 = types.lookup("c4-standard-8").unwrap(); // cap: 800 MiB/s, 50k IOPS

        // Valid within VM cap: no warnings
        let warns = validate_hyperdisk_balanced(500, Some(16_000), Some(800), Some(c4_8)).unwrap();
        assert!(warns.is_empty());

        // Valid for disk, exceeds VM cap: succeeds with warning
        let warns = validate_hyperdisk_balanced(500, Some(16_000), Some(1_200), Some(c4_8)).unwrap();
        assert_eq!(warns.len(), 1);
        assert!(warns[0].contains("800 MiB/s"));

        // Invalid: throughput > IOPS/4 (3000/4 = 750 MiB/s max)
        assert!(validate_hyperdisk_balanced(500, Some(3_000), Some(1_000), Some(c4_8)).is_err());

        // Invalid: IOPS > 500 * size_gib (10 GiB -> max 5,000 IOPS)
        assert!(validate_hyperdisk_balanced(10, Some(10_000), Some(200), Some(c4_8)).is_err());

        // Invalid: size < 6 GiB
        assert!(validate_hyperdisk_balanced(4, None, None, Some(c4_8)).is_err());
    }

    #[test]
    fn hyperdisk_throughput_and_extreme_bounds() {
        // 2 TiB = 2048 GiB -> throughput 20..=180 MiB/s
        assert_eq!(hyperdisk_throughput_bounds(2048), (20, 180));
        let (iops, _) = validate_hyperdisk_throughput(2048, 180, None).unwrap();
        assert_eq!(iops, 720);
        assert!(validate_hyperdisk_throughput(1024, 100, None).is_err()); // < 2 TiB

        // Hyperdisk Extreme: 100k IOPS -> 5000 MiB/s cap (100000 / 4 = 25000 -> capped 5000)
        let (tp, _) = validate_hyperdisk_extreme(500, 100_000, None).unwrap();
        assert_eq!(tp, 5_000);
        let (tp_small, _) = validate_hyperdisk_extreme(500, 8_000, None).unwrap();
        assert_eq!(tp_small, 2_000);
    }

    #[test]
    fn pd_performance_scales_and_caps_at_vm_limit() {
        let types = load_catalog();
        let n2_8 = types.lookup("n2-standard-8").unwrap(); // cap: 800 MiB/s, 15k IOPS
        let est = estimate_pd_performance(DiskType::PdSsd, 500, Some(n2_8));
        // 500 GiB pd-ssd: 18,000 IOPS -> capped at 15,000; 240 MiB/s < 800 -> 240
        assert_eq!(est.read_iops, 15_000);
        assert_eq!(est.write_mibs, 240);
    }

    #[test]
    fn dynamic_storage_class_yaml_is_valid() {
        let storage = StorageAnswers {
            storage_type: "hyperdisk-balanced".into(),
            size: "500Gi".into(),
            performance: Some("provisioned".into()),
            iops: Some(16_000),
            throughput_mibs: Some(800),
            local_ssd_count: None,
        };
        let yaml = render_dynamic_storage_class(&storage, "demo-benchmark", "demo").unwrap();
        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(doc["metadata"]["name"], "dimster-hyperdisk-balanced-fast");
        assert_eq!(doc["provisioner"], "pd.csi.storage.gke.io");
        assert_eq!(doc["parameters"]["type"], "hyperdisk-balanced");
        assert_eq!(doc["parameters"]["csi.storage.k8s.io/fstype"], "xfs");
        assert_eq!(doc["parameters"]["provisioned-iops-on-create"], "16000");
        assert_eq!(
            doc["parameters"]["provisioned-throughput-on-create"],
            "800Mi"
        );
        assert_eq!(
            doc["parameters"]["labels"],
            "dimster-cluster=demo-benchmark,dimster-name=demo"
        );
        assert_eq!(doc["reclaimPolicy"], "Delete");
        assert_eq!(doc["volumeBindingMode"], "WaitForFirstConsumer");
    }

    #[test]
    fn local_ssd_manifests_and_node_pool_flags() {
        let types = load_catalog();
        let c3_lssd = types.lookup("c3-standard-8-lssd").unwrap();
        let n2 = types.lookup("n2-standard-8").unwrap();

        let storage_fixed = StorageAnswers {
            storage_type: "local-ssd".into(),
            size: "675Gi".into(),
            performance: None,
            iops: None,
            throughput_mibs: None,
            local_ssd_count: Some(2),
        };
        assert_eq!(storage_class_name(&storage_fixed), "local-storage");
        assert_eq!(
            local_ssd_node_pool_flag(&storage_fixed, Some(c3_lssd)).as_deref(),
            Some("--local-nvme-ssd-block")
        );
        assert_eq!(
            local_ssd_node_pool_flag(&storage_fixed, Some(n2)).as_deref(),
            Some("--local-nvme-ssd-block=count=2")
        );

        let ds = render_nvme_format_daemonset();
        let ds_doc: serde_yaml::Value = serde_yaml::from_str(&ds).unwrap();
        assert_eq!(ds_doc["kind"], "DaemonSet");
        assert!(ds.contains("/dev/disk/by-id/google-local-nvme-ssd-*"));
        assert!(ds.contains("/mnt/disks/nvme0"));

        let pv = render_local_pv(0, "675Gi", "gke-demo-kafka-abc");
        let pv_doc: serde_yaml::Value = serde_yaml::from_str(&pv).unwrap();
        assert_eq!(pv_doc["spec"]["storageClassName"], "local-storage");
        assert_eq!(pv_doc["spec"]["persistentVolumeReclaimPolicy"], "Retain");
        assert_eq!(pv_doc["spec"]["local"]["path"], "/mnt/disks/nvme0");
    }
}
