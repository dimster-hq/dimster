//! Resource sizing math and Kubernetes quantity parsing/formatting helpers.
//!
//! Shared between cloud setup CLIs to compute sensible Kafka, worker, and
//! monitoring resource defaults from an instance type's vCPU and RAM specs.

use std::path::{Path, PathBuf};

use anyhow::{Result, bail};

/// Kubernetes / EKS / GKE tiered kubelet memory reservation in MiB:
/// 25% of first 4 GiB + 20% of next 4 GiB + 10% of next 8 GiB
/// + 6% of next 112 GiB + 2% above 128 GiB.
pub fn kubelet_tiered_memory_reserved_mib(instance_memory_gib: u32) -> f64 {
    let mem_mib = instance_memory_gib as f64 * 1024.0;
    let tiers: &[(f64, f64)] = &[
        (4096.0, 0.25),   // 25% of first 4 GiB
        (4096.0, 0.20),   // 20% of next 4 GiB
        (8192.0, 0.10),   // 10% of next 8 GiB
        (114688.0, 0.06), // 6% of next 112 GiB
    ];
    let mut reserved = 0.0;
    let mut remaining = mem_mib;
    for &(tier_size, pct) in tiers {
        let in_tier = remaining.min(tier_size);
        reserved += in_tier * pct;
        remaining -= in_tier;
        if remaining <= 0.0 {
            break;
        }
    }
    reserved + remaining * 0.02
}

/// Estimate allocatable workload memory in GiB on an AWS EKS node.
///
/// Accounts for tiered kubelet reservation + 255 MiB base + 100 MiB eviction
/// threshold + 1 GiB system pod overhead (kube-proxy, CNI, daemonsets).
pub fn eks_allocatable_memory_gib(instance_memory_gib: u32) -> u32 {
    let mem_mib = instance_memory_gib as f64 * 1024.0;
    let reserved = kubelet_tiered_memory_reserved_mib(instance_memory_gib)
        + 255.0   // EKS base kubelet reservation
        + 100.0   // eviction threshold
        + 1024.0; // system pods
    ((mem_mib - reserved) / 1024.0).floor().max(2.0) as u32
}

/// Estimate allocatable workload memory in GiB on a GKE Standard (COS) node.
///
/// Accounts for GKE's tiered memory reservation + 100 MiB eviction threshold
/// + ~500 MiB Container-Optimized OS / containerd overhead + 1 GiB system pods.
pub fn gke_allocatable_memory_gib(instance_memory_gib: u32) -> u32 {
    let mem_mib = instance_memory_gib as f64 * 1024.0;
    let reserved = kubelet_tiered_memory_reserved_mib(instance_memory_gib)
        + 100.0   // hard eviction threshold
        + 500.0   // COS / containerd base overhead
        + 1024.0; // system pods (kube-dns, metrics-agent, pd-csi, dataplane v2)
    ((mem_mib - reserved) / 1024.0).floor().max(2.0) as u32
}

/// Suggested Kafka JVM heap: 25% of instance memory, clamped to 1..=8 GiB.
pub fn default_kafka_heap(instance_memory_gib: u32) -> String {
    let heap_gib = (instance_memory_gib / 4).clamp(1, 8);
    format!("-Xms{}G -Xmx{}G", heap_gib, heap_gib)
}

/// Suggested worker JVM heap: max is 80% of allocatable memory, min is 1/3 of max.
pub fn default_worker_heap(allocatable_gib: u32) -> String {
    let max_gib = ((allocatable_gib as f64 * 0.8) as u32).max(1);
    let min_gib = (max_gib / 3).max(1);
    format!("-Xms{}G -Xmx{}G", min_gib, max_gib)
}

/// Suggested Kafka CPU request: vCPU - 2 (min 1) to leave room for OS/daemonsets.
pub fn default_kafka_cpu(vcpu: u32) -> String {
    let cpu = vcpu.saturating_sub(2).max(1);
    format!("{}", cpu)
}

/// Suggested worker CPU request: vCPU - 1 (min 1).
pub fn default_worker_cpu(vcpu: u32) -> String {
    let cpu = vcpu.saturating_sub(1).max(1);
    format!("{}", cpu)
}

/// Compute Prometheus and Grafana resource defaults from the infra node capacity
/// after subtracting the coordinator's requests.
///
/// Splits remaining memory 60/40 (Prometheus TSDB / Grafana) and remaining CPU 50/50.
pub fn compute_monitoring_defaults(
    infra_vcpu: Option<u32>,
    infra_allocatable_gib: Option<u32>,
    coord_cpu: &str,
    coord_memory: &str,
) -> (String, String, String, String) {
    let (vcpu, allocatable_gib) = match (infra_vcpu, infra_allocatable_gib) {
        (Some(c), Some(m)) => (c, m),
        _ => {
            return (
                "500m".into(),
                "1Gi".into(),
                "500m".into(),
                "512Mi".into(),
            );
        }
    };

    let coord_mem_mib = parse_memory_mib(coord_memory);
    let coord_cpu_millis = parse_cpu_millis(coord_cpu);

    let allocatable_mib = allocatable_gib as f64 * 1024.0;
    let remaining_mib = (allocatable_mib - coord_mem_mib as f64).max(512.0);

    let prom_mem_mib = ((remaining_mib * 0.6).floor() as u32).max(512);
    let grafana_mem_mib = ((remaining_mib * 0.4).floor() as u32).max(256);

    // Reserve ~1000m for kubelet + cloud system DaemonSets + cluster-level
    // kube-system pods (kube-dns, metrics-server, anetd/CNI, fluentbit,
    // node-exporter) that share the infra node before splitting remaining CPU.
    let usable_cpu_millis = (vcpu * 1000).saturating_sub(1000);
    let remaining_cpu = usable_cpu_millis.saturating_sub(coord_cpu_millis).max(400);
    let prom_cpu_millis = remaining_cpu / 2;
    let grafana_cpu_millis = remaining_cpu / 2;

    (
        format_cpu(prom_cpu_millis),
        format_memory(prom_mem_mib),
        format_cpu(grafana_cpu_millis),
        format_memory(grafana_mem_mib),
    )
}

/// Parse a Kubernetes memory string like "1Gi" or "512Mi" into MiB.
pub fn parse_memory_mib(s: &str) -> u32 {
    let s = s.trim();
    if let Some(gi) = s.strip_suffix("Gi") {
        gi.parse::<u32>().unwrap_or(1) * 1024
    } else if let Some(mi) = s.strip_suffix("Mi") {
        mi.parse::<u32>().unwrap_or(512)
    } else {
        1024
    }
}

/// Parse a Kubernetes storage capacity string ("500Gi", "2Ti", "100G", "2T") into GiB.
pub fn parse_storage_gib(s: &str) -> Result<u32> {
    let s = s.trim();
    if let Some(ti) = s.strip_suffix("Ti") {
        let v: u32 = ti
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid storage size '{}'", s))?;
        return Ok(v.saturating_mul(1024));
    }
    if let Some(gi) = s.strip_suffix("Gi") {
        let v: u32 = gi
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid storage size '{}'", s))?;
        return Ok(v);
    }
    if let Some(t) = s.strip_suffix('T') {
        let v: u32 = t
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid storage size '{}'", s))?;
        return Ok(v.saturating_mul(1024));
    }
    if let Some(g) = s.strip_suffix('G') {
        let v: u32 = g
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid storage size '{}'", s))?;
        return Ok(v);
    }
    if let Ok(v) = s.parse::<u32>() {
        return Ok(v);
    }
    bail!(
        "Unsupported storage size '{}' (expected e.g. '500Gi' or '2Ti')",
        s
    );
}

/// Parse a Kubernetes CPU string like "500m", "1", "2" into millicores.
pub fn parse_cpu_millis(s: &str) -> u32 {
    let s = s.trim();
    if let Some(m) = s.strip_suffix('m') {
        m.parse::<u32>().unwrap_or(500)
    } else {
        s.parse::<u32>().unwrap_or(1) * 1000
    }
}

/// Format millicores as a Kubernetes CPU quantity: exact whole cores or "Xm".
pub fn format_cpu(millis: u32) -> String {
    if millis >= 1000 && millis % 1000 == 0 {
        format!("{}", millis / 1000)
    } else {
        format!("{}m", millis)
    }
}

/// Format MiB as a Kubernetes memory quantity: exact GiB or "XMi".
pub fn format_memory(mib: u32) -> String {
    if mib >= 1024 && mib % 1024 == 0 {
        format!("{}Gi", mib / 1024)
    } else {
        format!("{}Mi", mib)
    }
}

/// Resolve an instance-types.yaml path: prefer the user-provided path if it
/// exists, otherwise try fallback paths (so running from repo root or inside
/// `cloud/<provider>-cli/` both work seamlessly).
pub fn resolve_instance_types_path(requested: &Path, fallbacks: &[&str]) -> PathBuf {
    if requested.exists() {
        return requested.to_path_buf();
    }
    for fb in fallbacks {
        let p = Path::new(fb);
        if p.exists() {
            return p.to_path_buf();
        }
    }
    requested.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocatable_memory_accounts_for_kubelet_tiers() {
        // 4 GiB: tiered=1024 MiB; EKS reserved=2403 MiB -> 1.65 GiB -> clamped to 2 GiB
        assert_eq!(eks_allocatable_memory_gib(4), 2);
        // 32 GiB: tiered = 1024 + 819.2 + 819.2 + 983.04 = 3645.44 MiB
        // EKS: + 1379 = 5024.44 MiB (~4.9 GiB) -> 27 GiB
        assert_eq!(eks_allocatable_memory_gib(32), 27);
        // GKE: + 1624 = 5269.44 MiB (~5.15 GiB) -> 26 GiB
        assert_eq!(gke_allocatable_memory_gib(32), 26);
        // 8 GiB GKE (e2-standard-2): tiered=1843.2 + 1624 = 3467.2 MiB -> 4 GiB
        assert_eq!(gke_allocatable_memory_gib(8), 4);
    }

    #[test]
    fn heap_and_cpu_defaults_clamp_correctly() {
        assert_eq!(default_kafka_heap(4), "-Xms1G -Xmx1G");
        assert_eq!(default_kafka_heap(32), "-Xms8G -Xmx8G");
        assert_eq!(default_kafka_heap(128), "-Xms8G -Xmx8G");
        assert_eq!(default_worker_heap(12), "-Xms3G -Xmx9G");
        assert_eq!(default_kafka_cpu(2), "1");
        assert_eq!(default_kafka_cpu(8), "6");
        assert_eq!(default_worker_cpu(4), "3");
    }

    #[test]
    fn parse_storage_gib_handles_units() {
        assert_eq!(parse_storage_gib("500Gi").unwrap(), 500);
        assert_eq!(parse_storage_gib("2Ti").unwrap(), 2048);
        assert_eq!(parse_storage_gib("100G").unwrap(), 100);
        assert_eq!(parse_storage_gib("1T").unwrap(), 1024);
        assert_eq!(parse_storage_gib("250").unwrap(), 250);
        assert!(parse_storage_gib("bogus").is_err());
    }

    #[test]
    fn monitoring_defaults_split_remaining_capacity() {
        let (pc, pm, gc, gm) = compute_monitoring_defaults(Some(2), Some(4), "500m", "1Gi");
        // Remaining memory = 3072 MiB -> 60% = 1843 MiB, 40% = 1228 MiB
        assert_eq!(pm, "1843Mi");
        assert_eq!(gm, "1228Mi");
        // Usable CPU = 2000m - 1000m (kube-system/kubelet) - 500m (coord) = 500m -> 250m each
        assert_eq!(pc, "250m");
        assert_eq!(gc, "250m");
    }
}
