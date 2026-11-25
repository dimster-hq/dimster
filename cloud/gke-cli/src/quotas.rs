//! GCP project and regional quota discovery, display, and pre-deploy validation.
//!
//! Queries Compute Engine regional/project quotas, Service Usage per-family &
//! Hyperdisk quotas, and live regional VM/disk usage in parallel using only the
//! IAM permissions `gke-setup` already requires (`roles/compute.viewer`,
//! `roles/compute.storageAdmin`, and default-enabled `serviceusage.googleapis.com`).

use std::collections::BTreeMap;
use std::process::Command;

use anyhow::Result;
use cloud_common::sizing::parse_storage_gib;

use crate::answers::Answers;
use crate::instances::{InstanceTypes, LOCAL_SSD_SIZE_GIB, LocalSsdMode};
use crate::steps::NODE_BOOT_DISK_SIZE_GB_U32;

/// Default PVCs deployed by Dimster on the cluster's default StorageClass:
/// `prometheus-data` (50Gi) on `role=infra` and `benchmark-results` (4Gi) on the coordinator pool.
pub const PROMETHEUS_PVC_GIB: u64 = 50;
pub const RESULTS_PVC_GIB: u64 = 4;
pub const INFRA_PVCS_TOTAL_GIB: u64 = PROMETHEUS_PVC_GIB + RESULTS_PVC_GIB;

/// Catalog VM families displayed in the regional quota summary.
const DISPLAY_VM_FAMILIES: [&str; 9] = ["E2", "N2", "N2D", "N4", "C3", "C3D", "C4", "C4A", "C4D"];

/// Limits at or above this threshold represent GCP "unlimited" sentinels (e.g. `i64::MAX` as f64).
const UNLIMITED_SENTINEL: f64 = 1.0e15;

/// Single numeric quota limit with optional current usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaEntry {
    pub limit: u64,
    pub usage: Option<u64>,
}

impl QuotaEntry {
    pub fn available(self) -> Option<u64> {
        self.usage.map(|u| self.limit.saturating_sub(u))
    }

    fn format_summary(self, unit: &str) -> String {
        let suffix = if unit.is_empty() {
            String::new()
        } else {
            format!(" {}", unit)
        };
        match self.usage {
            Some(u) => format!(
                "{} / {}{} ({} free)",
                u,
                self.limit,
                suffix,
                self.limit.saturating_sub(u)
            ),
            None => format!("{}{} limit", self.limit, suffix),
        }
    }
}

/// Snapshot of project-wide and regional GCP quotas relevant to GKE Dimster clusters.
#[derive(Debug, Clone, Default)]
pub struct RegionQuotas {
    pub project: String,
    pub region: String,
    /// Project-wide `CPUS_ALL_REGIONS` cap across all VM families.
    pub cpus_all_regions: Option<QuotaEntry>,
    /// Legacy regional `CPUS` pool (covers E2/N1 and general compute).
    pub regional_cpus: Option<QuotaEntry>,
    pub instances: Option<QuotaEntry>,
    /// Regional external IPs (`IN_USE_ADDRESSES` — 1 per public GKE node).
    pub in_use_addresses: Option<QuotaEntry>,
    /// Per-family regional vCPU caps (uppercase family key: `E2`, `N4`, `C3`, `C4`, ...).
    pub family_cpus: BTreeMap<String, QuotaEntry>,
    pub hyperdisk_balanced_gb: Option<QuotaEntry>,
    pub hyperdisk_extreme_gb: Option<QuotaEntry>,
    pub hyperdisk_throughput_gb: Option<QuotaEntry>,
    /// Regional `SSD_TOTAL_GB` (shared by `pd-ssd` and `pd-balanced`).
    pub ssd_total_gb: Option<QuotaEntry>,
    /// Regional Local SSD cap in GB keyed by uppercase VM family (`C3`, `N2`, ...).
    pub local_ssd_gb_by_family: BTreeMap<String, u64>,
    /// Non-fatal warnings collected if any quota probe failed.
    pub warnings: Vec<String>,
}

impl RegionQuotas {
    pub fn has_any_data(&self) -> bool {
        self.cpus_all_regions.is_some()
            || self.instances.is_some()
            || self.in_use_addresses.is_some()
            || !self.family_cpus.is_empty()
            || self.hyperdisk_balanced_gb.is_some()
            || self.ssd_total_gb.is_some()
    }
}

/// Result status for a single planned resource against GCP quotas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaStatus {
    /// Fits within currently free quota.
    Ok,
    /// Fits within project/region limit, but exceeds currently free headroom (existing resources running).
    ExceedsAvailable,
    /// Exceeds the project/region limit outright.
    ExceedsLimit,
}

/// Single line in the pre-confirmation quota assessment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaCheckItem {
    pub label: String,
    pub needed: u64,
    pub unit: &'static str,
    pub quota: QuotaEntry,
    pub status: QuotaStatus,
    pub note: Option<String>,
}

/// Query project-wide and regional GCP quotas in parallel (best-effort, read-only).
///
/// Mints a single OAuth access token via `gcloud auth print-access-token`, then
/// fetches Compute Engine and Service Usage REST endpoints concurrently so the
/// entire snapshot completes in ~3-4 seconds instead of spawning many `gcloud`
/// Python processes.
pub fn fetch_region_quotas(project: &str, region: &str) -> RegionQuotas {
    let mut result = RegionQuotas {
        project: project.to_string(),
        region: region.to_string(),
        ..Default::default()
    };

    let token = match mint_access_token() {
        Ok(t) => t,
        Err(e) => {
            result
                .warnings
                .push(format!("Could not mint gcloud access token: {}", e));
            return result;
        }
    };

    let compute_base = format!("https://compute.googleapis.com/compute/v1/projects/{}", project);
    let su_base = format!(
        "https://serviceusage.googleapis.com/v1beta1/projects/{}/services/compute.googleapis.com/consumerQuotaMetrics",
        project
    );

    let region_url = format!("{}/regions/{}?fields=quotas", compute_base, region);
    let proj_url = format!("{}?fields=quotas", compute_base);
    let inst_url = format!(
        "{}/aggregated/instances?fields=items.*.instances(machineType,status)",
        compute_base
    );
    let disk_url = format!(
        "{}/aggregated/disks?fields=items.*.disks(type,sizeGb)",
        compute_base
    );
    let su_cpus_url = format!("{}/compute.googleapis.com%2Fcpus_per_vm_family?view=FULL", su_base);
    let su_hdb_url = format!(
        "{}/compute.googleapis.com%2Fhyperdisk_balanced_total_storage?view=FULL",
        su_base
    );
    let su_hde_url = format!(
        "{}/compute.googleapis.com%2Fhyperdisk_extreme_total_storage?view=FULL",
        su_base
    );
    let su_hdt_url = format!(
        "{}/compute.googleapis.com%2Fhyperdisk_throughput_total_storage?view=FULL",
        su_base
    );
    let su_lssd_url = format!(
        "{}/compute.googleapis.com%2Flocal_ssd_total_storage_per_vm_family?view=FULL",
        su_base
    );

    let (
        region_res,
        proj_res,
        inst_res,
        disk_res,
        su_cpus_res,
        su_hdb_res,
        su_hde_res,
        su_hdt_res,
        su_lssd_res,
    ) = std::thread::scope(|s| {
        let t_region = s.spawn(|| fetch_gcp_json(&token, project, &region_url));
        let t_proj = s.spawn(|| fetch_gcp_json(&token, project, &proj_url));
        let t_inst = s.spawn(|| fetch_gcp_json(&token, project, &inst_url));
        let t_disk = s.spawn(|| fetch_gcp_json(&token, project, &disk_url));
        let t_su_cpus = s.spawn(|| fetch_gcp_json(&token, project, &su_cpus_url));
        let t_su_hdb = s.spawn(|| fetch_gcp_json(&token, project, &su_hdb_url));
        let t_su_hde = s.spawn(|| fetch_gcp_json(&token, project, &su_hde_url));
        let t_su_hdt = s.spawn(|| fetch_gcp_json(&token, project, &su_hdt_url));
        let t_su_lssd = s.spawn(|| fetch_gcp_json(&token, project, &su_lssd_url));

        (
            t_region.join().unwrap_or(Err("region quota thread panicked".into())),
            t_proj.join().unwrap_or(Err("project quota thread panicked".into())),
            t_inst.join().unwrap_or(Err("instances thread panicked".into())),
            t_disk.join().unwrap_or(Err("disks thread panicked".into())),
            t_su_cpus.join().unwrap_or(Err("vm_family cpus thread panicked".into())),
            t_su_hdb.join().unwrap_or(Err("hdb quota thread panicked".into())),
            t_su_hde.join().unwrap_or(Err("hde quota thread panicked".into())),
            t_su_hdt.join().unwrap_or(Err("hdt quota thread panicked".into())),
            t_su_lssd.join().unwrap_or(Err("lssd quota thread panicked".into())),
        )
    });

    // Live regional VM and disk usage (covers newer families & Hyperdisk that regions describe omits).
    let live_vcpu_usage = match inst_res {
        Ok(ref json) => parse_aggregated_instance_vcpu_usage(json, region).unwrap_or_default(),
        Err(e) => {
            result
                .warnings
                .push(format!("Could not list regional VMs for live usage: {}", e));
            BTreeMap::new()
        }
    };
    let live_disk_usage = match disk_res {
        Ok(ref json) => parse_aggregated_disk_gb_usage(json, region).unwrap_or_default(),
        Err(e) => {
            result
                .warnings
                .push(format!("Could not list regional disks for live usage: {}", e));
            BTreeMap::new()
        }
    };

    // 1. Project-wide quotas (`CPUS_ALL_REGIONS`).
    match proj_res {
        Ok(ref json) => {
            if let Ok(map) = parse_compute_quotas_json(json)
                && let Some(&(limit, usage)) = map.get("CPUS_ALL_REGIONS")
            {
                result.cpus_all_regions = Some(QuotaEntry {
                    limit,
                    usage: Some(usage),
                });
            }
        }
        Err(e) => result
            .warnings
            .push(format!("Could not query project quotas: {}", e)),
    }

    // 2. Regional Compute Engine quotas (legacy CPU families, SSD_TOTAL_GB, INSTANCES, IN_USE_ADDRESSES).
    match region_res {
        Ok(ref json) => {
            if let Ok(map) = parse_compute_quotas_json(json) {
                if let Some(&(limit, usage)) = map.get("CPUS") {
                    result.regional_cpus = Some(QuotaEntry {
                        limit,
                        usage: Some(usage),
                    });
                }
                if let Some(&(limit, usage)) = map.get("INSTANCES") {
                    result.instances = Some(QuotaEntry {
                        limit,
                        usage: Some(usage),
                    });
                }
                if let Some(&(limit, usage)) = map.get("IN_USE_ADDRESSES") {
                    result.in_use_addresses = Some(QuotaEntry {
                        limit,
                        usage: Some(usage),
                    });
                }
                if let Some(&(limit, usage)) = map.get("SSD_TOTAL_GB") {
                    result.ssd_total_gb = Some(QuotaEntry {
                        limit,
                        usage: Some(usage),
                    });
                }
                for (metric, fam) in [
                    ("E2_CPUS", "E2"),
                    ("N2_CPUS", "N2"),
                    ("N2D_CPUS", "N2D"),
                    ("C3_CPUS", "C3"),
                ] {
                    if let Some(&(limit, api_usage)) = map.get(metric) {
                        // E2 usage is sometimes accounted under CPUS rather than E2_CPUS.
                        let live = live_vcpu_usage.get(fam).copied().unwrap_or(0);
                        result.family_cpus.insert(
                            fam.to_string(),
                            QuotaEntry {
                                limit,
                                usage: Some(api_usage.max(live)),
                            },
                        );
                    }
                }
            }
        }
        Err(e) => result
            .warnings
            .push(format!("Could not query regional Compute quotas: {}", e)),
    }

    // 3. Service Usage quotas (N4/C3D/C4/C4A/C4D CPUs, Hyperdisk storage, Local SSD per family).
    let mut su_err: Option<String> = None;
    match su_cpus_res {
        Ok(ref json) => {
            if let Ok(fams) = parse_service_usage_family_limits(json, region) {
                for (fam, limit) in fams {
                    let usage = Some(live_vcpu_usage.get(&fam).copied().unwrap_or(0));
                    result
                        .family_cpus
                        .entry(fam)
                        .or_insert(QuotaEntry { limit, usage });
                }
            }
        }
        Err(e) => su_err = Some(e),
    }
    if let Ok(ref json) = su_hdb_res
        && let Ok(Some(limit)) = parse_service_usage_regional_limit(json, region)
    {
        let usage = Some(live_disk_usage.get("hyperdisk-balanced").copied().unwrap_or(0));
        result.hyperdisk_balanced_gb = Some(QuotaEntry { limit, usage });
    }
    if let Ok(ref json) = su_hde_res
        && let Ok(Some(limit)) = parse_service_usage_regional_limit(json, region)
    {
        let usage = Some(live_disk_usage.get("hyperdisk-extreme").copied().unwrap_or(0));
        result.hyperdisk_extreme_gb = Some(QuotaEntry { limit, usage });
    }
    if let Ok(ref json) = su_hdt_res
        && let Ok(Some(limit)) = parse_service_usage_regional_limit(json, region)
    {
        let usage = Some(
            live_disk_usage
                .get("hyperdisk-throughput")
                .copied()
                .unwrap_or(0),
        );
        result.hyperdisk_throughput_gb = Some(QuotaEntry { limit, usage });
    }
    if let Ok(ref json) = su_lssd_res
        && let Ok(map) = parse_service_usage_family_limits(json, region)
    {
        result.local_ssd_gb_by_family = map;
    }
    if let Some(e) = su_err {
        result
            .warnings
            .push(format!("Could not query Service Usage Hyperdisk/Gen-4 quotas: {}", e));
    }

    result
}

fn mint_access_token() -> std::result::Result<String, String> {
    let out = Command::new("gcloud")
        .args(["auth", "print-access-token"])
        .output()
        .map_err(|e| e.to_string())?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let first = stderr
            .lines()
            .find(|l| l.contains("ERROR") || !l.trim().is_empty())
            .unwrap_or("gcloud auth print-access-token failed");
        return Err(first.trim().to_string());
    }
    let token = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if token.is_empty() {
        return Err("empty OAuth access token".into());
    }
    Ok(token)
}

/// Perform an authenticated GET against a GCP REST endpoint and surface any
/// top-level JSON `"error"` object as `Err`.
fn fetch_gcp_json(
    token: &str,
    project: &str,
    url: &str,
) -> std::result::Result<String, String> {
    let out = Command::new("curl")
        .args([
            "-sS",
            "--max-time",
            "10",
            "-H",
            &format!("Authorization: Bearer {}", token),
            "-H",
            &format!("X-Goog-User-Project: {}", project),
            url,
        ])
        .output()
        .map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    let body = String::from_utf8_lossy(&out.stdout).to_string();
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(&body)
        && let Some(err) = val.get("error")
    {
        let status = err.get("status").and_then(|s| s.as_str()).unwrap_or("API_ERROR");
        let msg = err
            .get("message")
            .and_then(|m| m.as_str())
            .and_then(|m| m.split('.').next())
            .unwrap_or("request rejected");
        return Err(format!("{}: {}", status, msg.trim()));
    }
    Ok(body)
}

/// Extract uppercase VM family prefix from a GCP machine type (e.g. `c4a-standard-8-lssd` -> `C4A`).
pub fn machine_family(machine_type: &str) -> String {
    machine_type
        .split('-')
        .next()
        .unwrap_or(machine_type)
        .to_ascii_uppercase()
}

/// Infer vCPU count from a standard GCP machine type name when not present in `instance-types.yaml`
/// (e.g. `c4-standard-16-lssd` -> `16`, `n4-highmem-8` -> `8`).
pub fn infer_vcpus_from_name(machine_type: &str) -> Option<u32> {
    let trimmed = machine_type.strip_suffix("-lssd").unwrap_or(machine_type);
    trimmed.rsplit('-').next()?.parse::<u32>().ok()
}

/// Parse `gcloud compute regions describe` or `project-info describe` JSON (`{"quotas":[...]}`)
/// into `metric -> (limit, usage)`, filtering out GCP's unlimited sentinel values.
pub fn parse_compute_quotas_json(json: &str) -> Result<BTreeMap<String, (u64, u64)>> {
    let val: serde_json::Value = serde_json::from_str(json)?;
    let mut out = BTreeMap::new();
    let Some(arr) = val.get("quotas").and_then(|q| q.as_array()) else {
        return Ok(out);
    };
    for item in arr {
        let Some(metric) = item.get("metric").and_then(|m| m.as_str()) else {
            continue;
        };
        let Some(limit_f) = item.get("limit").and_then(|l| l.as_f64()) else {
            continue;
        };
        if limit_f < 0.0 || limit_f >= UNLIMITED_SENTINEL {
            continue;
        }
        let usage_f = item.get("usage").and_then(|u| u.as_f64()).unwrap_or(0.0);
        out.insert(
            metric.to_string(),
            (limit_f.round() as u64, usage_f.max(0.0).round() as u64),
        );
    }
    Ok(out)
}

/// Parse a regional Service Usage quota metric (`1/{project}/{region}`) for `region`.
/// Returns `Ok(None)` when the quota is unlimited (`-1`) or absent.
pub fn parse_service_usage_regional_limit(json: &str, region: &str) -> Result<Option<u64>> {
    let val: serde_json::Value = serde_json::from_str(json)?;
    let Some(limits) = val.get("consumerQuotaLimits").and_then(|l| l.as_array()) else {
        return Ok(None);
    };
    for lim in limits {
        let unit = lim.get("unit").and_then(|u| u.as_str()).unwrap_or("");
        if !unit.contains("{region}") || unit.contains("{vm_family}") {
            continue;
        }
        let Some(buckets) = lim.get("quotaBuckets").and_then(|b| b.as_array()) else {
            continue;
        };
        let mut fallback_default: Option<i64> = None;
        for b in buckets {
            let eff = parse_bucket_limit(b);
            let dims = b.get("dimensions").and_then(|d| d.as_object());
            match dims.and_then(|d| d.get("region")).and_then(|r| r.as_str()) {
                Some(r) if r == region => {
                    return Ok(eff.filter(|&v| v >= 0).map(|v| v as u64));
                }
                None if dims.is_none_or(|d| d.is_empty()) => {
                    fallback_default = eff;
                }
                _ => {}
            }
        }
        if let Some(v) = fallback_default {
            return Ok((v >= 0).then_some(v as u64));
        }
    }
    Ok(None)
}

/// Parse a Service Usage per-VM-family regional quota metric (`1/{project}/{region}/{vm_family}`).
/// Honors GCP bucket precedence: exact `{region, vm_family}` overrides generic `{vm_family}`.
pub fn parse_service_usage_family_limits(
    json: &str,
    region: &str,
) -> Result<BTreeMap<String, u64>> {
    let val: serde_json::Value = serde_json::from_str(json)?;
    let mut defaults: BTreeMap<String, Option<u64>> = BTreeMap::new();
    let mut regional: BTreeMap<String, Option<u64>> = BTreeMap::new();

    let Some(limits) = val.get("consumerQuotaLimits").and_then(|l| l.as_array()) else {
        return Ok(BTreeMap::new());
    };
    for lim in limits {
        let unit = lim.get("unit").and_then(|u| u.as_str()).unwrap_or("");
        if !unit.contains("{region}") || !unit.contains("{vm_family}") {
            continue;
        }
        let Some(buckets) = lim.get("quotaBuckets").and_then(|b| b.as_array()) else {
            continue;
        };
        for b in buckets {
            let Some(dims) = b.get("dimensions").and_then(|d| d.as_object()) else {
                continue;
            };
            let Some(fam) = dims.get("vm_family").and_then(|f| f.as_str()) else {
                continue;
            };
            let Some(eff) = parse_bucket_limit(b) else {
                continue;
            };
            let limit_opt = (eff >= 0).then_some(eff as u64);
            match dims.get("region").and_then(|r| r.as_str()) {
                Some(r) if r == region => {
                    regional.insert(fam.to_ascii_uppercase(), limit_opt);
                }
                None => {
                    defaults.insert(fam.to_ascii_uppercase(), limit_opt);
                }
                _ => {}
            }
        }
    }

    let mut merged = BTreeMap::new();
    for (fam, lim) in defaults {
        if let Some(v) = lim {
            merged.insert(fam, v);
        }
    }
    for (fam, lim) in regional {
        match lim {
            Some(v) => {
                merged.insert(fam, v);
            }
            None => {
                merged.remove(&fam);
            }
        }
    }
    Ok(merged)
}

fn parse_bucket_limit(bucket: &serde_json::Value) -> Option<i64> {
    let raw = bucket
        .get("effectiveLimit")
        .or_else(|| bucket.get("defaultLimit"))?;
    if let Some(s) = raw.as_str() {
        s.parse::<i64>().ok()
    } else {
        raw.as_i64()
    }
}

fn scope_matches_region(scope: &str, region: &str) -> bool {
    scope == format!("regions/{}", region) || scope.starts_with(&format!("zones/{}-", region))
}

/// Sum currently active vCPUs by uppercase VM family in `region` from Compute
/// Engine `aggregated/instances` REST JSON.
pub fn parse_aggregated_instance_vcpu_usage(
    json: &str,
    region: &str,
) -> Result<BTreeMap<String, u64>> {
    if json.trim().is_empty() {
        return Ok(BTreeMap::new());
    }
    let val: serde_json::Value = serde_json::from_str(json)?;
    let mut by_family = BTreeMap::new();
    let Some(items) = val.get("items").and_then(|i| i.as_object()) else {
        return Ok(by_family);
    };
    for (scope, entry) in items {
        if !scope_matches_region(scope, region) {
            continue;
        }
        let Some(instances) = entry.get("instances").and_then(|a| a.as_array()) else {
            continue;
        };
        for inst in instances {
            let status = inst.get("status").and_then(|s| s.as_str()).unwrap_or("");
            if status == "TERMINATED" {
                continue;
            }
            let Some(mt_url) = inst.get("machineType").and_then(|m| m.as_str()) else {
                continue;
            };
            let mt = mt_url.rsplit('/').next().unwrap_or(mt_url);
            if let Some(vcpus) = infer_vcpus_from_name(mt) {
                *by_family.entry(machine_family(mt)).or_insert(0) += vcpus as u64;
            }
        }
    }
    Ok(by_family)
}

/// Sum currently provisioned disk capacity (GB) in `region` by disk type basename
/// from Compute Engine `aggregated/disks` REST JSON.
pub fn parse_aggregated_disk_gb_usage(json: &str, region: &str) -> Result<BTreeMap<String, u64>> {
    if json.trim().is_empty() {
        return Ok(BTreeMap::new());
    }
    let val: serde_json::Value = serde_json::from_str(json)?;
    let mut by_type = BTreeMap::new();
    let Some(items) = val.get("items").and_then(|i| i.as_object()) else {
        return Ok(by_type);
    };
    for (scope, entry) in items {
        if !scope_matches_region(scope, region) {
            continue;
        }
        let Some(disks) = entry.get("disks").and_then(|a| a.as_array()) else {
            continue;
        };
        for disk in disks {
            let Some(type_url) = disk.get("type").and_then(|t| t.as_str()) else {
                continue;
            };
            let disk_type = type_url.rsplit('/').next().unwrap_or(type_url);
            let size_gb = disk
                .get("sizeGb")
                .and_then(|s| {
                    s.as_str()
                        .and_then(|v| v.parse::<u64>().ok())
                        .or_else(|| s.as_u64())
                })
                .unwrap_or(0);
            *by_type.entry(disk_type.to_string()).or_insert(0) += size_gb;
        }
    }
    Ok(by_type)
}

/// Print a concise overview of available project & regional quotas before machine/disk prompts.
pub fn print_available_quotas(quotas: &RegionQuotas) {
    println!("\n=== GCP Quotas ({} / {}) ===\n", quotas.project, quotas.region);

    if !quotas.has_any_data() {
        println!("  ⚠ Could not query GCP quotas — continuing without quota hints.");
        for w in &quotas.warnings {
            println!("    ({})", w);
        }
        return;
    }

    if let Some(q) = quotas.cpus_all_regions {
        println!(
            "  Project vCPUs (all regions): {}",
            q.format_summary("vCPUs")
        );
    }
    if let Some(q) = quotas.instances {
        println!("  Regional VM instances:       {}", q.format_summary("VMs"));
    }
    if let Some(q) = quotas.in_use_addresses {
        println!(
            "  Regional external IPs:       {}  (1 shared via Cloud NAT, or 1/node if public)",
            q.format_summary("IPs")
        );
    }

    let mut shown_families = Vec::new();
    for fam in DISPLAY_VM_FAMILIES {
        if let Some(&q) = quotas.family_cpus.get(fam) {
            shown_families.push((fam, q));
        }
    }
    if !shown_families.is_empty() {
        println!("\n  vCPU quotas by machine family ({}):", quotas.region);
        for (fam, q) in shown_families {
            println!("    {:<5} {}", fam, q.format_summary("vCPUs"));
        }
    }

    let has_storage = quotas.hyperdisk_balanced_gb.is_some()
        || quotas.ssd_total_gb.is_some()
        || quotas.hyperdisk_extreme_gb.is_some()
        || quotas.hyperdisk_throughput_gb.is_some()
        || !quotas.local_ssd_gb_by_family.is_empty();
    if has_storage {
        println!("\n  Disk quotas ({}):", quotas.region);
        if let Some(q) = quotas.hyperdisk_balanced_gb {
            println!(
                "    Hyperdisk Balanced (HDB):  {}  (N4/C4/C4A/C4D boot + Kafka data)",
                q.format_summary("GB")
            );
        }
        if let Some(q) = quotas.ssd_total_gb {
            println!(
                "    SSD / Balanced PD (SSD):   {}  (E2/N2/C3 boot + {} GB PD infra PVCs)",
                q.format_summary("GB"),
                INFRA_PVCS_TOTAL_GIB
            );
        }
        if let Some(q) = quotas.hyperdisk_extreme_gb {
            println!("    Hyperdisk Extreme (HDE):   {}", q.format_summary("GB"));
        }
        if let Some(q) = quotas.hyperdisk_throughput_gb {
            println!("    Hyperdisk Throughput:      {}", q.format_summary("GB"));
        }
        if let Some(&lssd_gb) = quotas
            .local_ssd_gb_by_family
            .get("C3")
            .or_else(|| quotas.local_ssd_gb_by_family.get("C4"))
            .or_else(|| quotas.local_ssd_gb_by_family.get("N2"))
        {
            println!(
                "    Local NVMe SSD:            {} GB limit per VM family ({}x 375 GiB)",
                lssd_gb,
                lssd_gb / LOCAL_SSD_SIZE_GIB as u64
            );
        }
    }

    println!(
        "\n  Note: GKE node boot disks use {} GB each (Hyperdisk on N4/C4/C4A/C4D; PD on E2/N2/C3).",
        NODE_BOOT_DISK_SIZE_GB_U32
    );
    if !quotas.warnings.is_empty() {
        for w in &quotas.warnings {
            println!("  ⚠ {}", w);
        }
    }
}

/// Evaluate a finalized `Answers` configuration against queried GCP quotas.
pub fn evaluate_plan_against_quotas(
    answers: &Answers,
    instance_types: &InstanceTypes,
    quotas: &RegionQuotas,
) -> Vec<QuotaCheckItem> {
    let mut checks = Vec::new();
    if !quotas.has_any_data() {
        return checks;
    }

    let vcpus_for = |mt: &str| -> u64 {
        instance_types
            .lookup(mt)
            .map(|s| s.vcpu as u64)
            .or_else(|| infer_vcpus_from_name(mt).map(|v| v as u64))
            .unwrap_or(0)
    };

    let kafka_nodes = if answers.is_external() {
        0u64
    } else {
        answers.kafka.count as u64
    };
    let worker_nodes = answers.workers.count as u64;
    let worker_max_nodes = answers.workers.max.max(answers.workers.count) as u64;
    let infra_nodes = 1u64;

    let total_nodes = kafka_nodes + worker_nodes + infra_nodes;
    let max_nodes = kafka_nodes + worker_max_nodes + infra_nodes;

    let kafka_vcpus = kafka_nodes * vcpus_for(&answers.kafka.instance_type);
    let worker_vcpus = worker_nodes * vcpus_for(&answers.workers.instance_type);
    let worker_max_vcpus = worker_max_nodes * vcpus_for(&answers.workers.instance_type);
    let infra_vcpus = infra_nodes * vcpus_for(&answers.coordinator.infra_instance_type);

    let total_vcpus = kafka_vcpus + worker_vcpus + infra_vcpus;
    let max_vcpus = kafka_vcpus + worker_max_vcpus + infra_vcpus;

    // 1. Project-wide CPUs
    if let Some(q) = quotas.cpus_all_regions {
        let note = (max_vcpus > total_vcpus && max_vcpus > q.limit).then(|| {
            format!(
                "autoscaling workers.max={} would need {} vCPUs (> {} limit)",
                answers.workers.max, max_vcpus, q.limit
            )
        });
        checks.push(build_check("Project vCPUs (CPUS_ALL_REGIONS)", total_vcpus, "vCPUs", q, note));
    }

    // 2. Per-family vCPUs
    let mut planned_by_family: BTreeMap<String, u64> = BTreeMap::new();
    let mut max_by_family: BTreeMap<String, u64> = BTreeMap::new();
    if kafka_nodes > 0 {
        let fam = machine_family(&answers.kafka.instance_type);
        *planned_by_family.entry(fam.clone()).or_insert(0) += kafka_vcpus;
        *max_by_family.entry(fam).or_insert(0) += kafka_vcpus;
    }
    {
        let fam = machine_family(&answers.workers.instance_type);
        *planned_by_family.entry(fam.clone()).or_insert(0) += worker_vcpus;
        *max_by_family.entry(fam).or_insert(0) += worker_max_vcpus;
    }
    {
        let fam = machine_family(&answers.coordinator.infra_instance_type);
        *planned_by_family.entry(fam.clone()).or_insert(0) += infra_vcpus;
        *max_by_family.entry(fam).or_insert(0) += infra_vcpus;
    }

    for (fam, needed) in &planned_by_family {
        if let Some(&q) = quotas.family_cpus.get(fam) {
            let max_need = max_by_family.get(fam).copied().unwrap_or(*needed);
            let note = (max_need > *needed && max_need > q.limit).then(|| {
                format!(
                    "autoscaling workers.max={} would need {} {} vCPUs (> {} limit)",
                    answers.workers.max, max_need, fam, q.limit
                )
            });
            checks.push(build_check(&format!("{} vCPUs", fam), *needed, "vCPUs", q, note));
        }
    }

    // 3. Nodes & External IPs
    if let Some(q) = quotas.in_use_addresses {
        if answers.gcp.private_nodes {
            // Private nodes share 1 regional Cloud NAT external IP regardless of node count.
            checks.push(build_check(
                "External IPs (Cloud NAT)",
                1,
                "IPs",
                q,
                None,
            ));
        } else {
            let note = (max_nodes > total_nodes && max_nodes > q.limit).then(|| {
                format!(
                    "autoscaling workers.max={} would need {} IPs (> {} limit)",
                    answers.workers.max, max_nodes, q.limit
                )
            });
            checks.push(build_check("External IPs (IN_USE_ADDRESSES)", total_nodes, "IPs", q, note));
        }
    }
    if let Some(q) = quotas.instances {
        checks.push(build_check("VM Instances (INSTANCES)", total_nodes, "VMs", q, None));
    }

    // 4. Storage quotas (50 GB boot disks + Prometheus 50Gi + results 4Gi + Kafka data disks)
    let boot_gb = NODE_BOOT_DISK_SIZE_GB_U32 as u64;
    let mut hdb_needed = 0u64;
    let mut hde_needed = 0u64;
    let mut hdt_needed = 0u64;
    let mut ssd_needed = 0u64;
    let mut hdb_max_extra = 0u64;
    let mut ssd_max_extra = 0u64;

    // Prometheus (50Gi) lands on infra; results (4Gi) lands on coordinator's pool.
    // On Hyperdisk-only pools `gke-setup` switches default SC to `dynamic-rwo` (Hyperdisk Balanced).
    if instance_types.boot_disk_for(&answers.coordinator.infra_instance_type) == "hyperdisk-balanced"
    {
        hdb_needed += PROMETHEUS_PVC_GIB;
    } else {
        ssd_needed += PROMETHEUS_PVC_GIB;
    }
    let coord_machine = if answers.coordinator.node_label == "role=worker" {
        &answers.workers.instance_type
    } else {
        &answers.coordinator.infra_instance_type
    };
    if instance_types.boot_disk_for(coord_machine) == "hyperdisk-balanced" {
        hdb_needed += RESULTS_PVC_GIB;
    } else {
        ssd_needed += RESULTS_PVC_GIB;
    }

    let mut add_boot = |mt: &str, count: u64, extra_max: u64| {
        if instance_types.boot_disk_for(mt) == "hyperdisk-balanced" {
            hdb_needed += count * boot_gb;
            hdb_max_extra += extra_max * boot_gb;
        } else {
            ssd_needed += count * boot_gb;
            ssd_max_extra += extra_max * boot_gb;
        }
    };
    if kafka_nodes > 0 {
        add_boot(&answers.kafka.instance_type, kafka_nodes, 0);
    }
    add_boot(
        &answers.workers.instance_type,
        worker_nodes,
        worker_max_nodes.saturating_sub(worker_nodes),
    );
    add_boot(&answers.coordinator.infra_instance_type, infra_nodes, 0);

    if kafka_nodes > 0 {
        let data_gib = parse_storage_gib(&answers.storage.size).unwrap_or(0) as u64;
        match answers.storage.storage_type.as_str() {
            "hyperdisk-balanced" => hdb_needed += kafka_nodes * data_gib,
            "hyperdisk-extreme" => hde_needed += kafka_nodes * data_gib,
            "hyperdisk-throughput" => hdt_needed += kafka_nodes * data_gib,
            "pd-ssd" | "pd-balanced" => ssd_needed += kafka_nodes * data_gib,
            "local-ssd" => {
                let kafka_spec = instance_types.lookup(&answers.kafka.instance_type);
                let lssd_per_node = match kafka_spec.map(|s| s.local_ssd_mode) {
                    Some(LocalSsdMode::Fixed) => kafka_spec.unwrap().local_ssd_count as u64,
                    _ => answers.storage.local_ssd_count.unwrap_or(1) as u64,
                };
                let lssd_gb = kafka_nodes * lssd_per_node * LOCAL_SSD_SIZE_GIB as u64;
                let fam = machine_family(&answers.kafka.instance_type);
                if let Some(&limit) = quotas.local_ssd_gb_by_family.get(&fam) {
                    checks.push(build_check(
                        &format!("Local SSD ({})", fam),
                        lssd_gb,
                        "GB",
                        QuotaEntry { limit, usage: None },
                        None,
                    ));
                }
            }
            _ => {}
        }
    }

    if hdb_needed > 0
        && let Some(q) = quotas.hyperdisk_balanced_gb
    {
        let max_hdb = hdb_needed + hdb_max_extra;
        let note = (max_hdb > hdb_needed && max_hdb > q.limit).then(|| {
            format!(
                "autoscaling workers.max={} boot disks would reach {} GB (> {} limit)",
                answers.workers.max, max_hdb, q.limit
            )
        });
        checks.push(build_check("Hyperdisk Balanced (HDB_TOTAL_GB)", hdb_needed, "GB", q, note));
    }
    if hde_needed > 0
        && let Some(q) = quotas.hyperdisk_extreme_gb
    {
        checks.push(build_check("Hyperdisk Extreme (HDE_TOTAL_GB)", hde_needed, "GB", q, None));
    }
    if hdt_needed > 0
        && let Some(q) = quotas.hyperdisk_throughput_gb
    {
        checks.push(build_check("Hyperdisk Throughput (HDT_TOTAL_GB)", hdt_needed, "GB", q, None));
    }
    if ssd_needed > 0
        && let Some(q) = quotas.ssd_total_gb
    {
        let max_ssd = ssd_needed + ssd_max_extra;
        let note = (max_ssd > ssd_needed && max_ssd > q.limit).then(|| {
            format!(
                "autoscaling workers.max={} boot disks would reach {} GB (> {} limit)",
                answers.workers.max, max_ssd, q.limit
            )
        });
        checks.push(build_check("SSD / Balanced PD (SSD_TOTAL_GB)", ssd_needed, "GB", q, note));
    }

    checks
}

fn build_check(
    label: &str,
    needed: u64,
    unit: &'static str,
    quota: QuotaEntry,
    note: Option<String>,
) -> QuotaCheckItem {
    let status = if needed > quota.limit {
        QuotaStatus::ExceedsLimit
    } else if quota.available().is_some_and(|avail| needed > avail) {
        QuotaStatus::ExceedsAvailable
    } else {
        QuotaStatus::Ok
    };
    QuotaCheckItem {
        label: label.to_string(),
        needed,
        unit,
        quota,
        status,
        note,
    }
}

/// Print pre-confirmation quota validation results.
pub fn print_quota_assessment(items: &[QuotaCheckItem], quotas: &RegionQuotas) {
    println!("\n=== Quota Check ({}) ===\n", quotas.region);

    if items.is_empty() {
        println!("  ℹ Skipped (GCP quotas could not be queried).");
        return;
    }

    let mut any_exceeds_limit = false;
    let mut any_exceeds_avail = false;

    for item in items {
        let icon = match item.status {
            QuotaStatus::Ok => "✓",
            QuotaStatus::ExceedsAvailable => {
                any_exceeds_avail = true;
                "⚠"
            }
            QuotaStatus::ExceedsLimit => {
                any_exceeds_limit = true;
                "✗"
            }
        };
        let suffix = match item.status {
            QuotaStatus::Ok => String::new(),
            QuotaStatus::ExceedsAvailable => " — exceeds currently free quota (fits within limit)".into(),
            QuotaStatus::ExceedsLimit => " — EXCEEDS LIMIT".into(),
        };
        println!(
            "  {} {:<36} needs {:>4} {:<5} | {}{}",
            icon,
            item.label,
            item.needed,
            item.unit,
            item.quota.format_summary(item.unit),
            suffix
        );
        if let Some(ref note) = item.note {
            println!("      ⚠ {}", note);
        }
    }

    if any_exceeds_limit {
        println!("\n  ✗ One or more resources exceed your project/regional quota limit.");
        println!("    Resize the cluster or request a quota increase in GCP Console before deploying.");
    } else if any_exceeds_avail {
        println!("\n  ⚠ Fits within project limits, but existing resources are currently using quota.");
        println!("    Tear down the running cluster first if deploying a fresh copy in this region.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::answers::*;
    use std::path::Path;

    fn load_catalog() -> InstanceTypes {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("instance-types.yaml");
        InstanceTypes::load(&path).unwrap()
    }

    fn sample_answers() -> Answers {
        Answers {
            env_name: "gke".into(),
            gcp: GcpAnswers {
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
            },
            deploy_mode: "full".into(),
            bootstrap_servers: None,
            kafka_client_version: None,
            cpu_manager_static: false,
            kafka: KafkaAnswers {
                version: "4.2.0".into(),
                instance_type: "n4-standard-2".into(),
                count: 3,
                heap: "-Xms2G -Xmx2G".into(),
                cpu: "1".into(),
                memory: "5Gi".into(),
                tier1_networking: false,
                broker_config: None,
                extra_config: None,
            },
            storage: StorageAnswers {
                storage_type: "hyperdisk-balanced".into(),
                size: "100Gi".into(),
                performance: Some("basic".into()),
                iops: None,
                throughput_mibs: None,
                local_ssd_count: None,
            },
            workers: WorkerAnswers {
                instance_type: "c3-standard-4".into(),
                count: 1,
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
            monitoring: MonitoringAnswers {
                prometheus_cpu: "250m".into(),
                prometheus_memory: "1843Mi".into(),
                grafana_cpu: "250m".into(),
                grafana_memory: "1228Mi".into(),
            },
        }
    }

    #[test]
    fn parses_compute_quotas_and_skips_unlimited_sentinels() {
        let json = r#"{
            "quotas": [
                {"metric": "C3_CPUS", "limit": 8.0, "usage": 4.0},
                {"metric": "SSD_TOTAL_GB", "limit": 250.0, "usage": 151.0},
                {"metric": "LOCAL_SSD_TOTAL_GB", "limit": 9.223372036854776e+18, "usage": 0.0}
            ]
        }"#;
        let map = parse_compute_quotas_json(json).unwrap();
        assert_eq!(map.get("C3_CPUS"), Some(&(8, 4)));
        assert_eq!(map.get("SSD_TOTAL_GB"), Some(&(250, 151)));
        assert!(!map.contains_key("LOCAL_SSD_TOTAL_GB"));
    }

    #[test]
    fn parses_service_usage_regional_and_family_buckets_with_precedence() {
        let hdb_json = r#"{
            "consumerQuotaLimits": [
                {"unit": "1/{project}/{zone}", "quotaBuckets": [{"effectiveLimit": "-1"}]},
                {"unit": "1/{project}/{region}", "quotaBuckets": [
                    {"effectiveLimit": "1000"},
                    {"dimensions": {"region": "europe-west1"}, "effectiveLimit": "500"}
                ]}
            ]
        }"#;
        assert_eq!(
            parse_service_usage_regional_limit(hdb_json, "europe-west1").unwrap(),
            Some(500)
        );
        assert_eq!(
            parse_service_usage_regional_limit(hdb_json, "us-central1").unwrap(),
            Some(1000)
        );

        let fam_json = r#"{
            "consumerQuotaLimits": [{
                "unit": "1/{project}/{region}/{vm_family}",
                "quotaBuckets": [
                    {"dimensions": {"vm_family": "C3"}, "effectiveLimit": "6000"},
                    {"dimensions": {"vm_family": "C4"}, "effectiveLimit": "6000"},
                    {"dimensions": {"region": "europe-west1", "vm_family": "C4"}, "effectiveLimit": "24"},
                    {"dimensions": {"region": "europe-west1", "vm_family": "N4"}, "effectiveLimit": "200"},
                    {"dimensions": {"region": "europe-west1", "vm_family": "H4D"}, "effectiveLimit": "-1"}
                ]
            }]
        }"#;
        let map = parse_service_usage_family_limits(fam_json, "europe-west1").unwrap();
        assert_eq!(map.get("C3"), Some(&6000));
        assert_eq!(map.get("C4"), Some(&24));
        assert_eq!(map.get("N4"), Some(&200));
        assert!(!map.contains_key("H4D"));
    }

    #[test]
    fn parses_live_vm_and_disk_usage() {
        let vms = r#"{
            "items": {
                "zones/europe-west1-b": {
                    "instances": [
                        {"machineType": "https://compute/zones/europe-west1-b/machineTypes/n4-standard-2", "status": "RUNNING"},
                        {"machineType": "https://compute/zones/europe-west1-b/machineTypes/n4-standard-2", "status": "RUNNING"},
                        {"machineType": "https://compute/zones/europe-west1-b/machineTypes/c3-standard-8-lssd", "status": "RUNNING"},
                        {"machineType": "https://compute/zones/europe-west1-b/machineTypes/c4-standard-16", "status": "TERMINATED"}
                    ]
                },
                "zones/us-central1-a": {
                    "instances": [
                        {"machineType": "https://compute/zones/us-central1-a/machineTypes/n4-standard-32", "status": "RUNNING"}
                    ]
                }
            }
        }"#;
        let vcpu_map = parse_aggregated_instance_vcpu_usage(vms, "europe-west1").unwrap();
        assert_eq!(vcpu_map.get("N4"), Some(&4));
        assert_eq!(vcpu_map.get("C3"), Some(&8));
        assert!(!vcpu_map.contains_key("C4"));

        let disks = r#"{
            "items": {
                "zones/europe-west1-b": {
                    "disks": [
                        {"type": "https://compute/zones/europe-west1-b/diskTypes/hyperdisk-balanced", "sizeGb": "50"},
                        {"type": "https://compute/zones/europe-west1-b/diskTypes/hyperdisk-balanced", "sizeGb": "100"},
                        {"type": "https://compute/zones/europe-west1-b/diskTypes/pd-balanced", "sizeGb": "50"}
                    ]
                },
                "zones/us-central1-a": {
                    "disks": [
                        {"type": "https://compute/zones/us-central1-a/diskTypes/hyperdisk-balanced", "sizeGb": "500"}
                    ]
                }
            }
        }"#;
        let disk_map = parse_aggregated_disk_gb_usage(disks, "europe-west1").unwrap();
        assert_eq!(disk_map.get("hyperdisk-balanced"), Some(&150));
        assert_eq!(disk_map.get("pd-balanced"), Some(&50));
    }

    #[test]
    fn evaluates_ok_exceeds_available_and_exceeds_limit_states() {
        let types = load_catalog();
        let mut answers = sample_answers();

        let mut quotas = RegionQuotas {
            project: "demo-proj".into(),
            region: "europe-west1".into(),
            cpus_all_regions: Some(QuotaEntry { limit: 12, usage: Some(0) }),
            instances: Some(QuotaEntry { limit: 24, usage: Some(0) }),
            in_use_addresses: Some(QuotaEntry { limit: 8, usage: Some(5) }),
            hyperdisk_balanced_gb: Some(QuotaEntry { limit: 500, usage: Some(0) }),
            ssd_total_gb: Some(QuotaEntry { limit: 250, usage: Some(0) }),
            ..Default::default()
        };
        quotas.family_cpus.insert("N4".into(), QuotaEntry { limit: 200, usage: Some(0) });
        quotas.family_cpus.insert("C3".into(), QuotaEntry { limit: 8, usage: Some(0) });
        quotas.family_cpus.insert("E2".into(), QuotaEntry { limit: 24, usage: Some(0) });

        // Plan: 3x n4-standard-2 (6 vCPU) + 1x c3-standard-4 (4 vCPU) + 1x e2-standard-2 (2 vCPU) = 12 vCPUs
        // HDB: 3x 50 boot + 3x 100 data = 450 GB <= 500; SSD: 2x 50 boot + 54 infra = 154 GB <= 250
        // Private nodes (default): needs only 1 shared Cloud NAT IP <= 3 free -> Ok
        let items_priv = evaluate_plan_against_quotas(&answers, &types, &quotas);
        let ips_priv = items_priv.iter().find(|i| i.label.contains("Cloud NAT")).unwrap();
        assert_eq!(ips_priv.needed, 1);
        assert_eq!(ips_priv.status, QuotaStatus::Ok);

        // Public nodes: needs 5 IPs, limit 8, only 3 free -> ExceedsAvailable
        answers.gcp.private_nodes = false;
        let items = evaluate_plan_against_quotas(&answers, &types, &quotas);
        let hdb = items.iter().find(|i| i.label.contains("HDB_TOTAL_GB")).unwrap();
        assert_eq!(hdb.needed, 450);
        assert_eq!(hdb.status, QuotaStatus::Ok);

        let ssd = items.iter().find(|i| i.label.contains("SSD_TOTAL_GB")).unwrap();
        assert_eq!(ssd.needed, 154);
        assert_eq!(ssd.status, QuotaStatus::Ok);

        let ips = items.iter().find(|i| i.label.contains("IN_USE_ADDRESSES")).unwrap();
        assert_eq!(ips.needed, 5);
        assert_eq!(ips.status, QuotaStatus::ExceedsAvailable);

        let proj_cpu = items.iter().find(|i| i.label.contains("CPUS_ALL_REGIONS")).unwrap();
        assert_eq!(proj_cpu.needed, 12);
        assert_eq!(proj_cpu.status, QuotaStatus::Ok);
        assert!(proj_cpu.note.as_deref().unwrap().contains("workers.max=4"));

        // Bump Kafka storage to 200Gi -> 3x50 + 3x200 = 750 GB > 500 GB limit -> ExceedsLimit
        answers.storage.size = "200Gi".into();
        let items_over = evaluate_plan_against_quotas(&answers, &types, &quotas);
        let hdb_over = items_over.iter().find(|i| i.label.contains("HDB_TOTAL_GB")).unwrap();
        assert_eq!(hdb_over.needed, 750);
        assert_eq!(hdb_over.status, QuotaStatus::ExceedsLimit);
    }
}
