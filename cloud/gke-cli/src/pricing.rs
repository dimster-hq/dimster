//! On-demand hourly cost estimator for GKE Dimster clusters.
//!
//! Computes an itemized USD/hr estimate (VMs, Kafka disks, node boot disks,
//! infra PVCs, optional `TIER_1` networking, and the GKE control plane fee)
//! from `instance-types.yaml` baseline `us-central1` rates and regional pricing
//! multipliers so users can review spend before confirming deployment.

use cloud_common::sizing::parse_storage_gib;

use crate::answers::Answers;
use crate::instances::{InstanceSpec, InstanceTypes, LOCAL_SSD_SIZE_GIB, LocalSsdMode};
use crate::quotas::{INFRA_STANDARD_RWO_GIB, infer_vcpus_from_name, machine_family};
use crate::steps::NODE_BOOT_DISK_SIZE_GB_U32;
use crate::storage::{HDB_BASE_IOPS, HDB_BASE_THROUGHPUT_MIBS, HDE_MIN_IOPS, HDT_MIN_THROUGHPUT_MIBS};

/// Standard GCP hours-per-month divisor used to convert monthly storage rates to hourly rates.
const HOURS_PER_MONTH: f64 = 730.0;

/// Global flat GKE Standard control-plane management fee (USD/hr per cluster).
pub const GKE_CONTROL_PLANE_USD_PER_HR: f64 = 0.10;

// Baseline `us-central1` on-demand monthly storage rates (USD / unit-month).
const HDB_SPACE_USD_PER_GIB_MO: f64 = 0.08;
const HDB_EXTRA_IOPS_USD_PER_IOPS_MO: f64 = 0.005;
const HDB_EXTRA_TP_USD_PER_MIBS_MO: f64 = 0.04;
const HDE_SPACE_USD_PER_GIB_MO: f64 = 0.125;
const HDE_IOPS_USD_PER_IOPS_MO: f64 = 0.032;
const HDT_SPACE_USD_PER_GIB_MO: f64 = 0.005;
const HDT_TP_USD_PER_MIBS_MO: f64 = 0.25;
const PD_SSD_USD_PER_GIB_MO: f64 = 0.17;
const PD_BALANCED_USD_PER_GIB_MO: f64 = 0.10;
const LOCAL_SSD_USD_PER_GIB_MO: f64 = 0.08;

/// Baseline `us-central1` Tier_1 networking add-on rate (USD / vCPU-hr).
const TIER1_USD_PER_VCPU_HR: f64 = 0.046;

/// Single line item in the pre-confirmation cost breakdown.
#[derive(Debug, Clone)]
pub struct CostLineItem {
    pub label: String,
    pub detail: String,
    pub hourly_usd: f64,
    pub approximated: bool,
}

/// Complete hourly cost estimate for a planned GKE cluster.
#[derive(Debug, Clone)]
pub struct CostBreakdown {
    pub region: String,
    pub regional_multiplier: f64,
    pub region_known: bool,
    pub items: Vec<CostLineItem>,
    pub total_hourly_usd: f64,
    /// Optional upper bound when worker autoscaling (`workers.max > workers.count`) scales up.
    pub max_autoscaled_hourly_usd: Option<f64>,
    pub workers_max: u32,
}

/// Regional on-demand price multiplier relative to `us-central1` (`1.00`).
/// Returns `(multiplier, is_known_region)`.
pub fn regional_price_multiplier(region: &str) -> (f64, bool) {
    match region {
        "us-central1" | "us-east1" | "us-west1" => (1.00, true),
        "us-east4" | "us-east5" | "us-south1" | "us-west4" | "northamerica-northeast1"
        | "northamerica-northeast2" => (1.10, true),
        "us-west2" | "us-west3" => (1.20, true),
        "europe-west1" | "europe-west4" | "europe-north1" | "europe-southwest1" => (1.10, true),
        "europe-west2" | "europe-west3" | "europe-west8" | "europe-west9" | "europe-central2"
        | "europe-west10" | "europe-west12" => (1.20, true),
        "europe-west6" => (1.30, true),
        "asia-east1" | "asia-southeast1" | "asia-south1" | "asia-south2" => (1.12, true),
        "asia-east2" | "asia-northeast1" | "asia-northeast2" | "asia-northeast3"
        | "asia-southeast2" => (1.22, true),
        "australia-southeast1" | "australia-southeast2" => (1.25, true),
        "southamerica-east1" | "southamerica-west1" => (1.35, true),
        r if r.starts_with("me-") || r.starts_with("africa-") => (1.20, true),
        _ => (1.10, false),
    }
}

/// Resolve baseline `us-central1` hourly USD for a machine type, falling back to
/// per-family vCPU estimates if the machine type is not in `instance-types.yaml`.
/// Returns `(usd_per_hr, approximated)`.
fn vm_base_hourly_usd(machine_type: &str, spec: Option<&InstanceSpec>) -> (f64, bool) {
    if let Some(s) = spec
        && let Some(rate) = s.hourly_usd
    {
        return (rate, false);
    }
    let vcpus = spec
        .map(|s| s.vcpu)
        .or_else(|| infer_vcpus_from_name(machine_type))
        .unwrap_or(4) as f64;
    let per_vcpu = match machine_family(machine_type).as_str() {
        "E2" => 0.0335,
        "N2D" => 0.0423,
        "C4A" => 0.0449,
        "N4" | "C3D" => 0.0454,
        "C4D" => 0.0468,
        "N2" => 0.0486,
        "C4" => 0.0494,
        "C3" => 0.0504,
        _ => 0.048,
    };
    (vcpus * per_vcpu, true)
}

fn monthly_to_hourly(monthly_usd: f64, mult: f64) -> f64 {
    (monthly_usd / HOURS_PER_MONTH) * mult
}

/// Compute an itemized hourly on-demand USD estimate for `answers`.
pub fn estimate_cluster_cost(answers: &Answers, instance_types: &InstanceTypes) -> CostBreakdown {
    let (mult, region_known) = regional_price_multiplier(&answers.gcp.region);
    let mut items = Vec::new();
    let mut tier1_hourly = 0.0f64;
    let mut tier1_details = Vec::new();

    // 1. Kafka VMs & Kafka Storage (full deploy mode only)
    let kafka_nodes = if answers.is_external() {
        0
    } else {
        answers.kafka.count
    };
    if kafka_nodes > 0 {
        let kafka_spec = instance_types.lookup(&answers.kafka.instance_type);
        let (base_rate, approx) = vm_base_hourly_usd(&answers.kafka.instance_type, kafka_spec);
        let kafka_vm_hr = base_rate * mult * kafka_nodes as f64;
        let lssd_note = if kafka_spec.is_some_and(|s| s.local_ssd_mode == LocalSsdMode::Fixed) {
            " (incl. bundled Local SSDs)"
        } else {
            ""
        };
        items.push(CostLineItem {
            label: "Kafka VMs".into(),
            detail: format!("{}x {}{}", kafka_nodes, answers.kafka.instance_type, lssd_note),
            hourly_usd: kafka_vm_hr,
            approximated: approx,
        });

        if answers.kafka.tier1_networking {
            let vcpus = kafka_spec
                .map(|s| s.vcpu)
                .or_else(|| infer_vcpus_from_name(&answers.kafka.instance_type))
                .unwrap_or(0);
            let add = (kafka_nodes * vcpus) as f64 * TIER1_USD_PER_VCPU_HR * mult;
            tier1_hourly += add;
            tier1_details.push(format!("Kafka {} vCPUs", kafka_nodes * vcpus));
        }

        let (storage_detail, storage_hr) =
            estimate_kafka_storage_hourly(answers, kafka_spec, kafka_nodes, mult);
        items.push(CostLineItem {
            label: "Kafka storage".into(),
            detail: storage_detail,
            hourly_usd: storage_hr,
            approximated: false,
        });
    }

    // 2. Worker VMs
    let worker_spec = instance_types.lookup(&answers.workers.instance_type);
    let (worker_base, worker_approx) = vm_base_hourly_usd(&answers.workers.instance_type, worker_spec);
    let worker_unit_hr = worker_base * mult;
    let worker_vm_hr = worker_unit_hr * answers.workers.count as f64;
    items.push(CostLineItem {
        label: "Worker VMs".into(),
        detail: format!("{}x {}", answers.workers.count, answers.workers.instance_type),
        hourly_usd: worker_vm_hr,
        approximated: worker_approx,
    });

    let worker_vcpus_each = worker_spec
        .map(|s| s.vcpu)
        .or_else(|| infer_vcpus_from_name(&answers.workers.instance_type))
        .unwrap_or(0);
    let mut extra_worker_tier1_each = 0.0f64;
    if answers.workers.tier1_networking {
        extra_worker_tier1_each = worker_vcpus_each as f64 * TIER1_USD_PER_VCPU_HR * mult;
        tier1_hourly += extra_worker_tier1_each * answers.workers.count as f64;
        tier1_details.push(format!(
            "Workers {} vCPUs",
            answers.workers.count * worker_vcpus_each
        ));
    }

    // 3. Infra VM (default-pool: coordinator + Prometheus + Grafana)
    let infra_spec = instance_types.lookup(&answers.coordinator.infra_instance_type);
    let (infra_base, infra_approx) =
        vm_base_hourly_usd(&answers.coordinator.infra_instance_type, infra_spec);
    items.push(CostLineItem {
        label: "Infra VM".into(),
        detail: format!("1x {} (default-pool)", answers.coordinator.infra_instance_type),
        hourly_usd: infra_base * mult,
        approximated: infra_approx,
    });

    // 4. Node boot disks (50 GB each) + Infra standard-rwo PVCs (51 GiB)
    let boot_gb = NODE_BOOT_DISK_SIZE_GB_U32 as f64;
    let boot_rate = |mt: &str| -> f64 {
        if instance_types.boot_disk_for(mt) == "hyperdisk-balanced" {
            HDB_SPACE_USD_PER_GIB_MO
        } else {
            PD_BALANCED_USD_PER_GIB_MO
        }
    };
    let total_nodes = kafka_nodes + answers.workers.count + 1;
    let boot_monthly = kafka_nodes as f64 * boot_gb * boot_rate(&answers.kafka.instance_type)
        + answers.workers.count as f64 * boot_gb * boot_rate(&answers.workers.instance_type)
        + boot_gb * boot_rate(&answers.coordinator.infra_instance_type);
    let infra_pvc_monthly = INFRA_STANDARD_RWO_GIB as f64 * PD_BALANCED_USD_PER_GIB_MO;
    items.push(CostLineItem {
        label: "Boot & infra disks".into(),
        detail: format!(
            "{}x {} GB boot + {} GiB infra PVCs",
            total_nodes, NODE_BOOT_DISK_SIZE_GB_U32, INFRA_STANDARD_RWO_GIB
        ),
        hourly_usd: monthly_to_hourly(boot_monthly + infra_pvc_monthly, mult),
        approximated: false,
    });

    // 5. Optional Tier_1 networking
    if tier1_hourly > 0.0 {
        items.push(CostLineItem {
            label: "Tier_1 networking".into(),
            detail: tier1_details.join(", "),
            hourly_usd: tier1_hourly,
            approximated: false,
        });
    }

    // 6. GKE control plane management fee (global flat $0.10/hr)
    items.push(CostLineItem {
        label: "GKE control plane".into(),
        detail: "1 zonal cluster ($0/hr if free-tier credit applies)".into(),
        hourly_usd: GKE_CONTROL_PLANE_USD_PER_HR,
        approximated: false,
    });

    let total_hourly_usd: f64 = items.iter().map(|i| i.hourly_usd).sum();

    // Compute autoscaled ceiling when workers.max > workers.count.
    let extra_workers = answers.workers.max.saturating_sub(answers.workers.count);
    let max_autoscaled_hourly_usd = if extra_workers > 0 {
        let extra_boot_hr = monthly_to_hourly(
            extra_workers as f64 * boot_gb * boot_rate(&answers.workers.instance_type),
            mult,
        );
        let extra_hr =
            extra_workers as f64 * (worker_unit_hr + extra_worker_tier1_each) + extra_boot_hr;
        Some(total_hourly_usd + extra_hr)
    } else {
        None
    };

    CostBreakdown {
        region: answers.gcp.region.clone(),
        regional_multiplier: mult,
        region_known,
        items,
        total_hourly_usd,
        max_autoscaled_hourly_usd,
        workers_max: answers.workers.max,
    }
}

fn estimate_kafka_storage_hourly(
    answers: &Answers,
    kafka_spec: Option<&InstanceSpec>,
    kafka_nodes: u32,
    mult: f64,
) -> (String, f64) {
    let n = kafka_nodes as f64;
    let size_gib = parse_storage_gib(&answers.storage.size).unwrap_or(0) as f64;

    match answers.storage.storage_type.as_str() {
        "hyperdisk-balanced" => {
            let space_mo = size_gib * HDB_SPACE_USD_PER_GIB_MO;
            if answers.storage.performance.as_deref() == Some("provisioned") {
                let iops = answers.storage.iops.unwrap_or(HDB_BASE_IOPS);
                let tp = answers
                    .storage
                    .throughput_mibs
                    .unwrap_or(HDB_BASE_THROUGHPUT_MIBS);
                let extra_iops = iops.saturating_sub(HDB_BASE_IOPS) as f64;
                let extra_tp = tp.saturating_sub(HDB_BASE_THROUGHPUT_MIBS) as f64;
                let per_disk_mo = space_mo
                    + extra_iops * HDB_EXTRA_IOPS_USD_PER_IOPS_MO
                    + extra_tp * HDB_EXTRA_TP_USD_PER_MIBS_MO;
                (
                    format!(
                        "{}x {} hyperdisk-balanced ({} IOPS, {} MiB/s)",
                        kafka_nodes, answers.storage.size, iops, tp
                    ),
                    monthly_to_hourly(n * per_disk_mo, mult),
                )
            } else {
                (
                    format!("{}x {} hyperdisk-balanced (baseline)", kafka_nodes, answers.storage.size),
                    monthly_to_hourly(n * space_mo, mult),
                )
            }
        }
        "hyperdisk-extreme" => {
            let iops = answers.storage.iops.unwrap_or(HDE_MIN_IOPS);
            let per_disk_mo =
                size_gib * HDE_SPACE_USD_PER_GIB_MO + iops as f64 * HDE_IOPS_USD_PER_IOPS_MO;
            (
                format!(
                    "{}x {} hyperdisk-extreme ({} IOPS)",
                    kafka_nodes, answers.storage.size, iops
                ),
                monthly_to_hourly(n * per_disk_mo, mult),
            )
        }
        "hyperdisk-throughput" => {
            let tp = answers
                .storage
                .throughput_mibs
                .unwrap_or(HDT_MIN_THROUGHPUT_MIBS);
            let per_disk_mo =
                size_gib * HDT_SPACE_USD_PER_GIB_MO + tp as f64 * HDT_TP_USD_PER_MIBS_MO;
            (
                format!(
                    "{}x {} hyperdisk-throughput ({} MiB/s)",
                    kafka_nodes, answers.storage.size, tp
                ),
                monthly_to_hourly(n * per_disk_mo, mult),
            )
        }
        "pd-ssd" => (
            format!("{}x {} pd-ssd", kafka_nodes, answers.storage.size),
            monthly_to_hourly(n * size_gib * PD_SSD_USD_PER_GIB_MO, mult),
        ),
        "pd-balanced" => (
            format!("{}x {} pd-balanced", kafka_nodes, answers.storage.size),
            monthly_to_hourly(n * size_gib * PD_BALANCED_USD_PER_GIB_MO, mult),
        ),
        "local-ssd" => {
            if kafka_spec.is_some_and(|s| s.local_ssd_mode == LocalSsdMode::Fixed) {
                let count = kafka_spec.unwrap().local_ssd_count;
                (
                    format!(
                        "{}x {}x{} GiB Local NVMe (bundled in -lssd VM price)",
                        kafka_nodes, count, LOCAL_SSD_SIZE_GIB
                    ),
                    0.0,
                )
            } else {
                let count = answers.storage.local_ssd_count.unwrap_or(1);
                let total_gib = n * (count * LOCAL_SSD_SIZE_GIB) as f64;
                (
                    format!(
                        "{}x {}x{} GiB attachable Local NVMe",
                        kafka_nodes, count, LOCAL_SSD_SIZE_GIB
                    ),
                    monthly_to_hourly(total_gib * LOCAL_SSD_USD_PER_GIB_MO, mult),
                )
            }
        }
        other => (format!("{}x {} {}", kafka_nodes, answers.storage.size, other), 0.0),
    }
}

/// Format a terse inline per-VM hourly price hint for interactive prompts
/// (e.g. `~$0.22/hr in europe-west1`).
pub fn format_vm_hourly_hint(
    machine_type: &str,
    spec: Option<&InstanceSpec>,
    region: &str,
) -> String {
    let (mult, _) = regional_price_multiplier(region);
    let (base, _) = vm_base_hourly_usd(machine_type, spec);
    format!("~${:.2}/hr in {}", base * mult, region)
}

/// Print the itemized hourly & daily USD cost breakdown before confirmation.
pub fn print_cost_estimate(cost: &CostBreakdown) {
    let region_note = if cost.region_known {
        format!("{}, {:.2}x vs us-central1", cost.region, cost.regional_multiplier)
    } else {
        format!(
            "{}, est. {:.2}x vs us-central1",
            cost.region, cost.regional_multiplier
        )
    };
    println!("\n=== Estimated On-Demand Cost ({}) ===\n", region_note);

    for item in &cost.items {
        let prefix = if item.approximated { "~$" } else { " $" };
        println!(
            "  {:<20} {:>2}{:>6.3}/hr   {}",
            item.label, prefix, item.hourly_usd, item.detail
        );
    }

    println!("  {}", "-".repeat(62));
    let daily = cost.total_hourly_usd * 24.0;
    let without_gke_fee = (cost.total_hourly_usd - GKE_CONTROL_PLANE_USD_PER_HR).max(0.0);
    println!(
        "  {:<20} ~${:>6.2}/hr   (~${:.2}/day; ~${:.2}/hr with GKE free-tier credit)",
        "Estimated total", cost.total_hourly_usd, daily, without_gke_fee
    );
    if let Some(max_hr) = cost.max_autoscaled_hourly_usd {
        println!(
            "  {:<20} ~${:>6.2}/hr   (if workers autoscale to max={})",
            "Max autoscaled", max_hr, cost.workers_max
        );
    }
    println!("\n  Note: Approximate on-demand list prices; excludes network egress & Artifact Registry.");
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

    fn base_answers() -> Answers {
        Answers {
            env_name: "gke".into(),
            gcp: GcpAnswers {
                project: "demo-proj".into(),
                dimster_name: "bench".into(),
                region: "us-central1".into(),
                zone: "us-central1-a".into(),
                release_channel: "regular".into(),
                gke_version: None,
                existing_image: None,
                dataplane_v2: true,
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
    fn computes_baseline_and_regional_scaled_cost() {
        let types = load_catalog();
        let us = base_answers();
        let cost_us = estimate_cluster_cost(&us, &types);
        assert_eq!(cost_us.regional_multiplier, 1.0);
        // 3*0.091 + 0.202 + 0.067 + storage(~0.033) + boot/infra(~0.037) + gke(0.10) ~= 0.712/hr
        assert!((cost_us.total_hourly_usd - 0.712).abs() < 0.02);
        // Autoscaling max=4 adds 3x c3-standard-4 (~0.606) + 3x 50GB pd-balanced boot (~0.021)
        let max_us = cost_us.max_autoscaled_hourly_usd.unwrap();
        assert!((max_us - (cost_us.total_hourly_usd + 0.627)).abs() < 0.02);

        let mut eu = us;
        eu.gcp.region = "europe-west1".into();
        let cost_eu = estimate_cluster_cost(&eu, &types);
        assert_eq!(cost_eu.regional_multiplier, 1.10);
        // Everything except the flat $0.10/hr GKE control plane scales by 1.10x.
        let expected_eu = (cost_us.total_hourly_usd - 0.10) * 1.10 + 0.10;
        assert!((cost_eu.total_hourly_usd - expected_eu).abs() < 0.001);
    }

    #[test]
    fn provisioned_hyperdisk_charges_above_free_baseline_only() {
        let types = load_catalog();
        let mut a = base_answers();
        a.kafka.instance_type = "c3-standard-8".into();
        a.storage = StorageAnswers {
            storage_type: "hyperdisk-balanced".into(),
            size: "500Gi".into(),
            performance: Some("provisioned".into()),
            iops: Some(16_000),
            throughput_mibs: Some(800),
            local_ssd_count: None,
        };
        let cost = estimate_cluster_cost(&a, &types);
        let storage_item = cost.items.iter().find(|i| i.label == "Kafka storage").unwrap();
        // Per disk monthly: 500*0.08 ($40) + 13000*0.005 ($65) + 660*0.04 ($26.40) = $131.40/mo -> $0.18/hr * 3 = $0.54/hr
        assert!((storage_item.hourly_usd - 0.54).abs() < 0.005);
    }

    #[test]
    fn fixed_lssd_avoids_double_counting_while_attachable_n2_adds_ssd_cost() {
        let types = load_catalog();

        let mut fixed = base_answers();
        fixed.kafka.instance_type = "c3-standard-8-lssd".into();
        fixed.storage = StorageAnswers {
            storage_type: "local-ssd".into(),
            size: "675Gi".into(),
            performance: None,
            iops: None,
            throughput_mibs: None,
            local_ssd_count: Some(2),
        };
        let fixed_cost = estimate_cluster_cost(&fixed, &types);
        let fixed_storage = fixed_cost.items.iter().find(|i| i.label == "Kafka storage").unwrap();
        assert_eq!(fixed_storage.hourly_usd, 0.0);

        let mut attach = base_answers();
        attach.kafka.instance_type = "n2-standard-8".into();
        attach.storage = StorageAnswers {
            storage_type: "local-ssd".into(),
            size: "675Gi".into(),
            performance: None,
            iops: None,
            throughput_mibs: None,
            local_ssd_count: Some(2),
        };
        let attach_cost = estimate_cluster_cost(&attach, &types);
        let attach_storage = attach_cost.items.iter().find(|i| i.label == "Kafka storage").unwrap();
        // 3 brokers * 2 * 375 GiB * $0.08 / 730 = $0.2466/hr
        assert!((attach_storage.hourly_usd - 0.2466).abs() < 0.005);
    }

    #[test]
    fn external_mode_and_tier1_networking_behave_correctly() {
        let types = load_catalog();
        let mut a = base_answers();
        a.deploy_mode = "external".into();
        a.workers.instance_type = "c3-standard-44".into();
        a.workers.count = 1;
        a.workers.max = 1;
        a.workers.tier1_networking = true;

        let cost = estimate_cluster_cost(&a, &types);
        assert!(cost.items.iter().all(|i| !i.label.starts_with("Kafka")));
        let tier1 = cost.items.iter().find(|i| i.label == "Tier_1 networking").unwrap();
        assert!((tier1.hourly_usd - 44.0 * 0.046).abs() < 0.001);
        assert!(cost.max_autoscaled_hourly_usd.is_none());
    }
}
