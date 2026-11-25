//! Interactive GKE configuration questionnaire with computed defaults.
//!
//! Guides the user through GCP project/zone, Kafka sizing, disk type and
//! throughput/IOPS/latency tuning, Local NVMe SSD selection, infra/monitoring,
//! and worker sizing — reading machine specs from `instance-types.yaml`.

use std::process::Command;

use anyhow::Result;
use cloud_common::sizing::{
    compute_monitoring_defaults, default_kafka_cpu, default_kafka_heap, default_worker_cpu,
    default_worker_heap, gke_allocatable_memory_gib, parse_storage_gib,
};
use dialoguer::{Confirm, Input, Select};

use crate::answers::*;
use crate::instances::{DiskType, InstanceSpec, InstanceTypes, LOCAL_SSD_SIZE_GIB, LocalSsdMode};
use crate::pricing::format_vm_hourly_hint;
use crate::quotas::{RegionQuotas, fetch_region_quotas, print_available_quotas};
use crate::storage::{
    HDB_BASE_IOPS, HDE_MIN_GIB, HDE_MIN_IOPS, HDT_MIN_GIB, available_disk_types,
    disk_type_menu_label, estimate_local_ssd_performance, estimate_pd_performance,
    hyperdisk_balanced_max_iops, hyperdisk_balanced_throughput_bounds,
    hyperdisk_throughput_bounds, validate_hyperdisk_balanced, validate_hyperdisk_extreme,
    validate_hyperdisk_throughput,
};

/// Collect all answers interactively, querying regional quotas up front and
/// computing sizing defaults from `instance_types`.
pub fn ask_all(instance_types: &InstanceTypes) -> Result<(Answers, RegionQuotas)> {
    let env_name: String = Input::new()
        .with_prompt("Environment name (key in dimster-config.yaml)")
        .default("gke".into())
        .interact_text()?;

    println!("\n=== GCP Configuration ===\n");

    let detected_project = gcloud_config_value("project");
    let project: String = if let Some(ref p) = detected_project {
        Input::new()
            .with_prompt("GCP project ID")
            .default(p.clone())
            .interact_text()?
    } else {
        Input::new()
            .with_prompt("GCP project ID")
            .interact_text()?
    };

    let dimster_name: String = loop {
        let name: String = Input::new()
            .with_prompt("Resource name prefix (lowercase letters, digits, hyphens)")
            .default("my-dimster".into())
            .interact_text()?;
        match validate_gcp_name(&name) {
            Ok(()) => break name,
            Err(e) => println!("  ⚠ {}", e),
        }
    };

    let default_region = gcloud_config_value("compute/region").unwrap_or_else(|| "us-central1".into());
    let region: String = Input::new()
        .with_prompt("GCP region")
        .default(default_region)
        .interact_text()?;

    let zone = ask_zone(&project, &region)?;

    println!("\nQuerying GCP quotas for {} ({})...", project, region);
    let region_quotas = fetch_region_quotas(&project, &region);
    print_available_quotas(&region_quotas);
    println!();

    let (release_channel, gke_version) = ask_gke_version()?;

    let private_nodes = Confirm::new()
        .with_prompt("Use private GKE nodes + Cloud NAT (no public IPs on nodes; recommended)?")
        .default(true)
        .interact()?;

    let cpu_manager_static = Confirm::new()
        .with_prompt("Enable kubelet static CPU Manager on Kafka/worker pools?")
        .default(false)
        .interact()?;

    // --- Deploy mode ---
    let deploy_options = [
        "Full (deploy Kafka + benchmark clients)",
        "External (connect to existing Kafka)",
    ];
    let deploy_selection = Select::new()
        .with_prompt("Deploy mode")
        .items(&deploy_options)
        .default(0)
        .interact()?;
    let deploy_mode = if deploy_selection == 1 {
        "external"
    } else {
        "full"
    };
    let is_external = deploy_mode == "external";

    let bootstrap_servers = if is_external {
        let bs: String = Input::new()
            .with_prompt("External Kafka bootstrap servers")
            .interact_text()?;
        Some(bs)
    } else {
        None
    };

    let (
        kafka_version,
        kafka_instance_type,
        kafka_spec,
        kafka_count,
        kafka_heap,
        kafka_cpu,
        kafka_memory,
        kafka_tier1,
        kafka_broker_config,
        kafka_extra_config,
        storage,
    ) = if is_external {
        (
            "4.2.0".to_string(),
            "c3-standard-8".to_string(),
            None::<InstanceSpec>,
            0u32,
            "-Xms1G -Xmx1G".to_string(),
            "1".to_string(),
            "4Gi".to_string(),
            false,
            None,
            None,
            StorageAnswers {
                storage_type: "hyperdisk-balanced".into(),
                size: "500Gi".into(),
                performance: Some("basic".into()),
                iops: None,
                throughput_mibs: None,
                local_ssd_count: None,
            },
        )
    } else {
        println!("\n=== Kafka ===\n");

        let kafka_version: String = Input::new()
            .with_prompt("Kafka version (broker image tag; also sets kafka_client_version if non-default)")
            .default("4.2.0".into())
            .interact_text()?;

        let kafka_instance_type: String = Input::new()
            .with_prompt("Kafka machine type")
            .default("c3-standard-8".into())
            .interact_text()?;

        let kafka_spec = instance_types.lookup(&kafka_instance_type).cloned();
        if let Some(ref spec) = kafka_spec {
            println!(
                "  → {}: {} ({})",
                kafka_instance_type,
                spec,
                format_vm_hourly_hint(&kafka_instance_type, Some(spec), &region)
            );
        } else {
            println!("  ⚠ Unknown machine type — auto-suggestions & compatibility checks limited");
        }

        let kafka_count: u32 = Input::new()
            .with_prompt("Number of Kafka brokers")
            .default(3)
            .interact_text()?;

        let kafka_tier1 = ask_tier1("Kafka", kafka_spec.as_ref())?;

        let kafka_heap = ask_heap(
            "Kafka JVM heap (-Xms/-Xmx)",
            kafka_spec.as_ref(),
            HeapRole::Kafka,
        )?;
        let kafka_cpu = ask_cpu("Kafka CPU request", kafka_spec.as_ref(), CpuRole::Kafka)?;
        let kafka_memory = ask_memory("Kafka memory request", kafka_spec.as_ref())?;

        let kafka_broker_config: String = Input::new()
            .with_prompt("Broker config file (empty for none)")
            .default("run/broker-configs/default.yaml".into())
            .interact_text()?;
        let kafka_broker_config = if kafka_broker_config.is_empty() {
            None
        } else {
            Some(kafka_broker_config)
        };

        let kafka_extra_config = {
            let cpu_cores: u32 = kafka_cpu.parse().unwrap_or(4);
            let default_net = cpu_cores.max(3);
            let default_io = (cpu_cores * 2).max(8);
            println!(
                "  → Auto-calculated: num.network.threads={}, num.io.threads={}",
                default_net, default_io
            );

            let net_threads: u32 = Input::new()
                .with_prompt("num.network.threads")
                .default(default_net)
                .interact_text()?;
            let io_threads: u32 = Input::new()
                .with_prompt("num.io.threads")
                .default(default_io)
                .interact_text()?;

            if net_threads != default_net || io_threads != default_io {
                Some(format!(
                    "num.network.threads={},num.io.threads={}",
                    net_threads, io_threads
                ))
            } else {
                None
            }
        };

        println!("\n=== Kafka Storage ===\n");
        let storage = ask_storage(kafka_spec.as_ref())?;

        (
            kafka_version,
            kafka_instance_type,
            kafka_spec,
            kafka_count,
            kafka_heap,
            kafka_cpu,
            kafka_memory,
            kafka_tier1,
            kafka_broker_config,
            kafka_extra_config,
            storage,
        )
    };

    // --- Infra + Coordinator + Monitoring ---
    println!("\n=== Infra Node (Grafana, Prometheus, and optionally coordinator) ===\n");

    let infra_instance_type: String = Input::new()
        .with_prompt("Infra machine type")
        .default("e2-standard-2".into())
        .interact_text()?;

    let infra_spec = instance_types.lookup(&infra_instance_type).cloned();
    if let Some(ref spec) = infra_spec {
        println!(
            "  → {}: {} ({})",
            infra_instance_type,
            spec,
            format_vm_hourly_hint(&infra_instance_type, Some(spec), &region)
        );
        let available = gke_allocatable_memory_gib(spec.memory_gib);
        println!(
            "  {}Gi allocatable on infra node",
            available
        );
        if spec.is_hyperdisk_only() {
            println!(
                "  ℹ {} is Hyperdisk-only: gke-setup will switch the cluster's default StorageClass",
                infra_instance_type
            );
            println!(
                "    to 'dynamic-rwo' so Prometheus (50Gi) and benchmark-results (4Gi) use Hyperdisk Balanced."
            );
        }
    } else {
        println!("  ⚠ Unknown machine type — auto-suggestions disabled");
    }

    let coord_pool_options = [
        "infra node pool (role=infra — default; keeps coordinator off benchmark workers)",
        "worker node pool (role=worker — frees CPU/RAM on a small infra node)",
    ];
    let coord_pool_sel = Select::new()
        .with_prompt("Coordinator node pool")
        .items(&coord_pool_options)
        .default(0)
        .interact()?;
    let coord_node_label = if coord_pool_sel == 1 {
        "role=worker".to_string()
    } else {
        "role=infra".to_string()
    };

    let coord_heap: String = Input::new()
        .with_prompt("Coordinator JVM heap")
        .default("-Xms512M -Xmx512M".into())
        .interact_text()?;

    let coord_cpu: String = Input::new()
        .with_prompt("Coordinator CPU request")
        .default("500m".into())
        .interact_text()?;

    let coord_memory: String = Input::new()
        .with_prompt("Coordinator memory request")
        .default("1Gi".into())
        .interact_text()?;

    println!("\n  --- Monitoring Resources (Prometheus + Grafana) ---\n");

    // Only subtract coordinator requests from infra monitoring defaults when
    // the coordinator actually runs on the infra node pool.
    let (infra_coord_cpu, infra_coord_mem) = if coord_node_label == "role=infra" {
        (coord_cpu.as_str(), coord_memory.as_str())
    } else {
        ("0", "0")
    };

    let (prom_cpu_default, prom_mem_default, grafana_cpu_default, grafana_mem_default) =
        compute_monitoring_defaults(
            infra_spec.as_ref().map(|s| s.vcpu),
            infra_spec
                .as_ref()
                .map(|s| gke_allocatable_memory_gib(s.memory_gib)),
            infra_coord_cpu,
            infra_coord_mem,
        );

    let prom_cpu: String = Input::new()
        .with_prompt("Prometheus CPU request")
        .default(prom_cpu_default)
        .interact_text()?;
    let prom_memory: String = Input::new()
        .with_prompt("Prometheus memory request")
        .default(prom_mem_default)
        .interact_text()?;
    let grafana_cpu: String = Input::new()
        .with_prompt("Grafana CPU request")
        .default(grafana_cpu_default)
        .interact_text()?;
    let grafana_memory: String = Input::new()
        .with_prompt("Grafana memory request")
        .default(grafana_mem_default)
        .interact_text()?;

    // --- Workers ---
    println!("\n=== Worker Nodes ===\n");

    let worker_instance_type: String = Input::new()
        .with_prompt("Worker machine type")
        .default("c3-standard-4".into())
        .interact_text()?;

    let worker_spec = instance_types.lookup(&worker_instance_type).cloned();
    if let Some(ref spec) = worker_spec {
        println!(
            "  → {}: {} ({})",
            worker_instance_type,
            spec,
            format_vm_hourly_hint(&worker_instance_type, Some(spec), &region)
        );
        let available = gke_allocatable_memory_gib(spec.memory_gib);
        println!("  {}Gi allocatable for worker pods", available);
        if coord_node_label == "role=worker" {
            println!(
                "  ℹ Coordinator will also run on role=worker — leave headroom ({} CPU, {} mem) on worker requests.",
                coord_cpu, coord_memory
            );
            if spec.is_hyperdisk_only() {
                println!(
                    "  ℹ {} is Hyperdisk-only: gke-setup will switch the default StorageClass to 'dynamic-rwo'",
                    worker_instance_type
                );
                println!("    so the coordinator's 4Gi benchmark-results PVC uses Hyperdisk Balanced.");
            }
        }
        if let Some(ref ks) = kafka_spec
            && ks.arch != spec.arch
        {
            println!(
                "  ⚠ Architecture mismatch: Kafka is {} while workers are {}. Keep all pools on the same architecture for single-arch Dimster builds.",
                ks.arch.as_str(),
                spec.arch.as_str()
            );
        }
    } else {
        println!("  ⚠ Unknown machine type — auto-suggestions disabled");
    }

    let worker_count: u32 = Input::new()
        .with_prompt("Number of workers")
        .default(2)
        .interact_text()?;
    let worker_min: u32 = Input::new()
        .with_prompt("Min workers (autoscaling)")
        .default(1)
        .interact_text()?;
    let worker_max: u32 = Input::new()
        .with_prompt("Max workers (autoscaling)")
        .default(worker_count.max(4))
        .interact_text()?;

    let worker_tier1 = ask_tier1("Worker", worker_spec.as_ref())?;
    let worker_heap = ask_heap(
        "Worker JVM heap (-Xms/-Xmx)",
        worker_spec.as_ref(),
        HeapRole::Worker,
    )?;
    let worker_cpu = ask_cpu("Worker CPU request", worker_spec.as_ref(), CpuRole::Worker)?;
    let worker_memory = ask_memory("Worker memory request", worker_spec.as_ref())?;

    let kafka_client_version = if !is_external && kafka_version != "4.2.0" {
        Some(kafka_version.clone())
    } else {
        None
    };

    Ok((
        Answers {
            env_name,
            gcp: GcpAnswers {
                project,
                dimster_name,
                region,
                zone,
                release_channel,
                gke_version,
                existing_image: None,
                dataplane_v2: true,
                private_nodes,
                master_ipv4_cidr: None,
                network: None,
                subnetwork: None,
                node_service_account: None,
            },
            deploy_mode: deploy_mode.to_string(),
            bootstrap_servers,
            kafka_client_version,
            cpu_manager_static,
            kafka: KafkaAnswers {
                version: kafka_version,
                instance_type: kafka_instance_type,
                count: kafka_count,
                heap: kafka_heap,
                cpu: kafka_cpu,
                memory: kafka_memory,
                tier1_networking: kafka_tier1,
                broker_config: kafka_broker_config,
                extra_config: kafka_extra_config,
            },
            storage,
            workers: WorkerAnswers {
                instance_type: worker_instance_type,
                count: worker_count,
                min: worker_min,
                max: worker_max,
                heap: worker_heap,
                cpu: worker_cpu,
                memory: worker_memory,
                tier1_networking: worker_tier1,
            },
            coordinator: CoordinatorAnswers {
                infra_instance_type,
                node_label: coord_node_label,
                heap: coord_heap,
                cpu: coord_cpu,
                memory: coord_memory,
            },
            monitoring: MonitoringAnswers {
                prometheus_cpu: prom_cpu,
                prometheus_memory: prom_memory,
                grafana_cpu,
                grafana_memory,
            },
        },
        region_quotas,
    ))
}

fn gcloud_config_value(key: &str) -> Option<String> {
    let out = Command::new("gcloud")
        .args(["config", "get-value", key])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let val = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if val.is_empty() || val == "(unset)" {
        None
    } else {
        Some(val)
    }
}

fn ask_zone(project: &str, region: &str) -> Result<String> {
    let filter = format!("region:({}) status=UP", region);
    let output = Command::new("gcloud")
        .args([
            "compute",
            "zones",
            "list",
            "--project",
            project,
            "--filter",
            &filter,
            "--format",
            "value(name)",
        ])
        .output();

    let default_zone = format!("{}-a", region);

    match output {
        Ok(out) if out.status.success() => {
            let text = String::from_utf8_lossy(&out.stdout);
            let mut zones: Vec<&str> = text.lines().map(str::trim).filter(|s| !s.is_empty()).collect();
            zones.sort();
            if zones.is_empty() {
                let z: String = Input::new()
                    .with_prompt("GCP zone")
                    .default(default_zone)
                    .interact_text()?;
                return Ok(z);
            }
            println!("  Available zones: {}", zones.join(", "));
            let sel = Select::new()
                .with_prompt("GCP zone (single-zone cluster — avoids cross-zone egress)")
                .items(&zones)
                .default(0)
                .interact()?;
            Ok(zones[sel].to_string())
        }
        _ => {
            println!("  ⚠ Could not query zones via gcloud — enter zone manually");
            let z: String = Input::new()
                .with_prompt("GCP zone")
                .default(default_zone)
                .interact_text()?;
            Ok(z)
        }
    }
}

fn ask_gke_version() -> Result<(String, Option<String>)> {
    let options = [
        "regular channel (recommended)",
        "stable channel",
        "rapid channel",
        "no release channel (pin explicit version)",
    ];
    let sel = Select::new()
        .with_prompt("GKE release channel")
        .items(&options)
        .default(0)
        .interact()?;

    match sel {
        0 => Ok(("regular".into(), None)),
        1 => Ok(("stable".into(), None)),
        2 => Ok(("rapid".into(), None)),
        _ => {
            let ver: String = Input::new()
                .with_prompt("GKE Kubernetes version (e.g. 1.32)")
                .default("1.32".into())
                .interact_text()?;
            Ok(("none".into(), Some(ver)))
        }
    }
}

fn ask_tier1(role: &str, spec: Option<&InstanceSpec>) -> Result<bool> {
    if let Some(s) = spec
        && let Some(tier1_gbps) = s.tier1_network_gbps
    {
        return Ok(Confirm::new()
            .with_prompt(format!(
                "Enable Tier_1 networking for {} pool ({:.0} Gbps vs {:.0} Gbps default)?",
                role, tier1_gbps, s.network_gbps
            ))
            .default(false)
            .interact()?);
    }
    Ok(false)
}

enum HeapRole {
    Kafka,
    Worker,
}

fn ask_heap(prompt: &str, spec: Option<&InstanceSpec>, role: HeapRole) -> Result<String> {
    let default = match (spec, role) {
        (Some(s), HeapRole::Kafka) => default_kafka_heap(s.memory_gib),
        (Some(s), HeapRole::Worker) => {
            let alloc = gke_allocatable_memory_gib(s.memory_gib);
            default_worker_heap(alloc)
        }
        _ => "-Xms1G -Xmx2G".into(),
    };
    let heap: String = Input::new()
        .with_prompt(prompt)
        .default(default)
        .interact_text()?;
    Ok(heap)
}

enum CpuRole {
    Kafka,
    Worker,
}

fn ask_cpu(prompt: &str, spec: Option<&InstanceSpec>, role: CpuRole) -> Result<String> {
    let default = match (spec, role) {
        (Some(s), CpuRole::Kafka) => default_kafka_cpu(s.vcpu),
        (Some(s), CpuRole::Worker) => default_worker_cpu(s.vcpu),
        _ => "2".into(),
    };
    let cpu: String = Input::new()
        .with_prompt(prompt)
        .default(default)
        .interact_text()?;
    Ok(cpu)
}

fn ask_memory(prompt: &str, spec: Option<&InstanceSpec>) -> Result<String> {
    let default = match spec {
        Some(s) => format!("{}Gi", gke_allocatable_memory_gib(s.memory_gib)),
        None => "8Gi".into(),
    };
    let memory: String = Input::new()
        .with_prompt(prompt)
        .default(default)
        .interact_text()?;
    Ok(memory)
}

fn ask_storage(kafka_spec: Option<&InstanceSpec>) -> Result<StorageAnswers> {
    if let Some(spec) = kafka_spec {
        println!(
            "  VM disk ceiling: up to {} MiB/s, {} IOPS (boot disk: {})",
            spec.disk_bandwidth_mibs, spec.disk_max_iops, spec.boot_disk_type
        );
    }

    let disk_types = available_disk_types(kafka_spec);
    let labels: Vec<String> = disk_types
        .iter()
        .map(|&dt| disk_type_menu_label(dt, kafka_spec))
        .collect();

    // Default to Local SSD automatically when user picked a bundled `-lssd` machine shape.
    let default_idx = if kafka_spec.is_some_and(|s| s.local_ssd_mode == LocalSsdMode::Fixed) {
        disk_types
            .iter()
            .position(|&d| d == DiskType::LocalSsd)
            .unwrap_or(0)
    } else {
        0
    };

    let sel = Select::new()
        .with_prompt("Kafka storage type")
        .items(&labels)
        .default(default_idx)
        .interact()?;
    let chosen = disk_types[sel];

    match chosen {
        DiskType::LocalSsd => ask_local_ssd_storage(kafka_spec),
        DiskType::HyperdiskBalanced => ask_hyperdisk_balanced(kafka_spec),
        DiskType::HyperdiskExtreme => ask_hyperdisk_extreme(kafka_spec),
        DiskType::HyperdiskThroughput => ask_hyperdisk_throughput(kafka_spec),
        DiskType::PdSsd | DiskType::PdBalanced => ask_pd_storage(chosen, kafka_spec),
    }
}

fn ask_local_ssd_storage(spec: Option<&InstanceSpec>) -> Result<StorageAnswers> {
    let count = match spec.map(|s| s.local_ssd_mode) {
        Some(LocalSsdMode::Fixed) => {
            let s = spec.unwrap();
            let c = s.local_ssd_count;
            println!(
                "  Bundled Local NVMe SSDs: {}x {} GiB ({} GiB raw)",
                c,
                LOCAL_SSD_SIZE_GIB,
                s.fixed_local_ssd_gib()
            );
            c
        }
        Some(LocalSsdMode::Attachable) => {
            let max = spec.unwrap().max_local_ssd_count;
            let choices: Vec<u32> = [1, 2, 4, 8].into_iter().filter(|&n| n <= max).collect();
            let labels: Vec<String> = choices
                .iter()
                .map(|&n| {
                    let est = estimate_local_ssd_performance(n);
                    format!(
                        "{}x {} GiB ({} GiB raw — ~{}R / {}W MiB/s, {}kR / {}kW IOPS)",
                        n,
                        LOCAL_SSD_SIZE_GIB,
                        n * LOCAL_SSD_SIZE_GIB,
                        est.read_mibs,
                        est.write_mibs,
                        est.read_iops / 1000,
                        est.write_iops / 1000,
                    )
                })
                .collect();
            let sel = Select::new()
                .with_prompt("Number of 375 GiB Local NVMe SSDs per broker")
                .items(&labels)
                .default(0)
                .interact()?;
            choices[sel]
        }
        _ => Input::new()
            .with_prompt("Number of 375 GiB Local NVMe SSDs per broker")
            .default(1)
            .interact_text()?,
    };

    let est = estimate_local_ssd_performance(count);
    println!(
        "  → Estimated Local NVMe perf (~100-200us latency): ~{} MiB/s read, ~{} MiB/s write ({}k/{}k IOPS)",
        est.read_mibs,
        est.write_mibs,
        est.read_iops / 1000,
        est.write_iops / 1000,
    );
    println!("  ℹ Local SSDs are ephemeral (wiped on node recreate/upgrade); PVs use StorageClass 'local-storage'");

    let total_gib = count * LOCAL_SSD_SIZE_GIB;
    let pv_gib = ((total_gib as f64) * 0.9) as u32;
    let size: String = Input::new()
        .with_prompt("PV capacity to advertise")
        .default(format!("{}Gi", pv_gib))
        .interact_text()?;

    Ok(StorageAnswers {
        storage_type: "local-ssd".into(),
        size,
        performance: None,
        iops: None,
        throughput_mibs: None,
        local_ssd_count: Some(count),
    })
}

fn ask_hyperdisk_balanced(spec: Option<&InstanceSpec>) -> Result<StorageAnswers> {
    let (size, size_gib) = ask_valid_size("Hyperdisk Balanced volume size", "500Gi", 6, 65_536)?;

    let perf_options = [
        "basic (free baseline: 3,000 IOPS, 140 MiB/s)",
        "provisioned (custom IOPS & throughput)",
    ];
    let perf_sel = Select::new()
        .with_prompt("Hyperdisk Balanced performance")
        .items(&perf_options)
        .default(1)
        .interact()?;

    if perf_sel == 0 {
        return Ok(StorageAnswers {
            storage_type: "hyperdisk-balanced".into(),
            size,
            performance: Some("basic".into()),
            iops: None,
            throughput_mibs: None,
            local_ssd_count: None,
        });
    }

    let max_iops = hyperdisk_balanced_max_iops(size_gib);
    let suggested_iops = spec
        .map(|s| s.disk_max_iops.min(16_000).min(max_iops))
        .unwrap_or(16_000)
        .max(HDB_BASE_IOPS);

    let iops: u32 = loop {
        let v: u32 = Input::new()
            .with_prompt(format!("Provisioned IOPS ({}..={})", HDB_BASE_IOPS, max_iops))
            .default(suggested_iops)
            .interact_text()?;
        if (HDB_BASE_IOPS..=max_iops).contains(&v) {
            break v;
        }
        println!(
            "  ⚠ Must be between {} and {} for a {} GiB disk.",
            HDB_BASE_IOPS, max_iops, size_gib
        );
    };

    let (min_tp, max_tp) = hyperdisk_balanced_throughput_bounds(iops);
    let suggested_tp = spec
        .map(|s| s.disk_bandwidth_mibs.clamp(min_tp, max_tp))
        .unwrap_or_else(|| 500u32.clamp(min_tp, max_tp));

    let throughput: u32 = loop {
        let v: u32 = Input::new()
            .with_prompt(format!(
                "Provisioned throughput MiB/s ({}..={})",
                min_tp, max_tp
            ))
            .default(suggested_tp)
            .interact_text()?;
        match validate_hyperdisk_balanced(size_gib, Some(iops), Some(v), spec) {
            Ok(warnings) => {
                for w in warnings {
                    println!("  ⚠ {}", w);
                }
                break v;
            }
            Err(e) => println!("  ⚠ {}", e),
        }
    };

    Ok(StorageAnswers {
        storage_type: "hyperdisk-balanced".into(),
        size,
        performance: Some("provisioned".into()),
        iops: Some(iops),
        throughput_mibs: Some(throughput),
        local_ssd_count: None,
    })
}

fn ask_hyperdisk_extreme(spec: Option<&InstanceSpec>) -> Result<StorageAnswers> {
    let (size, size_gib) = ask_valid_size("Hyperdisk Extreme volume size", "500Gi", HDE_MIN_GIB, 65_536)?;
    let max_iops = 350_000u32.min(size_gib.saturating_mul(1_000));
    let default_iops = spec
        .map(|s| s.disk_max_iops.min(100_000).min(max_iops))
        .unwrap_or(50_000)
        .max(HDE_MIN_IOPS);

    let iops: u32 = loop {
        let v: u32 = Input::new()
            .with_prompt(format!(
                "Provisioned IOPS ({}..={}, throughput = 250 MiB/s per 1k IOPS)",
                HDE_MIN_IOPS, max_iops
            ))
            .default(default_iops)
            .interact_text()?;
        match validate_hyperdisk_extreme(size_gib, v, spec) {
            Ok((coupled_tp, warnings)) => {
                println!("  → Auto-coupled throughput: {} MiB/s", coupled_tp);
                for w in warnings {
                    println!("  ⚠ {}", w);
                }
                break v;
            }
            Err(e) => println!("  ⚠ {}", e),
        }
    };

    Ok(StorageAnswers {
        storage_type: "hyperdisk-extreme".into(),
        size,
        performance: Some("provisioned".into()),
        iops: Some(iops),
        throughput_mibs: None,
        local_ssd_count: None,
    })
}

fn ask_hyperdisk_throughput(spec: Option<&InstanceSpec>) -> Result<StorageAnswers> {
    let (size, size_gib) =
        ask_valid_size("Hyperdisk Throughput volume size (min 2Ti)", "2Ti", HDT_MIN_GIB, 32_768)?;
    let (min_tp, max_tp) = hyperdisk_throughput_bounds(size_gib);
    let default_tp = spec
        .map(|s| s.disk_bandwidth_mibs.clamp(min_tp, max_tp))
        .unwrap_or(max_tp);

    let throughput: u32 = loop {
        let v: u32 = Input::new()
            .with_prompt(format!(
                "Provisioned throughput MiB/s ({}..={}, 4 IOPS per MiB/s)",
                min_tp, max_tp
            ))
            .default(default_tp)
            .interact_text()?;
        match validate_hyperdisk_throughput(size_gib, v, spec) {
            Ok((coupled_iops, warnings)) => {
                println!("  → Auto-coupled IOPS: {}", coupled_iops);
                for w in warnings {
                    println!("  ⚠ {}", w);
                }
                break v;
            }
            Err(e) => println!("  ⚠ {}", e),
        }
    };

    Ok(StorageAnswers {
        storage_type: "hyperdisk-throughput".into(),
        size,
        performance: Some("provisioned".into()),
        iops: None,
        throughput_mibs: Some(throughput),
        local_ssd_count: None,
    })
}

fn ask_pd_storage(disk_type: DiskType, spec: Option<&InstanceSpec>) -> Result<StorageAnswers> {
    let prompt = format!("{} volume size", disk_type.as_str());
    let (size, size_gib) = ask_valid_size(&prompt, "500Gi", 10, 65_536)?;
    let est = estimate_pd_performance(disk_type, size_gib, spec);
    println!(
        "  → Effective {} perf for {}: {} IOPS, {} MiB/s (bounded by VM ceiling)",
        disk_type.as_str(),
        size,
        est.read_iops,
        est.write_mibs
    );

    Ok(StorageAnswers {
        storage_type: disk_type.as_str().into(),
        size,
        performance: None,
        iops: None,
        throughput_mibs: None,
        local_ssd_count: None,
    })
}

fn ask_valid_size(prompt: &str, default: &str, min_gib: u32, max_gib: u32) -> Result<(String, u32)> {
    loop {
        let s: String = Input::new()
            .with_prompt(prompt)
            .default(default.into())
            .interact_text()?;
        match parse_storage_gib(&s) {
            Ok(gib) if (min_gib..=max_gib).contains(&gib) => return Ok((s, gib)),
            Ok(gib) => println!(
                "  ⚠ Size must be {}..={} GiB (got {} GiB).",
                min_gib, max_gib, gib
            ),
            Err(e) => println!("  ⚠ {}", e),
        }
    }
}
