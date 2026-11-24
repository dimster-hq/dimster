//! Interactive EKS configuration questionnaire with computed defaults.
//!
//! Delegates Kubernetes/EKS sizing math to `cloud_common::sizing` while owning
//! AWS-specific prompts (region, AZ discovery, EKS version, gp3 vs NVMe).

use std::process::Command;

use anyhow::Result;
use cloud_common::sizing::{
    compute_monitoring_defaults, default_kafka_cpu, default_kafka_heap, default_worker_cpu,
    default_worker_heap, eks_allocatable_memory_gib,
};
use dialoguer::{Input, Select};

use crate::answers::*;
use crate::instances::{InstanceSpec, InstanceTypes};

/// Collect all answers interactively, using defaults computed from instance specs.
pub fn ask_all(instance_types: &InstanceTypes) -> Result<Answers> {
    let env_name: String = Input::new()
        .with_prompt("Environment name (key in dimster-config.yaml)")
        .default("aws".into())
        .interact_text()?;

    println!("\n=== AWS Configuration ===\n");

    let dimster_name: String = Input::new()
        .with_prompt("Resource name prefix")
        .default("my-dimster".into())
        .interact_text()?;

    let region: String = Input::new()
        .with_prompt("AWS region")
        .default("us-west-2".into())
        .interact_text()?;

    let az = ask_az(&region)?;

    let eks_version: String = Input::new()
        .with_prompt("EKS Kubernetes version")
        .default("1.32".into())
        .interact_text()?;

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
        _kafka_spec,
        kafka_count,
        kafka_heap,
        kafka_cpu,
        kafka_memory,
        kafka_broker_config,
        kafka_extra_config,
        storage,
    ) = if is_external {
        (
            "4.2.0".to_string(),
            "m6i.2xlarge".to_string(),
            None::<InstanceSpec>,
            0u32,
            "-Xms1G -Xmx1G".to_string(),
            "1".to_string(),
            "4Gi".to_string(),
            None,
            None,
            StorageAnswers {
                storage_type: "ebs".into(),
                size: "10Gi".into(),
                performance: None,
                iops: None,
                throughput_mbs: None,
            },
        )
    } else {
        println!("\n=== Kafka ===\n");

        let kafka_version: String = Input::new()
            .with_prompt(
                "Kafka version (broker image tag; also sets kafka_client_version if non-default)",
            )
            .default("4.2.0".into())
            .interact_text()?;

        let kafka_instance_type: String = Input::new()
            .with_prompt("Kafka instance type")
            .default("m6i.2xlarge".into())
            .interact_text()?;

        let kafka_spec = instance_types.lookup(&kafka_instance_type).cloned();
        if let Some(ref spec) = kafka_spec {
            println!("  → {}: {}", kafka_instance_type, spec);
        } else {
            println!("  ⚠ Unknown instance type — auto-suggestions disabled");
        }

        let kafka_count: u32 = Input::new()
            .with_prompt("Number of Kafka brokers")
            .default(3)
            .interact_text()?;

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
            kafka_broker_config,
            kafka_extra_config,
            storage,
        )
    };

    // --- Infra + Coordinator ---
    println!("\n=== Infra Node (coordinator, Grafana, Prometheus) ===\n");

    let infra_instance_type: String = Input::new()
        .with_prompt("Infra instance type")
        .default("t3.medium".into())
        .interact_text()?;

    let infra_spec = instance_types.lookup(&infra_instance_type).cloned();
    if let Some(ref spec) = infra_spec {
        println!("  → {}: {}", infra_instance_type, spec);
        let available = eks_allocatable_memory_gib(spec.memory_gib);
        println!(
            "  {}Gi allocatable (shared by coordinator, Prometheus, Grafana)",
            available
        );
    } else {
        println!("  ⚠ Unknown instance type — auto-suggestions disabled");
    }

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

    let (prom_cpu_default, prom_mem_default, grafana_cpu_default, grafana_mem_default) =
        compute_monitoring_defaults(
            infra_spec.as_ref().map(|s| s.vcpu),
            infra_spec
                .as_ref()
                .map(|s| eks_allocatable_memory_gib(s.memory_gib)),
            &coord_cpu,
            &coord_memory,
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
        .with_prompt("Worker instance type")
        .default("m6i.xlarge".into())
        .interact_text()?;

    let worker_spec = instance_types.lookup(&worker_instance_type).cloned();
    if let Some(ref spec) = worker_spec {
        println!("  → {}: {}", worker_instance_type, spec);
        let available = eks_allocatable_memory_gib(spec.memory_gib);
        println!("  {}Gi allocatable for worker pods", available);
    } else {
        println!("  ⚠ Unknown instance type — auto-suggestions disabled");
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

    Ok(Answers {
        env_name,
        aws: AwsAnswers {
            dimster_name,
            region,
            az,
            eks_version,
        },
        deploy_mode: deploy_mode.to_string(),
        bootstrap_servers,
        kafka_client_version,
        kafka: KafkaAnswers {
            version: kafka_version,
            instance_type: kafka_instance_type,
            count: kafka_count,
            heap: kafka_heap,
            cpu: kafka_cpu,
            memory: kafka_memory,
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
        },
        coordinator: CoordinatorAnswers {
            infra_instance_type,
            heap: coord_heap,
            cpu: coord_cpu,
            memory: coord_memory,
        },
        monitoring: MonitoringAnswers {
            prometheus_cpu: prom_cpu,
            prometheus_memory: prom_memory,
            grafana_cpu: grafana_cpu,
            grafana_memory: grafana_memory,
        },
    })
}

fn ask_az(region: &str) -> Result<String> {
    let output = Command::new("aws")
        .args([
            "ec2",
            "describe-availability-zones",
            "--region",
            region,
            "--query",
            "AvailabilityZones[].ZoneName",
            "--output",
            "text",
        ])
        .output();

    let default_az = format!("{}b", region);

    match output {
        Ok(out) if out.status.success() => {
            let azs_text = String::from_utf8_lossy(&out.stdout);
            let azs: Vec<&str> = azs_text.trim().split_whitespace().collect();
            if azs.is_empty() {
                let az: String = Input::new()
                    .with_prompt("Availability zone")
                    .default(default_az)
                    .interact_text()?;
                return Ok(az);
            }

            println!("  Available AZs: {}", azs.join(", "));
            let selection = Select::new()
                .with_prompt("Availability zone")
                .items(&azs)
                .default(0)
                .interact()?;
            Ok(azs[selection].to_string())
        }
        _ => {
            println!("  ⚠ Could not query AZs (aws cli issue)");
            let az: String = Input::new()
                .with_prompt("Availability zone")
                .default(default_az)
                .interact_text()?;
            Ok(az)
        }
    }
}

enum HeapRole {
    Kafka,
    Worker,
}

fn ask_heap(prompt: &str, spec: Option<&InstanceSpec>, role: HeapRole) -> Result<String> {
    let default = match (spec, role) {
        (Some(s), HeapRole::Kafka) => default_kafka_heap(s.memory_gib),
        (Some(s), HeapRole::Worker) => {
            let available = eks_allocatable_memory_gib(s.memory_gib);
            default_worker_heap(available)
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
        Some(s) => format!("{}Gi", eks_allocatable_memory_gib(s.memory_gib)),
        None => "8Gi".into(),
    };

    let memory: String = Input::new()
        .with_prompt(prompt)
        .default(default)
        .interact_text()?;
    Ok(memory)
}

fn ask_storage(kafka_spec: Option<&InstanceSpec>) -> Result<StorageAnswers> {
    let has_nvme = kafka_spec.is_some_and(|s| s.has_instance_store());

    if has_nvme {
        let spec = kafka_spec.unwrap();
        println!(
            "  Instance store detected: {}x {} GB NVMe",
            spec.nvme_count, spec.nvme_size_gb
        );
        println!("  Local PVs will be created after cluster is up");

        let pv_gb = (spec.total_nvme_gb() as f64 * 0.9) as u32;
        let default_size = format!("{}Gi", pv_gb);

        let size: String = Input::new()
            .with_prompt("PV capacity to advertise")
            .default(default_size)
            .interact_text()?;

        return Ok(StorageAnswers {
            storage_type: "instance-store".into(),
            size,
            performance: None,
            iops: None,
            throughput_mbs: None,
        });
    }

    let size: String = Input::new()
        .with_prompt("EBS volume size")
        .default("500Gi".into())
        .interact_text()?;

    let perf_options = [
        "basic (125 MB/s, 3000 IOPS)",
        "provisioned (custom IOPS & throughput)",
    ];
    let perf_selection = Select::new()
        .with_prompt("gp3 performance")
        .items(&perf_options)
        .default(0)
        .interact()?;

    if perf_selection == 1 {
        let iops: u32 = loop {
            let v: u32 = Input::new()
                .with_prompt("IOPS (max 16000)")
                .default(16000)
                .interact_text()?;
            if v <= 16000 {
                break v;
            }
            println!("  ⚠ gp3 max IOPS is 16000, got {}. Please try again.", v);
        };

        let throughput: u32 = loop {
            let v: u32 = Input::new()
                .with_prompt("Throughput MB/s (max 1000)")
                .default(1000)
                .interact_text()?;
            if v <= 1000 {
                break v;
            }
            println!(
                "  ⚠ gp3 max throughput is 1000 MB/s, got {}. Please try again.",
                v
            );
        };

        Ok(StorageAnswers {
            storage_type: "ebs".into(),
            size,
            performance: Some("provisioned".into()),
            iops: Some(iops),
            throughput_mbs: Some(throughput),
        })
    } else {
        Ok(StorageAnswers {
            storage_type: "ebs".into(),
            size,
            performance: Some("basic".into()),
            iops: None,
            throughput_mbs: None,
        })
    }
}
