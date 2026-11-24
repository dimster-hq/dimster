//! GKE provisioning, teardown, preflight checks, and shell-script generation.
//!
//! Orchestrates `gcloud` (Artifact Registry, GKE Standard single-zone cluster
//! & node pools, orphaned GCE disk cleanup) and `kubectl` (StorageClass, Local
//! NVMe SSD formatting DaemonSet & static PVs) while delegating Dimster config
//! rendering and subprocess execution to `cloud-common`.

use std::process::Command;

use anyhow::{Result, bail};
use cloud_common::config_gen::{
    CoordinatorRenderSpec, CpuManagerSpec, DimsterEnvSpec, KafkaRenderSpec, MonitoringRenderSpec,
    WorkersRenderSpec, save_or_insert_config,
};
use cloud_common::exec::{
    Mode, RunOptions, confirm_and_apply, confirm_and_run, confirm_and_run_inherit,
    delete_cluster_pvcs, emit_pvc_cleanup_script, verify_nodes,
};
use cloud_common::script::Script;

use crate::answers::{Answers, gke_kubectl_context};
use crate::instances::{InstanceSpec, InstanceTypes};
use crate::storage::{
    hardware_storage_summary, local_ssd_node_pool_flag, render_dynamic_storage_class,
    render_local_pv, render_local_storage_class, render_nvme_format_daemonset, storage_class_name,
    validate_storage_answers,
};

const KUBELET_STATIC_CPU_CONFIG_YAML: &str = "kubeletConfig:\n  cpuManagerPolicy: static\n";
/// Compact boot disk size (GB) for GKE COS nodes. GKE's 100 GB default burns
/// 300+ GB of a project's default 500 GB regional Hyperdisk Balanced quota
/// (`HDB_TOTAL_GB`) on 3-node N4/C4 pools before any Kafka PVC is created.
pub const NODE_BOOT_DISK_SIZE_GB_U32: u32 = 50;
const NODE_BOOT_DISK_SIZE_GB: &str = "50";

// ---------------------------------------------------------------------------
// Preflight check (`gke-setup check`)
// ---------------------------------------------------------------------------

/// Verify local CLI tools, active GCP project, impersonated service account,
/// and IAM permissions before provisioning a GKE cluster.
pub fn run_check(project_override: Option<&str>) -> Result<()> {
    println!("Dimster GKE Preflight Check");
    println!("===========================\n");

    let mut all_ok = true;

    // 1. Local tools
    for (bin, args) in [
        ("gcloud", &["--version"][..]),
        ("kubectl", &["version", "--client"][..]),
        ("gke-gcloud-auth-plugin", &["--version"][..]),
    ] {
        match Command::new(bin).args(args).output() {
            Ok(out) if out.status.success() => {
                let first_line = String::from_utf8_lossy(&out.stdout)
                    .lines()
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string();
                println!("  ✓ {} ({})", bin, first_line);
            }
            _ => {
                println!("  ✗ {} not found or failed", bin);
                all_ok = false;
            }
        }
    }

    // 2. Active gcloud config & impersonation
    let account = gcloud_get_value("account").unwrap_or_else(|| "<unset>".into());
    let impersonate = gcloud_get_value("auth/impersonate_service_account");
    let project = project_override
        .map(str::to_string)
        .or_else(|| gcloud_get_value("project"));

    println!("\n  gcloud configuration:");
    println!("    Account:     {}", account);
    match &impersonate {
        Some(sa) => println!("    Impersonate: {}", sa),
        None => println!("    Impersonate: (none — using direct account credentials)"),
    }
    match &project {
        Some(p) => println!("    Project:     {}", p),
        None => {
            println!("    Project:     <unset> — run `gcloud config set project <PROJECT_ID>`");
            all_ok = false;
        }
    }

    // 3. Probe IAM permissions on the target project (best-effort)
    if let Some(ref proj) = project {
        println!("\n  Probing GCP APIs & permissions on '{}'...", proj);
        let required = [
            "container.clusters.create",
            "container.clusters.get",
            "container.clusters.delete",
            "artifactregistry.repositories.create",
            "artifactregistry.repositories.delete",
            "compute.zones.list",
            "compute.disks.list",
            "compute.disks.delete",
        ];

        let mut used_fallback = false;
        let token_out = Command::new("gcloud")
            .args(["auth", "print-access-token"])
            .output();

        match token_out {
            Ok(out) if out.status.success() => {
                let token = String::from_utf8_lossy(&out.stdout).trim().to_string();
                let body = serde_json::json!({ "permissions": required }).to_string();
                let url = format!(
                    "https://cloudresourcemanager.googleapis.com/v1/projects/{}:testIamPermissions",
                    proj
                );
                let resp = Command::new("curl")
                    .args([
                        "-sS",
                        "-X",
                        "POST",
                        "-H",
                        &format!("Authorization: Bearer {}", token),
                        "-H",
                        &format!("X-Goog-User-Project: {}", proj),
                        "-H",
                        "Content-Type: application/json",
                        "-d",
                        &body,
                        &url,
                    ])
                    .output();

                match resp {
                    Ok(r) if r.status.success() => {
                        match parse_test_iam_permissions(&r.stdout) {
                            TestIamOutcome::Permissions(granted) => {
                                for perm in &required {
                                    if granted.iter().any(|g| g == perm) {
                                        println!("    ✓ {}", perm);
                                    } else {
                                        println!("    ✗ {} (missing)", perm);
                                        all_ok = false;
                                    }
                                }
                            }
                            TestIamOutcome::ApiError(msg) => {
                                println!(
                                    "    ⚠ testIamPermissions unavailable ({}) — running direct API probes:",
                                    msg
                                );
                                used_fallback = true;
                            }
                        }
                    }
                    _ => {
                        println!("    ⚠ Could not reach testIamPermissions API — running direct API probes:");
                        used_fallback = true;
                    }
                }
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                println!("    ✗ gcloud auth print-access-token failed: {}", stderr.trim());
                all_ok = false;
            }
            Err(e) => {
                println!("    ✗ Could not invoke gcloud auth: {}", e);
                all_ok = false;
            }
        }

        if used_fallback {
            let probes: &[(&str, &[&str])] = &[
                (
                    "GKE API (container.clusters.list)",
                    &["container", "clusters", "list", "--project", proj, "--limit=1", "--format=value(name)"],
                ),
                (
                    "Artifact Registry API (artifacts.repositories.list)",
                    &["artifacts", "repositories", "list", "--project", proj, "--limit=1", "--format=value(name)"],
                ),
                (
                    "Compute Zones API (compute.zones.list)",
                    &["compute", "zones", "list", "--project", proj, "--limit=1", "--format=value(name)"],
                ),
                (
                    "Compute Disks API (compute.disks.list)",
                    &["compute", "disks", "list", "--project", proj, "--limit=1", "--format=value(name)"],
                ),
            ];
            for (label, args) in probes {
                match Command::new("gcloud").args(*args).output() {
                    Ok(o) if o.status.success() => println!("    ✓ {}", label),
                    Ok(o) => {
                        let err = String::from_utf8_lossy(&o.stderr);
                        let summary = err
                            .lines()
                            .find(|l| !l.trim().is_empty() && !l.starts_with("WARNING:"))
                            .unwrap_or("failed")
                            .trim();
                        println!("    ✗ {} ({})", label, summary);
                        all_ok = false;
                    }
                    Err(e) => {
                        println!("    ✗ {} ({})", label, e);
                        all_ok = false;
                    }
                }
            }
            println!(
                "    ℹ Note: direct probes confirm read access; write permissions (create/delete) are verified when Cloud Resource Manager API is enabled."
            );
        }
    }

    println!(
        "\n  Reminder: GKE cluster creation also requires `roles/iam.serviceAccountUser`"
    );
    println!(
        "  on the node VM service account (default Compute SA or custom --service-account)."
    );
    if !all_ok {
        println!(
            "  To grant all required roles to a service account, run:\n    \
             ./cloud/gke-cli/grant-sa-permissions.sh --project <PROJECT_ID> --sa <SA_EMAIL>"
        );
    } else {
        println!("\n  ✓ Preflight checks passed!");
    }

    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum TestIamOutcome {
    Permissions(Vec<String>),
    ApiError(String),
}

/// Distinguish an API error (e.g., Cloud Resource Manager API disabled) from
/// a valid empty permissions response so `gke-setup check` never misreports an
/// API error as missing IAM roles.
fn parse_test_iam_permissions(body: &[u8]) -> TestIamOutcome {
    let parsed: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return TestIamOutcome::ApiError(format!("invalid JSON: {}", e)),
    };
    if let Some(err) = parsed.get("error") {
        let status = err
            .get("status")
            .and_then(|s| s.as_str())
            .unwrap_or("ERROR");
        let msg = err
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown error");
        let short_msg = msg.split('.').next().unwrap_or(msg);
        return TestIamOutcome::ApiError(format!("{}: {}", status, short_msg));
    }
    let perms = parsed
        .get("permissions")
        .and_then(|p| p.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    TestIamOutcome::Permissions(perms)
}

fn gcloud_get_value(key: &str) -> Option<String> {
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

// ---------------------------------------------------------------------------
// Create orchestration
// ---------------------------------------------------------------------------

pub fn run_all(
    answers: &Answers,
    instance_types: &InstanceTypes,
    opts: RunOptions,
) -> Result<()> {
    // Validate storage config upfront before provisioning any GCP resources.
    if !answers.is_external() {
        let kafka_spec = instance_types.lookup(&answers.kafka.instance_type);
        let warnings = validate_storage_answers(&answers.storage, kafka_spec)?;
        if opts.mode != Mode::GenerateScript {
            for w in warnings {
                println!("  ⚠ {}", w);
            }
        }
    }

    if opts.mode == Mode::GenerateScript {
        let script = generate_create_script(answers, instance_types)?;
        print!("{}", script);
        return Ok(());
    }

    step_1_registry(answers, opts)?;
    step_2_gke(answers, instance_types, opts)?;
    step_3_verify_csi(answers, instance_types, opts)?;

    if answers.is_external() {
        println!("\n=== Step 4/5: Create StorageClass — skipped (external) ===\n");
    } else {
        step_4_storage(answers, opts)?;
    }

    let inserted = step_5_generate(answers, opts)?;

    println!("\n=== Done! ===\n");
    let name = &answers.gcp.dimster_name;
    let region = &answers.gcp.region;
    let env_name = &answers.env_name;

    println!("  Next steps:");
    let mut step = 1;
    if inserted {
        println!("    ✓ Config updated in dimster-config.yaml");
    } else {
        println!("    {}. Review dimster-config-{}.yaml", step, name);
        step += 1;
        println!(
            "    {}. Copy: cp dimster-config-{}.yaml dimster-config.yaml",
            step, name
        );
        step += 1;
    }
    if answers.gcp.existing_image.is_none() {
        println!(
            "    {}. Configure Docker auth for Artifact Registry (one-time per region):",
            step
        );
        println!(
            "       gcloud auth configure-docker {}-docker.pkg.dev",
            region
        );
        println!(
            "       # Or with explicit short-lived token (works with SA impersonation):"
        );
        println!(
            "       # gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://{}-docker.pkg.dev",
            region
        );
        step += 1;
    }
    println!("    {}. Build and push: ./dimster build {}", step, env_name);
    step += 1;
    println!("    {}. Deploy: ./dimster deploy {}", step, env_name);

    Ok(())
}

// ---------------------------------------------------------------------------
// Pure command builders (shared between Execute, DryRun, GenerateScript & tests)
// ---------------------------------------------------------------------------

/// Build `gcloud container clusters create` arguments.
///
/// Reuses GKE's initial `default-pool` directly as the 1-node `role=infra` pool
/// (coordinator, Prometheus, Grafana), saving ~4 minutes per cluster creation.
pub fn build_cluster_create_args(
    answers: &Answers,
    instance_types: &InstanceTypes,
) -> Vec<String> {
    let cluster_name = answers.cluster_name();
    let infra_boot_disk = instance_types.boot_disk_for(&answers.coordinator.infra_instance_type);

    let mut args = vec![
        "container".into(),
        "clusters".into(),
        "create".into(),
        cluster_name,
        "--project".into(),
        answers.gcp.project.clone(),
        "--zone".into(),
        answers.gcp.zone.clone(),
        "--machine-type".into(),
        answers.coordinator.infra_instance_type.clone(),
        "--disk-type".into(),
        infra_boot_disk.into(),
        "--disk-size".into(),
        NODE_BOOT_DISK_SIZE_GB.into(),
        "--num-nodes".into(),
        "1".into(),
        "--node-labels".into(),
        "role=infra".into(),
        "--enable-gvnic".into(),
        "--addons".into(),
        "GcePersistentDiskCsiDriver".into(),
    ];

    if answers.gcp.dataplane_v2 {
        args.push("--enable-dataplane-v2".into());
    }

    if answers.gcp.release_channel == "none" {
        args.push("--release-channel".into());
        args.push("None".into());
        if let Some(ref ver) = answers.gcp.gke_version {
            args.push("--cluster-version".into());
            args.push(ver.clone());
        }
    } else {
        args.push("--release-channel".into());
        args.push(answers.gcp.release_channel.clone());
        if let Some(ref ver) = answers.gcp.gke_version {
            args.push("--cluster-version".into());
            args.push(ver.clone());
        }
    }

    if let Some(ref net) = answers.gcp.network {
        args.push("--network".into());
        args.push(net.clone());
    }
    if let Some(ref subnet) = answers.gcp.subnetwork {
        args.push("--subnetwork".into());
        args.push(subnet.clone());
    }
    if let Some(ref sa) = answers.gcp.node_service_account {
        args.push("--service-account".into());
        args.push(sa.clone());
    }

    args
}

/// Build `gcloud container node-pools create kafka` arguments.
pub fn build_kafka_pool_args(
    answers: &Answers,
    instance_types: &InstanceTypes,
    system_config_path: Option<&str>,
) -> Vec<String> {
    let kafka_spec: Option<&InstanceSpec> = instance_types.lookup(&answers.kafka.instance_type);
    let boot_disk = instance_types.boot_disk_for(&answers.kafka.instance_type);

    let mut args = vec![
        "container".into(),
        "node-pools".into(),
        "create".into(),
        "kafka".into(),
        "--cluster".into(),
        answers.cluster_name(),
        "--project".into(),
        answers.gcp.project.clone(),
        "--zone".into(),
        answers.gcp.zone.clone(),
        "--machine-type".into(),
        answers.kafka.instance_type.clone(),
        "--disk-type".into(),
        boot_disk.into(),
        "--disk-size".into(),
        NODE_BOOT_DISK_SIZE_GB.into(),
        "--num-nodes".into(),
        answers.kafka.count.to_string(),
        "--node-labels".into(),
        "role=kafka".into(),
        "--enable-gvnic".into(),
    ];

    if let Some(ssd_flag) = local_ssd_node_pool_flag(&answers.storage, kafka_spec) {
        args.push(ssd_flag);
    }
    if answers.kafka.tier1_networking {
        args.push("--network-performance-configs=total-egress-bandwidth-tier=TIER_1".into());
    }
    if let Some(path) = system_config_path {
        args.push("--system-config-from-file".into());
        args.push(path.into());
    }
    if let Some(ref sa) = answers.gcp.node_service_account {
        args.push("--service-account".into());
        args.push(sa.clone());
    }

    args
}

/// Build `gcloud container node-pools create workers` arguments.
pub fn build_workers_pool_args(
    answers: &Answers,
    instance_types: &InstanceTypes,
    system_config_path: Option<&str>,
) -> Vec<String> {
    let boot_disk = instance_types.boot_disk_for(&answers.workers.instance_type);

    let mut args = vec![
        "container".into(),
        "node-pools".into(),
        "create".into(),
        "workers".into(),
        "--cluster".into(),
        answers.cluster_name(),
        "--project".into(),
        answers.gcp.project.clone(),
        "--zone".into(),
        answers.gcp.zone.clone(),
        "--machine-type".into(),
        answers.workers.instance_type.clone(),
        "--disk-type".into(),
        boot_disk.into(),
        "--disk-size".into(),
        NODE_BOOT_DISK_SIZE_GB.into(),
        "--num-nodes".into(),
        answers.workers.count.to_string(),
        "--enable-autoscaling".into(),
        "--min-nodes".into(),
        answers.workers.min.to_string(),
        "--max-nodes".into(),
        answers.workers.max.to_string(),
        "--node-labels".into(),
        "role=worker".into(),
        "--enable-gvnic".into(),
    ];

    if answers.workers.tier1_networking {
        args.push("--network-performance-configs=total-egress-bandwidth-tier=TIER_1".into());
    }
    if let Some(path) = system_config_path {
        args.push("--system-config-from-file".into());
        args.push(path.into());
    }
    if let Some(ref sa) = answers.gcp.node_service_account {
        args.push("--service-account".into());
        args.push(sa.clone());
    }

    args
}

/// Build the shared `DimsterEnvSpec` used for standalone, script, and in-place config output.
pub fn build_dimster_env_spec(answers: &Answers, context: String) -> DimsterEnvSpec {
    let gke_label = answers
        .gcp
        .gke_version
        .as_deref()
        .unwrap_or(&answers.gcp.release_channel);

    let hardware = if answers.is_external() {
        format!(
            "external, workers={}x{}, region={}, zone={}, gke={}",
            answers.workers.count,
            answers.workers.instance_type,
            answers.gcp.region,
            answers.gcp.zone,
            gke_label
        )
    } else {
        format!(
            "kafka={}x{}, workers={}x{}, storage={}, region={}, zone={}, gke={}",
            answers.kafka.count,
            answers.kafka.instance_type,
            answers.workers.count,
            answers.workers.instance_type,
            hardware_storage_summary(&answers.storage),
            answers.gcp.region,
            answers.gcp.zone,
            gke_label
        )
    };

    let kafka = if answers.is_external() {
        None
    } else {
        Some(KafkaRenderSpec {
            replicas: answers.kafka.count,
            version: answers.kafka.version.clone(),
            pvc_size: answers.storage.size.clone(),
            pvc_storage_class: storage_class_name(&answers.storage).to_string(),
            heap: answers.kafka.heap.clone(),
            cpu: answers.kafka.cpu.clone(),
            memory: answers.kafka.memory.clone(),
            broker_config: answers.kafka.broker_config.clone(),
            extra_config: answers.kafka.extra_config.clone(),
        })
    };

    let cpu_manager = if answers.cpu_manager_static {
        Some(CpuManagerSpec {
            policy: "static".into(),
            full_pcpus_only: false,
        })
    } else {
        None
    };

    let system = if answers.is_external() {
        None
    } else {
        Some(format!("Apache Kafka {}", answers.kafka.version))
    };

    DimsterEnvSpec {
        env_name: answers.env_name.clone(),
        image: answers.image_repo(),
        context,
        hardware,
        system,
        kafka_client_version: answers.kafka_client_version.clone(),
        deploy_mode: answers.deploy_mode.clone(),
        bootstrap_servers: answers.bootstrap_servers.clone(),
        cpu_manager,
        kafka,
        workers: WorkersRenderSpec {
            replicas: answers.workers.count,
            heap: answers.workers.heap.clone(),
            cpu: answers.workers.cpu.clone(),
            memory: answers.workers.memory.clone(),
        },
        coordinator: CoordinatorRenderSpec {
            heap: answers.coordinator.heap.clone(),
            cpu: answers.coordinator.cpu.clone(),
            memory: answers.coordinator.memory.clone(),
        },
        monitoring: MonitoringRenderSpec {
            cgroup_metrics: true,
            prometheus_cpu: answers.monitoring.prometheus_cpu.clone(),
            prometheus_memory: answers.monitoring.prometheus_memory.clone(),
            grafana_cpu: answers.monitoring.grafana_cpu.clone(),
            grafana_memory: answers.monitoring.grafana_memory.clone(),
        },
    }
}

// ---------------------------------------------------------------------------
// Step implementations (Execute / DryRun)
// ---------------------------------------------------------------------------

fn step_1_registry(answers: &Answers, opts: RunOptions) -> Result<()> {
    println!("\n=== Step 1/5: Artifact Registry Repository ===\n");

    if let Some(ref existing) = answers.gcp.existing_image {
        println!("  ✓ Using existing image repository: {}", existing);
        return Ok(());
    }

    let name = &answers.gcp.dimster_name;
    let region = &answers.gcp.region;
    let project = &answers.gcp.project;

    let create_args = [
        "artifacts",
        "repositories",
        "create",
        name,
        "--repository-format=docker",
        "--location",
        region,
        "--project",
        project,
    ];

    if opts.mode == Mode::DryRun {
        println!("  [dry-run] gcloud {}", create_args.join(" "));
        return Ok(());
    }

    let check = Command::new("gcloud")
        .args([
            "artifacts",
            "repositories",
            "describe",
            name,
            "--location",
            region,
            "--project",
            project,
        ])
        .output()?;

    if check.status.success() {
        println!("  ✓ Artifact Registry repository '{}' already exists", name);
        return Ok(());
    }

    confirm_and_run(opts, "gcloud", &create_args)?;
    println!("  ✓ Artifact Registry repository '{}' created", name);
    Ok(())
}

fn step_2_gke(
    answers: &Answers,
    instance_types: &InstanceTypes,
    opts: RunOptions,
) -> Result<()> {
    println!("\n=== Step 2/5: Create GKE Cluster & Node Pools ===\n");

    let cluster_name = answers.cluster_name();
    let context = answers.kubectl_context();
    let cluster_args = build_cluster_create_args(answers, instance_types);
    let sys_cfg_path = if answers.cpu_manager_static {
        Some(format!(
            "/tmp/gke-system-config-{}.yaml",
            answers.gcp.dimster_name
        ))
    } else {
        None
    };

    let kafka_args = if answers.is_external() {
        None
    } else {
        Some(build_kafka_pool_args(
            answers,
            instance_types,
            sys_cfg_path.as_deref(),
        ))
    };
    let workers_args = build_workers_pool_args(answers, instance_types, sys_cfg_path.as_deref());

    if opts.mode == Mode::DryRun {
        println!("  [dry-run] gcloud {}", cluster_args.join(" "));
        println!(
            "  [dry-run] gcloud container clusters get-credentials {} --zone {} --project {}",
            cluster_name, answers.gcp.zone, answers.gcp.project
        );
        if let Some(ref p) = sys_cfg_path {
            println!("  [dry-run] Write kubelet static CPU config to {}", p);
        }
        if let Some(ref ka) = kafka_args {
            println!("  [dry-run] gcloud {}", ka.join(" "));
        }
        println!("  [dry-run] gcloud {}", workers_args.join(" "));
        return Ok(());
    }

    if let Some(ref p) = sys_cfg_path {
        std::fs::write(p, KUBELET_STATIC_CPU_CONFIG_YAML)?;
        println!("  Wrote kubelet static CPU config to {}", p);
    }

    let check = Command::new("gcloud")
        .args([
            "container",
            "clusters",
            "describe",
            &cluster_name,
            "--zone",
            &answers.gcp.zone,
            "--project",
            &answers.gcp.project,
        ])
        .output()?;

    if check.status.success() {
        println!("  ✓ GKE cluster '{}' already exists", cluster_name);
    } else {
        let arg_refs: Vec<&str> = cluster_args.iter().map(String::as_str).collect();
        confirm_and_run_inherit(opts, "gcloud", &arg_refs)?;
        println!("  ✓ GKE cluster '{}' created", cluster_name);
    }

    // Fetch kubeconfig credentials so kubectl targets this cluster.
    println!("  Updating kubeconfig...");
    let creds = Command::new("gcloud")
        .args([
            "container",
            "clusters",
            "get-credentials",
            &cluster_name,
            "--zone",
            &answers.gcp.zone,
            "--project",
            &answers.gcp.project,
        ])
        .output()?;
    if !creds.status.success() {
        let stderr = String::from_utf8_lossy(&creds.stderr);
        bail!("gcloud get-credentials failed: {}", stderr.trim());
    }
    println!("  ✓ Kubeconfig updated ({})", context);

    // Ensure kafka and workers node pools exist (idempotent).
    let existing_pools = list_node_pools(answers)?;
    if let Some(ref ka) = kafka_args {
        if existing_pools.iter().any(|p| p == "kafka") {
            println!("  ✓ Node pool 'kafka' already exists");
        } else {
            let refs: Vec<&str> = ka.iter().map(String::as_str).collect();
            confirm_and_run_inherit(opts, "gcloud", &refs)?;
            println!("  ✓ Node pool 'kafka' created");
        }
    }

    if existing_pools.iter().any(|p| p == "workers") {
        println!("  ✓ Node pool 'workers' already exists");
    } else {
        let refs: Vec<&str> = workers_args.iter().map(String::as_str).collect();
        confirm_and_run_inherit(opts, "gcloud", &refs)?;
        println!("  ✓ Node pool 'workers' created");
    }

    verify_nodes(Some(&context))?;
    Ok(())
}

fn list_node_pools(answers: &Answers) -> Result<Vec<String>> {
    let out = Command::new("gcloud")
        .args([
            "container",
            "node-pools",
            "list",
            "--cluster",
            &answers.cluster_name(),
            "--zone",
            &answers.gcp.zone,
            "--project",
            &answers.gcp.project,
            "--format",
            "value(name)",
        ])
        .output()?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        bail!("Failed to list node pools: {}", stderr.trim());
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect())
}

fn step_3_verify_csi(
    answers: &Answers,
    instance_types: &InstanceTypes,
    opts: RunOptions,
) -> Result<()> {
    println!("\n=== Step 3/5: Verify GKE Persistent Disk CSI Driver ===\n");

    let context = answers.kubectl_context();
    if opts.mode == Mode::DryRun {
        println!(
            "  [dry-run] kubectl --context {} get csidrivers pd.csi.storage.gke.io",
            context
        );
        return Ok(());
    }

    let check = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "get",
            "csidrivers",
            "pd.csi.storage.gke.io",
        ])
        .output()?;
    if check.status.success() {
        println!("  ✓ Compute Engine PD CSI driver (pd.csi.storage.gke.io) active");
    } else {
        println!("  ⚠ pd.csi.storage.gke.io not yet visible — cluster addons may still be starting");
    }

    if instance_types
        .lookup(&answers.coordinator.infra_instance_type)
        .is_some_and(|s| s.is_hyperdisk_only())
    {
        println!(
            "  ⚠ Infra node '{}' is Hyperdisk-only; ensure your default StorageClass supports Hyperdisk for benchmark-results PVC.",
            answers.coordinator.infra_instance_type
        );
    }

    Ok(())
}

fn step_4_storage(answers: &Answers, opts: RunOptions) -> Result<()> {
    println!("\n=== Step 4/5: Create StorageClass ===\n");

    let context = answers.kubectl_context();
    if answers.storage.storage_type == "local-ssd" {
        setup_local_ssd_storage(answers, &context, opts)
    } else {
        let manifest = render_dynamic_storage_class(
            &answers.storage,
            &answers.cluster_name(),
            &answers.gcp.dimster_name,
        )?;
        let sc_name = storage_class_name(&answers.storage);

        if opts.mode == Mode::DryRun {
            println!("  [dry-run] kubectl --context {} apply StorageClass '{}':", context, sc_name);
            for line in manifest.lines() {
                println!("    {}", line);
            }
            return Ok(());
        }

        confirm_and_apply(
            opts,
            Some(&context),
            &format!("StorageClass '{}'", sc_name),
            &manifest,
        )?;
        println!("  ✓ StorageClass '{}' created", sc_name);
        Ok(())
    }
}

fn setup_local_ssd_storage(answers: &Answers, context: &str, opts: RunOptions) -> Result<()> {
    let sc_manifest = render_local_storage_class();
    let ds_manifest = render_nvme_format_daemonset();

    if opts.mode == Mode::DryRun {
        println!("  [dry-run] kubectl --context {} apply StorageClass 'local-storage'", context);
        println!("  [dry-run] kubectl --context {} apply DaemonSet 'nvme-format'", context);
        println!(
            "  [dry-run] Create static local PVs ({} each) for all role=kafka nodes",
            answers.storage.size
        );
        return Ok(());
    }

    confirm_and_apply(opts, Some(context), "local-storage StorageClass", sc_manifest)?;
    confirm_and_apply(opts, Some(context), "NVMe format DaemonSet", &ds_manifest)?;

    println!("  Waiting for NVMe format DaemonSet on all kafka nodes...");
    let _ = Command::new("kubectl")
        .args([
            "--context",
            context,
            "rollout",
            "status",
            "daemonset/nvme-format",
            "-n",
            "kube-system",
            "--timeout=180s",
        ])
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status();

    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "get",
            "nodes",
            "-l",
            "role=kafka",
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ])
        .output()?;

    let nodes_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if nodes_str.is_empty() {
        bail!("No nodes found with label role=kafka");
    }

    let nodes: Vec<&str> = nodes_str.split_whitespace().collect();
    println!("  Found {} kafka node(s): {}", nodes.len(), nodes.join(", "));

    for (i, node) in nodes.iter().enumerate() {
        let pv = render_local_pv(i, &answers.storage.size, node);
        confirm_and_apply(
            opts,
            Some(context),
            &format!("PV kafka-nvme-{} on {}", i, node),
            &pv,
        )?;
    }

    println!("  ✓ Local NVMe storage configured ({} PVs)", nodes.len());
    println!("  ℹ NVMe format DaemonSet left running to maintain mount propagation");
    Ok(())
}

fn step_5_generate(answers: &Answers, opts: RunOptions) -> Result<bool> {
    println!("\n=== Step 5/5: Generate Configuration ===\n");

    let spec = build_dimster_env_spec(answers, answers.kubectl_context());
    let answers_path = format!("gke-answers-{}.yaml", answers.gcp.dimster_name);

    let inserted = save_or_insert_config(
        &spec,
        "gke-setup",
        &answers.gcp.dimster_name,
        &answers_path,
        opts,
    )?;

    if opts.mode != Mode::DryRun {
        answers.save(&answers_path)?;
        println!("  ✓ Wrote {}", answers_path);
    }

    Ok(inserted)
}

// ---------------------------------------------------------------------------
// Script generation (`--generate-script`)
// ---------------------------------------------------------------------------

pub fn generate_create_script(
    answers: &Answers,
    instance_types: &InstanceTypes,
) -> Result<String> {
    let mut s = Script::new();
    let name = &answers.gcp.dimster_name;
    let project = &answers.gcp.project;
    let region = &answers.gcp.region;
    let zone = &answers.gcp.zone;
    let cluster_name = answers.cluster_name();
    let context = answers.kubectl_context();

    s.cmd("#!/usr/bin/env bash");
    s.cmd("set -euo pipefail");
    s.blank();
    s.comment(&format!("GKE setup script for '{}'", name));
    s.comment("Generated by gke-setup. Review and edit before running.");

    s.section("Variables");
    s.cmd(&format!("GCP_PROJECT=\"{}\"", project));
    s.cmd(&format!("GCP_REGION=\"{}\"", region));
    s.cmd(&format!("GCP_ZONE=\"{}\"", zone));
    s.cmd(&format!("DIMSTER_NAME=\"{}\"", name));
    s.cmd(&format!("CLUSTER_NAME=\"{}\"", cluster_name));
    s.cmd(&format!("KUBE_CONTEXT=\"{}\"", context));

    // Step 1: Artifact Registry
    s.section("Step 1: Create Artifact Registry Repository");
    if let Some(ref existing) = answers.gcp.existing_image {
        s.comment(&format!("Skipped — using existing image repo: {}", existing));
    } else {
        s.cmd("if ! gcloud artifacts repositories describe \"$DIMSTER_NAME\" --location=\"$GCP_REGION\" --project=\"$GCP_PROJECT\" &>/dev/null; then");
        s.cmd("  gcloud artifacts repositories create \"$DIMSTER_NAME\" \\");
        s.cmd("    --repository-format=docker \\");
        s.cmd("    --location=\"$GCP_REGION\" \\");
        s.cmd("    --project=\"$GCP_PROJECT\"");
        s.cmd("  echo \"Artifact Registry repository created\"");
        s.cmd("else");
        s.cmd("  echo \"Artifact Registry repository already exists\"");
        s.cmd("fi");
    }

    // Step 2: GKE Cluster & Node Pools
    s.section("Step 2: Create GKE Cluster & Node Pools");

    let sys_cfg_path = if answers.cpu_manager_static {
        let p = format!("/tmp/gke-system-config-{}.yaml", name);
        s.heredoc(
            &format!("cat > \"{}\"", p),
            KUBELET_STATIC_CPU_CONFIG_YAML,
        );
        s.blank();
        Some(p)
    } else {
        None
    };

    let cluster_args = build_cluster_create_args(answers, instance_types);
    s.cmd("if ! gcloud container clusters describe \"$CLUSTER_NAME\" --zone=\"$GCP_ZONE\" --project=\"$GCP_PROJECT\" &>/dev/null; then");
    s.cmd("  echo \"Creating GKE cluster $CLUSTER_NAME (typically 5-8 minutes)...\"");
    s.cmd(&format!("  gcloud {}", shell_join(&cluster_args)));
    s.cmd("else");
    s.cmd("  echo \"GKE cluster $CLUSTER_NAME already exists\"");
    s.cmd("fi");
    s.blank();

    s.cmd("gcloud container clusters get-credentials \"$CLUSTER_NAME\" --zone=\"$GCP_ZONE\" --project=\"$GCP_PROJECT\"");
    s.blank();

    if !answers.is_external() {
        let kafka_args = build_kafka_pool_args(answers, instance_types, sys_cfg_path.as_deref());
        s.cmd("if ! gcloud container node-pools describe kafka --cluster=\"$CLUSTER_NAME\" --zone=\"$GCP_ZONE\" --project=\"$GCP_PROJECT\" &>/dev/null; then");
        s.cmd(&format!("  gcloud {}", shell_join(&kafka_args)));
        s.cmd("else");
        s.cmd("  echo \"Node pool 'kafka' already exists\"");
        s.cmd("fi");
        s.blank();
    }

    let workers_args = build_workers_pool_args(answers, instance_types, sys_cfg_path.as_deref());
    s.cmd("if ! gcloud container node-pools describe workers --cluster=\"$CLUSTER_NAME\" --zone=\"$GCP_ZONE\" --project=\"$GCP_PROJECT\" &>/dev/null; then");
    s.cmd(&format!("  gcloud {}", shell_join(&workers_args)));
    s.cmd("else");
    s.cmd("  echo \"Node pool 'workers' already exists\"");
    s.cmd("fi");
    s.blank();

    s.cmd("kubectl --context \"$KUBE_CONTEXT\" get nodes -o custom-columns=NAME:.metadata.name,ZONE:.metadata.labels.topology\\\\.kubernetes\\\\.io/zone,ROLE:.metadata.labels.role");

    // Step 3: Verify PD CSI driver
    s.section("Step 3: Verify GKE Persistent Disk CSI Driver");
    s.cmd("kubectl --context \"$KUBE_CONTEXT\" get csidrivers pd.csi.storage.gke.io");

    // Step 4: StorageClass
    s.section("Step 4: Create StorageClass");
    if answers.is_external() {
        s.comment("Skipped — external deploy mode");
    } else if answers.storage.storage_type == "local-ssd" {
        s.heredoc(
            "kubectl --context \"$KUBE_CONTEXT\" apply -f -",
            render_local_storage_class(),
        );
        s.blank();
        s.heredoc(
            "kubectl --context \"$KUBE_CONTEXT\" apply -f -",
            &render_nvme_format_daemonset(),
        );
        s.blank();
        s.cmd("kubectl --context \"$KUBE_CONTEXT\" rollout status daemonset/nvme-format -n kube-system --timeout=180s || true");
        s.blank();
        s.cmd("KAFKA_NODES=$(kubectl --context \"$KUBE_CONTEXT\" get nodes -l role=kafka -o jsonpath='{.items[*].metadata.name}')");
        s.cmd("INDEX=0");
        s.cmd("for NODE in $KAFKA_NODES; do");
        s.cmd(&format!(
            "  kubectl --context \"$KUBE_CONTEXT\" apply -f - <<PV_EOF\n{}PV_EOF",
            render_local_pv(0, &answers.storage.size, "$NODE")
                .replace("kafka-nvme-0", "kafka-nvme-$INDEX")
        ));
        s.cmd("  INDEX=$((INDEX + 1))");
        s.cmd("done");
    } else {
        let sc = render_dynamic_storage_class(&answers.storage, &cluster_name, name)?;
        s.heredoc("kubectl --context \"$KUBE_CONTEXT\" apply -f -", &sc);
    }

    // Step 5: Generate dimster-config-{name}.yaml
    s.section("Step 5: Generate Dimster Configuration");
    let spec = build_dimster_env_spec(answers, context);
    let config_yaml = spec.render_standalone_config("gke-setup", name)?;
    let config_path = format!("dimster-config-{}.yaml", name);
    s.heredoc_with_tag(
        &format!("cat > \"{}\"", config_path),
        "CONFIG_EOF",
        true,
        &config_yaml,
    );
    s.cmd(&format!("echo \"Wrote {}\"", config_path));
    s.blank();
    s.cmd("echo \"\"");
    s.cmd("echo \"Done! Next steps:\"");
    s.cmd(&format!("echo \"  1. Review {}\"", config_path));
    s.cmd(&format!(
        "echo \"  2. Copy: cp {} dimster-config.yaml\"",
        config_path
    ));
    if answers.gcp.existing_image.is_none() {
        s.cmd(&format!(
            "echo \"  3. Configure Docker auth: gcloud auth configure-docker {}-docker.pkg.dev\"",
            region
        ));
    }
    s.cmd(&format!(
        "echo \"  4. Build and push: ./dimster build {}\"",
        answers.env_name
    ));
    s.cmd(&format!(
        "echo \"  5. Deploy: ./dimster deploy {}\"",
        answers.env_name
    ));

    Ok(s.render())
}

// ---------------------------------------------------------------------------
// Delete orchestration & script generation
// ---------------------------------------------------------------------------

pub fn delete_all(
    dimster_name: &str,
    project: &str,
    region: &str,
    zone: &str,
    keep_registry: bool,
    opts: RunOptions,
) -> Result<()> {
    if opts.mode == Mode::GenerateScript {
        let script = generate_delete_script(dimster_name, project, region, zone, keep_registry);
        print!("{}", script);
        return Ok(());
    }

    let cluster_name = format!("{}-benchmark", dimster_name);
    let context = gke_kubectl_context(project, zone, &cluster_name);

    // Step 1: Graceful PVC delete pinned explicitly to this cluster's context.
    println!("\n=== Step 1/4: Delete PVCs (graceful) ===\n");
    delete_cluster_pvcs(
        opts,
        Some(&context),
        &format!("GKE cluster '{}' in {}", cluster_name, zone),
    )?;

    // Step 2: Delete GKE cluster
    println!("\n=== Step 2/4: Delete GKE Cluster ===\n");
    let del_args = [
        "container",
        "clusters",
        "delete",
        &cluster_name,
        "--zone",
        zone,
        "--project",
        project,
        "--quiet",
    ];
    if opts.mode == Mode::DryRun {
        println!("  [dry-run] gcloud {}", del_args.join(" "));
    } else {
        let check = Command::new("gcloud")
            .args([
                "container",
                "clusters",
                "describe",
                &cluster_name,
                "--zone",
                zone,
                "--project",
                project,
            ])
            .output()?;
        if !check.status.success() {
            println!(
                "  ✓ GKE cluster '{}' does not exist (already deleted)",
                cluster_name
            );
        } else {
            confirm_and_run_inherit(opts, "gcloud", &del_args)?;
            println!("  ✓ GKE cluster '{}' deleted", cluster_name);
        }
    }

    // Step 3: Sweep orphaned GCE disks labeled with this cluster (graceful on 403)
    println!("\n=== Step 3/4: Sweep Orphaned GCE Disks ===\n");
    sweep_orphaned_disks(&cluster_name, project, zone, opts)?;

    // Step 4: Delete Artifact Registry repository
    println!("\n=== Step 4/4: Delete Artifact Registry Repository ===\n");
    if keep_registry {
        println!("  Skipped (--keep-registry)");
    } else if opts.mode == Mode::DryRun {
        println!(
            "  [dry-run] gcloud artifacts repositories delete {} --location {} --project {} --quiet",
            dimster_name, region, project
        );
    } else {
        let check = Command::new("gcloud")
            .args([
                "artifacts",
                "repositories",
                "describe",
                dimster_name,
                "--location",
                region,
                "--project",
                project,
            ])
            .output()?;
        if !check.status.success() {
            println!(
                "  ✓ Artifact Registry repository '{}' does not exist (already deleted)",
                dimster_name
            );
        } else {
            confirm_and_run(
                opts,
                "gcloud",
                &[
                    "artifacts",
                    "repositories",
                    "delete",
                    dimster_name,
                    "--location",
                    region,
                    "--project",
                    project,
                    "--quiet",
                ],
            )?;
            println!(
                "  ✓ Artifact Registry repository '{}' deleted",
                dimster_name
            );
        }
    }

    println!("\n=== Done! ===\n");
    println!(
        "  Note: Generated config files (dimster-config-{}.yaml, gke-answers-{}.yaml)",
        dimster_name, dimster_name
    );
    println!("  were left on disk. Delete them manually if no longer needed.");

    Ok(())
}

/// Extract names of unattached GCE disks belonging to `cluster_name` from
/// `gcloud compute disks list --format=json` output.
///
/// Matches either our custom StorageClass label (`dimster-cluster`) or GKE's
/// built-in cluster label (`goog-k8s-cluster-name`, which covers default
/// `standard-rwo` PVCs such as `benchmark-results` and `prometheus-data`).
/// Filtering `users` client-side avoids GCE's invalid server-side `-users:*`
/// filter on repeated fields.
pub fn find_orphaned_cluster_disks(json_output: &str, cluster_name: &str) -> Result<Vec<String>> {
    if json_output.trim().is_empty() {
        return Ok(Vec::new());
    }
    let disks: Vec<serde_json::Value> = serde_json::from_str(json_output)?;
    let mut orphaned = Vec::new();
    for disk in disks {
        let attached = disk
            .get("users")
            .and_then(|u| u.as_array())
            .is_some_and(|arr| !arr.is_empty());
        if attached {
            continue;
        }
        let Some(labels) = disk.get("labels").and_then(|l| l.as_object()) else {
            continue;
        };
        let matches_cluster = labels
            .get("dimster-cluster")
            .and_then(|v| v.as_str())
            .is_some_and(|v| v == cluster_name)
            || labels
                .get("goog-k8s-cluster-name")
                .and_then(|v| v.as_str())
                .is_some_and(|v| v == cluster_name);
        if !matches_cluster {
            continue;
        }
        if let Some(name) = disk.get("name").and_then(|n| n.as_str()) {
            orphaned.push(name.to_string());
        }
    }
    Ok(orphaned)
}

fn sweep_orphaned_disks(
    cluster_name: &str,
    project: &str,
    zone: &str,
    opts: RunOptions,
) -> Result<()> {
    let label_filter = format!(
        "labels.dimster-cluster={} OR labels.goog-k8s-cluster-name={}",
        cluster_name, cluster_name
    );

    if opts.mode == Mode::DryRun {
        println!(
            "  [dry-run] gcloud compute disks list --project {} --zones {} --filter \"{}\" --format json",
            project, zone, label_filter
        );
        println!(
            "  [dry-run] for each unattached disk (users empty): gcloud compute disks delete <name> --zone {} --project {} --quiet",
            zone, project
        );
        return Ok(());
    }

    let output = Command::new("gcloud")
        .args([
            "compute",
            "disks",
            "list",
            "--project",
            project,
            "--zones",
            zone,
            "--filter",
            &label_filter,
            "--format",
            "json",
        ])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        println!(
            "  ⚠ Could not list GCE disks (missing compute.disks.list permission?): {}",
            stderr.trim()
        );
        println!("    Check manually with:");
        println!(
            "      gcloud compute disks list --project {} --zones {} --filter \"{}\"",
            project, zone, label_filter
        );
        return Ok(());
    }

    let raw = String::from_utf8_lossy(&output.stdout);
    let disks = find_orphaned_cluster_disks(&raw, cluster_name)?;

    if disks.is_empty() {
        println!("  ✓ No orphaned GCE disks found for cluster '{}'", cluster_name);
        return Ok(());
    }

    println!(
        "  Found {} orphaned GCE disk(s) for cluster '{}':",
        disks.len(),
        cluster_name
    );
    for d in &disks {
        println!("    - {}", d);
    }

    for disk in &disks {
        confirm_and_run(
            opts,
            "gcloud",
            &[
                "compute",
                "disks",
                "delete",
                disk,
                "--zone",
                zone,
                "--project",
                project,
                "--quiet",
            ],
        )?;
        println!("  ✓ Deleted disk {}", disk);
    }
    Ok(())
}

pub fn generate_delete_script(
    dimster_name: &str,
    project: &str,
    region: &str,
    zone: &str,
    keep_registry: bool,
) -> String {
    let mut s = Script::new();
    let cluster_name = format!("{}-benchmark", dimster_name);
    let context = gke_kubectl_context(project, zone, &cluster_name);

    s.cmd("#!/usr/bin/env bash");
    s.cmd("set -euo pipefail");
    s.blank();
    s.comment(&format!("GKE teardown script for '{}'", dimster_name));
    s.comment("Generated by gke-setup. Review before running.");

    s.section("Variables");
    s.cmd(&format!("GCP_PROJECT=\"{}\"", project));
    s.cmd(&format!("GCP_REGION=\"{}\"", region));
    s.cmd(&format!("GCP_ZONE=\"{}\"", zone));
    s.cmd(&format!("DIMSTER_NAME=\"{}\"", dimster_name));
    s.cmd(&format!("CLUSTER_NAME=\"{}\"", cluster_name));
    s.cmd(&format!("KUBE_CONTEXT=\"{}\"", context));

    s.section("Step 1: Delete PVCs (graceful)");
    s.cmd("if kubectl --context \"$KUBE_CONTEXT\" get ns --request-timeout=10s &>/dev/null; then");
    emit_pvc_cleanup_script(&mut s, "$KUBE_CONTEXT", "PD CSI driver", "GCE disks");
    s.cmd("else");
    s.cmd("  echo \"kubectl cannot reach cluster $KUBE_CONTEXT — skipping graceful PVC delete\"");
    s.cmd("fi");

    s.section("Step 2: Delete GKE Cluster");
    s.cmd("if gcloud container clusters describe \"$CLUSTER_NAME\" --zone=\"$GCP_ZONE\" --project=\"$GCP_PROJECT\" &>/dev/null; then");
    s.cmd("  echo \"Deleting GKE cluster $CLUSTER_NAME...\"");
    s.cmd("  gcloud container clusters delete \"$CLUSTER_NAME\" --zone=\"$GCP_ZONE\" --project=\"$GCP_PROJECT\" --quiet");
    s.cmd("  echo \"GKE cluster deleted\"");
    s.cmd("else");
    s.cmd("  echo \"GKE cluster $CLUSTER_NAME does not exist\"");
    s.cmd("fi");

    s.section("Step 3: Sweep Orphaned GCE Disks");
    s.cmd("ORPHANED_DISKS=$(gcloud compute disks list \\");
    s.cmd("  --project=\"$GCP_PROJECT\" \\");
    s.cmd("  --zones=\"$GCP_ZONE\" \\");
    s.cmd("  --filter=\"labels.dimster-cluster=$CLUSTER_NAME OR labels.goog-k8s-cluster-name=$CLUSTER_NAME\" \\");
    s.cmd("  --format=\"csv[no-heading](name,users.len())\" | \\");
    s.cmd("  awk -F, '($2 == \"\" || $2 == \"0\") && $1 != \"\" {print $1}' || true)");
    s.cmd("if [ -z \"$ORPHANED_DISKS\" ]; then");
    s.cmd("  echo \"No orphaned GCE disks found for $CLUSTER_NAME\"");
    s.cmd("else");
    s.cmd("  for DISK in $ORPHANED_DISKS; do");
    s.cmd("    gcloud compute disks delete \"$DISK\" --zone=\"$GCP_ZONE\" --project=\"$GCP_PROJECT\" --quiet");
    s.cmd("    echo \"Deleted disk $DISK\"");
    s.cmd("  done");
    s.cmd("fi");

    if keep_registry {
        s.section("Step 4: Delete Artifact Registry Repository — skipped (--keep-registry)");
    } else {
        s.section("Step 4: Delete Artifact Registry Repository");
        s.cmd("if gcloud artifacts repositories describe \"$DIMSTER_NAME\" --location=\"$GCP_REGION\" --project=\"$GCP_PROJECT\" &>/dev/null; then");
        s.cmd("  gcloud artifacts repositories delete \"$DIMSTER_NAME\" --location=\"$GCP_REGION\" --project=\"$GCP_PROJECT\" --quiet");
        s.cmd("  echo \"Artifact Registry repository deleted\"");
        s.cmd("else");
        s.cmd("  echo \"Artifact Registry repository $DIMSTER_NAME does not exist\"");
        s.cmd("fi");
    }

    s.blank();
    s.cmd("echo \"Done! Cluster and resources have been deleted.\"");
    s.cmd(&format!(
        "echo \"Note: Config files (dimster-config-{}.yaml, gke-answers-{}.yaml) left on disk.\"",
        dimster_name, dimster_name
    ));
    s.render()
}

fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|a| {
            if a.contains(' ') || a.contains(',') || a.contains('=') {
                format!("'{}'", a.replace('\'', "'\\''"))
            } else {
                a.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
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

    fn sample_answers(machine: &str, storage: StorageAnswers) -> Answers {
        Answers {
            env_name: "gke".into(),
            gcp: GcpAnswers {
                project: "my-proj".into(),
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
                instance_type: machine.into(),
                count: 3,
                heap: "-Xms8G -Xmx8G".into(),
                cpu: "6".into(),
                memory: "26Gi".into(),
                tier1_networking: false,
                broker_config: Some("run/broker-configs/default.yaml".into()),
                extra_config: None,
            },
            storage,
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
                heap: "-Xms512M -Xmx512M".into(),
                cpu: "500m".into(),
                memory: "1Gi".into(),
            },
            monitoring: MonitoringAnswers {
                prometheus_cpu: "750m".into(),
                prometheus_memory: "2Gi".into(),
                grafana_cpu: "750m".into(),
                grafana_memory: "1Gi".into(),
            },
        }
    }

    #[test]
    fn c4_pool_uses_hyperdisk_boot_disk_and_static_cpu_manager() {
        let types = load_catalog();
        let mut answers = sample_answers(
            "c4-standard-8",
            StorageAnswers {
                storage_type: "hyperdisk-balanced".into(),
                size: "500Gi".into(),
                performance: Some("provisioned".into()),
                iops: Some(16_000),
                throughput_mibs: Some(800),
                local_ssd_count: None,
            },
        );
        answers.cpu_manager_static = true;

        let args = build_kafka_pool_args(&answers, &types, Some("/tmp/sys.yaml"));
        assert!(args.windows(2).any(|w| w == ["--disk-type", "hyperdisk-balanced"]));
        assert!(args.windows(2).any(|w| w == ["--disk-size", "50"]));
        assert!(args.windows(2).any(|w| w == ["--system-config-from-file", "/tmp/sys.yaml"]));
        assert!(args.contains(&"--enable-gvnic".to_string()));
    }

    #[test]
    fn generate_create_script_for_hyperdisk_and_local_ssd() {
        let types = load_catalog();
        let hdb_answers = sample_answers(
            "c3-standard-8",
            StorageAnswers {
                storage_type: "hyperdisk-balanced".into(),
                size: "500Gi".into(),
                performance: Some("provisioned".into()),
                iops: Some(16_000),
                throughput_mibs: Some(800),
                local_ssd_count: None,
            },
        );
        let script = generate_create_script(&hdb_answers, &types).unwrap();
        assert!(script.contains("gcloud artifacts repositories create"));
        assert!(script.contains("provisioned-iops-on-create: \"16000\""));
        assert!(script.contains("provisioned-throughput-on-create: \"800Mi\""));
        assert!(script.contains("pvcSize: 500Gi"));
        assert!(script.contains("pvcStorageClass: dimster-hyperdisk-balanced-fast"));
        assert!(script.contains("cgroupMetrics: true"));

        let lssd_answers = sample_answers(
            "c3-standard-8-lssd",
            StorageAnswers {
                storage_type: "local-ssd".into(),
                size: "675Gi".into(),
                performance: None,
                iops: None,
                throughput_mibs: None,
                local_ssd_count: Some(2),
            },
        );
        let lssd_script = generate_create_script(&lssd_answers, &types).unwrap();
        assert!(lssd_script.contains("--local-nvme-ssd-block"));
        assert!(!lssd_script.contains("--local-nvme-ssd-block=count="));
        assert!(lssd_script.contains("google-local-nvme-ssd-*"));
        assert!(lssd_script.contains("pvcStorageClass: local-storage"));
    }

    #[test]
    fn generate_delete_script_targets_explicit_context() {
        let script = generate_delete_script("bench", "my-proj", "us-central1", "us-central1-a", false);
        assert!(script.contains("KUBE_CONTEXT=\"gke_my-proj_us-central1-a_bench-benchmark\""));
        assert!(script.contains("kubectl --context \"$KUBE_CONTEXT\" delete statefulset,deployment,job --all -n \"$NS\""));
        assert!(script.contains("kubectl --context \"$KUBE_CONTEXT\" delete pvc --all -n \"$NS\""));
        assert!(script.contains("labels.dimster-cluster=$CLUSTER_NAME OR labels.goog-k8s-cluster-name=$CLUSTER_NAME"));
        assert!(!script.contains("-users:*"));
        assert!(script.contains("gcloud artifacts repositories delete \"$DIMSTER_NAME\""));

        let keep_script = generate_delete_script("bench", "my-proj", "us-central1", "us-central1-a", true);
        assert!(!keep_script.contains("gcloud artifacts repositories delete"));
    }

    #[test]
    fn find_orphaned_cluster_disks_filters_unattached_matching_labels() {
        let json = r#"[
            {"name": "pvc-orphan-kafka", "labels": {"dimster-cluster": "bench-benchmark"}},
            {"name": "pvc-orphan-std", "labels": {"goog-k8s-cluster-name": "bench-benchmark"}, "users": []},
            {"name": "pvc-attached", "labels": {"dimster-cluster": "bench-benchmark"}, "users": ["https://compute/instances/node-1"]},
            {"name": "pvc-other-cluster", "labels": {"dimster-cluster": "other-benchmark"}},
            {"name": "disk-unlabeled"}
        ]"#;
        let orphaned = find_orphaned_cluster_disks(json, "bench-benchmark").unwrap();
        assert_eq!(orphaned, vec!["pvc-orphan-kafka", "pvc-orphan-std"]);
        assert!(find_orphaned_cluster_disks("  ", "bench-benchmark").unwrap().is_empty());
    }

    #[test]
    fn external_mode_and_existing_image_skip_kafka_and_registry() {
        let types = load_catalog();
        let mut answers = sample_answers(
            "c3-standard-8",
            StorageAnswers {
                storage_type: "hyperdisk-balanced".into(),
                size: "500Gi".into(),
                performance: Some("basic".into()),
                iops: None,
                throughput_mibs: None,
                local_ssd_count: None,
            },
        );
        answers.deploy_mode = "external".into();
        answers.bootstrap_servers = Some("pkc-xyz.us-central1.gcp.confluent.cloud:9092".into());
        answers.gcp.existing_image = Some("ghcr.io/example/dimster".into());

        let script = generate_create_script(&answers, &types).unwrap();
        assert!(!script.contains("gcloud artifacts repositories create"));
        assert!(!script.contains("node-pools describe kafka"));
        assert!(script.contains("deploy: external"));
        assert!(script.contains("pkc-xyz.us-central1.gcp.confluent.cloud:9092"));
        assert!(script.contains("ghcr.io/example/dimster"));
    }

    #[test]
    fn attachable_n2_local_ssd_emits_explicit_count_flag() {
        let types = load_catalog();
        let answers = sample_answers(
            "n2-standard-8",
            StorageAnswers {
                storage_type: "local-ssd".into(),
                size: "675Gi".into(),
                performance: None,
                iops: None,
                throughput_mibs: None,
                local_ssd_count: Some(2),
            },
        );
        let args = build_kafka_pool_args(&answers, &types, None);
        assert!(args.contains(&"--local-nvme-ssd-block=count=2".to_string()));
        assert!(args.windows(2).any(|w| w == ["--disk-type", "pd-balanced"]));
    }

    #[test]
    fn parse_test_iam_permissions_distinguishes_api_errors() {
        let ok = br#"{"permissions":["container.clusters.create","compute.zones.list"]}"#;
        assert_eq!(
            parse_test_iam_permissions(ok),
            TestIamOutcome::Permissions(vec![
                "container.clusters.create".into(),
                "compute.zones.list".into(),
            ])
        );

        let err = br#"{"error":{"code":403,"status":"PERMISSION_DENIED","message":"Cloud Resource Manager API has not been used in project foo. Enable it."}}"#;
        assert_eq!(
            parse_test_iam_permissions(err),
            TestIamOutcome::ApiError(
                "PERMISSION_DENIED: Cloud Resource Manager API has not been used in project foo"
                    .into()
            )
        );
    }
}
