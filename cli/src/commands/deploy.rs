use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::commands::broker_config;
use crate::commands::flink;
use crate::commands::is_registry_image;
use crate::commands::kafka_tls;
use crate::config::{CpuManagerConfig, CpuManagerPolicy, DimsterConfig, Environment};
use crate::kubectl::Kubectl;

/// Annotation that records the exact Kafka manifest shape Dimster last
/// deployed. Kafka owns persistent state, so deploy uses this small piece of
/// state to decide whether a Kafka deploy can be an ordinary apply or must
/// first delete the StatefulSet and its generated data PVCs.
const KAFKA_SPEC_HASH_ANNOTATION: &str = "dimster.dev/kafka-spec-hash";
const KAFKA_DATA_PVC_PREFIX: &str = "kafka-data-kafka-";

/// Same `name:` + `properties:` shape as broker configs, but consumed by
/// the entrypoint as pipe-separated key=value pairs (commas appear in
/// log4j layout patterns, so `,` can't be the separator). Kept local to
/// deploy because the serialization to KAFKA_LOG4J_CONFIG isn't shared
/// with anything else.
#[derive(Deserialize)]
struct Log4jConfigFile {
    name: String,
    #[serde(default)]
    properties: BTreeMap<String, serde_yaml::Value>,
}

/// Deployable components, matching the sections in dimster-config.yaml.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Component {
    Kafka,
    Workers,
    Coordinator,
    Monitoring,
    /// The Flink session cluster; only deployed for envs with a `flink:` block.
    Flink,
}

/// What deploy knows about the currently deployed Kafka StatefulSet.
///
/// The distinction between "missing" and "present without a hash" matters:
/// a missing StatefulSet is an initial deploy, while a present StatefulSet
/// without a hash was deployed by an older Dimster and therefore has unknown
/// compatibility with the newly rendered Kafka spec.
#[derive(Debug, PartialEq, Eq)]
enum LiveKafkaSpecHash {
    Missing,
    Present(Option<String>),
}

/// Recreate decision for the Kafka part of a deploy.
///
/// This enum keeps the policy separate from the kubectl mechanics so the
/// behavior is easy to test: only known, matching hashes preserve PVCs.
#[derive(Debug, PartialEq, Eq)]
enum KafkaRecreateDecision {
    ApplyOnly,
    Recreate(&'static str),
}

impl Component {
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "kafka" => Ok(Component::Kafka),
            "workers" => Ok(Component::Workers),
            "coordinator" => Ok(Component::Coordinator),
            "monitoring" => Ok(Component::Monitoring),
            "flink" => Ok(Component::Flink),
            other => Err(anyhow::anyhow!(
                "Unknown component '{}'. Valid components: kafka, workers, coordinator, monitoring, flink",
                other
            )),
        }
    }

    pub(crate) fn all() -> HashSet<Component> {
        HashSet::from([
            Component::Kafka,
            Component::Workers,
            Component::Coordinator,
            Component::Monitoring,
            Component::Flink,
        ])
    }
}

/// Parse the --only flag into a component set. Empty input means all components.
pub(crate) fn parse_components(only: &[String]) -> Result<HashSet<Component>> {
    if only.is_empty() {
        return Ok(Component::all());
    }
    let mut set = HashSet::new();
    for name in only {
        set.insert(Component::from_str(name)?);
    }
    Ok(set)
}

pub fn run(
    kubectl: &Kubectl,
    config: &DimsterConfig,
    env: &str,
    k8s_dir: &Path,
    log_config_override: Option<&str>,
    only: &[String],
) -> Result<()> {
    let ns = kubectl.namespace();
    let env_config = config.get_env(env)?;
    let external = env_config.is_external();
    let mut components = parse_components(only)?;

    // Flink is deployed only for envs that ask for it.
    if env_config.flink.is_none() && components.remove(&Component::Flink) && !only.is_empty() {
        bail!(
            "Environment '{}' has no flink block, so there is no Flink to deploy. \
             Add a flink section to the environment in dimster-config.yaml.",
            env
        );
    }

    // External mode never deploys Kafka
    if external && components.contains(&Component::Kafka) && !only.is_empty() {
        eprintln!(
            "Warning: ignoring 'kafka' component — environment '{}' uses external Kafka",
            env
        );
    }

    if external {
        println!(
            "==> Deploying Dimster ({}) to namespace {} (external Kafka)...",
            env, ns
        );
    } else {
        println!("==> Deploying Dimster ({}) to namespace {}...", env, ns);
    }

    if !only.is_empty() {
        let names: Vec<&str> = only.iter().map(|s| s.as_str()).collect();
        println!("    Components: {}", names.join(", "));
    }

    run_cpu_manager_preflight(kubectl, env_config, external, &components)?;

    deploy(
        kubectl,
        config,
        env,
        env_config,
        k8s_dir,
        log_config_override,
        &components,
    )?;

    // Restart deployments to ensure pods pick up the latest image.
    // Only restart components that were deployed.
    let mut restart_targets: Vec<&str> = Vec::new();
    if components.contains(&Component::Workers) {
        restart_targets.push("deployment/benchmark-worker");
    }
    if components.contains(&Component::Coordinator) {
        restart_targets.push("deployment/benchmark-coordinator");
    }
    if !restart_targets.is_empty() {
        let mut args: Vec<&str> = vec!["rollout", "restart"];
        args.extend(&restart_targets);
        let _ = kubectl.run(&args);
    }

    if !external && components.contains(&Component::Kafka) {
        println!("==> Waiting for Kafka to be ready...");
        if let Err(e) = kubectl.run(&["rollout", "status", "statefulset/kafka", "--timeout=300s"]) {
            show_deploy_diagnostics(kubectl, "app=kafka");
            return Err(e);
        }
    }

    if components.contains(&Component::Workers) {
        println!("==> Waiting for workers to be ready...");
        wait_for_deployment(kubectl, "benchmark-worker", "app=benchmark-worker")?;
    }

    if components.contains(&Component::Coordinator) {
        println!("==> Waiting for coordinator to be ready...");
        wait_for_deployment(
            kubectl,
            "benchmark-coordinator",
            "app=benchmark-coordinator",
        )?;
    }

    if components.contains(&Component::Flink) {
        println!("==> Waiting for Flink to be ready...");
        wait_for_deployment(
            kubectl,
            "flink-jobmanager",
            "app=flink,component=jobmanager",
        )?;
        wait_for_deployment(
            kubectl,
            "flink-taskmanager",
            "app=flink,component=taskmanager",
        )?;
    }

    println!("==> Deployment complete.");
    println!();

    if !external && components.contains(&Component::Kafka) {
        println!("Kafka brokers:");
        let _ = kubectl.run_inherit(&["get", "pods", "-l", "app=kafka"]);
        println!();
    }
    if components.contains(&Component::Coordinator) {
        println!("Benchmark coordinator:");
        let _ = kubectl.run_inherit(&["get", "pods", "-l", "app=benchmark-coordinator"]);
        println!();
    }
    if components.contains(&Component::Workers) {
        println!("Benchmark workers:");
        let _ = kubectl.run_inherit(&["get", "pods", "-l", "app=benchmark-worker"]);
    }
    if components.contains(&Component::Flink) {
        println!();
        println!("Flink:");
        let _ = kubectl.run_inherit(&["get", "pods", "-l", flink::POD_LABEL]);
    }

    Ok(())
}

/// Validate kubelet CPU Manager state before deploy mutates any cluster
/// resources. CPU Manager is node configuration, so Dimster deliberately does
/// not try to apply it through namespace manifests; instead this preflight
/// checks that the selected benchmark nodes already match the env's declared
/// requirement and fails with an actionable error if they do not.
fn run_cpu_manager_preflight(
    kubectl: &Kubectl,
    env_config: &Environment,
    external: bool,
    components: &HashSet<Component>,
) -> Result<()> {
    let Some(cpu_manager) = env_config.cpu_manager.as_ref() else {
        return Ok(());
    };
    if cpu_manager.policy == CpuManagerPolicy::None {
        return Ok(());
    }

    let targets = benchmark_cpu_targets(env_config, external, components);
    if targets.is_empty() {
        return Ok(());
    }

    for target in &targets {
        require_whole_cpu_request(target.component, target.cpu_request)?;
    }

    println!("==> Validating kubelet CPU Manager settings...");
    let nodes_json = kubectl
        .run_no_ns(&["get", "nodes", "-o", "json"])
        .context("Cannot list nodes for CPU Manager preflight")?;
    let nodes = parse_nodes(&nodes_json)?;
    let selected = select_required_cpu_manager_nodes(&nodes, &targets)?;
    if selected.is_empty() {
        bail!(
            "cpuManager.policy is static, but no schedulable nodes matched the benchmark components. \
             Check kafka.nodeLabel/workers.nodeLabel or set cpuManager.policy: none."
        );
    }

    for node_name in selected {
        let path = format!("/api/v1/nodes/{}/proxy/configz", node_name);
        let configz = kubectl
            .run_no_ns(&["get", "--raw", &path])
            .with_context(|| {
                format!(
                    "Cannot read kubelet /configz for node '{}'. Grant access to nodes/proxy, \
                     configure kubelet manually and retry, or set cpuManager.policy: none.",
                    node_name
                )
            })?;
        let actual = parse_kubelet_cpu_manager_config(&configz)
            .with_context(|| format!("Cannot parse kubelet /configz for node '{}'", node_name))?;
        validate_kubelet_cpu_manager_config(&node_name, cpu_manager, &actual)?;
    }

    Ok(())
}

/// One benchmark component whose pod resource shape must be compatible with
/// kubelet static CPU Manager. Coordinator and monitoring are intentionally
/// excluded: they affect observability and orchestration, while Kafka and
/// workers are the pods whose CPU placement changes benchmark fidelity.
#[derive(Debug, Clone, Copy)]
struct BenchmarkCpuTarget<'a> {
    component: &'static str,
    cpu_request: &'a str,
    node_label: Option<&'a str>,
}

fn benchmark_cpu_targets<'a>(
    env_config: &'a Environment,
    external: bool,
    components: &HashSet<Component>,
) -> Vec<BenchmarkCpuTarget<'a>> {
    let is_minimal = env_config.is_minimal();
    let mut targets = Vec::new();

    if !external && components.contains(&Component::Kafka) {
        let kafka = env_config.kafka.as_ref();
        let default_cpu = if is_minimal { "250m" } else { "2" };
        targets.push(BenchmarkCpuTarget {
            component: "kafka",
            cpu_request: kafka
                .and_then(|k| k.requests.as_ref())
                .and_then(|r| r.cpu.as_deref())
                .unwrap_or(default_cpu),
            node_label: kafka.and_then(|k| k.node_label.as_deref()),
        });
    }

    if components.contains(&Component::Workers) {
        let workers = env_config.workers.as_ref();
        let default_cpu = if is_minimal { "250m" } else { "2" };
        targets.push(BenchmarkCpuTarget {
            component: "workers",
            cpu_request: workers
                .and_then(|w| w.requests.as_ref())
                .and_then(|r| r.cpu.as_deref())
                .unwrap_or(default_cpu),
            node_label: workers.and_then(|w| w.node_label.as_deref()),
        });
    }

    targets
}

/// Static CPU Manager only grants exclusive CPUs to Guaranteed pods with a
/// whole-CPU request. Dimster already renders equal requests and limits for
/// CPU and memory, so this check focuses on the value that can silently push a
/// pod back into the shared pool: fractional CPU.
fn require_whole_cpu_request(component: &str, cpu: &str) -> Result<u32> {
    let trimmed = cpu.trim();
    let cores = if let Some(millis) = trimmed.strip_suffix('m') {
        let millis: u32 = millis
            .parse()
            .with_context(|| format!("Invalid {} CPU request '{}'", component, cpu))?;
        if millis == 0 || millis % 1000 != 0 {
            bail!(
                "cpuManager.policy: static requires whole CPU requests for benchmark pods; \
                 {} requests cpu: '{}'. Use values like \"1\", \"2\", or \"4\".",
                component,
                cpu
            );
        }
        millis / 1000
    } else {
        let cores: u32 = trimmed
            .parse()
            .with_context(|| format!("Invalid {} CPU request '{}'", component, cpu))?;
        if cores == 0 {
            bail!(
                "cpuManager.policy: static requires positive CPU requests for benchmark pods; \
                 {} requests cpu: '{}'.",
                component,
                cpu
            );
        }
        cores
    };
    Ok(cores)
}

#[derive(Debug, Deserialize)]
struct NodeList {
    #[serde(default)]
    items: Vec<NodeInfo>,
}

#[derive(Debug, Deserialize)]
struct NodeInfo {
    metadata: NodeMetadata,
    #[serde(default)]
    spec: NodeSpec,
}

#[derive(Debug, Deserialize)]
struct NodeMetadata {
    name: String,
    #[serde(default)]
    labels: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Default)]
struct NodeSpec {
    #[serde(default)]
    unschedulable: bool,
}

fn parse_nodes(json: &str) -> Result<Vec<NodeInfo>> {
    let list: NodeList =
        serde_json::from_str(json).context("Cannot parse kubectl get nodes JSON")?;
    Ok(list.items)
}

fn select_required_cpu_manager_nodes(
    nodes: &[NodeInfo],
    targets: &[BenchmarkCpuTarget<'_>],
) -> Result<HashSet<String>> {
    let mut selected = HashSet::new();
    for target in targets {
        let matches = nodes_matching_target(nodes, target)?;
        if matches.is_empty() {
            match target.node_label {
                Some(label) => bail!(
                    "cpuManager.policy is static, but no schedulable nodes matched {}.nodeLabel '{}'.",
                    target.component,
                    label
                ),
                None => bail!(
                    "cpuManager.policy is static, but no schedulable nodes are available for {}.",
                    target.component
                ),
            }
        }
        selected.extend(matches);
    }
    Ok(selected)
}

fn nodes_matching_target(
    nodes: &[NodeInfo],
    target: &BenchmarkCpuTarget<'_>,
) -> Result<Vec<String>> {
    let required_label = match target.node_label {
        Some(label) => Some(parse_node_label(label)?),
        None => None,
    };

    let mut matches = Vec::new();
    for node in nodes {
        if node.spec.unschedulable {
            continue;
        }
        if let Some((key, value)) = required_label {
            if node.metadata.labels.get(key).map(|s| s.as_str()) != Some(value) {
                continue;
            }
        }
        matches.push(node.metadata.name.clone());
    }
    Ok(matches)
}

#[derive(Debug, Default, PartialEq, Eq)]
struct KubeletCpuManagerConfig {
    policy: Option<String>,
    full_pcpus_only: bool,
}

fn parse_kubelet_cpu_manager_config(configz: &str) -> Result<KubeletCpuManagerConfig> {
    let root: serde_json::Value = serde_json::from_str(configz)?;
    let cfg = root
        .get("kubeletconfig")
        .or_else(|| root.get("kubeletConfig"))
        .unwrap_or(&root);

    let policy = cfg
        .get("cpuManagerPolicy")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let full_pcpus_only = cfg
        .get("cpuManagerPolicyOptions")
        .and_then(|v| v.as_object())
        .and_then(|options| options.get("full-pcpus-only"))
        .map(kubelet_option_is_true)
        .unwrap_or(false);

    Ok(KubeletCpuManagerConfig {
        policy,
        full_pcpus_only,
    })
}

fn kubelet_option_is_true(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Bool(v) => *v,
        serde_json::Value::String(v) => v.eq_ignore_ascii_case("true"),
        _ => false,
    }
}

fn validate_kubelet_cpu_manager_config(
    node_name: &str,
    expected: &CpuManagerConfig,
    actual: &KubeletCpuManagerConfig,
) -> Result<()> {
    if expected.policy == CpuManagerPolicy::Static && actual.policy.as_deref() != Some("static") {
        bail!(
            "Node '{}' does not have kubelet cpuManagerPolicy=static (actual: {}). \
             Configure kubelet CPU Manager static policy on benchmark nodes or set cpuManager.policy: none.",
            node_name,
            actual.policy.as_deref().unwrap_or("<missing>")
        );
    }
    if expected.full_pcpus_only && !actual.full_pcpus_only {
        bail!(
            "Node '{}' does not have kubelet cpuManagerPolicyOptions.full-pcpus-only=true. \
             Enable the option on benchmark nodes or set cpuManager.fullPcpusOnly: false.",
            node_name
        );
    }
    Ok(())
}

fn wait_for_deployment(kubectl: &Kubectl, name: &str, label: &str) -> Result<()> {
    let deployment = format!("deployment/{}", name);
    match kubectl.run(&["rollout", "status", &deployment, "--timeout=300s"]) {
        Ok(_) => Ok(()),
        Err(e) => {
            let err_msg = format!("{:#}", e);
            if err_msg.contains("exceeded its progress deadline") {
                // Deployment was stuck from a previous failed attempt.
                // Delete pods so the deployment controller creates fresh ones.
                eprintln!(
                    "    Deployment {} has a stale progress deadline, deleting pods to retry...",
                    name
                );
                let _ = kubectl.run(&["delete", "pods", "-l", label]);
                // Wait again with fresh pods
                match kubectl.run(&["rollout", "status", &deployment, "--timeout=300s"]) {
                    Ok(_) => Ok(()),
                    Err(e2) => {
                        show_deploy_diagnostics(kubectl, label);
                        Err(e2)
                    }
                }
            } else {
                show_deploy_diagnostics(kubectl, label);
                Err(e)
            }
        }
    }
}

fn show_deploy_diagnostics(kubectl: &Kubectl, label: &str) {
    eprintln!("\n==> Deployment timed out. Diagnostics:");
    eprintln!("\n--- Pod status ---");
    let _ = kubectl.run_inherit(&["get", "pods", "-l", label, "-o", "wide"]);
    eprintln!("\n--- Pod events ---");
    let _ = kubectl.run_inherit(&[
        "get",
        "events",
        "--field-selector",
        &format!("involvedObject.kind=Pod"),
        "--sort-by",
        ".lastTimestamp",
    ]);
    eprintln!("\n--- Pending pod details ---");
    if let Ok(output) = kubectl.run(&[
        "get",
        "pods",
        "-l",
        label,
        "--field-selector",
        "status.phase=Pending",
        "-o",
        "jsonpath={.items[*].metadata.name}",
    ]) {
        for pod in output.split_whitespace() {
            eprintln!("\nPod: {}", pod);
            let _ = kubectl.run_inherit(&["describe", "pod", pod]);
        }
    }
}

/// Build the deploy-time kustomize resource list for the selected components.
/// Each resource path is prefixed with "base/" for the temporary overlay directory.
fn build_resource_list(components: &HashSet<Component>, external: bool) -> String {
    build_component_resource_list(
        components,
        external,
        true,
        if external {
            PrometheusConfigResource::ExternalGenerated
        } else {
            PrometheusConfigResource::Base
        },
    )
}

/// Build the teardown-time kustomize resource list for the selected
/// components. Partial teardowns deliberately omit the Namespace object so
/// that omitted components keep running inside the same namespace.
pub(crate) fn build_teardown_resource_list(
    components: &HashSet<Component>,
    external: bool,
    include_namespace: bool,
) -> String {
    build_component_resource_list(
        components,
        external,
        include_namespace,
        PrometheusConfigResource::Base,
    )
}

/// Which prometheus-config manifest path should be listed in the temporary
/// overlay. Deploy in external mode writes a generated file at the overlay
/// root; teardown can always use the base resource because delete only needs
/// the resource identity, not the exact ConfigMap data.
#[derive(Clone, Copy)]
enum PrometheusConfigResource {
    Base,
    ExternalGenerated,
}

fn build_component_resource_list(
    components: &HashSet<Component>,
    external: bool,
    include_namespace: bool,
    prometheus_config: PrometheusConfigResource,
) -> String {
    let mut resources = Vec::new();
    if include_namespace {
        resources.push("  - base/namespace.yaml".to_string());
    }

    if !external && components.contains(&Component::Kafka) {
        resources.push("  - base/kafka/configmap.yaml".into());
        resources.push("  - base/kafka/service.yaml".into());
        resources.push("  - base/kafka/statefulset.yaml".into());
        // JMX exporter config is a dependency of the Kafka StatefulSet
        resources.push("  - base/monitoring/kafka-jmx-config.yaml".into());
    }

    if components.contains(&Component::Workers) {
        resources.push("  - base/workers/deployment.yaml".into());
        resources.push("  - base/workers/service.yaml".into());
    }

    if components.contains(&Component::Coordinator) {
        resources.push("  - base/coordinator/deployment.yaml".into());
        resources.push("  - base/coordinator/service.yaml".into());
        resources.push("  - base/coordinator/rbac.yaml".into());
        resources.push("  - base/results/pvc.yaml".into());
    }

    if components.contains(&Component::Flink) {
        for resource in flink::RESOURCES {
            resources.push(format!("  - {resource}"));
        }
    }

    if components.contains(&Component::Monitoring) {
        match prometheus_config {
            PrometheusConfigResource::Base => {
                resources.push("  - base/monitoring/prometheus-config.yaml".into());
            }
            PrometheusConfigResource::ExternalGenerated => {
                // External deploy mode uses an inline prometheus config without
                // the Kafka scrape job, written at the temporary overlay root.
                resources.push("  - prometheus-config.yaml".into());
            }
        }
        resources.push("  - base/monitoring/prometheus.yaml".into());
        resources.push("  - base/monitoring/grafana-config.yaml".into());
        resources.push("  - base/monitoring/grafana-dashboards.yaml".into());
        resources.push("  - base/monitoring/grafana-dashboards-system.yaml".into());
        resources.push("  - base/monitoring/grafana.yaml".into());
        resources.push("  - base/monitoring/node-exporter.yaml".into());
        resources.push("  - base/monitoring/dashboard-export-configmap.yaml".into());
    }

    resources.join("\n")
}

fn deploy(
    kubectl: &Kubectl,
    config: &DimsterConfig,
    _env: &str,
    env_config: &Environment,
    k8s_dir: &Path,
    log_config_override: Option<&str>,
    components: &HashSet<Component>,
) -> Result<()> {
    let is_minimal = env_config.is_minimal();
    let external = env_config.is_external();
    let workers = env_config.workers.as_ref();
    let coordinator = env_config.coordinator.as_ref();

    let (def_worker_replicas, def_worker_heap, def_worker_cpu, def_worker_memory) = if is_minimal {
        (1u32, "-Xms256M -Xmx512M", "250m", "768Mi")
    } else {
        (2u32, "-Xms4G -Xmx8G", "2", "6Gi")
    };
    let (def_coord_heap, def_coord_cpu, def_coord_memory) = if is_minimal {
        ("-Xms256M -Xmx512M", "250m", "768Mi")
    } else {
        ("-Xms512M -Xmx1G", "500m", "2Gi")
    };

    let worker_replicas = workers
        .and_then(|w| w.replicas)
        .unwrap_or(def_worker_replicas);
    let worker_heap = workers
        .and_then(|w| w.heap.as_deref())
        .unwrap_or(def_worker_heap);
    let worker_cpu = workers
        .and_then(|w| w.requests.as_ref())
        .and_then(|r| r.cpu.as_deref())
        .unwrap_or(def_worker_cpu);
    let worker_memory = workers
        .and_then(|w| w.requests.as_ref())
        .and_then(|r| r.memory.as_deref())
        .unwrap_or(def_worker_memory);
    let worker_node_label = workers.and_then(|w| w.node_label.as_deref());
    let worker_log_config_path = workers.and_then(|w| w.log_config.as_deref());

    let coord_heap = coordinator
        .and_then(|c| c.heap.as_deref())
        .unwrap_or(def_coord_heap);
    let coord_cpu = coordinator
        .and_then(|c| c.requests.as_ref())
        .and_then(|r| r.cpu.as_deref())
        .unwrap_or(def_coord_cpu);
    let coord_memory = coordinator
        .and_then(|c| c.requests.as_ref())
        .and_then(|r| r.memory.as_deref())
        .unwrap_or(def_coord_memory);

    let monitoring = env_config.monitoring.as_ref();
    let monitoring_node_label = monitoring.and_then(|m| m.node_label.as_deref());

    // Determine imagePullPolicy based on image name
    let env_image = env_config.image.as_deref().unwrap_or("dimster");
    let pull_policy = if is_registry_image(env_image) {
        "Always"
    } else {
        "IfNotPresent"
    };

    // Build temporary kustomization overlay
    let tmp_dir = tempfile::tempdir()?;
    let tmp = tmp_dir.path();

    // Copy the base directory into the temp dir.
    // Kustomize refuses to follow symlinks for security, so we must copy.
    let base_dir = k8s_dir.join("base");
    copy_dir_recursive(&base_dir, &tmp.join("base"))?;

    // Re-template the Namespace resource. Kustomize's top-level `namespace:`
    // field rewrites `metadata.namespace` on every namespaced resource, but
    // it does NOT rename the Namespace resource itself (its name lives in
    // `metadata.name`). So the copied base/namespace.yaml — which carries
    // the literal `name: dimster` — gets replaced with one that names the
    // resolved namespace. Without this, deploys to non-default namespaces
    // would still create a `dimster` namespace alongside the real target.
    write_namespace_yaml(&tmp.join("base/namespace.yaml"), kubectl.namespace())?;

    // Rewrite hardcoded `dimster.svc.cluster.local` DNS strings inside the
    // copied manifests. Kustomize's `namespace:` field rewrites resource
    // metadata.namespace fields but does NOT touch literal text inside
    // ConfigMap data sections — and these ConfigMaps embed shell scripts
    // and YAML configs whose Kafka quorum / Prometheus scrape / Grafana
    // datasource targets all need to point at services in the deployed
    // namespace, not the literal "dimster". Without this rewrite, a deploy
    // into `dimster2` would have its kafka pods trying to reach
    // `kafka-0.kafka-headless.dimster.svc.cluster.local` and silently fail.
    rewrite_dns_namespace(tmp, kubectl.namespace())?;

    // ---- mTLS: provision broker + client certs as Secrets ----
    // Done BEFORE applying kustomize so the Secrets exist when broker /
    // worker pods come up. Namespace must already exist (we apply it first
    // when TLS is on); on subsequent deploys both already exist and
    // ensure_secrets reuses them. Lives outside the per-component patch
    // blocks because the same Secrets are needed by both broker pods and
    // worker/coordinator pods.
    let tls_settings = if !external && env_config.tls_config().is_some() {
        let tls = env_config.tls_config().unwrap();
        let kafka_replicas = env_config
            .kafka
            .as_ref()
            .and_then(|k| k.replicas)
            .unwrap_or(if is_minimal { 1 } else { 3 });
        // Make sure the namespace is in place — kustomize apply creates it
        // later, but secret creation needs it now. apply is idempotent.
        let ns_path = tmp.join("base/namespace.yaml");
        kubectl.run_no_ns(&["apply", "-f", &ns_path.to_string_lossy()])?;
        kafka_tls::ensure_secrets(kubectl, kafka_replicas, kubectl.namespace())?;
        Some(tls.clone())
    } else {
        None
    };

    // Write external prometheus config if needed (no kafka scrape job).
    // Templated with the resolved namespace so the ConfigMap lands in the
    // right place AND the dns_sd_configs DNS names point at services in
    // that namespace.
    if external && components.contains(&Component::Monitoring) {
        write_external_prometheus_config(tmp, kubectl.namespace())?;
    }
    // Prometheus reads its config only at startup, so the pod template
    // carries a hash of the rendered config: a changed config (such as the
    // Flink scrape job appearing) rolls the pod, an unchanged one doesn't.
    let prometheus_config_hash = if components.contains(&Component::Monitoring) {
        let prometheus_config = if external {
            tmp.join("prometheus-config.yaml")
        } else {
            tmp.join("base/monitoring/prometheus-config.yaml")
        };
        let mut content = std::fs::read_to_string(&prometheus_config)?;
        // Scrape Flink's reporters only when the env has Flink.
        if env_config.flink.is_some() {
            content.push_str(&flink::scrape_job(kubectl.namespace()));
            std::fs::write(&prometheus_config, &content)?;
        }
        let mut hash = Fnv1a64::new();
        hash.update(content.as_bytes());
        Some(format!("{:016x}", hash.finish()))
    } else {
        None
    };

    // Build resource list based on selected components
    let resource_list = build_resource_list(components, external);

    // Build patches separately — only emit the `patches:` key if there are entries.
    // Kustomize rejects an empty `patches:` key.
    let mut patches = String::new();

    if let Some(hash) = &prometheus_config_hash {
        patches.push_str(&format!(
            r#"  - target:
      kind: Deployment
      name: prometheus
    patch: |
      - op: add
        path: /spec/template/metadata/annotations
        value:
          dimster.dev/config-hash: "{hash}"
"#
        ));
    }

    // --- Kafka patches (full deployment only) ---
    if !external && components.contains(&Component::Kafka) {
        let kafka = env_config.kafka.as_ref();
        let (
            def_kafka_replicas,
            def_kafka_storage,
            def_kafka_heap,
            def_kafka_cpu,
            def_kafka_memory,
        ) = if is_minimal {
            (1u32, "1Gi", "-Xms128M -Xmx256M", "250m", "512Mi")
        } else {
            (3u32, "500Gi", "-Xms8G -Xmx8G", "2", "12Gi")
        };

        let kafka_replicas = kafka.and_then(|k| k.replicas).unwrap_or(def_kafka_replicas);
        let kafka_storage = kafka
            .and_then(|k| k.pvc_size.as_deref())
            .unwrap_or(def_kafka_storage);
        let kafka_heap = kafka
            .and_then(|k| k.heap.as_deref())
            .unwrap_or(def_kafka_heap);
        let kafka_cpu = kafka
            .and_then(|k| k.requests.as_ref())
            .and_then(|r| r.cpu.as_deref())
            .unwrap_or(def_kafka_cpu);
        let kafka_memory = kafka
            .and_then(|k| k.requests.as_ref())
            .and_then(|r| r.memory.as_deref())
            .unwrap_or(def_kafka_memory);
        let kafka_image = kafka
            .and_then(|k| k.image.as_deref())
            .unwrap_or("apache/kafka");
        let kafka_version = kafka.and_then(|k| k.version.as_deref());
        let storage_class = kafka.and_then(|k| k.pvc_storage_class.as_deref());
        let kafka_inline_extra = kafka.and_then(|k| k.extra_config.as_deref());
        let kafka_node_label = kafka.and_then(|k| k.node_label.as_deref());

        // Resolve log config: CLI flag > environment config > none
        let log_config_path =
            log_config_override.or_else(|| kafka.and_then(|k| k.log_config.as_deref()));

        // Log rotation overrides (applied on top of log config, or standalone)
        let log_max_file_size = kafka.and_then(|k| k.log_max_file_size.as_deref());
        let log_max_backup_index = kafka.and_then(|k| k.log_max_backup_index);

        // Resolve broker config from environment config. The same merge is also
        // performed at run time by run_configs (to capture the effective config
        // alongside results), so the two views can never disagree.
        let broker_config_path = kafka.and_then(|k| k.broker_config.as_deref());
        let has_broker_config = broker_config_path.is_some();

        let effective = broker_config::resolve_effective(broker_config_path, kafka_inline_extra)?;
        if let (Some(eff), Some(path)) = (effective.as_ref(), broker_config_path) {
            println!("    Using broker config: {} ({})", eff.name, path);
        }
        let kafka_extra_config: Option<String> = effective
            .as_ref()
            .map(|e| e.to_extra_config_string())
            .filter(|s| !s.is_empty());

        // Derive KAFKA_CPU (integer cores for thread calculation) from K8s CPU value
        let kafka_cpu_cores = cpu_to_cores(kafka_cpu);

        // Build merged env var map:
        // 1. Start with heap as KAFKA_HEAP_OPTS (if set)
        // 2. Overlay kafka.env (if set) — explicit wins over sugar
        let mut user_env: HashMap<String, String> = HashMap::new();
        if let Some(h) = kafka.and_then(|k| k.heap.as_deref()) {
            user_env.insert("KAFKA_HEAP_OPTS".to_string(), h.to_string());
        }
        if let Some(k) = kafka {
            for (name, value) in &k.env {
                user_env.insert(name.clone(), value.clone());
            }
        }

        // Effective heap value: user_env if present (handles env override), else kafka_heap default
        let effective_kafka_heap = user_env
            .get("KAFKA_HEAP_OPTS")
            .map(|s| s.as_str())
            .unwrap_or(kafka_heap);

        patches.push_str(&format!(
            r#"  - target:
      kind: StatefulSet
      name: kafka
    patch: |
      - op: replace
        path: /spec/replicas
        value: {kafka_replicas}
      - op: replace
        path: /spec/template/spec/containers/0/env/0
        value:
          name: KAFKA_REPLICAS
          value: "{kafka_replicas}"
      - op: replace
        path: /spec/template/spec/containers/0/resources
        value:
          requests:
            cpu: "{kafka_cpu}"
            memory: "{kafka_memory}"
          limits:
            cpu: "{kafka_cpu}"
            memory: "{kafka_memory}"
      - op: replace
        path: /spec/template/spec/containers/0/env/2
        value:
          name: KAFKA_HEAP_OPTS
          value: "{effective_kafka_heap}"
      - op: replace
        path: /spec/template/spec/containers/0/env/3
        value:
          name: KAFKA_CPU
          value: "{kafka_cpu_cores}"
      - op: replace
        path: /spec/volumeClaimTemplates/0/spec/resources/requests/storage
        value: {kafka_storage}
"#
        ));

        // If user_env contains KAFKA_OPTS, replace env/4 with JMX agent + user opts.
        // The JMX agent is always prepended to preserve Grafana metrics.
        if let Some(user_opts) = user_env.get("KAFKA_OPTS") {
            let combined_opts = format!(
                "-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9404:/opt/jmx-exporter/config.yml {}",
                user_opts
            );
            patches.push_str(&format!(
                r#"  - target:
      kind: StatefulSet
      name: kafka
    patch: |
      - op: replace
        path: /spec/template/spec/containers/0/env/4
        value:
          name: KAFKA_OPTS
          value: "{}"
"#,
                combined_opts
            ));
        }

        // Patch in any additional env vars from kafka.env that aren't hardcoded above.
        // The hardcoded indices are: 0=KAFKA_REPLICAS, 1=KAFKA_CLUSTER_ID, 2=KAFKA_HEAP_OPTS,
        // 3=KAFKA_CPU, 4=KAFKA_OPTS. Skip those and add everything else starting at index 5.
        let hardcoded = [
            "KAFKA_REPLICAS",
            "KAFKA_CLUSTER_ID",
            "KAFKA_HEAP_OPTS",
            "KAFKA_CPU",
            "KAFKA_OPTS",
        ];
        let mut extra_env_index = 5;
        for (name, value) in user_env.iter() {
            if !hardcoded.contains(&name.as_str()) {
                patches.push_str(&format!(
                    r#"  - target:
      kind: StatefulSet
      name: kafka
    patch: |
      - op: add
        path: /spec/template/spec/containers/0/env/{}
        value:
          name: {}
          value: "{}"
"#,
                    extra_env_index, name, value
                ));
                extra_env_index += 1;
            }
        }

        // Kafka node affinity
        if let Some(label) = kafka_node_label {
            let (key, value) = parse_node_label(label)?;
            patches.push_str(&format!(
                r#"  - target:
      kind: StatefulSet
      name: kafka
    patch: |
      - op: add
        path: /spec/template/spec/affinity
        value:
          nodeAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
              nodeSelectorTerms:
                - matchExpressions:
                    - key: {key}
                      operator: In
                      values: ["{value}"]
"#
            ));
        }

        // Single-broker ConfigMap patch when replicas == 1 and no broker config file specified
        if kafka_replicas == 1 && !has_broker_config {
            patches.push_str(
                r#"  - target:
      kind: ConfigMap
      name: kafka-config
    patch: |
      - op: replace
        path: /data/server.properties
        value: |
          # KRaft mode settings
          process.roles=broker,controller
          node.id=${NODE_ID}
          controller.quorum.voters=${CONTROLLER_QUORUM_VOTERS}
          controller.listener.names=CONTROLLER

          # Listeners
          listeners=PLAINTEXT://:9092,CONTROLLER://:9093
          advertised.listeners=PLAINTEXT://${ADVERTISED_HOSTNAME}:9092
          listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
          inter.broker.listener.name=PLAINTEXT

          # Log dirs
          log.dirs=/var/kafka-data/kafka-logs

          # Single-broker defaults
          num.partitions=1
          default.replication.factor=1
          min.insync.replicas=1
          offsets.topic.replication.factor=1
          transaction.state.log.replication.factor=1
          transaction.state.log.min.isr=1

          # Performance (num.network.threads and num.io.threads set by entrypoint based on CPU)
          socket.send.buffer.bytes=131072
          socket.receive.buffer.bytes=131072
          socket.request.max.bytes=104857600

          # Share groups (single-broker)
          share.coordinator.state.topic.replication.factor=1
          share.coordinator.state.topic.min.isr=1

          # Log retention
          log.retention.hours=1
          log.segment.bytes=1073741824
"#,
            );
        }

        // Storage class
        if let Some(sc) = storage_class {
            patches.push_str(&format!(
                r#"  - target:
      kind: StatefulSet
      name: kafka
    patch: |
      - op: add
        path: /spec/volumeClaimTemplates/0/spec/storageClassName
        value: {sc}
"#
            ));
        }

        // Kafka image override (image name + version tag)
        if let Some(version) = kafka_version {
            patches.push_str(&format!(
                r#"  - target:
      kind: StatefulSet
      name: kafka
    patch: |
      - op: replace
        path: /spec/template/spec/containers/0/image
        value: {kafka_image}:{version}
"#
            ));
        }

        // Extra Kafka config (broker config file properties + inline extraConfig)
        if let Some(ref extra) = kafka_extra_config {
            patches.push_str(&format!(
                r#"  - target:
      kind: StatefulSet
      name: kafka
    patch: |
      - op: add
        path: /spec/template/spec/containers/0/env/-
        value:
          name: KAFKA_EXTRA_CONFIG
          value: "{extra}"
"#
            ));
        }

        // Log4j config (custom Kafka broker logging).
        // Rotation overrides (logMaxFileSize, logMaxBackupIndex) are applied on top
        // of the log config file properties, or used standalone with default INFO logging.
        {
            let mut log4j_props: HashMap<String, String> = HashMap::new();

            if let Some(path) = log_config_path {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| anyhow::anyhow!("Cannot read log config '{}': {}", path, e))?;
                let lc: Log4jConfigFile = serde_yaml::from_str(&content)
                    .map_err(|e| anyhow::anyhow!("Cannot parse log config '{}': {}", path, e))?;
                println!("    Using log config: {} ({})", lc.name, path);
                for (k, v) in &lc.properties {
                    let val = match v {
                        serde_yaml::Value::String(s) => s.clone(),
                        other => serde_yaml::to_string(other)
                            .unwrap_or_default()
                            .trim()
                            .to_string(),
                    };
                    log4j_props.insert(k.clone(), val);
                }
            } else if log_max_file_size.is_some() || log_max_backup_index.is_some() {
                // No log config file, but rotation overrides specified — use INFO defaults
                // with a server file appender so the overrides have something to apply to.
                log4j_props.insert("log4j.rootLogger".into(), "INFO, stdout, serverFile".into());
                log4j_props.insert(
                    "log4j.appender.stdout".into(),
                    "org.apache.log4j.ConsoleAppender".into(),
                );
                log4j_props.insert(
                    "log4j.appender.stdout.layout".into(),
                    "org.apache.log4j.PatternLayout".into(),
                );
                log4j_props.insert(
                    "log4j.appender.stdout.layout.ConversionPattern".into(),
                    "[%d] %p %m (%c)%n".into(),
                );
                log4j_props.insert(
                    "log4j.appender.serverFile".into(),
                    "org.apache.log4j.RollingFileAppender".into(),
                );
                log4j_props.insert(
                    "log4j.appender.serverFile.File".into(),
                    "/opt/kafka/logs/server.log".into(),
                );
                log4j_props.insert(
                    "log4j.appender.serverFile.MaxFileSize".into(),
                    "100MB".into(),
                );
                log4j_props.insert(
                    "log4j.appender.serverFile.MaxBackupIndex".into(),
                    "5".into(),
                );
                log4j_props.insert(
                    "log4j.appender.serverFile.layout".into(),
                    "org.apache.log4j.PatternLayout".into(),
                );
                log4j_props.insert(
                    "log4j.appender.serverFile.layout.ConversionPattern".into(),
                    "[%d] %p %m (%c)%n".into(),
                );
                log4j_props.insert("log4j.logger.kafka".into(), "INFO".into());
            }

            // Apply rotation overrides on top of the loaded/default properties
            if let Some(size) = log_max_file_size {
                log4j_props.insert(
                    "log4j.appender.serverFile.MaxFileSize".into(),
                    size.to_string(),
                );
                println!("    Log rotation override: MaxFileSize={}", size);
            }
            if let Some(count) = log_max_backup_index {
                log4j_props.insert(
                    "log4j.appender.serverFile.MaxBackupIndex".into(),
                    count.to_string(),
                );
                println!("    Log rotation override: MaxBackupIndex={}", count);
            }

            if !log4j_props.is_empty() {
                let mut sorted_props: Vec<_> = log4j_props.iter().collect();
                sorted_props.sort_by_key(|(k, _)| (*k).clone());
                let log_config_value = sorted_props
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join("|");
                patches.push_str(&format!(
                    r#"  - target:
      kind: StatefulSet
      name: kafka
    patch: |
      - op: add
        path: /spec/template/spec/containers/0/env/-
        value:
          name: KAFKA_LOG4J_CONFIG
          value: "{log_config_value}"
"#
                ));
            }
        }

        // mTLS: SSL listener + broker secret mount.
        // Patches go after the base broker config patches so the
        // KAFKA_TLS_CONFIG env var (which the entrypoint appends LAST to
        // server.properties) overrides the base PLAINTEXT-only listeners
        // line. Skipped entirely when TLS is off — the deploy stays
        // bit-for-bit identical to the pre-mTLS behavior.
        if let Some(tls) = tls_settings.as_ref() {
            let ssl_port = tls.resolved_ssl_port();
            let tls_lines = kafka_tls::broker_tls_lines(ssl_port, tls.inter_broker).join("|");
            // KAFKA_TLS_CONFIG carries pipe-separated key=value pairs that
            // the entrypoint expands into server.properties (see
            // run/deploy/kubernetes/base/kafka/configmap.yaml). Pipe — not
            // comma — because some SSL property values contain commas.
            patches.push_str(&format!(
                r#"  - target:
      kind: StatefulSet
      name: kafka
    patch: |
      - op: add
        path: /spec/template/spec/containers/0/env/-
        value:
          name: KAFKA_TLS_CONFIG
          value: "{tls_lines}"
      - op: add
        path: /spec/template/spec/containers/0/ports/-
        value:
          containerPort: {ssl_port}
          name: broker-ssl
      - op: add
        path: /spec/template/spec/containers/0/volumeMounts/-
        value:
          name: kafka-tls
          mountPath: {mount_path}
          readOnly: true
      - op: add
        path: /spec/template/spec/volumes/-
        value:
          name: kafka-tls
          secret:
            secretName: {secret}
            defaultMode: 0400
"#,
                tls_lines = tls_lines,
                ssl_port = ssl_port,
                mount_path = kafka_tls::BROKER_MOUNT_PATH,
                secret = kafka_tls::BROKER_SECRET,
            ));

            // Add the SSL port to both Services so DNS / bootstrap traffic
            // can reach it. JSON-Patch /-/ appends to the ports array.
            for svc in &["kafka-headless", "kafka-bootstrap"] {
                patches.push_str(&format!(
                    r#"  - target:
      kind: Service
      name: {svc}
    patch: |
      - op: add
        path: /spec/ports/-
        value:
          name: broker-ssl
          port: {ssl_port}
          targetPort: {ssl_port}
"#,
                    svc = svc,
                    ssl_port = ssl_port,
                ));
            }
        }
    }

    // --- Worker patches ---
    if components.contains(&Component::Workers) {
        patches.push_str(&format!(
            r#"  - target:
      kind: Deployment
      name: benchmark-worker
    patch: |
      - op: replace
        path: /spec/replicas
        value: {worker_replicas}
      - op: replace
        path: /spec/template/spec/containers/0/resources
        value:
          requests:
            cpu: "{worker_cpu}"
            memory: "{worker_memory}"
          limits:
            cpu: "{worker_cpu}"
            memory: "{worker_memory}"
      - op: replace
        path: /spec/template/spec/containers/0/env/0
        value:
          name: HEAP_OPTS
          value: "{worker_heap}"
      - op: replace
        path: /spec/template/spec/containers/0/imagePullPolicy
        value: {pull_policy}
"#
        ));

        // Worker log4j2 config. Same envelope as broker configs but the
        // entrypoint writes it to /tmp/log4j2.properties and points the JVM
        // at it with -Dlog4j2.configurationFile (workers run log4j2, not 1.x).
        if let Some(path) = worker_log_config_path {
            let content = std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("Cannot read worker log config '{}': {}", path, e))?;
            let lc: Log4jConfigFile = serde_yaml::from_str(&content)
                .map_err(|e| anyhow::anyhow!("Cannot parse worker log config '{}': {}", path, e))?;
            println!("    Using worker log config: {} ({})", lc.name, path);

            let mut log4j_props: HashMap<String, String> = HashMap::new();
            for (k, v) in &lc.properties {
                let val = match v {
                    serde_yaml::Value::String(s) => s.clone(),
                    other => serde_yaml::to_string(other)
                        .unwrap_or_default()
                        .trim()
                        .to_string(),
                };
                log4j_props.insert(k.clone(), val);
            }

            if !log4j_props.is_empty() {
                let mut sorted_props: Vec<_> = log4j_props.iter().collect();
                sorted_props.sort_by_key(|(k, _)| (*k).clone());
                let log_config_value = sorted_props
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join("|");
                patches.push_str(&format!(
                    r#"  - target:
      kind: Deployment
      name: benchmark-worker
    patch: |
      - op: add
        path: /spec/template/spec/containers/0/env/-
        value:
          name: WORKER_LOG4J_CONFIG
          value: "{log_config_value}"
"#
                ));
            }
        }
    }

    // --- Coordinator patches ---
    if components.contains(&Component::Coordinator) {
        patches.push_str(&format!(
            r#"  - target:
      kind: Deployment
      name: benchmark-coordinator
    patch: |
      - op: replace
        path: /spec/template/spec/containers/0/resources
        value:
          requests:
            cpu: "{coord_cpu}"
            memory: "{coord_memory}"
          limits:
            cpu: "{coord_cpu}"
            memory: "{coord_memory}"
      - op: replace
        path: /spec/template/spec/containers/0/env/0
        value:
          name: HEAP_OPTS
          value: "{coord_heap}"
      - op: replace
        path: /spec/template/spec/containers/0/imagePullPolicy
        value: {pull_policy}
"#
        ));
    }

    // mTLS: mount the client cert Secret into worker + coordinator pods so
    // any workload that opts in to TLS at run time finds the keystore /
    // truststore at the expected path. Mounted unconditionally when TLS is
    // on for the env (rather than gated on a per-workload flag) — this
    // means PLAINTEXT-only workloads ignore the mount, but TLS workloads
    // can be added/removed without re-deploying.
    if tls_settings.is_some() {
        let mount_path = kafka_tls::CLIENT_MOUNT_PATH;
        let secret = kafka_tls::CLIENT_SECRET;

        // Both worker and coordinator base manifests now declare
        // volumeMounts/volumes (workers carry the dimster-logs emptyDir,
        // coordinator the results PVC), so we always append with `/-`.
        if components.contains(&Component::Workers) {
            patches.push_str(&format!(
                r#"  - target:
      kind: Deployment
      name: benchmark-worker
    patch: |
      - op: add
        path: /spec/template/spec/containers/0/volumeMounts/-
        value:
          name: dimster-tls
          mountPath: {mount_path}
          readOnly: true
      - op: add
        path: /spec/template/spec/volumes/-
        value:
          name: dimster-tls
          secret:
            secretName: {secret}
            defaultMode: 0400
"#,
                mount_path = mount_path,
                secret = secret,
            ));
        }
        if components.contains(&Component::Coordinator) {
            patches.push_str(&format!(
                r#"  - target:
      kind: Deployment
      name: benchmark-coordinator
    patch: |
      - op: add
        path: /spec/template/spec/containers/0/volumeMounts/-
        value:
          name: dimster-tls
          mountPath: {mount_path}
          readOnly: true
      - op: add
        path: /spec/template/spec/volumes/-
        value:
          name: dimster-tls
          secret:
            secretName: {secret}
            defaultMode: 0400
"#,
                mount_path = mount_path,
                secret = secret,
            ));
        }
    }

    // --- Flink patches ---
    if components.contains(&Component::Flink) {
        if let Some(flink_config) = &env_config.flink {
            let tls = tls_settings
                .as_ref()
                .map(|_| (kafka_tls::CLIENT_MOUNT_PATH, kafka_tls::CLIENT_SECRET));
            patches.push_str(&flink::patches(flink_config, tls)?);
        }
    }

    // Worker node affinity (applies when workers are being deployed)
    if components.contains(&Component::Workers) {
        if let Some(label) = worker_node_label {
            let (key, value) = parse_node_label(label)?;
            patches.push_str(&format!(
                r#"  - target:
      kind: Deployment
      name: benchmark-worker
    patch: |
      - op: add
        path: /spec/template/spec/affinity
        value:
          nodeAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
              nodeSelectorTerms:
                - matchExpressions:
                    - key: {key}
                      operator: In
                      values: ["{value}"]
"#
            ));
            // Coordinator goes on worker nodes only if no monitoring label is set
            if monitoring_node_label.is_none() && components.contains(&Component::Coordinator) {
                patches.push_str(&format!(
                    r#"  - target:
      kind: Deployment
      name: benchmark-coordinator
    patch: |
      - op: add
        path: /spec/template/spec/affinity
        value:
          nodeAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
              nodeSelectorTerms:
                - matchExpressions:
                    - key: {key}
                      operator: In
                      values: ["{value}"]
"#
                ));
            }
        }
    }

    // Monitoring node affinity — coordinator, Grafana, and Prometheus
    if let Some(label) = monitoring_node_label {
        let (key, value) = parse_node_label(label)?;
        // Only apply to components that are being deployed
        let mut targets: Vec<&str> = Vec::new();
        if components.contains(&Component::Coordinator) {
            targets.push("benchmark-coordinator");
        }
        if components.contains(&Component::Monitoring) {
            targets.push("grafana");
            targets.push("prometheus");
        }
        for name in targets {
            patches.push_str(&format!(
                r#"  - target:
      kind: Deployment
      name: {name}
    patch: |
      - op: add
        path: /spec/template/spec/affinity
        value:
          nodeAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
              nodeSelectorTerms:
                - matchExpressions:
                    - key: {key}
                      operator: In
                      values: ["{value}"]
"#
            ));
        }
    }

    // Monitoring resource overrides — Prometheus and Grafana
    if components.contains(&Component::Monitoring) {
        if let Some(reqs) = monitoring
            .and_then(|m| m.prometheus.as_ref())
            .and_then(|p| p.requests.as_ref())
        {
            if let (Some(cpu), Some(memory)) = (reqs.cpu.as_deref(), reqs.memory.as_deref()) {
                patches.push_str(&format!(
                    r#"  - target:
      kind: Deployment
      name: prometheus
    patch: |
      - op: replace
        path: /spec/template/spec/containers/0/resources
        value:
          requests:
            cpu: "{cpu}"
            memory: "{memory}"
          limits:
            cpu: "{cpu}"
            memory: "{memory}"
"#
                ));
            }
        }

        if let Some(reqs) = monitoring
            .and_then(|m| m.grafana.as_ref())
            .and_then(|g| g.requests.as_ref())
        {
            if let (Some(cpu), Some(memory)) = (reqs.cpu.as_deref(), reqs.memory.as_deref()) {
                patches.push_str(&format!(
                    r#"  - target:
      kind: Deployment
      name: grafana
    patch: |
      - op: replace
        path: /spec/template/spec/containers/0/resources
        value:
          requests:
            cpu: "{cpu}"
            memory: "{memory}"
          limits:
            cpu: "{cpu}"
            memory: "{memory}"
"#
                ));
            }
        }
    }

    // Assemble final kustomization from resources + patches + images.
    // The top-level `namespace:` field tells kustomize to rewrite
    // `metadata.namespace` on every namespaced resource — this is what
    // lets the same base manifests (which all hardcode `namespace: dimster`)
    // be deployed into any namespace the user chooses per-env.
    let ns = kubectl.namespace();
    let mut kustomization = format!(
        "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: {ns}\n\nresources:\n{resource_list}\n"
    );

    if !patches.is_empty() {
        kustomization.push_str("\npatches:\n");
        kustomization.push_str(&patches);
    }

    // Image override — remap the base image name to the environment's registry,
    // and append the per-version suffix to the tag so the manifests reference
    // the actual image produced by `dimster build` (which always emits
    // <name>:<tag>-kafka<kafka-clients-version>; there is no version-less image any more).
    // Only needed when deploying components that use the dimster image.
    let has_image_components =
        components.contains(&Component::Workers) || components.contains(&Component::Coordinator);
    if has_image_components {
        let base_image = config
            .environments
            .get("local")
            .and_then(|e| e.image.as_deref())
            .unwrap_or("dimster");
        let image_tag = &config.image.tag;
        let (_flavor, version) = env_config.kafka_flavor()?;
        let resolved_tag = format!("{}-kafka{}", image_tag, version);

        // Patch is always emitted: even when env_image == base_image, the
        // source manifests reference plain "dimster:latest" (a placeholder
        // tag in the K8s YAML) while the built image is
        // "dimster:<image_tag>-kafka<version>". Kustomize swaps the tag to
        // the real one at deploy time.
        kustomization.push_str(&format!(
            r#"
images:
  - name: {base_image}
    newName: {env_image}
    newTag: {resolved_tag}
"#
        ));
    }

    std::fs::write(tmp.join("kustomization.yaml"), &kustomization)?;

    let desired_kafka_hash = if !external && components.contains(&Component::Kafka) {
        let rendered = kubectl
            .run_no_ns(&["kustomize", &tmp.to_string_lossy()])
            .context("Failed to render deploy overlay for Kafka drift detection")?;
        let desired_hash = kafka_spec_hash_from_rendered_yaml(&rendered)
            .context("Failed to hash rendered Kafka resources")?;
        let live_hash = live_kafka_spec_hash(kubectl)?;
        match kafka_recreate_decision(&live_hash, &desired_hash) {
            KafkaRecreateDecision::ApplyOnly => {
                if live_hash == LiveKafkaSpecHash::Missing {
                    println!("==> Kafka not deployed yet; applying Kafka resources.");
                } else {
                    println!("==> Kafka spec unchanged; preserving Kafka PVCs.");
                }
            }
            KafkaRecreateDecision::Recreate(reason) => {
                println!("==> Kafka spec {reason}; recreating Kafka and deleting Kafka PVCs...");
                let reset_local_storage_pvs = env_config
                    .kafka
                    .as_ref()
                    .and_then(|k| k.pvc_storage_class.as_deref())
                    == Some("local-storage");
                cleanup_kafka_cluster(kubectl, reset_local_storage_pvs)
                    .context("Failed to clean up existing Kafka resources before deploy")?;
            }
        }
        Some(desired_hash)
    } else {
        None
    };

    kubectl.run_no_ns(&["apply", "-k", &tmp.to_string_lossy()])?;

    if let Some(hash) = desired_kafka_hash {
        annotate_kafka_spec_hash(kubectl, &hash)
            .context("Failed to record deployed Kafka spec hash")?;
    }

    Ok(())
}

/// Convert K8s CPU value to integer cores for Kafka thread calculation.
/// "250m" → 1, "500m" → 1, "1500m" → 2, "2" → 2, "6" → 6
fn cpu_to_cores(cpu: &str) -> u32 {
    if let Some(millis_str) = cpu.strip_suffix('m') {
        let millis: u32 = millis_str.parse().unwrap_or(1000);
        (millis / 1000).max(1)
    } else {
        cpu.parse().unwrap_or(1)
    }
}

/// Reset Released/Failed local-storage PVs by clearing their stale claimRef.
/// This allows new PVCs to bind to them on redeploy.
fn reset_local_pvs(kubectl: &Kubectl) {
    let output = kubectl.run_no_ns(&[
        "get", "pv",
        "-o", "jsonpath={range .items[?(@.spec.storageClassName==\"local-storage\")]}{.metadata.name} {.status.phase}{\"\\n\"}{end}",
    ]);

    let pvs = match output {
        Ok(s) => s,
        Err(_) => return, // No PVs or kubectl error — skip silently
    };

    for line in pvs.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 2 && (parts[1] == "Released" || parts[1] == "Failed") {
            let pv_name = parts[0];
            println!("  Resetting {} PV: {}", parts[1], pv_name);
            let _ = kubectl.run_no_ns(&[
                "patch",
                "pv",
                pv_name,
                "--type",
                "json",
                "-p",
                "[{\"op\":\"remove\",\"path\":\"/spec/claimRef\"}]",
            ]);
        }
    }
}

/// Delete the in-cluster Kafka deployment and its generated data PVCs.
///
/// This helper is intentionally scoped to Kafka's own state. It does not touch
/// workers, coordinator, results PVCs, or monitoring resources, which lets both
/// `deploy` and `teardown` reuse the same reliable Kafka-state cleanup without
/// duplicating the storage-sensitive parts.
pub(crate) fn cleanup_kafka_cluster(
    kubectl: &Kubectl,
    reset_local_storage_pvs: bool,
) -> Result<()> {
    kubectl.run(&[
        "delete",
        "statefulset",
        "kafka",
        "--cascade=foreground",
        "--ignore-not-found",
    ])?;

    // Delete by label as well as deleting the StatefulSet. Older Dimster
    // deploys used --cascade=orphan for Kafka, so a cluster can legitimately
    // contain kafka pods with no StatefulSet owner left to garbage collect.
    kubectl.run(&["delete", "pods", "-l", "app=kafka", "--ignore-not-found"])?;
    wait_for_kafka_pods_deleted(kubectl)?;

    let pvcs = kafka_data_pvc_names(kubectl)?;
    for pvc in pvcs {
        kubectl.run(&["delete", "pvc", &pvc, "--ignore-not-found"])?;
        wait_for_resource_deleted(kubectl, &format!("pvc/{pvc}"))?;
    }

    if reset_local_storage_pvs {
        reset_local_pvs(kubectl);
    }

    Ok(())
}

fn wait_for_kafka_pods_deleted(kubectl: &Kubectl) -> Result<()> {
    let pods = kubectl.run(&[
        "get",
        "pods",
        "-l",
        "app=kafka",
        "-o",
        "jsonpath={.items[*].metadata.name}",
    ])?;
    if pods.split_whitespace().next().is_none() {
        return Ok(());
    }
    match kubectl.run(&[
        "wait",
        "--for=delete",
        "pod",
        "-l",
        "app=kafka",
        "--timeout=300s",
    ]) {
        Ok(_) => Ok(()),
        Err(e) if is_kubectl_no_matching_resources(&e) => Ok(()),
        Err(e) => Err(e),
    }
}

fn wait_for_resource_deleted(kubectl: &Kubectl, resource: &str) -> Result<()> {
    match kubectl.run(&["wait", "--for=delete", resource, "--timeout=300s"]) {
        Ok(_) => Ok(()),
        Err(e) if is_kubectl_no_matching_resources(&e) => Ok(()),
        Err(e) => Err(e),
    }
}

fn is_kubectl_no_matching_resources(err: &anyhow::Error) -> bool {
    let msg = format!("{err:#}");
    msg.contains("no matching resources found") || msg.contains("NotFound")
}

fn kafka_data_pvc_names(kubectl: &Kubectl) -> Result<Vec<String>> {
    let json = kubectl.run(&["get", "pvc", "-o", "json"])?;
    kafka_data_pvc_names_from_json(&json)
}

fn kafka_data_pvc_names_from_json(json: &str) -> Result<Vec<String>> {
    #[derive(Deserialize)]
    struct PvcList {
        #[serde(default)]
        items: Vec<PvcItem>,
    }

    #[derive(Deserialize)]
    struct PvcItem {
        metadata: PvcMetadata,
    }

    #[derive(Deserialize)]
    struct PvcMetadata {
        name: String,
    }

    let list: PvcList = serde_json::from_str(json).context("Cannot parse kubectl get pvc JSON")?;
    let mut names: Vec<String> = list
        .items
        .into_iter()
        .map(|item| item.metadata.name)
        .filter(|name| name.starts_with(KAFKA_DATA_PVC_PREFIX))
        .collect();
    names.sort();
    Ok(names)
}

fn live_kafka_spec_hash(kubectl: &Kubectl) -> Result<LiveKafkaSpecHash> {
    let json = kubectl.run(&[
        "get",
        "statefulset",
        "kafka",
        "--ignore-not-found",
        "-o",
        "json",
    ])?;
    if json.trim().is_empty() {
        return Ok(LiveKafkaSpecHash::Missing);
    }

    let value: serde_json::Value =
        serde_json::from_str(&json).context("Cannot parse live Kafka StatefulSet JSON")?;
    let name = value.pointer("/metadata/name").and_then(|v| v.as_str());
    if name != Some("kafka") {
        return Ok(LiveKafkaSpecHash::Missing);
    }

    let hash = value
        .pointer(&format!(
            "/metadata/annotations/{}",
            json_pointer_escape(KAFKA_SPEC_HASH_ANNOTATION)
        ))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    Ok(LiveKafkaSpecHash::Present(hash))
}

fn json_pointer_escape(s: &str) -> String {
    s.replace('~', "~0").replace('/', "~1")
}

fn kafka_recreate_decision(live: &LiveKafkaSpecHash, desired_hash: &str) -> KafkaRecreateDecision {
    match live {
        LiveKafkaSpecHash::Missing => KafkaRecreateDecision::ApplyOnly,
        LiveKafkaSpecHash::Present(Some(live_hash)) if live_hash == desired_hash => {
            KafkaRecreateDecision::ApplyOnly
        }
        LiveKafkaSpecHash::Present(Some(_)) => KafkaRecreateDecision::Recreate("changed"),
        LiveKafkaSpecHash::Present(None) => {
            KafkaRecreateDecision::Recreate("has no recorded spec hash")
        }
    }
}

fn annotate_kafka_spec_hash(kubectl: &Kubectl, hash: &str) -> Result<()> {
    let annotation = format!("{KAFKA_SPEC_HASH_ANNOTATION}={hash}");
    kubectl.run(&[
        "annotate",
        "statefulset",
        "kafka",
        &annotation,
        "--overwrite",
    ])?;
    Ok(())
}

fn kafka_spec_hash_from_rendered_yaml(rendered: &str) -> Result<String> {
    let mut resources = extract_kafka_resources(rendered)?;
    if resources.is_empty() {
        bail!("rendered overlay did not contain any Kafka resources");
    }
    resources.sort_by(|a, b| a.key.cmp(&b.key));

    let mut hash = Fnv1a64::new();
    for resource in resources {
        hash.update(resource.key.as_bytes());
        hash.update(b"\n");
        let serialized = serde_json::to_string(&resource.value)
            .context("Cannot serialize Kafka resource for hashing")?;
        hash.update(serialized.as_bytes());
        hash.update(b"\n---\n");
    }
    Ok(format!("{:016x}", hash.finish()))
}

#[derive(Debug)]
struct KafkaResourceForHash {
    key: String,
    value: serde_yaml::Value,
}

fn extract_kafka_resources(rendered: &str) -> Result<Vec<KafkaResourceForHash>> {
    let mut resources = Vec::new();
    for document in serde_yaml::Deserializer::from_str(rendered) {
        let value = serde_yaml::Value::deserialize(document)
            .context("Cannot parse rendered Kubernetes YAML document")?;
        if value.is_null() {
            continue;
        }
        let Some(kind) = yaml_field_as_str(&value, "kind") else {
            continue;
        };
        let Some(name) = yaml_metadata_name(&value) else {
            continue;
        };
        if is_kafka_owned_resource(kind, name) {
            resources.push(KafkaResourceForHash {
                key: format!("{kind}/{name}"),
                value,
            });
        }
    }
    Ok(resources)
}

fn is_kafka_owned_resource(kind: &str, name: &str) -> bool {
    matches!(
        (kind, name),
        ("StatefulSet", "kafka")
            | ("ConfigMap", "kafka-config")
            | ("ConfigMap", "kafka-jmx-config")
            | ("Service", "kafka-headless")
            | ("Service", "kafka-bootstrap")
    )
}

fn yaml_metadata_name(value: &serde_yaml::Value) -> Option<&str> {
    yaml_mapping_get(value, "metadata").and_then(|metadata| yaml_field_as_str(metadata, "name"))
}

fn yaml_field_as_str<'a>(value: &'a serde_yaml::Value, key: &str) -> Option<&'a str> {
    yaml_mapping_get(value, key).and_then(|v| v.as_str())
}

fn yaml_mapping_get<'a>(value: &'a serde_yaml::Value, key: &str) -> Option<&'a serde_yaml::Value> {
    let serde_yaml::Value::Mapping(map) = value else {
        return None;
    };
    map.get(serde_yaml::Value::String(key.to_string()))
}

/// Tiny deterministic hash implementation to avoid pulling a new dependency
/// into the CLI just to store a deploy drift token. It is not used for
/// security, only for stable equality across Dimster runs.
struct Fnv1a64(u64);

impl Fnv1a64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    fn new() -> Self {
        Self(Self::OFFSET)
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }

    fn finish(self) -> u64 {
        self.0
    }
}

/// Recursively copy a directory tree. Exposed to other commands (notably
/// teardown) so they can stage their own kustomize overlays the same way
/// deploy does — kustomize refuses to follow symlinks for security, so a
/// real copy is the only reliable approach.
pub(crate) fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

/// Write a `Namespace` resource for the given namespace name to the given
/// path. Shared with teardown because both commands need to materialize a
/// Namespace whose name matches the env's resolved namespace (kustomize's
/// `namespace:` field rewrites `metadata.namespace` but not the Namespace
/// resource's own `metadata.name`).
pub(crate) fn write_namespace_yaml(path: &Path, ns: &str) -> Result<()> {
    let content = format!("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {ns}\n");
    std::fs::write(path, content)?;
    Ok(())
}

/// Rewrite the literal substring `dimster.svc.cluster.local` to
/// `<ns>.svc.cluster.local` in every base manifest that embeds an FQDN
/// inside a ConfigMap data section. Kustomize can rewrite resource
/// `metadata.namespace` but not arbitrary text inside data fields, so
/// the kafka entrypoint script (kafka quorum voters + advertised
/// hostname), the prometheus scrape config, and the grafana datasource
/// URL all need this string-level fix-up before kustomize sees them.
///
/// When the env's namespace is literally `dimster` this is a no-op;
/// otherwise it's the difference between a working multi-namespace
/// deploy and Kafka silently failing DNS resolution to the wrong namespace.
pub(crate) fn rewrite_dns_namespace(tmp: &Path, ns: &str) -> Result<()> {
    let needle = "dimster.svc.cluster.local";
    let replacement = format!("{ns}.svc.cluster.local");
    if needle == replacement {
        return Ok(());
    }
    // Files known to carry the hardcoded DNS string. If a future base
    // manifest adds another such reference, add its path here.
    let targets = [
        "base/kafka/configmap.yaml",
        "base/monitoring/prometheus-config.yaml",
        "base/monitoring/grafana-config.yaml",
    ];
    for rel in targets {
        let path = tmp.join(rel);
        if !path.exists() {
            continue;
        }
        let content = std::fs::read_to_string(&path)?;
        let rewritten = content.replace(needle, &replacement);
        std::fs::write(&path, rewritten)?;
    }
    Ok(())
}

/// Write a prometheus ConfigMap YAML without the kafka-jmx scrape job.
/// Used in external deploy mode where there are no Kafka pods to scrape.
/// Templated with the env's namespace so both the ConfigMap itself and
/// the dns_sd_configs entries (which look up worker / node-exporter
/// services by FQDN) line up with the rest of the deployment.
fn write_external_prometheus_config(dir: &Path, ns: &str) -> Result<()> {
    let content = format!(
        r#"apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
  namespace: {ns}
data:
  prometheus.yml: |
    global:
      scrape_interval: 10s
      evaluation_interval: 10s

    scrape_configs:
      - job_name: 'benchmark-workers'
        dns_sd_configs:
          - names:
              - 'benchmark-worker.{ns}.svc.cluster.local'
            type: A
            port: 9090

      - job_name: 'node-exporter'
        dns_sd_configs:
          - names:
              - 'node-exporter.{ns}.svc.cluster.local'
            type: A
            port: 9100
"#
    );
    std::fs::write(dir.join("prometheus-config.yaml"), content)?;
    Ok(())
}

fn parse_node_label(label: &str) -> Result<(&str, &str)> {
    label.split_once('=').ok_or_else(|| {
        anyhow::anyhow!("Invalid nodeLabel '{}': expected format 'key=value'", label)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Namespace resource's name comes through verbatim — kustomize's
    /// `namespace:` field rewrites `metadata.namespace` on namespaced
    /// resources but NOT the Namespace's own `metadata.name`, so we have
    /// to template this file ourselves.
    #[test]
    fn write_namespace_yaml_uses_provided_name() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("namespace.yaml");
        write_namespace_yaml(&path, "dimster2").unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("kind: Namespace"));
        assert!(content.contains("name: dimster2"));
        assert!(!content.contains("name: dimster\n"));
    }

    /// External-mode prometheus config must template the namespace into
    /// both metadata.namespace AND the dns_sd_configs FQDNs. Without the
    /// FQDN substitution, prometheus would scrape the wrong namespace
    /// (or `dimster` from a sibling instance) and silently report empty
    /// metrics. The previous version of this function hardcoded `dimster`
    /// in three places.
    #[test]
    fn external_prometheus_config_substitutes_namespace_everywhere() {
        let tmp = tempfile::tempdir().unwrap();
        write_external_prometheus_config(tmp.path(), "dimster3").unwrap();
        let content = std::fs::read_to_string(tmp.path().join("prometheus-config.yaml")).unwrap();
        assert!(content.contains("namespace: dimster3"));
        assert!(content.contains("benchmark-worker.dimster3.svc.cluster.local"));
        assert!(content.contains("node-exporter.dimster3.svc.cluster.local"));
        // Hard guarantee against the old hardcoded values leaking back in.
        assert!(!content.contains("dimster.svc.cluster.local"));
        assert!(!content.contains("namespace: dimster\n"));
    }

    /// The DNS rewrite has to swap every occurrence of
    /// `dimster.svc.cluster.local` inside ConfigMap data sections —
    /// kafka quorum voters, kafka advertised hostname, prometheus
    /// scrape targets, grafana datasource. Missing any one of them
    /// would leave a sibling-namespace deploy silently broken (the
    /// failure mode that prompted this whole change).
    #[test]
    fn rewrite_dns_namespace_substitutes_in_all_known_files() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().join("base");
        std::fs::create_dir_all(base.join("kafka")).unwrap();
        std::fs::create_dir_all(base.join("monitoring")).unwrap();
        std::fs::write(
            base.join("kafka/configmap.yaml"),
            "VOTERS=kafka-0.kafka-headless.dimster.svc.cluster.local:9093\n\
             ADVERTISED=kafka-${NODE_ID}.kafka-headless.dimster.svc.cluster.local\n",
        )
        .unwrap();
        std::fs::write(
            base.join("monitoring/prometheus-config.yaml"),
            "- 'benchmark-worker.dimster.svc.cluster.local'\n\
             - 'kafka-headless.dimster.svc.cluster.local'\n\
             - 'node-exporter.dimster.svc.cluster.local'\n",
        )
        .unwrap();
        std::fs::write(
            base.join("monitoring/grafana-config.yaml"),
            "url: http://prometheus.dimster.svc.cluster.local:9090\n",
        )
        .unwrap();

        rewrite_dns_namespace(tmp.path(), "dimster2").unwrap();

        for rel in [
            "base/kafka/configmap.yaml",
            "base/monitoring/prometheus-config.yaml",
            "base/monitoring/grafana-config.yaml",
        ] {
            let content = std::fs::read_to_string(tmp.path().join(rel)).unwrap();
            assert!(
                !content.contains("dimster.svc.cluster.local"),
                "{} still has the old DNS string:\n{}",
                rel,
                content
            );
            assert!(
                content.contains("dimster2.svc.cluster.local"),
                "{} missing the rewritten DNS string:\n{}",
                rel,
                content
            );
        }
    }

    /// When the env's namespace is literally `dimster` the rewrite is a
    /// no-op. Skipping the file walk avoids unnecessary I/O AND avoids
    /// failing on absent files in the unlikely case the manifests get
    /// renamed before the targets list is updated.
    #[test]
    fn rewrite_dns_namespace_is_noop_for_default_namespace() {
        let tmp = tempfile::tempdir().unwrap();
        // No files exist; if the function tried to walk them this would
        // error. The early-return for ns == "dimster" prevents that.
        rewrite_dns_namespace(tmp.path(), "dimster").unwrap();
    }

    #[test]
    fn kafka_resource_hash_uses_only_kafka_owned_resources() {
        let base = r#"
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
spec:
  replicas: 1
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-config
data:
  server.properties: "log.dirs=/var/kafka-data/kafka-logs"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: benchmark-worker
spec:
  replicas: 1
  template:
    spec:
      containers:
        - name: worker
          image: dimster:old
"#;
        let worker_changed = base.replace("dimster:old", "dimster:new");
        let kafka_changed = base.replace(
            "log.dirs=/var/kafka-data/kafka-logs",
            "log.retention.hours=2",
        );

        let base_hash = kafka_spec_hash_from_rendered_yaml(base).unwrap();
        assert_eq!(
            base_hash,
            kafka_spec_hash_from_rendered_yaml(&worker_changed).unwrap(),
            "worker-only changes must not trigger a Kafka PVC recreate"
        );
        assert_ne!(
            base_hash,
            kafka_spec_hash_from_rendered_yaml(&kafka_changed).unwrap(),
            "Kafka config changes must trigger a Kafka PVC recreate"
        );
    }

    #[test]
    fn kafka_resource_hash_is_stable_across_document_order() {
        let first = r#"
apiVersion: v1
kind: Service
metadata:
  name: kafka-bootstrap
spec:
  ports:
    - name: broker
      port: 9092
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
spec:
  replicas: 3
"#;
        let reversed = r#"
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
spec:
  replicas: 3
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-bootstrap
spec:
  ports:
    - name: broker
      port: 9092
"#;

        assert_eq!(
            kafka_spec_hash_from_rendered_yaml(first).unwrap(),
            kafka_spec_hash_from_rendered_yaml(reversed).unwrap()
        );
    }

    #[test]
    fn kafka_recreate_decision_preserves_only_known_matching_hash() {
        assert_eq!(
            kafka_recreate_decision(&LiveKafkaSpecHash::Missing, "abc"),
            KafkaRecreateDecision::ApplyOnly
        );
        assert_eq!(
            kafka_recreate_decision(&LiveKafkaSpecHash::Present(Some("abc".into())), "abc"),
            KafkaRecreateDecision::ApplyOnly
        );
        assert_eq!(
            kafka_recreate_decision(&LiveKafkaSpecHash::Present(Some("old".into())), "abc"),
            KafkaRecreateDecision::Recreate("changed")
        );
        assert_eq!(
            kafka_recreate_decision(&LiveKafkaSpecHash::Present(None), "abc"),
            KafkaRecreateDecision::Recreate("has no recorded spec hash")
        );
    }

    #[test]
    fn kafka_data_pvc_discovery_uses_statefulset_name_prefix() {
        let json = r#"{
          "items": [
            {"metadata": {"name": "kafka-data-kafka-0"}},
            {"metadata": {"name": "benchmark-results"}},
            {"metadata": {"name": "kafka-data-kafka-2"}},
            {"metadata": {"name": "not-kafka-data-kafka-0"}},
            {"metadata": {"name": "kafka-data-kafka-1"}}
          ]
        }"#;

        assert_eq!(
            kafka_data_pvc_names_from_json(json).unwrap(),
            vec![
                "kafka-data-kafka-0".to_string(),
                "kafka-data-kafka-1".to_string(),
                "kafka-data-kafka-2".to_string(),
            ]
        );
    }

    #[test]
    fn teardown_kafka_resources_exclude_namespace_and_include_jmx_config() {
        let components = HashSet::from([Component::Kafka]);
        let resources = build_teardown_resource_list(&components, false, false);

        assert!(!resources.contains("base/namespace.yaml"));
        assert!(resources.contains("base/kafka/configmap.yaml"));
        assert!(resources.contains("base/kafka/service.yaml"));
        assert!(resources.contains("base/kafka/statefulset.yaml"));
        assert!(resources.contains("base/monitoring/kafka-jmx-config.yaml"));
        assert!(!resources.contains("base/workers/deployment.yaml"));
    }

    #[test]
    fn teardown_coordinator_resources_include_results_pvc() {
        let components = HashSet::from([Component::Coordinator]);
        let resources = build_teardown_resource_list(&components, false, false);

        assert!(resources.contains("base/coordinator/deployment.yaml"));
        assert!(resources.contains("base/coordinator/service.yaml"));
        assert!(resources.contains("base/results/pvc.yaml"));
        assert!(!resources.contains("base/kafka/statefulset.yaml"));
        assert!(!resources.contains("base/monitoring/prometheus.yaml"));
    }

    #[test]
    fn teardown_workers_resources_are_component_scoped() {
        let components = HashSet::from([Component::Workers]);
        let resources = build_teardown_resource_list(&components, false, false);

        assert!(resources.contains("base/workers/deployment.yaml"));
        assert!(resources.contains("base/workers/service.yaml"));
        assert!(!resources.contains("base/kafka/statefulset.yaml"));
        assert!(!resources.contains("base/coordinator/deployment.yaml"));
        assert!(!resources.contains("base/monitoring/prometheus.yaml"));
    }

    #[test]
    fn teardown_kafka_coordinator_workers_omits_monitoring_and_namespace() {
        let components =
            HashSet::from([Component::Kafka, Component::Coordinator, Component::Workers]);
        let resources = build_teardown_resource_list(&components, false, false);

        assert!(!resources.contains("base/namespace.yaml"));
        assert!(resources.contains("base/kafka/statefulset.yaml"));
        assert!(resources.contains("base/coordinator/deployment.yaml"));
        assert!(resources.contains("base/workers/deployment.yaml"));
        assert!(!resources.contains("base/monitoring/prometheus.yaml"));
        assert!(!resources.contains("base/monitoring/grafana.yaml"));
    }

    #[test]
    fn flink_resources_are_component_scoped() {
        let flink = build_teardown_resource_list(&HashSet::from([Component::Flink]), false, false);
        for resource in flink::RESOURCES {
            assert!(flink.contains(resource), "{flink}");
        }
        assert!(!flink.contains("base/workers/deployment.yaml"));

        let workers =
            build_teardown_resource_list(&HashSet::from([Component::Workers]), false, false);
        assert!(!workers.contains("base/flink/"));
    }

    #[test]
    fn full_teardown_includes_flink() {
        let resources = build_teardown_resource_list(&Component::all(), false, true);
        assert!(resources.contains("base/flink/jobmanager.yaml"));
        assert_eq!(
            parse_components(&["flink".to_string()]).unwrap(),
            HashSet::from([Component::Flink])
        );
    }

    #[test]
    fn whole_cpu_request_accepts_integer_and_whole_millicores() {
        assert_eq!(require_whole_cpu_request("workers", "1").unwrap(), 1);
        assert_eq!(require_whole_cpu_request("workers", "2").unwrap(), 2);
        assert_eq!(require_whole_cpu_request("workers", "4").unwrap(), 4);
        assert_eq!(require_whole_cpu_request("workers", "1000m").unwrap(), 1);
        assert_eq!(require_whole_cpu_request("workers", "4000m").unwrap(), 4);
    }

    #[test]
    fn whole_cpu_request_rejects_fractional_or_zero_effective_cpu() {
        for cpu in ["250m", "1500m", "1.5", "0"] {
            let err = require_whole_cpu_request("workers", cpu).unwrap_err();
            let msg = format!("{err:#}");
            assert!(
                msg.contains("workers") || msg.contains("Invalid workers CPU request"),
                "unexpected error for {cpu}: {msg}"
            );
        }
    }

    #[test]
    fn kubelet_configz_parses_policy_options_as_bool_or_string() {
        let bool_config = parse_kubelet_cpu_manager_config(
            r#"{
              "kubeletconfig": {
                "cpuManagerPolicy": "static",
                "cpuManagerPolicyOptions": {
                  "full-pcpus-only": true
                }
              }
            }"#,
        )
        .unwrap();
        assert_eq!(bool_config.policy.as_deref(), Some("static"));
        assert!(bool_config.full_pcpus_only);

        let string_config = parse_kubelet_cpu_manager_config(
            r#"{
              "kubeletconfig": {
                "cpuManagerPolicy": "static",
                "cpuManagerPolicyOptions": {
                  "full-pcpus-only": "true"
                }
              }
            }"#,
        )
        .unwrap();
        assert_eq!(string_config.policy.as_deref(), Some("static"));
        assert!(string_config.full_pcpus_only);
    }

    #[test]
    fn kubelet_config_validation_requires_static_and_full_pcpus_when_requested() {
        let expected = CpuManagerConfig {
            policy: CpuManagerPolicy::Static,
            full_pcpus_only: true,
        };
        let ok = KubeletCpuManagerConfig {
            policy: Some("static".into()),
            full_pcpus_only: true,
        };
        validate_kubelet_cpu_manager_config("node-a", &expected, &ok).unwrap();

        let missing_static = KubeletCpuManagerConfig {
            policy: Some("none".into()),
            full_pcpus_only: true,
        };
        let err =
            validate_kubelet_cpu_manager_config("node-a", &expected, &missing_static).unwrap_err();
        assert!(err.to_string().contains("cpuManagerPolicy=static"));

        let missing_full_cores = KubeletCpuManagerConfig {
            policy: Some("static".into()),
            full_pcpus_only: false,
        };
        let err = validate_kubelet_cpu_manager_config("node-a", &expected, &missing_full_cores)
            .unwrap_err();
        assert!(err.to_string().contains("full-pcpus-only"));
    }

    #[test]
    fn node_selection_honors_labels_and_ignores_unschedulable_nodes() {
        let nodes = parse_nodes(
            r#"{
              "items": [
                {
                  "metadata": {"name": "worker-a", "labels": {"role": "worker"}},
                  "spec": {}
                },
                {
                  "metadata": {"name": "worker-b", "labels": {"role": "worker"}},
                  "spec": {"unschedulable": true}
                },
                {
                  "metadata": {"name": "kafka-a", "labels": {"role": "kafka"}},
                  "spec": {}
                }
              ]
            }"#,
        )
        .unwrap();
        let target = BenchmarkCpuTarget {
            component: "workers",
            cpu_request: "2",
            node_label: Some("role=worker"),
        };
        let matches = nodes_matching_target(&nodes, &target).unwrap();
        assert_eq!(matches, vec!["worker-a"]);
    }

    #[test]
    fn node_selection_without_label_uses_all_schedulable_nodes() {
        let nodes = parse_nodes(
            r#"{
              "items": [
                {"metadata": {"name": "node-a", "labels": {}}, "spec": {}},
                {"metadata": {"name": "node-b", "labels": {}}, "spec": {"unschedulable": true}},
                {"metadata": {"name": "node-c", "labels": {}}, "spec": {}}
              ]
            }"#,
        )
        .unwrap();
        let target = BenchmarkCpuTarget {
            component: "workers",
            cpu_request: "2",
            node_label: None,
        };
        let matches = nodes_matching_target(&nodes, &target).unwrap();
        assert_eq!(matches, vec!["node-a", "node-c"]);
    }
}
