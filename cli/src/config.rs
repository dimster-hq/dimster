use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// Default kafka-clients version for envs that don't set `kafka_client_version`.
/// Picked so that pre-multi-version env files keep producing 4.2.0 builds
/// without any change. Any future change here should be a deliberate decision
/// to flip the implicit default for existing configs.
pub const DEFAULT_KAFKA_VERSION: &str = "4.2.0";

/// Available Kafka client flavors, in ascending version order. Each entry
/// defines a flavor module that can be used for versions >= the min version
/// and < the next flavor's min version (or unbounded if it's the last entry).
/// The tuple is (min_major, min_minor, min_patch, flavor_name).
static KAFKA_FLAVORS: &[(u32, u32, u32, &str)] = &[
    (3, 0, 0, "3.x"),   // 3.0.0 <= v < 4.1.0
    (4, 1, 0, "4.1"),   // 4.1.0 <= v < 4.2.0
    (4, 2, 0, "4.2.0"), // 4.2.0 <= v (highest available flavor)
];

/// Maps a concrete kafka-clients version string (e.g. "3.7.2", "4.1.0",
/// "4.2.0", "4.3.0") to the dimster-kafka-X impl module that should be built
/// against it. Returns (flavor, version) where flavor is the module suffix
/// (e.g. "3.x", "4.1", "4.2.0") and version is the requested version string
/// passed through unchanged.
///
/// Automatically matches the requested version to the highest available flavor
/// that is <= the requested version. This means:
///   - 3.x.y versions use the "3.x" flavor
///   - 4.0.y versions use the "3.x" flavor (4.0 < 4.1, so uses highest below)
///   - 4.1.y versions use the "4.1" flavor
///   - 4.2.y and above (4.3.x, 4.4.x, etc.) use the "4.2.0" flavor
///
/// To add support for a new Kafka line: create the dimster-kafka-X module,
/// add the .dockerignore re-include, then add an entry to KAFKA_FLAVORS above.
pub fn parse_kafka_client_version(version: &str) -> Result<(&'static str, &str)> {
    let parts: Vec<&str> = version.split('.').collect();
    if parts.len() < 3 {
        bail!(
            "Invalid Kafka version '{}'. Expected format: major.minor.patch",
            version
        );
    }

    let major: u32 = parts[0].parse().context("Invalid major version number")?;
    let minor: u32 = parts[1].parse().context("Invalid minor version number")?;
    let patch: u32 = parts[2].parse().context("Invalid patch version number")?;

    // Find the highest flavor where flavor_version <= requested_version
    let mut best_flavor: Option<&'static str> = None;
    for &(min_major, min_minor, min_patch, flavor) in KAFKA_FLAVORS {
        if (major, minor, patch) >= (min_major, min_minor, min_patch) {
            best_flavor = Some(flavor);
        } else {
            break; // flavors are sorted ascending, so we can stop early
        }
    }

    match best_flavor {
        Some(flavor) => Ok((flavor, version)),
        None => bail!(
            "Kafka version {} is older than the minimum supported version (3.0.0)",
            version
        ),
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct DimsterConfig {
    #[serde(default)]
    pub kubernetes: KubernetesConfig,
    #[serde(default)]
    pub image: ImageConfig,
    #[serde(default)]
    pub results: ResultsConfig,
    #[serde(default)]
    pub environments: HashMap<String, Environment>,
}

#[derive(Debug, Deserialize)]
pub struct KubernetesConfig {
    #[serde(default = "default_namespace")]
    pub namespace: String,
    pub context: Option<String>,
}

// Hand-written rather than derived: when the whole `kubernetes:` block is
// absent, `#[serde(default)]` on DimsterConfig uses this impl, and a derived
// Default would leave the namespace empty instead of the field default.
impl Default for KubernetesConfig {
    fn default() -> Self {
        Self {
            namespace: default_namespace(),
            context: None,
        }
    }
}

fn default_namespace() -> String {
    "dimster".to_string()
}

#[derive(Debug, Deserialize)]
pub struct ImageConfig {
    #[serde(default = "default_tag")]
    pub tag: String,
}

// Hand-written for the same reason as KubernetesConfig: `dimster init` writes
// no `image:` block, and a derived Default would give an empty tag (images
// tagged `dimster:-kafka4.2.0`, which docker rejects).
impl Default for ImageConfig {
    fn default() -> Self {
        Self { tag: default_tag() }
    }
}

// Default image tag is the CLI's own package version (set at compile time
// from cli/Cargo.toml). Keeping the tag in lock-step with the CLI version
// means a freshly-built `dimster` and the images it builds always agree on
// what "this version" is, so two co-existing checkouts at different versions
// don't overwrite each other's local image. Users can still override with
// `image.tag` in dimster-config.yaml when they need a custom tag (e.g. a
// registry pipeline that tags differently).
fn default_tag() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[derive(Debug, Deserialize)]
pub struct ResultsConfig {
    #[serde(default = "default_results_dir")]
    pub dir: String,
}

impl Default for ResultsConfig {
    fn default() -> Self {
        Self {
            dir: default_results_dir(),
        }
    }
}

fn default_results_dir() -> String {
    "./results".to_string()
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DeployMode {
    #[default]
    Full,
    External,
}

/// Kubelet CPU Manager mode that a Dimster environment expects its target
/// nodes to be running with. This is cluster/node configuration, not a
/// namespaced Kubernetes object, so Dimster validates it during deploy rather
/// than trying to mutate kubelet settings.
#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CpuManagerPolicy {
    /// Kubernetes' default CPU placement behaviour. In Dimster this also means
    /// "do not run CPU Manager preflight validation" for the environment.
    #[default]
    #[serde(alias = "None", alias = "NONE")]
    None,
    /// Kubelet static CPU Manager policy. Guaranteed pods with integer CPU
    /// requests can receive exclusive CPUs from the node cpuset.
    #[serde(alias = "Static", alias = "STATIC")]
    Static,
}

/// Desired kubelet CPU Manager shape for benchmark-fidelity deployments.
/// The struct exists at the environment level because CPU Manager settings are
/// properties of the nodes selected by an environment, while Kafka/worker
/// manifests only provide the Guaranteed, integer-CPU pod shape that lets the
/// kubelet use those settings.
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CpuManagerConfig {
    #[serde(default)]
    pub policy: CpuManagerPolicy,
    /// When true, `policy` must be `static`, and deploy validates that kubelet
    /// has `cpuManagerPolicyOptions.full-pcpus-only` enabled. Kubernetes then
    /// allocates whole physical cores on SMT systems instead of individual
    /// hardware threads.
    #[serde(default)]
    pub full_pcpus_only: bool,
}

/// Local Kubernetes distribution. Determines how a freshly built image is
/// loaded into the cluster — each distro has its own loader command (or
/// none, in the case of Docker Desktop / OrbStack which share the host's
/// Docker daemon with the embedded cluster). Only meaningful for envs
/// that build local images (image name without a registry); ignored for
/// envs whose image is pushed to a registry.
// Implements both serde::Deserialize (for parsing dimster-config.yaml)
// AND clap::ValueEnum (for use as a CLI flag value, e.g.
// `dimster init --local-k8s-distro k3d`). clap's default rename is
// kebab-case-lower of the variant name, which already matches our serde
// rename map (Minikube → "minikube", DockerDesktop → "docker-desktop", …)
// so no per-variant clap attribute is needed.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Distro {
    #[serde(rename = "minikube")]
    Minikube,
    #[serde(rename = "k3d")]
    K3d,
    #[serde(rename = "kind")]
    Kind,
    /// Covers both Docker Desktop's built-in Kubernetes and OrbStack's
    /// Kubernetes — both expose the cluster via the host docker daemon,
    /// so a `docker build` is immediately visible to the cluster and no
    /// explicit image-load step is required.
    #[serde(rename = "docker-desktop", alias = "orbstack")]
    DockerDesktop,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct Environment {
    pub image: Option<String>,
    pub context: Option<String>,
    /// Per-environment namespace override. When set, this env deploys
    /// into the named namespace; when absent, falls back to the global
    /// `kubernetes.namespace` field. Lets you run two `dimster deploy`
    /// instances side-by-side (e.g. `local` and `localTwo`) without
    /// keeping separate config files — even on the same cluster.
    pub namespace: Option<String>,
    /// Local Kubernetes distribution. Only consulted when the image is
    /// local (no registry URL). Defaults to Minikube when absent so
    /// existing configs without a `distro:` field keep their prior
    /// behavior.
    pub distro: Option<Distro>,
    pub system: Option<String>,
    pub hardware: Option<String>,
    /// Concrete kafka-clients version this env's worker and coordinator pods
    /// should be built against (e.g. "3.7.2", "3.9.0", "4.1.0", "4.2.0").
    /// `parse_kafka_client_version` maps it to the matching dimster-kafka-X
    /// impl module and to the `-Dkafka.version=...` Maven override. Defaults
    /// to `DEFAULT_KAFKA_VERSION` ("4.2.0") so env files written before the
    /// multi-version refactor keep deploying the historical 4.2.0 image.
    #[serde(default)]
    pub kafka_client_version: Option<String>,
    #[serde(default)]
    pub deploy: DeployMode,
    /// Optional validation-only kubelet CPU Manager expectation. Omitted or
    /// `policy: none` keeps current behaviour. `policy: static` asks deploy to
    /// verify that benchmark pods can receive exclusive CPUs on their selected
    /// nodes before any manifests are applied.
    #[serde(default, rename = "cpuManager")]
    pub cpu_manager: Option<CpuManagerConfig>,
    #[serde(default)]
    pub connection: Option<ConnectionConfig>,
    #[serde(default)]
    pub kafka: Option<KafkaEnvConfig>,
    #[serde(default)]
    pub workers: Option<WorkersEnvConfig>,
    #[serde(default)]
    pub coordinator: Option<CoordinatorEnvConfig>,
    #[serde(default)]
    pub monitoring: Option<MonitoringEnvConfig>,
}

/// Kubernetes resource requests (cpu and memory) for a pod.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct ResourceRequests {
    pub cpu: Option<String>,
    pub memory: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct KafkaEnvConfig {
    pub replicas: Option<u32>,
    /// Docker image name (without tag). Defaults to "apache/kafka".
    /// Combined with `version` to form the full image reference, e.g. "apache/kafka:4.2.0".
    pub image: Option<String>,
    pub version: Option<String>,
    pub pvc_size: Option<String>,
    pub pvc_storage_class: Option<String>,
    pub heap: Option<String>,
    /// Arbitrary environment variables to set on broker pods. Env var names
    /// are case-sensitive and passed through verbatim (e.g. "KAFKA_OPTS",
    /// "CUSTOM_VAR"). When both `heap` and `env.KAFKA_HEAP_OPTS` are set,
    /// the explicit env map value wins.
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub requests: Option<ResourceRequests>,
    pub broker_config: Option<String>,
    pub extra_config: Option<String>,
    pub log_config: Option<String>,
    /// Override log4j MaxFileSize for the server log file (e.g. "200MB").
    /// Applied on top of the log config file if specified.
    pub log_max_file_size: Option<String>,
    /// Override log4j MaxBackupIndex for the server log file (e.g. 20).
    /// Applied on top of the log config file if specified.
    pub log_max_backup_index: Option<u32>,
    pub node_label: Option<String>,
    /// Optional TLS / mTLS configuration. When `enabled: true`, deploy adds
    /// an SSL listener (default port 9094) alongside the existing PLAINTEXT
    /// listener (9092), provisions a self-signed CA + broker + client cert
    /// into Kubernetes Secrets, and mounts those secrets into the broker,
    /// worker, and coordinator pods. Workloads opt in to TLS by adding
    /// `commonConfig.security.protocol: SSL` to their kafkaConfig — the
    /// run command auto-rewrites the bootstrap port and injects the SSL
    /// keystore/truststore properties at run time. Without that key, a
    /// workload uses PLAINTEXT, so a single campaign can mix TLS and
    /// non-TLS workloads against the same cluster.
    pub tls: Option<KafkaTlsConfig>,
}

/// In-cluster mTLS settings for `full` deploy mode. All fields optional —
/// `enabled` is the only switch a user typically sets. Lives under
/// `kafka.tls` in dimster-config.yaml.
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTlsConfig {
    /// Master switch. When false (or the section is absent), deploy behaves
    /// exactly as before: no SSL listener, no certificate provisioning.
    #[serde(default)]
    pub enabled: bool,
    /// When true, also flip inter-broker traffic to SSL. Default false so
    /// the SSL listener only adds **client-side** TLS overhead — useful for
    /// isolating the cost of TLS for producers/consumers from the cost of
    /// TLS for replication.
    #[serde(default)]
    pub inter_broker: bool,
    /// Port for the SSL listener. Defaults to 9094 (keeping 9092 for
    /// PLAINTEXT and 9093 for the controller listener).
    pub ssl_port: Option<u16>,
}

impl KafkaTlsConfig {
    /// Resolved SSL listener port — explicit value or 9094 default.
    pub fn resolved_ssl_port(&self) -> u16 {
        self.ssl_port.unwrap_or(9094)
    }
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WorkersEnvConfig {
    pub replicas: Option<u32>,
    pub heap: Option<String>,
    #[serde(default)]
    pub requests: Option<ResourceRequests>,
    pub node_label: Option<String>,
    pub log_config: Option<String>,
    /// Producer/consumer split across the worker pool. When absent, the
    /// coordinator defaults to `mixed` (every worker runs both roles).
    /// In `separated` mode, exactly one of `ratio` or (`producers` +
    /// `consumers`) must be set.
    #[serde(default)]
    pub assignment: Option<WorkerAssignmentConfig>,
}

/// How producer and consumer instances are distributed across the worker
/// pool. Lives under `workers.assignment` in the env config; consumed by
/// the `run` command and forwarded to the coordinator.
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WorkerAssignmentConfig {
    /// `mixed` (default) or `separated`.
    pub mode: Option<String>,
    /// Producer:consumer ratio, e.g. "1:2". Only valid in separated mode.
    pub ratio: Option<String>,
    /// Explicit producer worker count (alternative to `ratio`).
    pub producers: Option<u32>,
    /// Explicit consumer worker count (alternative to `ratio`).
    pub consumers: Option<u32>,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CoordinatorEnvConfig {
    pub heap: Option<String>,
    #[serde(default)]
    pub requests: Option<ResourceRequests>,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MonitoringEnvConfig {
    pub node_label: Option<String>,
    pub prometheus: Option<MonitoringResourceConfig>,
    pub grafana: Option<MonitoringResourceConfig>,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MonitoringResourceConfig {
    #[serde(default)]
    pub requests: Option<ResourceRequests>,
}

/// Connection details for an external Kafka-compatible cluster.
/// All keys use standard Kafka dot-syntax (e.g. `bootstrap.servers`,
/// `security.protocol`) and are injected as-is into client configs.
#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionConfig {
    #[serde(flatten)]
    pub properties: BTreeMap<String, String>,
}

impl ConnectionConfig {
    /// Returns the `bootstrap.servers` value, or an error if missing.
    pub fn bootstrap_servers(&self) -> Result<&str> {
        self.properties
            .get("bootstrap.servers")
            .map(|s| s.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!("connection section is missing required key 'bootstrap.servers'")
            })
    }

    /// Format all connection properties as Java-style key=value lines.
    pub fn to_common_config_lines(&self) -> Vec<String> {
        self.properties
            .iter()
            .map(|(key, value)| format!("{}: \"{}\"", key, value))
            .collect()
    }
}

impl Environment {
    /// Returns true when the environment connects to an external Kafka cluster.
    pub fn is_external(&self) -> bool {
        self.deploy == DeployMode::External
    }

    /// Returns the resolved TLS config when the env has it enabled. None
    /// when TLS is absent or `enabled: false`. Lets callers check both
    /// presence and the master switch in one call.
    pub fn tls_config(&self) -> Option<&KafkaTlsConfig> {
        self.kafka
            .as_ref()
            .and_then(|k| k.tls.as_ref())
            .filter(|t| t.enabled)
    }

    /// Resolved local-Kubernetes distribution for this env. Defaults to
    /// Minikube when no `distro:` field is set, preserving prior behavior
    /// for configs written before the field existed.
    pub fn distro(&self) -> Distro {
        self.distro.unwrap_or(Distro::Minikube)
    }

    /// Resolves this env's kafka-clients version to a (flavor, version) pair.
    /// The `flavor` is the dimster-kafka-X / package-X module suffix used in
    /// the build pipeline; the `version` is the concrete kafka-clients version
    /// to pass to Maven via `-Dkafka.version=...` and to use as the Docker
    /// image tag suffix. See `parse_kafka_client_version` for the mapping
    /// rules and supported lines.
    ///
    /// Returns the parsed result of `kafka_client_version` if set, or the
    /// `DEFAULT_KAFKA_VERSION` parsed otherwise. Errors on unsupported
    /// versions so typos surface at command time instead of as a confusing
    /// "image not found" later.
    pub fn kafka_flavor(&self) -> Result<(&'static str, &str)> {
        let version = self
            .kafka_client_version
            .as_deref()
            .unwrap_or(DEFAULT_KAFKA_VERSION);
        parse_kafka_client_version(version)
    }

    /// A minimal environment has no kafka, workers, or coordinator config
    /// sections — just identity fields (image, context, distro, etc.).
    /// Minimal environments receive tiny resource defaults suitable for
    /// quick local testing on constrained clusters; non-minimal environments
    /// receive larger production-grade defaults for unspecified fields.
    pub fn is_minimal(&self) -> bool {
        self.kafka.is_none() && self.workers.is_none() && self.coordinator.is_none()
    }

    /// Validate the environment configuration.
    pub fn validate(&self, name: &str) -> Result<()> {
        if self.deploy == DeployMode::External {
            match &self.connection {
                None => bail!(
                    "Environment '{}' has deploy: external but no connection section. \
                     External mode requires a connection with at least bootstrap.servers.",
                    name
                ),
                Some(conn) => {
                    conn.bootstrap_servers()?;
                }
            }
        }
        if let Some(cpu_manager) = &self.cpu_manager {
            if cpu_manager.policy == CpuManagerPolicy::None && cpu_manager.full_pcpus_only {
                bail!(
                    "Environment '{}' sets cpuManager.fullPcpusOnly but cpuManager.policy is none. \
                     Set policy: static or remove fullPcpusOnly.",
                    name
                );
            }
        }
        Ok(())
    }
}

impl DimsterConfig {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let config: DimsterConfig = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;
        Ok(config)
    }

    /// Load the config file and apply repeatable `dimster deploy
    /// --env-override path=value` entries to one named environment before
    /// deserializing into the normal typed config model.
    ///
    /// The override layer intentionally lives here instead of in deploy.rs:
    /// deploy should not have a second configuration interpretation path. It
    /// should receive an `Environment` exactly as if the user had edited
    /// `dimster-config.yaml` for this one command invocation.
    pub fn load_with_env_overrides(
        path: &Path,
        env_name: &str,
        overrides: &[String],
    ) -> Result<Self> {
        if overrides.is_empty() {
            return Self::load(path);
        }
        let root = Self::yaml_root_with_env_overrides(path, env_name, overrides)?;
        let config = config_from_yaml_root(path, root)?;
        config.get_env(env_name)?;
        Ok(config)
    }

    pub(crate) fn yaml_root_with_env_overrides(
        path: &Path,
        env_name: &str,
        overrides: &[String],
    ) -> Result<serde_yaml::Value> {
        if !path.exists() {
            bail!(
                "Cannot apply --env-override because config file does not exist: {}",
                path.display()
            );
        }
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let mut root: serde_yaml::Value = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;
        if !overrides.is_empty() {
            apply_env_overrides(&mut root, env_name, overrides)?;
        }
        Ok(root)
    }

    pub fn get_env(&self, name: &str) -> Result<&Environment> {
        let env = self.environments.get(name).with_context(|| {
            let available: Vec<&String> = self.environments.keys().collect();
            format!("Unknown environment '{}'. Available: {:?}", name, available)
        })?;
        env.validate(name)?;
        Ok(env)
    }

    /// Get the kubectl context for an environment, falling back to global config.
    pub fn kubectl_context(&self, env_name: &str) -> Option<String> {
        self.environments
            .get(env_name)
            .and_then(|e| e.context.clone())
            .or_else(|| self.kubernetes.context.clone())
    }

    /// Get the kubernetes namespace for an environment, falling back to
    /// the global `kubernetes.namespace`. Mirrors `kubectl_context` so
    /// each env can pin its own namespace (enables side-by-side deploys
    /// of two envs on the same cluster, into different namespaces).
    pub fn kubectl_namespace(&self, env_name: &str) -> String {
        self.environments
            .get(env_name)
            .and_then(|e| e.namespace.clone())
            .unwrap_or_else(|| self.kubernetes.namespace.clone())
    }
}

fn config_from_yaml_root(path: &Path, root: serde_yaml::Value) -> Result<DimsterConfig> {
    // Deserialize through YAML text, matching `load`. `serde_yaml::from_str`
    // accepts common config conveniences such as `cpu: 4` for string-backed
    // resource fields, while `from_value` is stricter and would make
    // deploy-only overrides reject config files that otherwise load fine.
    let overridden = serde_yaml::to_string(&root).with_context(|| {
        format!(
            "Failed to serialize overridden config file: {}",
            path.display()
        )
    })?;
    let config: DimsterConfig = serde_yaml::from_str(&overridden)
        .with_context(|| format!("Failed to parse overridden config file: {}", path.display()))?;
    Ok(config)
}

fn apply_env_overrides(
    root: &mut serde_yaml::Value,
    env_name: &str,
    overrides: &[String],
) -> Result<()> {
    let environments = root
        .get_mut("environments")
        .and_then(|v| v.as_mapping_mut())
        .ok_or_else(|| anyhow::anyhow!("Config has no environments mapping"))?;
    let env_key = serde_yaml::Value::String(env_name.to_string());
    let env = environments
        .get_mut(&env_key)
        .ok_or_else(|| anyhow::anyhow!("Environment '{}' not found", env_name))?;

    for assignment in overrides {
        let (path, value) = parse_override_assignment(assignment)?;
        set_nested_yaml_value(env, &path, value)
            .with_context(|| format!("Invalid --env-override '{}'", assignment))?;
    }
    Ok(())
}

fn parse_override_assignment(raw: &str) -> Result<(Vec<String>, serde_yaml::Value)> {
    let (path, value_raw) = raw
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("expected key=value"))?;
    let path: Vec<String> = path.split('.').map(|s| s.to_string()).collect();
    if path.is_empty() || path.iter().any(|s| s.is_empty()) {
        bail!("override path must contain non-empty dot-separated segments");
    }
    Ok((path, coerce_yaml_value(value_raw)))
}

fn coerce_yaml_value(raw: &str) -> serde_yaml::Value {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        return serde_yaml::Value::Bool(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return serde_yaml::Value::Bool(false);
    }
    if trimmed.eq_ignore_ascii_case("null") {
        return serde_yaml::Value::Null;
    }
    if let Ok(n) = trimmed.parse::<i64>() {
        return serde_yaml::Value::Number(n.into());
    }
    if let Ok(n) = trimmed.parse::<f64>() {
        return serde_yaml::Value::Number(n.into());
    }
    serde_yaml::Value::String(raw.to_string())
}

fn set_nested_yaml_value(
    root: &mut serde_yaml::Value,
    path: &[String],
    value: serde_yaml::Value,
) -> Result<()> {
    let mut current = root;
    for segment in &path[..path.len() - 1] {
        let map = current
            .as_mapping_mut()
            .ok_or_else(|| anyhow::anyhow!("'{}' is not a mapping", segment))?;
        let key = serde_yaml::Value::String(segment.clone());
        if !map.contains_key(&key) {
            map.insert(key.clone(), serde_yaml::Value::Mapping(Default::default()));
        }
        current = map.get_mut(&key).unwrap();
    }

    let map = current
        .as_mapping_mut()
        .ok_or_else(|| anyhow::anyhow!("target parent is not a mapping"))?;
    map.insert(
        serde_yaml::Value::String(path.last().unwrap().clone()),
        value,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_top_level_blocks_use_field_defaults() {
        let config: DimsterConfig = serde_yaml::from_str("environments: {}").unwrap();
        assert_eq!(config.kubernetes.namespace, "dimster");
        assert_eq!(config.image.tag, env!("CARGO_PKG_VERSION"));
        assert_eq!(config.results.dir, "./results");
    }

    fn config_with_namespaces(global: &str, envs: &[(&str, Option<&str>)]) -> DimsterConfig {
        let mut environments = HashMap::new();
        for (name, ns) in envs {
            environments.insert(
                (*name).into(),
                Environment {
                    namespace: ns.map(|s| s.to_string()),
                    ..Default::default()
                },
            );
        }
        DimsterConfig {
            kubernetes: KubernetesConfig {
                namespace: global.into(),
                context: None,
            },
            environments,
            ..Default::default()
        }
    }

    /// The whole point of `Environment.namespace`: an env without an
    /// override inherits the global namespace, but an env that sets one
    /// gets its own namespace. This is how two envs in the same config
    /// file can deploy side-by-side on the same cluster.
    #[test]
    fn kubectl_namespace_prefers_env_override_then_falls_back_to_global() {
        let cfg = config_with_namespaces(
            "dimster",
            &[("local", None), ("localTwo", Some("dimster2"))],
        );
        assert_eq!(cfg.kubectl_namespace("local"), "dimster");
        assert_eq!(cfg.kubectl_namespace("localTwo"), "dimster2");
        // Unknown env falls back to global rather than panicking — keeps
        // make_kubectl resilient when called early in command dispatch
        // before env validation runs.
        assert_eq!(cfg.kubectl_namespace("unknown"), "dimster");
    }

    #[test]
    fn default_environment_is_minimal() {
        let env = Environment::default();
        assert!(env.is_minimal());
    }

    #[test]
    fn environment_with_kafka_section_is_not_minimal() {
        let env = Environment {
            kafka: Some(KafkaEnvConfig::default()),
            ..Default::default()
        };
        assert!(!env.is_minimal());
    }

    #[test]
    fn environment_with_workers_section_is_not_minimal() {
        let env = Environment {
            workers: Some(WorkersEnvConfig::default()),
            ..Default::default()
        };
        assert!(!env.is_minimal());
    }

    #[test]
    fn environment_with_coordinator_section_is_not_minimal() {
        let env = Environment {
            coordinator: Some(CoordinatorEnvConfig::default()),
            ..Default::default()
        };
        assert!(!env.is_minimal());
    }

    /// Omitting `cpuManager` keeps old configs validation-free. This is
    /// important because CPU Manager is node/kubelet state that many local and
    /// managed clusters will not expose or configure by default.
    #[test]
    fn cpu_manager_omitted_means_no_policy_requirement() {
        let cfg: DimsterConfig = serde_yaml::from_str(
            r#"
environments:
  local:
    image: dimster
"#,
        )
        .unwrap();
        let env = cfg.get_env("local").unwrap();
        assert!(env.cpu_manager.is_none());
    }

    #[test]
    fn cpu_manager_policy_none_disables_validation_intent() {
        let cfg: DimsterConfig = serde_yaml::from_str(
            r#"
environments:
  local:
    cpuManager:
      policy: none
"#,
        )
        .unwrap();
        let env = cfg.get_env("local").unwrap();
        let cpu_manager = env.cpu_manager.as_ref().unwrap();
        assert_eq!(cpu_manager.policy, CpuManagerPolicy::None);
        assert!(!cpu_manager.full_pcpus_only);
    }

    #[test]
    fn cpu_manager_static_with_full_pcpus_deserializes() {
        let cfg: DimsterConfig = serde_yaml::from_str(
            r#"
environments:
  local:
    cpuManager:
      policy: static
      fullPcpusOnly: true
"#,
        )
        .unwrap();
        let env = cfg.get_env("local").unwrap();
        let cpu_manager = env.cpu_manager.as_ref().unwrap();
        assert_eq!(cpu_manager.policy, CpuManagerPolicy::Static);
        assert!(cpu_manager.full_pcpus_only);
    }

    #[test]
    fn cpu_manager_full_pcpus_requires_static_policy() {
        let cfg: DimsterConfig = serde_yaml::from_str(
            r#"
environments:
  local:
    cpuManager:
      policy: none
      fullPcpusOnly: true
"#,
        )
        .unwrap();
        let err = cfg.get_env("local").unwrap_err();
        assert!(
            err.to_string().contains("cpuManager.fullPcpusOnly"),
            "unexpected error: {err:#}"
        );
    }

    /// Default version must match the historical 4.2.0 image so that env
    /// configs written before the multi-version refactor keep deploying the
    /// same Kafka client version.
    #[test]
    fn kafka_flavor_unset_returns_default() {
        let env = Environment::default();
        let (flavor, version) = env.kafka_flavor().unwrap();
        assert_eq!(flavor, "4.2.0");
        assert_eq!(version, DEFAULT_KAFKA_VERSION);
        assert_eq!(DEFAULT_KAFKA_VERSION, "4.2.0");
    }

    /// 3.x.y of any patch version maps to the 3.x impl module. The version
    /// string is passed through unchanged so the Maven build can override
    /// kafka.version to that exact patch.
    #[test]
    fn kafka_flavor_3x_versions_all_map_to_3x_module() {
        for v in &["3.0.0", "3.5.1", "3.7.2", "3.9.0"] {
            let env = Environment {
                kafka_client_version: Some((*v).into()),
                ..Default::default()
            };
            let (flavor, version) = env.kafka_flavor().unwrap();
            assert_eq!(flavor, "3.x", "version {v}");
            assert_eq!(version, *v, "version {v}");
        }
    }

    #[test]
    fn kafka_flavor_41_versions_map_to_41_module() {
        for v in &["4.1.0", "4.1.1", "4.1.5"] {
            let env = Environment {
                kafka_client_version: Some((*v).into()),
                ..Default::default()
            };
            let (flavor, version) = env.kafka_flavor().unwrap();
            assert_eq!(flavor, "4.1", "version {v}");
            assert_eq!(version, *v, "version {v}");
        }
    }

    #[test]
    fn kafka_flavor_420_maps_to_420_module() {
        let env = Environment {
            kafka_client_version: Some("4.2.0".into()),
            ..Default::default()
        };
        let (flavor, version) = env.kafka_flavor().unwrap();
        assert_eq!(flavor, "4.2.0");
        assert_eq!(version, "4.2.0");
    }

    /// 4.0 routes to the 3.x flavor (classic-only). The 3.x impl module
    /// compiles cleanly against 4.0 and throws on share-group methods,
    /// which is the correct semantic for 4.0 anyway (its admin share-group
    /// APIs are incomplete). Lets users benchmark classic workloads on a
    /// 4.0 broker; share-group workloads fail fast like on 3.x.
    #[test]
    fn kafka_flavor_40_routes_to_3x() {
        for v in &["4.0.0", "4.0.1"] {
            let env = Environment {
                kafka_client_version: Some((*v).into()),
                ..Default::default()
            };
            let (flavor, version) = env.kafka_flavor().unwrap();
            assert_eq!(flavor, "3.x", "version {v}");
            assert_eq!(version, *v, "version {v}");
        }
    }

    /// 4.2.x and higher versions (4.3.x, 4.4.x, etc.) all use the 4.2.0 flavor
    /// (the highest available flavor).
    #[test]
    fn kafka_flavor_42x_and_higher_use_420_flavor() {
        for v in &["4.2.1", "4.3.0", "4.4.5", "4.10.0"] {
            let env = Environment {
                kafka_client_version: Some((*v).into()),
                ..Default::default()
            };
            let (flavor, version) = env.kafka_flavor().unwrap();
            assert_eq!(flavor, "4.2.0", "version {v}");
            assert_eq!(version, *v, "version {v}");
        }
    }

    /// Invalid version formats and versions below 3.0.0 error loudly at
    /// command time rather than as a confusing "ImagePullBackOff" later.
    #[test]
    fn kafka_flavor_invalid_versions_error() {
        for v in &["garbage", "3", "2.8.0"] {
            let env = Environment {
                kafka_client_version: Some((*v).into()),
                ..Default::default()
            };
            let err = env.kafka_flavor().unwrap_err();
            let msg = format!("{err}");
            assert!(
                msg.contains("Invalid") || msg.contains("minimum supported"),
                "version {v}: msg {msg}"
            );
        }
    }

    /// Future Kafka versions (beyond our highest flavor) automatically use
    /// the highest available flavor.
    #[test]
    fn kafka_flavor_future_versions_use_highest_flavor() {
        for v in &["4.3.0", "5.0.0", "6.1.2", "10.0.0"] {
            let env = Environment {
                kafka_client_version: Some((*v).into()),
                ..Default::default()
            };
            let (flavor, version) = env.kafka_flavor().unwrap();
            assert_eq!(flavor, "4.2.0", "version {v} should use highest flavor");
            assert_eq!(version, *v, "version {v}");
        }
    }

    #[test]
    fn environment_with_only_metadata_fields_is_still_minimal() {
        let env = Environment {
            namespace: Some("ns".into()),
            system: Some("Apache Kafka 4.2".into()),
            hardware: Some("minikube".into()),
            monitoring: Some(MonitoringEnvConfig::default()),
            ..Default::default()
        };
        assert!(env.is_minimal());
    }

    #[test]
    fn env_overrides_apply_to_selected_environment_only() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg_path = tmp.path().join("dimster-config.yaml");
        std::fs::write(
            &cfg_path,
            r#"
environments:
  local:
    namespace: dimster
    kafka:
      replicas: 1
      tls:
        enabled: false
  other:
    kafka:
      replicas: 2
"#,
        )
        .unwrap();

        let cfg = DimsterConfig::load_with_env_overrides(
            &cfg_path,
            "local",
            &["kafka.replicas=5".into(), "kafka.tls.enabled=true".into()],
        )
        .unwrap();

        let local = cfg.environments.get("local").unwrap();
        assert_eq!(local.kafka.as_ref().unwrap().replicas, Some(5));
        assert!(local.tls_config().is_some());
        let other = cfg.environments.get("other").unwrap();
        assert_eq!(other.kafka.as_ref().unwrap().replicas, Some(2));
    }

    #[test]
    fn env_overrides_keep_normal_yaml_string_coercions() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg_path = tmp.path().join("dimster-config.yaml");
        std::fs::write(
            &cfg_path,
            r#"
environments:
  local:
    kafka:
      replicas: 3
      requests:
        cpu: 4
        memory: "16Gi"
      tls:
        enabled: true
        interBroker: true
"#,
        )
        .unwrap();

        let cfg = DimsterConfig::load_with_env_overrides(
            &cfg_path,
            "local",
            &[
                "kafka.tls.enabled=false".into(),
                "kafka.tls.interBroker=false".into(),
            ],
        )
        .unwrap();

        let kafka = cfg
            .environments
            .get("local")
            .unwrap()
            .kafka
            .as_ref()
            .unwrap();
        assert_eq!(kafka.requests.as_ref().unwrap().cpu.as_deref(), Some("4"));
        assert_eq!(kafka.tls.as_ref().unwrap().enabled, false);
        assert_eq!(kafka.tls.as_ref().unwrap().inter_broker, false);
    }

    #[test]
    fn env_override_errors_on_missing_assignment_separator() {
        let err = parse_override_assignment("kafka.replicas").unwrap_err();
        assert!(err.to_string().contains("expected key=value"));
    }
}
