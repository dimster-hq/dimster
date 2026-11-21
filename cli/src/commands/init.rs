/// Init command. Writes `dimster-config.yaml` with a curated set of starter
/// environments that the user can edit:
///
///   • `local`              — active minimal env (no kafka/workers/coordinator
///                             sections), so deploy uses tiny built-in defaults.
///   • `exampleLocalFull`   — commented: chosen distro + Kafka deployed in-cluster.
///   • `exampleLocalExt`    — commented: chosen distro + external Kafka.
///   • `exampleRemoteFull`  — commented: remote k8s + Kafka deployed in-cluster.
///   • `exampleRemoteExt`   — commented: remote k8s + external Kafka.
///   • `exampleLocalShared` — commented: a second env sharing exampleLocalFull's
///                             Kafka (deploy: external-env).
///
/// Refuses to overwrite: if `dimster-config.yaml` already exists the command
/// exits with an error telling the user to delete or rename the file first.
/// This keeps the implementation simple — no merge logic, no risk of corrupting
/// an existing user-edited file. The new file always opens with a commented
/// guidance header covering image-naming rules per target, kubectl context
/// discovery, CPU/memory sizing tips, and node-label conventions.
use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::config::Distro;

// ---------------------------------------------------------------------------
// Per-distro defaults
// ---------------------------------------------------------------------------

/// Per-distro values rendered into the three `local*` env templates. The
/// kubectl context defaults follow the conventions used elsewhere in these
/// docs (`k3d-dimster` / `kind-dimster` / `docker-desktop` / `minikube`); the
/// host_internal hostname is what a pod inside the cluster uses to reach a
/// service running on the host (used in the external-Kafka example's
/// `bootstrap.servers` line). All three values come from each distro's
/// documented host-networking conventions and stay copy-paste editable by
/// the user.
struct LocalK8sDefaults {
    /// kubectl context name written into `context:`.
    context: &'static str,
    /// String written into `distro:` — mirrors the Distro variant name.
    /// Also reused in leader comments so the user sees their chosen distro
    /// named in every commented example (e.g. "Example: k3d + deploy: full").
    distro: &'static str,
    /// DNS name a pod uses to reach a service on the host. Each distro
    /// publishes a different one; we substitute it into the external-Kafka
    /// example so the line is editable but already correct for the chosen
    /// distro out of the box.
    host_internal: &'static str,
}

fn defaults_for(distro: Distro) -> LocalK8sDefaults {
    match distro {
        Distro::Minikube => LocalK8sDefaults {
            context: "minikube",
            distro: "minikube",
            host_internal: "host.minikube.internal",
        },
        Distro::K3d => LocalK8sDefaults {
            context: "k3d-dimster",
            distro: "k3d",
            host_internal: "host.k3d.internal",
        },
        Distro::Kind => LocalK8sDefaults {
            context: "kind-dimster",
            distro: "kind",
            // kind clusters run inside a docker container that shares
            // host.docker.internal with the rest of the docker network.
            host_internal: "host.docker.internal",
        },
        Distro::DockerDesktop => LocalK8sDefaults {
            context: "docker-desktop",
            distro: "docker-desktop",
            host_internal: "host.docker.internal",
        },
    }
}

/// Substitute the `__CONTEXT__`, `__DISTRO__`, and `__HOST_INTERNAL__`
/// placeholders in a template body. All three local templates share this
/// helper so per-distro values stay consistent across them. Templates that
/// do not contain a given placeholder are unaffected (the .replace call is
/// a no-op).
fn render(template: &str, distro: Distro) -> String {
    let d = defaults_for(distro);
    template
        .replace("__CONTEXT__", d.context)
        .replace("__DISTRO__", d.distro)
        .replace("__HOST_INTERNAL__", d.host_internal)
}

// ---------------------------------------------------------------------------
// Template-mode static blocks
// ---------------------------------------------------------------------------
//
// These six constants are the entire content emitted by the command. Each
// block is hand-authored YAML text already indented by 2 spaces so it nests
// directly under `environments:`. The first (`local`) is uncommented and
// usable as-is; the five `example*` blocks are entirely commented so they
// serve as copy-paste-and-edit starting points for the four common shapes
// (local-distro/remote × full/external), plus a second env sharing a Kafka
// cluster (external-env).
//
// Trade-off: when a field is added to KafkaEnvConfig (or any other env
// section), these constants do NOT update automatically — the maintainer
// must edit each block. We accept this in exchange for full presentation
// control and avoiding double-`#` artefacts that would arise from line-
// prefixing a generated env body.
//
// Inline `# field: …` lines inside an example body intentionally use a
// SINGLE `#` (not `# # …`). When the user uncomments the block by removing
// the leading `# ` from each line, those inner placeholder lines remain
// commented — which is the right behavior, since they document optional
// knobs the user can enable individually.
//
// The three `local*` constants embed `__CONTEXT__`, `__DISTRO__`, and
// (`TEMPLATE_LOCAL_EXT` only) `__HOST_INTERNAL__` placeholders, substituted
// by `render()` based on the user's `--local-k8s-distro` choice. The two
// `remote*` constants are static (their `context:` is a registry-style
// placeholder the user must edit anyway).

/// Starter minimal env. Active (uncommented). Has no kafka/workers/coordinator
/// sections, so deploy uses the built-in tiny defaults (1 broker, 250m CPU, 768Mi memory).
///
/// Note: each TEMPLATE_* constant intentionally begins with two literal
/// spaces (the indent under `environments:`). Do not use `"\` line
/// continuation here — it would strip the leading whitespace.
const TEMPLATE_LOCAL: &str = "  # Minimal env — no kafka/workers/coordinator sections, so deploy uses tiny built-in defaults.
  local:
    image: dimster
    context: __CONTEXT__
    distro: __DISTRO__                  # Options: minikube | k3d | kind | docker-desktop
";

/// Commented example: local k8s + deploy: full.
const TEMPLATE_LOCAL_FULL: &str = "  # Example: __DISTRO__ + deploy: full (Kafka deployed alongside benchmark clients).
  #exampleLocalFull:
  #  image: dimster
  #  context: __CONTEXT__
  #  distro: __DISTRO__                              # Options: minikube | k3d | kind | docker-desktop
  #  system: \"Apache Kafka 4.2\"
  #  # hardware: \"...\"                                # description recorded in results
  #  deploy: full
  #  # Experimental: cpuManager validates existing kubelet settings; Dimster does not configure kubelet.
  #  # cpuManager:
  #  #   policy: static                              # none | static
  #  #   fullPcpusOnly: true                         # require whole physical cores
  #  kafka:
  #    # image: apache/kafka                          # default: apache/kafka
  #    version: \"4.2.0\"
  #    replicas: 1
  #    pvcSize: \"1Gi\"
  #    heap: \"-Xms256M -Xmx512M\"
  #    requests:
  #      cpu: \"250m\"
  #      memory: \"512Mi\"
  #    brokerConfig: run/broker-configs/default.yaml
  #    # extraConfig: \"num.replica.fetchers=4\"       # extra overrides on top of brokerConfig
  #    logConfig: run/log4j-configs/info.yaml
  #    # logMaxFileSize: 100MB                        # log4j MaxFileSize override
  #    # logMaxBackupIndex: 5                         # log4j MaxBackupIndex override
  #  workers:
  #    replicas: 1
  #    heap: \"-Xms256M -Xmx512M\"
  #    requests:
  #      cpu: \"250m\"
  #      memory: \"768Mi\"
  #  coordinator:
  #    heap: \"-Xms256M -Xmx512M\"
  #    requests:
  #      cpu: \"250m\"
  #      memory: \"768Mi\"
";

/// Commented example: local k8s + deploy: external. Connects to a Kafka cluster
/// running outside the k8s cluster (e.g. Docker Compose on the host). The host
/// hostname differs by distro (`host.minikube.internal`, `host.k3d.internal`,
/// `host.docker.internal`) — `__HOST_INTERNAL__` picks the right one.
const TEMPLATE_LOCAL_EXT: &str = "  # Example: __DISTRO__ + deploy: external (benchmark clients run in k8s, Kafka runs
  # elsewhere — e.g. Docker Compose on the host, reachable via __HOST_INTERNAL__).
  #exampleLocalExt:
  #  image: dimster
  #  context: __CONTEXT__
  #  distro: __DISTRO__                              # Options: minikube | k3d | kind | docker-desktop
  #  system: \"Apache Kafka 4.2 (Docker Compose)\"
  #  # hardware: \"...\"                                # description recorded in results
  #  deploy: external
  #  # Experimental: cpuManager validates existing kubelet settings for worker pods.
  #  # cpuManager:
  #  #   policy: static                              # none | static
  #  #   fullPcpusOnly: true                         # require whole physical cores
  #  connection:
  #    bootstrap.servers: \"__HOST_INTERNAL__:19092\"
  #    # security.protocol: SASL_SSL                     # Kafka client security protocol
  #    # sasl.mechanism: PLAIN                           # SASL mechanism
  #    # sasl.jaas.config: \"...\"                       # JAAS login config string
  #  workers:
  #    replicas: 1
  #    heap: \"-Xms256M -Xmx512M\"
  #    requests:
  #      cpu: \"250m\"
  #      memory: \"768Mi\"
  #  coordinator:
  #    heap: \"-Xms256M -Xmx512M\"
  #    requests:
  #      cpu: \"250m\"
  #      memory: \"768Mi\"
";

/// Commented example: remote k8s (EKS/GKE/etc.) + deploy: full. Beefy
/// resources and node labels for nodegroup separation.
const TEMPLATE_REMOTE_FULL: &str = "  # Example: remote k8s (EKS/GKE/etc.) + deploy: full
  # — beefy resources and node labels for separation across nodegroups.
  #exampleRemoteFull:
  #  image: 123456789.dkr.ecr.us-west-2.amazonaws.com/dimster
  #  context: arn:aws:eks:us-west-2:123456789:cluster/my-cluster
  #  system: \"Apache Kafka 4.2\"
  #  hardware: \"kafka=3xm6i.2xlarge, workers=2xm6i.2xlarge, storage=ebs/500Gi/3000iops/300MBs\"
  #  deploy: full
  #  # Experimental: cpuManager validates existing kubelet settings on benchmark nodes.
  #  # cpuManager:
  #  #   policy: static                              # none | static
  #  #   fullPcpusOnly: true                         # require whole physical cores
  #  kafka:
  #    # image: apache/kafka                          # default: apache/kafka
  #    version: \"4.2.0\"
  #    replicas: 3
  #    pvcSize: \"500Gi\"
  #    pvcStorageClass: gp3
  #    heap: \"-Xms8G -Xmx8G\"
  #    requests:
  #      cpu: \"6\"
  #      memory: \"24Gi\"
  #    brokerConfig: run/broker-configs/default.yaml
  #    # extraConfig: \"num.replica.fetchers=4\"       # extra overrides on top of brokerConfig
  #    logConfig: run/log4j-configs/info.yaml
  #    # logMaxFileSize: 200MB                        # log4j MaxFileSize override
  #    # logMaxBackupIndex: 20                        # log4j MaxBackupIndex override
  #    nodeLabel: role=kafka
  #  workers:
  #    replicas: 2
  #    heap: \"-Xms2G -Xmx10G\"
  #    requests:
  #      cpu: \"4\"
  #      memory: \"12Gi\"
  #    nodeLabel: role=worker
  #  coordinator:
  #    heap: \"-Xms512M -Xmx1G\"
  #    requests:
  #      cpu: \"500m\"
  #      memory: \"2Gi\"
  #    nodeLabel: role=infra
  #  monitoring:
  #    nodeLabel: role=infra
  #    prometheus:
  #      requests:
  #        cpu: \"1\"
  #        memory: \"4Gi\"
  #    grafana:
  #      requests:
  #        cpu: \"1\"
  #        memory: \"2Gi\"
";

/// Commented example: remote k8s + deploy: external (e.g. Confluent Cloud,
/// MSK, or any externally-managed Kafka).
const TEMPLATE_REMOTE_EXT: &str = "  # Example: remote k8s (EKS/GKE/etc.) + deploy: external
  # — clients run in k8s, Kafka runs elsewhere (Confluent Cloud, MSK, etc.).
  #exampleRemoteExt:
  #  image: 123456789.dkr.ecr.us-west-2.amazonaws.com/dimster
  #  context: arn:aws:eks:us-west-2:123456789:cluster/my-cluster
  #  system: \"Confluent Cloud\"
  #  hardware: \"workers=2xm6i.xlarge, region=us-west-2\"
  #  deploy: external
  #  # Experimental: cpuManager validates existing kubelet settings on worker nodes.
  #  # cpuManager:
  #  #   policy: static                              # none | static
  #  #   fullPcpusOnly: true                         # require whole physical cores
  #  connection:
  #    bootstrap.servers: \"broker.example.com:9092\"
  #    security.protocol: SASL_SSL
  #    sasl.mechanism: PLAIN
  #    sasl.jaas.config: \"org.apache.kafka.common.security.plain.PlainLoginModule required username=\\\"...\\\" password=\\\"...\\\";\"
  #  workers:
  #    replicas: 2
  #    heap: \"-Xms2G -Xmx10G\"
  #    requests:
  #      cpu: \"4\"
  #      memory: \"12Gi\"
  #    nodeLabel: role=worker
  #  coordinator:
  #    heap: \"-Xms512M -Xmx1G\"
  #    requests:
  #      cpu: \"500m\"
  #      memory: \"2Gi\"
  #    nodeLabel: role=infra
  #  monitoring:
  #    nodeLabel: role=infra
  #    prometheus:
  #      requests:
  #        cpu: \"1\"
  #        memory: \"4Gi\"
  #    grafana:
  #      requests:
  #        cpu: \"1\"
  #        memory: \"2Gi\"
";

/// Commented example: deploy: external-env. A second env, with its own
/// coordinator, workers and monitoring in its own namespace, running workloads
/// against exampleLocalFull's brokers at the same time as exampleLocalFull.
const TEMPLATE_LOCAL_SHARED: &str = "  # Example: __DISTRO__ + deploy: external-env (a second env running its own workloads against
  # exampleLocalFull's Kafka, at the same time — uncomment both). Its own pods need their own
  # namespace; its topics are named dimster-<topicTag>-... so neither env deletes the other's.
  #exampleLocalShared:
  #  image: dimster
  #  context: __CONTEXT__                            # same k8s cluster as the Kafka env
  #  distro: __DISTRO__                              # Options: minikube | k3d | kind | docker-desktop
  #  namespace: dimster-shared                       # must differ from exampleLocalFull's
  #  deploy: external-env
  #  externalEnv:
  #    environment: exampleLocalFull                 # the env hosting the Kafka cluster
  #  # topicTag: shared                              # defaults to the env name
  #  workers:
  #    replicas: 1
  #    heap: \"-Xms256M -Xmx512M\"
  #    requests:
  #      cpu: \"250m\"
  #      memory: \"768Mi\"
  #  coordinator:
  #    heap: \"-Xms256M -Xmx512M\"
  #    requests:
  #      cpu: \"250m\"
  #      memory: \"768Mi\"
";

/// Build the ordered (name, body) list of env blocks to emit. Ordering:
/// `local` first (active), then the four examples in increasing complexity
/// (local-distro before remote, full before external), then the env sharing
/// exampleLocalFull's Kafka. Each `local*` body gets distro-substituted; the
/// two remote bodies are static.
fn template_envs(distro: Distro) -> Vec<(&'static str, String)> {
    vec![
        ("local", render(TEMPLATE_LOCAL, distro)),
        ("exampleLocalFull", render(TEMPLATE_LOCAL_FULL, distro)),
        ("exampleLocalExt", render(TEMPLATE_LOCAL_EXT, distro)),
        ("exampleRemoteFull", TEMPLATE_REMOTE_FULL.to_string()),
        ("exampleRemoteExt", TEMPLATE_REMOTE_EXT.to_string()),
        ("exampleLocalShared", render(TEMPLATE_LOCAL_SHARED, distro)),
    ]
}

/// Commented guidance header written at the top of newly-created config files.
/// Static; no per-env tailoring. Distills the most useful tips the wizard
/// used to give interactively (image-naming rules per target, kubectl context
/// discovery, memory > heap, node-label conventions, system/hardware labels).
const NEW_FILE_HEADER: &str = "# Dimster configuration file
# Generated by: dimster init
#
# Tips for filling out an environment:
#
#  • image
#      - For local k8s:  use a plain name (e.g. \"dimster\"); the CLI loads it
#        into the chosen distro (see `distro:` below).
#      - For remote k8s (EKS/GKE/etc.): use a full registry URL containing a
#        dot, e.g.
#          EKS:  <account>.dkr.ecr.<region>.amazonaws.com/dimster
#          GCP:  <region>-docker.pkg.dev/<project>/<repo>/dimster
#        so the image can be pushed and pulled.
#
#  • context
#      - kubectl context to target. List available contexts with:
#          kubectl config get-contexts
#
#  • distro (local k8s only — ignored when image is a registry URL)
#      - Selects how `dimster build` makes the freshly built image visible to
#        the cluster. Each distro has its own loader:
#          minikube         → minikube image load <image>
#          k3d              → k3d image import <image> -c <cluster>
#          kind             → kind load docker-image <image> --name <cluster>
#          docker-desktop   → no-op (Docker Desktop / OrbStack share the host
#                             docker daemon with the cluster, so a freshly
#                             built image is already visible)
#      - For k3d/kind, the cluster name is derived from the kubectl context
#        (e.g. context `k3d-foo` → cluster `foo`).
#      - Defaults to `minikube` when omitted.
#
#  • CPU / memory
#      - Pod requests/limits should match your nodes. Inspect a node with:
#          kubectl describe node <name>
#      - Always set memory limit > JVM max heap, leaving room for OS pods.
#
#  • nodeLabel
#      - Used as a k8s nodeSelector. Conventional labels in multi-node setups:
#          role=kafka   role=worker   role=infra
#        Omit (or remove the line) to let k8s schedule the pod freely — useful
#        for single-node setups like minikube.
#
#  • system / hardware
#      - Free-form labels recorded into result files so you can identify what
#        was being tested and on what hardware later.
";

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Public entry point invoked from `Commands::Init` in main.rs.
///
/// Refuses to overwrite an existing file: if the path already exists the
/// command bails with a message telling the user to delete or rename it.
/// Otherwise writes the full template (header + standard sections + five
/// env blocks, with the three `local*` envs distro-substituted).
pub fn run(config_path: &Path, distro: Distro) -> Result<()> {
    if config_path.exists() {
        bail!(
            "{} already exists. Delete or rename it to generate a fresh one.",
            config_path.display()
        );
    }
    write_new_template_file(config_path, distro)?;

    println!();
    println!(
        "Template written to {} — added: local, exampleLocalFull, exampleLocalExt, exampleRemoteFull, exampleRemoteExt, exampleLocalShared",
        config_path.display()
    );
    println!("Edit the file to set image registries, kubectl contexts, and resource sizes.");
    println!("Uncomment an example block by removing the leading '#' from each of its lines.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

/// Create the config file containing the commented guidance header, the
/// standard top-level sections, and all five template env blocks. The
/// caller is responsible for ensuring the file does not already exist —
/// this function will happily overwrite if invoked directly. Text-only
/// concatenation: going through serde_yaml would strip the comment lines
/// that are the entire point of the template.
fn write_new_template_file(config_path: &Path, distro: Distro) -> Result<()> {
    let envs = template_envs(distro);
    // Each block already includes its own indent and trailing newline. Join
    // with a blank line between blocks for readability.
    let envs_text: String = envs
        .iter()
        .map(|(_, body)| body.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    // No `image:` block by default — the dimster CLI defaults the tag to its
    // own version (env!("CARGO_PKG_VERSION") in config::default_tag), which
    // keeps generated configs in lock-step with whichever CLI binary the
    // user is running, even after an upgrade. The commented hint below makes
    // the override path discoverable, including `tag: latest` (every build
    // also produces a :latest-kafka<ver> alias) for users who don't want
    // to edit the config after each rebuild.
    let content = format!(
        "{header}\n\
         kubernetes:\n\
        \x20\x20namespace: dimster\n\
         \n\
         # Override only if you need a custom image tag. The default is the\n\
         # dimster CLI's own version. Every `dimster build` also produces a\n\
         # :latest-kafka<kver> alias, so `tag: latest` is a valid choice.\n\
         # image:\n\
         #   tag: 0.1.0       # pin to a specific version\n\
         #   # tag: latest    # always resolve to the most recently built image\n\
         \n\
         environments:\n\
         {envs}\
         \n\
         results:\n\
        \x20\x20dir: ./results\n",
        header = NEW_FILE_HEADER,
        envs = envs_text,
    );

    std::fs::write(config_path, content)
        .with_context(|| format!("Failed to write {}", config_path.display()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Uncomment a template's env block the way the header tells users to:
    /// drop the first `#` of every line from `#<name>:` on.
    fn uncomment(template: &str, name: &str) -> String {
        let start = template.find(&format!("  #{}:", name)).unwrap();
        template[start..]
            .lines()
            .map(|l| l.replacen('#', "", 1) + "\n")
            .collect()
    }

    #[test]
    fn shared_example_uncommented_uses_local_full_kafka() {
        let yaml = format!(
            "environments:\n{}{}",
            uncomment(
                &render(TEMPLATE_LOCAL_FULL, Distro::Minikube),
                "exampleLocalFull"
            ),
            uncomment(
                &render(TEMPLATE_LOCAL_SHARED, Distro::Minikube),
                "exampleLocalShared"
            ),
        );
        let cfg: DimsterConfig = serde_yaml::from_str(&yaml).unwrap();
        cfg.get_env("exampleLocalShared").unwrap();
        assert_eq!(
            cfg.kafka_source("exampleLocalShared").unwrap(),
            crate::config::KafkaSource::InCluster {
                env: "exampleLocalFull".into(),
                namespace: "dimster".into(),
            }
        );
    }
    use crate::config::DimsterConfig;

    /// Each `local*` template must contain the placeholders that `render`
    /// substitutes; remote templates must NOT (they're static). Guards
    /// against accidental drift in the constants.
    #[test]
    fn test_local_templates_have_placeholders() {
        for (name, tmpl) in [
            ("TEMPLATE_LOCAL", TEMPLATE_LOCAL),
            ("TEMPLATE_LOCAL_FULL", TEMPLATE_LOCAL_FULL),
            ("TEMPLATE_LOCAL_EXT", TEMPLATE_LOCAL_EXT),
        ] {
            assert!(tmpl.contains("__CONTEXT__"), "{name} missing __CONTEXT__");
            assert!(tmpl.contains("__DISTRO__"), "{name} missing __DISTRO__");
        }
        // host_internal is only used in the external example.
        assert!(TEMPLATE_LOCAL_EXT.contains("__HOST_INTERNAL__"));
        // Remote templates are fully static — any `__` would be a placeholder
        // bug (no remote-specific substitution exists).
        assert!(!TEMPLATE_REMOTE_FULL.contains("__"));
        assert!(!TEMPLATE_REMOTE_EXT.contains("__"));
    }

    /// Every distro substitutes ALL placeholders in ALL three local templates,
    /// leaving no `__X__` markers behind. Locks in the per-distro mappings.
    #[test]
    fn test_render_per_distro() {
        for (d, ctx, dis, host) in [
            (
                Distro::Minikube,
                "minikube",
                "minikube",
                "host.minikube.internal",
            ),
            (Distro::K3d, "k3d-dimster", "k3d", "host.k3d.internal"),
            (Distro::Kind, "kind-dimster", "kind", "host.docker.internal"),
            (
                Distro::DockerDesktop,
                "docker-desktop",
                "docker-desktop",
                "host.docker.internal",
            ),
        ] {
            for tmpl in [TEMPLATE_LOCAL, TEMPLATE_LOCAL_FULL, TEMPLATE_LOCAL_EXT] {
                let rendered = render(tmpl, d);
                assert!(
                    rendered.contains(&format!("context: {ctx}"))
                        || rendered.contains(&format!("context: {ctx}\n")),
                    "{:?}: missing 'context: {}'",
                    d,
                    ctx
                );
                assert!(
                    rendered.contains(&format!("distro: {dis}")),
                    "{:?}: missing 'distro: {}'",
                    d,
                    dis
                );
                assert!(
                    !rendered.contains("__CONTEXT__"),
                    "{:?}: unrendered __CONTEXT__",
                    d
                );
                assert!(
                    !rendered.contains("__DISTRO__"),
                    "{:?}: unrendered __DISTRO__",
                    d
                );
            }
            // host_internal substitution is only relevant in the external template.
            let ext = render(TEMPLATE_LOCAL_EXT, d);
            assert!(
                ext.contains(host),
                "{:?}: missing host_internal '{}'",
                d,
                host
            );
            assert!(
                !ext.contains("__HOST_INTERNAL__"),
                "{:?}: unrendered __HOST_INTERNAL__",
                d
            );
        }
    }

    /// End-to-end: writing a fresh file produces parseable YAML where the
    /// active `local` env reflects the chosen distro and the four `example*`
    /// blocks survive as raw text (commented, not parsed envs). Uses K3d to
    /// prove the substitution flows all the way through, not just minikube.
    #[test]
    fn test_creates_new_file_with_chosen_distro() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("dimster-config.yaml");

        run(&config_path, Distro::K3d).unwrap();

        let content = std::fs::read_to_string(&config_path).unwrap();

        // File parses as valid DimsterConfig.
        let cfg: DimsterConfig = serde_yaml::from_str(&content).unwrap_or_else(|e| {
            panic!(
                "template file should parse, error: {}\n--- file ---\n{}",
                e, content
            )
        });

        // Active `local` env picks up the chosen distro's defaults.
        let local = cfg.get_env("local").unwrap();
        assert_eq!(local.image.as_deref(), Some("dimster"));
        assert_eq!(local.context.as_deref(), Some("k3d-dimster"));
        assert_eq!(local.distro(), Distro::K3d);

        // The examples are commented and therefore must NOT show up as
        // parsed envs.
        assert!(cfg.environments.get("exampleLocalShared").is_none());
        assert!(cfg.environments.get("exampleLocalFull").is_none());
        assert!(cfg.environments.get("exampleLocalExt").is_none());
        assert!(cfg.environments.get("exampleRemoteFull").is_none());
        assert!(cfg.environments.get("exampleRemoteExt").is_none());

        // Each example's commented env-name line is in the raw text. Uses
        // `#<name>` (no space) so removing the leading `#` yields a properly
        // 2-space-indented YAML key.
        assert!(content.contains("#exampleLocalFull:"));
        assert!(content.contains("#exampleLocalExt:"));
        assert!(content.contains("#exampleRemoteFull:"));
        assert!(content.contains("#exampleRemoteExt:"));
        assert!(content.contains("#exampleLocalShared:"));

        // The chosen distro's context appears 4 times: once in the active
        // `local`, once each in commented exampleLocalFull, exampleLocalExt
        // and exampleLocalShared. The remote examples use a different
        // (registry) context so they don't add to the count.
        assert_eq!(
            content.matches("k3d-dimster").count(),
            4,
            "expected 'k3d-dimster' in active local + 3 commented locals\n{}",
            content
        );

        // The external example's bootstrap.servers uses the k3d-specific
        // host hostname, not the literal "minikube" value.
        assert!(
            content.contains("host.k3d.internal:19092"),
            "expected k3d host in external example bootstrap.servers"
        );
        assert!(
            !content.contains("host.minikube.internal"),
            "minikube host hostname leaked into k3d-rendered file"
        );

        // Standard top-level sections present.
        assert!(content.contains("kubernetes:\n  namespace: dimster"));
        assert!(content.contains("results:\n  dir: ./results"));
        // The new-file guidance header tips are present.
        assert!(content.contains("Tips for filling out an environment"));
        assert!(content.contains("kubectl config get-contexts"));
        assert!(content.contains("kubectl describe node"));
        assert!(content.contains("memory limit > JVM max heap"));
        assert!(content.contains("role=kafka   role=worker   role=infra"));
    }

    /// Minikube is the default distro and the historical baseline; verify
    /// it still produces the values existing docs reference (`minikube`
    /// context, `host.minikube.internal` host hostname).
    #[test]
    fn test_creates_new_file_with_minikube_default() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("dimster-config.yaml");

        run(&config_path, Distro::Minikube).unwrap();

        let content = std::fs::read_to_string(&config_path).unwrap();
        let cfg: DimsterConfig = serde_yaml::from_str(&content).unwrap();

        let local = cfg.get_env("local").unwrap();
        assert_eq!(local.context.as_deref(), Some("minikube"));
        assert_eq!(local.distro(), Distro::Minikube);
        assert!(content.contains("host.minikube.internal:19092"));
    }

    /// Refuses to overwrite an existing file. The user's pre-existing
    /// content must be left byte-identical and an "already exists" error
    /// must be returned. Replaces the previous append/idempotency dance:
    /// `init` is now a one-shot create, never a merge.
    #[test]
    fn test_refuses_when_file_exists() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("dimster-config.yaml");

        let pre_existing = "# user's hand-written config\n\
                            kubernetes:\n  namespace: dimster\n\
                            environments:\n  myEnv:\n    image: dimster\n";
        std::fs::write(&config_path, pre_existing).unwrap();

        let err = run(&config_path, Distro::Minikube).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("already exists"),
            "expected 'already exists' in error, got: {}",
            msg
        );

        // File is byte-identical — refused without any modification.
        let after = std::fs::read_to_string(&config_path).unwrap();
        assert_eq!(after, pre_existing);
    }
}
