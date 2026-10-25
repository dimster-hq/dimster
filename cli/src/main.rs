mod commands;
mod config;
mod kubectl;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

use config::{DimsterConfig, Distro, MetricsOutput};
use kubectl::Kubectl;

#[derive(Parser)]
#[command(
    name = "dimster",
    about = "Dimster - DIMensional teSTER for Apache Kafka (or compatible)"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// Parsed `--metrics-output` value. A newtype so clap treats the whole
/// comma-separated list as one value; it would otherwise read
/// `Option<Vec<_>>` as a repeatable multi-value flag.
#[derive(Clone, Debug)]
struct MetricsOutputArg(Vec<MetricsOutput>);

fn parse_metrics_output(s: &str) -> std::result::Result<MetricsOutputArg, String> {
    config::parse_metrics_output_arg(s)
        .map(MetricsOutputArg)
        .map_err(|e| e.to_string())
}

/// `--metrics-output`, shared by every command that ends by collecting a
/// run's results.
#[derive(Args, Debug)]
struct MetricsOutputOpt {
    /// Metrics to save after the run: comma-separated list of html and
    /// snapshot, or none. Overrides the environment's metricsOutput
    /// (default: html).
    #[arg(long = "metrics-output", value_parser = parse_metrics_output, value_name = "OUTPUTS")]
    metrics_output: Option<MetricsOutputArg>,
}

impl MetricsOutputOpt {
    /// Resolve against the env config: flag > env `metricsOutput` > [html].
    fn resolve(self, config: &DimsterConfig, env: &str) -> Vec<MetricsOutput> {
        config::resolve_metrics_output(
            self.metrics_output.map(|a| a.0),
            config.environments.get(env),
        )
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Build the project and Docker image
    Build {
        /// Environment name
        env: String,

        /// Run the Maven build inside Docker instead of on the host.
        /// Default is host-build (faster on Apple Silicon, avoids
        /// Rosetta JIT issues under amd64 Colima). Use this when you
        /// don't have a JDK installed or want a fully reproducible
        /// container build.
        #[arg(long)]
        in_docker: bool,
    },

    /// Deploy Kafka + workers to Kubernetes
    Deploy {
        /// Environment name (e.g. local, aws)
        env: String,

        /// Temporarily override an environment field for this deploy, e.g. kafka.replicas=5
        #[arg(long = "env-override")]
        env_overrides: Vec<String>,

        /// Path to log4j config YAML (overrides environment's logConfig)
        #[arg(short = 'l', long = "log-config")]
        log_config: Option<String>,

        /// Deploy only specific components (comma-separated: kafka,workers,coordinator,monitoring)
        #[arg(short = 'o', long = "only", value_delimiter = ',')]
        only: Vec<String>,
    },

    /// Run a benchmark, or modify a running one with `--modify`.
    ///
    /// MODIFY MODE: `dimster run <env> --modify <flags>` sends a side-channel
    /// request to the coordinator to change the in-progress test point.
    /// Modify accepts:
    ///   --terminate                end the current test point early
    ///   --skip-phase               end ramp-up or warm-up early
    ///   --discard-stats            wipe accumulated stats (test-phase only)
    ///   --run-for-minutes <N>      reset remaining test duration
    ///   --restart-consumers <N>    restart N random consumers
    ///   --restart-producers <N>    restart N random producers
    ///   --set <key>=<value> ...    override workload or kafkaConfig fields
    /// In modify mode -k and -w are ignored.
    Run {
        /// Environment name (e.g. local, aws)
        env: String,

        /// Path to Kafka config YAML (optional if config is embedded in workload)
        #[arg(short = 'k', long = "kafka-config")]
        kafka_config: Option<String>,

        /// Path to workload YAML (can be specified multiple times).
        #[arg(short = 'w', long = "workload")]
        workloads: Vec<String>,

        /// Extra result metadata as key=value, e.g. series=broker-count
        #[arg(short = 'm', long = "metadata")]
        metadata: Vec<String>,

        /// Extra infrastructure dimension as key=value, e.g. brokers=5
        #[arg(short = 'd', long = "extra-dimension")]
        extra_dimensions: Vec<String>,

        #[command(flatten)]
        metrics: MetricsOutputOpt,
    },

    /// Explore performance limits of the Kafka cluster
    Explore {
        /// Environment name (e.g. local, aws)
        env: String,

        /// Path to Kafka config YAML (optional if config is embedded in workload)
        #[arg(short = 'k', long = "kafka-config")]
        kafka_config: Option<String>,

        /// Path to explore workload YAML
        #[arg(short = 'w', long = "workload")]
        workload: String,

        /// Extra result metadata as key=value, e.g. series=broker-count
        #[arg(short = 'm', long = "metadata")]
        metadata: Vec<String>,

        /// Extra infrastructure dimension as key=value, e.g. brokers=5
        #[arg(short = 'd', long = "extra-dimension")]
        extra_dimensions: Vec<String>,

        #[command(flatten)]
        metrics: MetricsOutputOpt,
    },

    /// Run a correctness check
    Correctness {
        /// Environment name (e.g. local, aws)
        env: String,

        /// Path to Kafka config YAML (optional if config is embedded in workload)
        #[arg(short = 'k', long = "kafka-config")]
        kafka_config: Option<String>,

        /// Path to correctness workload YAML
        #[arg(short = 'w', long = "workload")]
        workload: String,

        /// Extra result metadata as key=value, e.g. series=broker-count
        #[arg(short = 'm', long = "metadata")]
        metadata: Vec<String>,

        /// Extra infrastructure dimension as key=value, e.g. brokers=5
        #[arg(short = 'd', long = "extra-dimension")]
        extra_dimensions: Vec<String>,

        #[command(flatten)]
        metrics: MetricsOutputOpt,
    },

    /// Measure backlog drain time
    DrainBacklog {
        /// Environment name (e.g. local, aws)
        env: String,

        /// Path to Kafka config YAML (optional if config is embedded in workload)
        #[arg(short = 'k', long = "kafka-config")]
        kafka_config: Option<String>,

        /// Path to drain-backlog workload YAML
        #[arg(short = 'w', long = "workload")]
        workload: String,

        /// Extra result metadata as key=value, e.g. series=broker-count
        #[arg(short = 'm', long = "metadata")]
        metadata: Vec<String>,

        /// Extra infrastructure dimension as key=value, e.g. brokers=5
        #[arg(short = 'd', long = "extra-dimension")]
        extra_dimensions: Vec<String>,

        #[command(flatten)]
        metrics: MetricsOutputOpt,
    },

    /// Resume a benchmark from the last checkpoint
    Resume {
        /// Environment name
        env: String,

        #[command(flatten)]
        metrics: MetricsOutputOpt,
    },

    /// Reattach to a running benchmark
    Reattach {
        /// Environment name
        env: String,

        #[command(flatten)]
        metrics: MetricsOutputOpt,
    },

    /// Show theoretical resource requirements for workloads
    Resources {
        /// Number of brokers in the cluster
        #[arg(short = 'b', long = "brokers", required = true)]
        broker_count: u32,

        /// Path to workload YAML (can be specified multiple times)
        #[arg(short = 'w', long = "workload", required = true)]
        workloads: Vec<String>,

        /// Path to Kafka config YAML (optional)
        #[arg(short = 'k', long = "kafka-config")]
        kafka_config: Option<String>,
    },

    /// Gracefully stop a running benchmark
    Stop {
        /// Environment name
        env: String,
    },

    /// Copy results from K8s to local machine
    Results {
        /// Environment name
        env: String,

        /// Output directory (default: from config)
        output_dir: Option<String>,

        /// Download result JSON files from PVC
        #[arg(long)]
        data: bool,

        /// Download coordinator/worker/Kafka logs
        #[arg(long)]
        logs: bool,

        /// Export the run's metrics. Without a value, uses the environment's
        /// metricsOutput (default: html); otherwise a comma-separated list of
        /// html and snapshot, or none.
        #[arg(
            long = "metrics-output",
            value_parser = parse_metrics_output,
            value_name = "OUTPUTS",
            num_args = 0..=1
        )]
        metrics_output: Option<Option<MetricsOutputArg>>,

        /// Deprecated alias for `--metrics-output html`
        #[arg(long, hide = true, conflicts_with = "metrics_output")]
        dashboards: bool,

        /// Generate charts from result JSON
        #[arg(long)]
        charts: bool,

        /// Capture the full coordinator/worker log files (and any rolled
        /// .gz backups) without the per-run time filter. Use this when
        /// debugging post-run state — for example, a worker whose Kafka
        /// client kept logging after the run's recorded end timestamp.
        /// No effect on Kafka pod logs (always captured raw) or when
        /// `--logs` collection is not happening.
        #[arg(long)]
        full: bool,
    },

    /// Generate CSV from local JSON result files
    Csv {
        /// Path to JSON result file or directory
        path: String,

        /// Output file (default: a .csv next to each .json)
        #[arg(short, long)]
        output: Option<String>,
    },

    /// Remove resources from Kubernetes
    Teardown {
        /// Environment name
        env: String,

        /// Tear down only specific components (comma-separated: kafka,workers,coordinator,monitoring)
        #[arg(short = 'o', long = "only", value_delimiter = ',')]
        only: Vec<String>,
    },

    /// Generate charts from results
    Chart {
        /// Results directory (default: from config)
        results_dir: Option<String>,
    },

    /// Compare run or correctness result files side-by-side
    Compare {
        /// Labeled result files: LABEL=FILE (at least 2 required unless --directory is used)
        #[arg(num_args = 0.., value_name = "LABEL=FILE")]
        files: Vec<String>,

        /// Directory containing result files; labels are read from result metadata
        #[arg(short = 'd', long = "directory")]
        directory: Option<String>,

        /// Output directory (default: directory of first file)
        #[arg(short, long)]
        output: Option<String>,
    },

    /// Pivot a single result file: scenarios become the comparison axis,
    /// each array index (test label) becomes its own section
    Pivot {
        /// Path to a single run result JSON file
        file: String,

        /// Output directory (default: directory of input file)
        #[arg(short, long)]
        output: Option<String>,

        /// CSV of 0-indexed scenario positions to include (e.g. "0,3,5").
        /// Order is preserved, so this also reorders the legend.
        #[arg(short, long)]
        scenarios: Option<String>,
    },

    /// Port-forward Grafana + Prometheus, or with --snapshot, view a saved
    /// run's Prometheus snapshot in a local Grafana (needs Docker, no cluster)
    Dashboard {
        /// Environment name (not used with --snapshot)
        #[arg(required_unless_present = "snapshot")]
        env: Option<String>,

        /// Result directory containing a prometheus-snapshot/ to view locally
        #[arg(long, value_name = "RESULT_DIR", conflicts_with = "env")]
        snapshot: Option<PathBuf>,

        /// Grafana local port (default: 3000). Not used with --snapshot,
        /// which uses 3000/9090 when free and otherwise any free ports.
        #[arg(default_value = "3000")]
        grafana_port: u16,

        /// Prometheus local port (default: 9090)
        #[arg(default_value = "9090")]
        prometheus_port: u16,
    },

    /// View logs from worker, coordinator, or kafka pods
    Logs {
        /// Environment name
        env: String,

        /// Component to view logs for (worker, coordinator, kafka)
        #[arg(default_value = "worker")]
        component: String,

        /// Follow log output
        #[arg(short, long)]
        follow: bool,

        /// Show logs from previous (crashed) container
        #[arg(short, long)]
        previous: bool,

        /// Number of lines to show from end of logs
        #[arg(long)]
        tail: Option<u32>,
    },

    /// Download Kafka server log files from broker pods
    DownloadKafkaLogs {
        /// Environment name
        env: String,

        /// Output directory
        #[arg(short, long, default_value = ".")]
        output: String,
    },

    /// Check health of all worker pods
    Health {
        /// Environment name
        env: String,
    },

    /// Inspect Kafka topics, consumer groups, and share groups
    Inspect {
        /// Environment name
        env: String,

        /// Restrict output to dimster-created topics (`dimster*`) and groups
        /// (`dimster-grp-*`). Internal/offset topics like `__consumer_offsets`
        /// are excluded under this filter.
        #[arg(long)]
        dimster_only: bool,

        /// Emit only the TOPICS section. Combinable with `--group-offsets`
        /// and/or `--group-members` to add specific group subsections back in.
        #[arg(long)]
        topics: bool,

        /// Within group sections, emit only the OFFSETS subsection (per-group
        /// header is still shown). Combinable with `--group-members`.
        #[arg(long)]
        group_offsets: bool,

        /// Within group sections, emit only the MEMBERS subsection. Combinable
        /// with `--group-offsets`.
        #[arg(long)]
        group_members: bool,

        /// Emit only the METADATA QUORUM section (KRaft leader/voters/observers
        /// from describeMetadataQuorum). Fails gracefully on ZooKeeper-mode
        /// clusters with a single error line.
        #[arg(long)]
        metadata_quorum: bool,
    },

    /// Run Kafka CLI tools in a broker pod
    Kafka {
        /// Environment name
        env: String,

        /// Broker number (default: 0)
        #[arg(default_value = "0")]
        broker_id: u32,

        /// Command to run (after --)
        #[arg(last = true)]
        command: Vec<String>,
    },

    /// Initialize dimster-config.yaml with a starter set of environments:
    /// one active `local` env (minimal — no kafka/workers/coordinator
    /// sections, so deploy uses tiny defaults) plus four commented examples
    /// (minikube/remote × full/external) covering every
    /// supported shape. Idempotent: env names already present in the file
    /// are skipped. Creates dimster-config.yaml (with a commented guidance
    /// header) if it does not exist.
    Init {
        /// Local Kubernetes distribution to pre-fill the active `local`
        /// env with (its `context:` and `distro:` fields). Defaults to
        /// minikube. The four commented exampleLocal* templates always
        /// stay literal "minikube" — they're illustrative.
        #[arg(long, value_enum)]
        local_k8s_distro: Option<Distro>,
    },
}

fn main() {
    // Intercept `dimster run <env> --modify ...` BEFORE clap runs. The
    // modify path has its own `--set key=value ...` and mutually-exclusive
    // action grammar, which is clearer to validate as one request than as
    // fields on the normal run command. If the intercept handles it, we exit;
    // otherwise we fall through to normal clap parsing.
    let raw_args: Vec<String> = std::env::args().collect();
    if let Some(result) = commands::run::try_handle_modify(&raw_args) {
        if let Err(e) = result {
            eprintln!("ERROR: {:#}", e);
            std::process::exit(1);
        }
        return;
    }

    let cli = Cli::parse();

    if let Err(e) = run_command(cli) {
        eprintln!("ERROR: {:#}", e);
        std::process::exit(1);
    }
}

fn run_command(cli: Cli) -> Result<()> {
    let script_dir = find_script_dir()?;
    let config_path = std::env::var("DIMSTER_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| script_dir.join("dimster-config.yaml"));
    let config = DimsterConfig::load(&config_path)?;
    let k8s_dir = script_dir.join("run/deploy/kubernetes");

    // Global stop signal for Ctrl+C. First press sets the flag for graceful
    // shutdown; second press force-exits for when graceful shutdown hangs.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();
    ctrlc::set_handler(move || {
        if stop_clone.load(Ordering::Relaxed) {
            eprintln!("\nForce exit.");
            std::process::exit(1);
        }
        eprintln!("\nInterrupted. Press Ctrl+C again to force exit.");
        stop_clone.store(true, Ordering::Relaxed);
    })?;

    match cli.command {
        Commands::Build { env, in_docker } => {
            config.get_env(&env)?;
            commands::build::run(&config, vec![env], &script_dir, in_docker)?;
        }

        Commands::Deploy {
            env,
            env_overrides,
            log_config,
            only,
        } => {
            let deploy_config =
                DimsterConfig::load_with_env_overrides(&config_path, &env, &env_overrides)?;
            let kubectl = require_kubectl(&deploy_config, &env)?;
            commands::deploy::run(
                &kubectl,
                &deploy_config,
                &env,
                &k8s_dir,
                log_config.as_deref(),
                &only,
            )?;
            commands::run_configs::write_deploy_env_capture(
                &deploy_config.results.dir,
                &config_path,
                &env,
                &env_overrides,
            )?;
        }

        Commands::Run {
            env,
            kafka_config,
            workloads,
            metadata,
            extra_dimensions,
            metrics,
        } => {
            let kubectl = require_kubectl(&config, &env)?;
            // The --modify branch is intercepted BEFORE clap runs (see
            // try_handle_modify in main()). If we reach this Run dispatch,
            // it's a normal benchmark start.
            if workloads.is_empty() {
                anyhow::bail!(
                    "at least one --workload (-w) is required for a run \
                     (only --modify can omit it)"
                );
            }
            let env_config = config.get_env(&env)?;
            // Worker assignment lives under workers.assignment in the env
            // config — pull each field out so the run request can carry
            // them through to the coordinator.
            let assignment = env_config
                .workers
                .as_ref()
                .and_then(|w| w.assignment.as_ref());
            let run_args = commands::run::RunArgs {
                mode: "run".to_string(),
                kafka_config,
                workloads,
                worker_assignment: assignment.and_then(|a| a.mode.clone()),
                worker_ratio: assignment.and_then(|a| a.ratio.clone()),
                producer_workers: assignment.and_then(|a| a.producers),
                consumer_workers: assignment.and_then(|a| a.consumers),
                system: env_config.system.clone(),
                environment: env.clone(),
                hardware: env_config.hardware.clone(),
                connection: env_config.connection.clone(),
                broker_config: env_config
                    .kafka
                    .as_ref()
                    .and_then(|k| k.broker_config.clone()),
                extra_config: env_config
                    .kafka
                    .as_ref()
                    .and_then(|k| k.extra_config.clone()),
                tls_config: env_config.tls_config().cloned(),
                metadata,
                extra_dimensions,
                metrics_output: metrics.resolve(&config, &env),
            };
            let output_dir = config.results.dir.clone();
            commands::run::run(
                &kubectl,
                run_args,
                &output_dir,
                &script_dir,
                &config_path,
                stop,
            )?;
        }

        Commands::Explore {
            env,
            kafka_config,
            workload,
            metadata,
            extra_dimensions,
            metrics,
        } => {
            let kubectl = require_kubectl(&config, &env)?;
            if !std::path::Path::new(&workload).exists() {
                anyhow::bail!("Explore workload not found: {}", workload);
            }
            let workload_path = workload;
            let env_config = config.get_env(&env)?;
            let run_args = commands::run::RunArgs {
                mode: "explore".to_string(),
                kafka_config,
                workloads: vec![workload_path],
                worker_assignment: None,
                worker_ratio: None,
                producer_workers: None,
                consumer_workers: None,
                system: env_config.system.clone(),
                environment: env.clone(),
                hardware: env_config.hardware.clone(),
                connection: env_config.connection.clone(),
                broker_config: env_config
                    .kafka
                    .as_ref()
                    .and_then(|k| k.broker_config.clone()),
                extra_config: env_config
                    .kafka
                    .as_ref()
                    .and_then(|k| k.extra_config.clone()),
                tls_config: env_config.tls_config().cloned(),
                metadata,
                extra_dimensions,
                metrics_output: metrics.resolve(&config, &env),
            };
            let output_dir = config.results.dir.clone();
            commands::run::run(
                &kubectl,
                run_args,
                &output_dir,
                &script_dir,
                &config_path,
                stop,
            )?;
        }

        Commands::Correctness {
            env,
            kafka_config,
            workload,
            metadata,
            extra_dimensions,
            metrics,
        } => {
            let kubectl = require_kubectl(&config, &env)?;
            if !std::path::Path::new(&workload).exists() {
                anyhow::bail!("Correctness workload not found: {}", workload);
            }
            let workload_path = workload;
            let env_config = config.get_env(&env)?;
            let run_args = commands::run::RunArgs {
                mode: "correctness".to_string(),
                kafka_config,
                workloads: vec![workload_path],
                worker_assignment: None,
                worker_ratio: None,
                producer_workers: None,
                consumer_workers: None,
                system: env_config.system.clone(),
                environment: env.clone(),
                hardware: env_config.hardware.clone(),
                connection: env_config.connection.clone(),
                broker_config: env_config
                    .kafka
                    .as_ref()
                    .and_then(|k| k.broker_config.clone()),
                extra_config: env_config
                    .kafka
                    .as_ref()
                    .and_then(|k| k.extra_config.clone()),
                tls_config: env_config.tls_config().cloned(),
                metadata,
                extra_dimensions,
                metrics_output: metrics.resolve(&config, &env),
            };
            let output_dir = config.results.dir.clone();
            commands::run::run(
                &kubectl,
                run_args,
                &output_dir,
                &script_dir,
                &config_path,
                stop,
            )?;
        }

        Commands::DrainBacklog {
            env,
            kafka_config,
            workload,
            metadata,
            extra_dimensions,
            metrics,
        } => {
            let kubectl = require_kubectl(&config, &env)?;
            if !std::path::Path::new(&workload).exists() {
                anyhow::bail!("Drain-backlog workload not found: {}", workload);
            }
            let workload_path = workload;
            let env_config = config.get_env(&env)?;
            let run_args = commands::run::RunArgs {
                mode: "drain-backlog".to_string(),
                kafka_config,
                workloads: vec![workload_path],
                worker_assignment: None,
                worker_ratio: None,
                producer_workers: None,
                consumer_workers: None,
                system: env_config.system.clone(),
                environment: env.clone(),
                hardware: env_config.hardware.clone(),
                connection: env_config.connection.clone(),
                broker_config: env_config
                    .kafka
                    .as_ref()
                    .and_then(|k| k.broker_config.clone()),
                extra_config: env_config
                    .kafka
                    .as_ref()
                    .and_then(|k| k.extra_config.clone()),
                tls_config: env_config.tls_config().cloned(),
                metadata,
                extra_dimensions,
                metrics_output: metrics.resolve(&config, &env),
            };
            let output_dir = config.results.dir.clone();
            commands::run::run(
                &kubectl,
                run_args,
                &output_dir,
                &script_dir,
                &config_path,
                stop,
            )?;
        }

        Commands::Resume { env, metrics } => {
            let kubectl = require_kubectl(&config, &env)?;
            let output_dir = config.results.dir.clone();
            let outputs = metrics.resolve(&config, &env);
            commands::resume::run(&kubectl, &output_dir, &script_dir, stop, &env, &outputs)?;
        }

        Commands::Reattach { env, metrics } => {
            let kubectl = require_kubectl(&config, &env)?;
            let output_dir = config.results.dir.clone();
            let outputs = metrics.resolve(&config, &env);
            commands::reattach::run(&kubectl, &output_dir, &script_dir, stop, &env, &outputs)?;
        }

        Commands::Resources {
            broker_count,
            workloads,
            kafka_config,
        } => {
            commands::resources::run(broker_count, &workloads, kafka_config.as_deref())?;
        }

        Commands::Stop { env } => {
            let kubectl = require_kubectl(&config, &env)?;
            commands::stop::run(&kubectl, &stop)?;
        }

        Commands::Results {
            env,
            output_dir,
            data,
            logs,
            metrics_output,
            dashboards,
            charts,
            full,
        } => {
            let kubectl = require_kubectl(&config, &env)?;
            let dir = output_dir.unwrap_or_else(|| config.results.dir.clone());
            let (metrics_requested, explicit) = results_metrics_request(metrics_output, dashboards);
            let outputs = MetricsOutputOpt {
                metrics_output: explicit,
            }
            .resolve(&config, &env);
            commands::results::run(
                &kubectl,
                &dir,
                &script_dir,
                data,
                logs,
                metrics_requested,
                &outputs,
                charts,
                full,
                &stop,
            )?;
        }

        Commands::Csv { path, output } => {
            commands::csv::run(&path, output.as_deref())?;
        }

        Commands::Teardown { env, only } => {
            let kubectl = require_kubectl(&config, &env)?;
            let env_config = config.get_env(&env)?;
            let reset_local_storage_pvs = env_config
                .kafka
                .as_ref()
                .and_then(|k| k.pvc_storage_class.as_deref())
                == Some("local-storage");
            commands::teardown::run(
                &kubectl,
                &env,
                &k8s_dir,
                reset_local_storage_pvs,
                env_config.is_external(),
                &only,
            )?;
        }

        Commands::Chart { results_dir } => {
            let dir = results_dir.unwrap_or_else(|| config.results.dir.clone());
            commands::chart::run(&dir, &script_dir)?;
        }

        Commands::Compare {
            files,
            directory,
            output,
        } => {
            commands::compare::run(&files, directory.as_deref(), output.as_deref(), &script_dir)?;
        }

        Commands::Pivot {
            file,
            output,
            scenarios,
        } => {
            commands::pivot::run(&file, output.as_deref(), scenarios.as_deref(), &script_dir)?;
        }

        Commands::Dashboard {
            env,
            snapshot,
            grafana_port,
            prometheus_port,
        } => match (snapshot, env) {
            // Snapshot mode picks its own ports: the positionals would be
            // parsed into `env`, which conflicts with --snapshot.
            (Some(result_dir), _) => {
                commands::snapshot_viewer::run(&result_dir, stop)?;
            }
            (None, Some(env)) => {
                let kubectl = require_kubectl(&config, &env)?;
                commands::dashboard::run(&kubectl, grafana_port, prometheus_port, stop)?;
            }
            // clap's required_unless_present guarantees one of the two.
            (None, None) => unreachable!("dashboard needs an env or --snapshot"),
        },

        Commands::Logs {
            env,
            component,
            follow,
            previous,
            tail,
        } => {
            let kubectl = require_kubectl(&config, &env)?;
            commands::logs::run(&kubectl, &component, follow, previous, tail)?;
        }

        Commands::DownloadKafkaLogs { env, output } => {
            let kubectl = require_kubectl(&config, &env)?;
            commands::download_kafka_logs::run(&kubectl, &output)?;
        }

        Commands::Health { env } => {
            let kubectl = require_kubectl(&config, &env)?;
            commands::health::run(&kubectl)?;
        }

        Commands::Inspect {
            env,
            dimster_only,
            topics,
            group_offsets,
            group_members,
            metadata_quorum,
        } => {
            let env_config = config.get_env(&env)?;
            let kubectl = require_kubectl(&config, &env)?;
            let filters = commands::inspect::InspectFilters {
                dimster_only,
                topics,
                group_offsets,
                group_members,
                metadata_quorum,
            };
            if env_config.is_external() {
                // External mode: no kafka-0 pod exists, so we route through a
                // dedicated long-lived dimster-inspect pod that talks to the
                // remote cluster via AdminClient. See inspect::run_external.
                let conn = env_config.connection.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "external mode requires a 'connection:' section in the env config"
                    )
                })?;
                commands::inspect::run_external(&kubectl, conn, &filters)?;
            } else {
                commands::inspect::run(&kubectl, &filters)?;
            }
        }

        Commands::Kafka {
            env,
            broker_id,
            command,
        } => {
            let env_config = config.get_env(&env)?;
            if env_config.is_external() {
                anyhow::bail!(
                    "The 'kafka' command is not available in external deploy mode (no Kafka pods deployed)."
                );
            }
            let kubectl = require_kubectl(&config, &env)?;
            let cmd = if command.is_empty() {
                None
            } else {
                Some(command)
            };
            commands::kafka::run(&kubectl, broker_id, cmd)?;
        }

        Commands::Init { local_k8s_distro } => {
            commands::init::run(&config_path, local_k8s_distro.unwrap_or(Distro::Minikube))?;
        }
    }

    Ok(())
}

/// Create Kubectl after validating the environment exists in config.
fn require_kubectl(config: &DimsterConfig, env: &str) -> Result<Kubectl> {
    config.get_env(env)?;
    Ok(make_kubectl(config, env))
}

fn make_kubectl(config: &DimsterConfig, env: &str) -> Kubectl {
    let context = config.kubectl_context(env);
    let ns = config.kubectl_namespace(env);

    if let Some(ref ctx) = context {
        println!("    Using kubectl context: {}", ctx);
        // SAFETY: We are single-threaded at this point and the env var is only
        // read by child processes we spawn later.
        unsafe { std::env::set_var("DIMSTER_K8S_CONTEXT", ctx) };
    }

    Kubectl::new(context, ns)
}

/// Find the project root directory (where pom.xml lives).
fn find_script_dir() -> Result<PathBuf> {
    // Try the directory of the current executable first
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            let mut dir = parent.to_path_buf();
            for _ in 0..5 {
                if dir.join("pom.xml").exists() {
                    return Ok(dir);
                }
                if let Some(p) = dir.parent() {
                    dir = p.to_path_buf();
                } else {
                    break;
                }
            }
        }
    }

    // Fall back to current directory, walking up
    let mut dir = std::env::current_dir()?;
    for _ in 0..5 {
        if dir.join("pom.xml").exists() {
            return Ok(dir);
        }
        if let Some(p) = dir.parent() {
            dir = p.to_path_buf();
        } else {
            break;
        }
    }

    Ok(std::env::current_dir()?)
}

/// Interpret `dimster results`' metrics flags as (run the metrics step?,
/// explicit outputs). `--metrics-output` with no value runs the step with the
/// env's outputs; the hidden `--dashboards` alias means `--metrics-output html`.
fn results_metrics_request(
    metrics_output: Option<Option<MetricsOutputArg>>,
    dashboards: bool,
) -> (bool, Option<MetricsOutputArg>) {
    match metrics_output {
        Some(explicit) => (true, explicit),
        None if dashboards => (true, Some(MetricsOutputArg(vec![MetricsOutput::Html]))),
        None => (false, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn teardown_accepts_comma_separated_only_components() {
        let cli = Cli::try_parse_from([
            "dimster",
            "teardown",
            "local",
            "--only",
            "kafka,coordinator,workers",
        ])
        .unwrap();

        match cli.command {
            Commands::Teardown { env, only } => {
                assert_eq!(env, "local");
                assert_eq!(
                    only,
                    vec![
                        "kafka".to_string(),
                        "coordinator".to_string(),
                        "workers".to_string(),
                    ]
                );
            }
            _ => panic!("expected teardown command"),
        }
    }

    #[test]
    fn deploy_accepts_repeatable_env_overrides() {
        let cli = Cli::try_parse_from([
            "dimster",
            "deploy",
            "local",
            "--env-override",
            "kafka.replicas=5",
            "--env-override",
            "kafka.tls.enabled=true",
        ])
        .unwrap();

        match cli.command {
            Commands::Deploy {
                env, env_overrides, ..
            } => {
                assert_eq!(env, "local");
                assert_eq!(
                    env_overrides,
                    vec![
                        "kafka.replicas=5".to_string(),
                        "kafka.tls.enabled=true".to_string(),
                    ]
                );
            }
            _ => panic!("expected deploy command"),
        }
    }

    #[test]
    fn run_accepts_metadata_and_extra_dimensions() {
        let cli = Cli::try_parse_from([
            "dimster",
            "run",
            "local",
            "-w",
            "workload.yaml",
            "-m",
            "series=broker-count",
            "-d",
            "brokers=5",
        ])
        .unwrap();

        match cli.command {
            Commands::Run {
                metadata,
                extra_dimensions,
                ..
            } => {
                assert_eq!(metadata, vec!["series=broker-count".to_string()]);
                assert_eq!(extra_dimensions, vec!["brokers=5".to_string()]);
            }
            _ => panic!("expected run command"),
        }
    }

    fn metrics_of(args: &[&str]) -> Option<Vec<MetricsOutput>> {
        let cli = Cli::try_parse_from(args).unwrap();
        match cli.command {
            Commands::Run { metrics, .. }
            | Commands::Explore { metrics, .. }
            | Commands::Correctness { metrics, .. }
            | Commands::DrainBacklog { metrics, .. }
            | Commands::Resume { metrics, .. }
            | Commands::Reattach { metrics, .. } => metrics.metrics_output.map(|a| a.0),
            _ => panic!("expected a command that collects results"),
        }
    }

    #[test]
    fn run_type_commands_accept_metrics_output() {
        use MetricsOutput::{Html, Snapshot};
        assert_eq!(
            metrics_of(&[
                "dimster",
                "run",
                "local",
                "-w",
                "w.yaml",
                "--metrics-output",
                "html,snapshot"
            ]),
            Some(vec![Html, Snapshot])
        );
        assert_eq!(
            metrics_of(&[
                "dimster",
                "run",
                "local",
                "-w",
                "w.yaml",
                "--metrics-output",
                "none"
            ]),
            Some(vec![])
        );
        assert_eq!(
            metrics_of(&["dimster", "run", "local", "-w", "w.yaml"]),
            None
        );
        assert_eq!(
            metrics_of(&[
                "dimster",
                "explore",
                "local",
                "-w",
                "w.yaml",
                "--metrics-output",
                "snapshot"
            ]),
            Some(vec![Snapshot])
        );
        assert_eq!(
            metrics_of(&[
                "dimster",
                "reattach",
                "local",
                "--metrics-output",
                "snapshot"
            ]),
            Some(vec![Snapshot])
        );
        assert_eq!(
            metrics_of(&["dimster", "resume", "local", "--metrics-output", "html"]),
            Some(vec![Html])
        );
    }

    #[test]
    fn metrics_output_rejects_bad_values() {
        for bad in ["none,html", "pdf", ""] {
            assert!(
                Cli::try_parse_from([
                    "dimster",
                    "run",
                    "local",
                    "-w",
                    "w.yaml",
                    "--metrics-output",
                    bad
                ])
                .is_err(),
                "expected '{}' to be rejected",
                bad
            );
        }
    }

    fn results_request(args: &[&str]) -> (bool, Option<Vec<MetricsOutput>>) {
        let cli = Cli::try_parse_from(args).unwrap();
        match cli.command {
            Commands::Results {
                metrics_output,
                dashboards,
                ..
            } => {
                let (requested, explicit) = results_metrics_request(metrics_output, dashboards);
                (requested, explicit.map(|a| a.0))
            }
            _ => panic!("expected results command"),
        }
    }

    #[test]
    fn results_metrics_flag_forms() {
        use MetricsOutput::{Html, Snapshot};
        // No flag: step not requested on its own (runs only as part of "all").
        assert_eq!(
            results_request(&["dimster", "results", "local"]),
            (false, None)
        );
        // Bare flag: run the step with the env's outputs.
        assert_eq!(
            results_request(&["dimster", "results", "local", "--metrics-output"]),
            (true, None)
        );
        assert_eq!(
            results_request(&[
                "dimster",
                "results",
                "local",
                "--metrics-output",
                "snapshot"
            ]),
            (true, Some(vec![Snapshot]))
        );
        // Deprecated alias keeps its old meaning.
        assert_eq!(
            results_request(&["dimster", "results", "local", "--dashboards"]),
            (true, Some(vec![Html]))
        );
        assert!(
            Cli::try_parse_from([
                "dimster",
                "results",
                "local",
                "--dashboards",
                "--metrics-output",
                "html"
            ])
            .is_err()
        );
    }

    #[test]
    fn dashboard_snapshot_mode_needs_no_env() {
        let cli = Cli::try_parse_from(["dimster", "dashboard", "--snapshot", "results/x"]).unwrap();
        match cli.command {
            Commands::Dashboard {
                env,
                snapshot,
                grafana_port,
                ..
            } => {
                assert_eq!(env, None);
                assert_eq!(snapshot, Some(PathBuf::from("results/x")));
                assert_eq!(grafana_port, 3000);
            }
            _ => panic!("expected dashboard command"),
        }
        assert!(
            Cli::try_parse_from(["dimster", "dashboard", "local", "--snapshot", "results/x"])
                .is_err()
        );
        assert!(Cli::try_parse_from(["dimster", "dashboard"]).is_err());
        assert!(Cli::try_parse_from(["dimster", "dashboard", "local"]).is_ok());
    }
}
