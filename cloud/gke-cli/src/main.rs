//! Entry point for `gke-setup` — interactive GKE cluster setup CLI for Dimster.

mod answers;
mod instances;
mod pricing;
mod questions;
mod quotas;
mod steps;
mod storage;

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use cloud_common::exec::{Mode, RunOptions};
use cloud_common::sizing::resolve_instance_types_path;

#[derive(Parser)]
#[command(
    name = "gke-setup",
    about = "Interactive GKE cluster setup for Dimster benchmarks"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Verify gcloud, kubectl, gke-gcloud-auth-plugin, impersonation, and IAM permissions
    Check {
        /// GCP project ID (defaults to active gcloud config project)
        #[arg(long)]
        project: Option<String>,
    },

    /// Create a new GKE cluster and Artifact Registry repo for benchmarking
    Create {
        /// Path to answers YAML file (replay mode — skips interactive prompts)
        #[arg(long)]
        answers: Option<String>,

        /// Path to GCP instance types YAML file
        #[arg(long, default_value = "cloud/gke-cli/instance-types.yaml")]
        instance_types: PathBuf,

        /// Show what would be executed without running anything
        #[arg(long)]
        dry_run: bool,

        /// Generate a shell script instead of executing commands
        #[arg(long)]
        generate_script: bool,

        /// Skip all interactive confirmations
        #[arg(long, short = 'y')]
        yes: bool,
    },

    /// Delete a GKE cluster, orphaned GCE disks, and Artifact Registry repo
    Delete {
        /// Path to answers YAML file (provides cluster name, project, region, zone)
        #[arg(long)]
        answers: Option<String>,

        /// Dimster name (alternative to --answers; derives cluster name {name}-benchmark)
        #[arg(long, required_unless_present = "answers")]
        name: Option<String>,

        /// GCP project ID (required with --name)
        #[arg(long, required_unless_present = "answers")]
        project: Option<String>,

        /// GCP region for Artifact Registry (required with --name)
        #[arg(long, required_unless_present = "answers")]
        region: Option<String>,

        /// GCP zone for GKE cluster & disks (required with --name)
        #[arg(long, required_unless_present = "answers")]
        zone: Option<String>,

        /// Keep the Artifact Registry repository (only delete cluster & disks)
        #[arg(long)]
        keep_registry: bool,

        /// Show what would be executed without running anything
        #[arg(long)]
        dry_run: bool,

        /// Generate a shell script instead of executing commands
        #[arg(long)]
        generate_script: bool,

        /// Skip all interactive confirmations
        #[arg(long, short = 'y')]
        yes: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Check { project } => steps::run_check(project.as_deref()),

        Commands::Create {
            answers,
            instance_types,
            dry_run,
            generate_script,
            yes,
        } => run_create(answers, instance_types, dry_run, generate_script, yes),

        Commands::Delete {
            answers,
            name,
            project,
            region,
            zone,
            keep_registry,
            dry_run,
            generate_script,
            yes,
        } => run_delete(
            answers,
            name,
            project,
            region,
            zone,
            keep_registry,
            dry_run,
            generate_script,
            yes,
        ),
    }
}

fn run_create(
    answers_path: Option<String>,
    instance_types: PathBuf,
    dry_run: bool,
    generate_script: bool,
    yes: bool,
) -> Result<()> {
    let opts = RunOptions::from_flags(dry_run, generate_script, yes);

    if opts.mode != Mode::GenerateScript {
        println!("Dimster GKE Setup");
        println!("=================\n");
    }

    let resolved_path = resolve_instance_types_path(
        &instance_types,
        &[
            "cloud/gke-cli/instance-types.yaml",
            "gke-cli/instance-types.yaml",
            "instance-types.yaml",
        ],
    );
    let instance_types = instances::InstanceTypes::load(&resolved_path)
        .with_context(|| format!("Failed to load instance types from {:?}", resolved_path))?;

    let (answers, region_quotas) = if let Some(ref path) = answers_path {
        if opts.mode != Mode::GenerateScript {
            println!("Loading answers from {}...\n", path);
        }
        let loaded = answers::Answers::load(path)?;
        let q = if opts.mode != Mode::GenerateScript {
            println!(
                "Querying GCP quotas for {} ({})...",
                loaded.gcp.project, loaded.gcp.region
            );
            Some(quotas::fetch_region_quotas(
                &loaded.gcp.project,
                &loaded.gcp.region,
            ))
        } else {
            None
        };
        (loaded, q)
    } else {
        let (a, q) = questions::ask_all(&instance_types)?;
        (a, Some(q))
    };

    if opts.mode != Mode::GenerateScript {
        print_summary(&answers);
        if let Some(ref q) = region_quotas {
            let checks = quotas::evaluate_plan_against_quotas(&answers, &instance_types, q);
            quotas::print_quota_assessment(&checks, q);
        }
        let cost = pricing::estimate_cluster_cost(&answers, &instance_types);
        pricing::print_cost_estimate(&cost);
    }

    if opts.mode == Mode::Execute && !opts.auto_accept {
        let proceed = dialoguer::Confirm::new()
            .with_prompt("\nProceed with setup?")
            .default(true)
            .interact()?;
        if !proceed {
            println!("Aborted.");
            return Ok(());
        }
    }

    steps::run_all(&answers, &instance_types, opts)
}

fn print_summary(answers: &answers::Answers) {
    println!("\n=== Configuration Summary ===\n");
    println!("  Env key:  {}", answers.env_name);
    println!("  Project:  {}", answers.gcp.project);
    println!("  Cluster:  {}", answers.cluster_name());
    println!("  Location: {} ({})", answers.gcp.region, answers.gcp.zone);
    println!("  Image:    {}", answers.image_repo());
    if answers.cpu_manager_static {
        println!("  CPU Mgr:  static (kubelet cpuManagerPolicy=static)");
    }
    if answers.is_external() {
        println!("  Mode:     external (connect to existing Kafka)");
        if let Some(ref bs) = answers.bootstrap_servers {
            println!("  Bootstrap: {}", bs);
        }
    } else {
        println!(
            "  Kafka:    {} {}x {} (heap: {}, cpu: {}, mem: {})",
            answers.kafka.version,
            answers.kafka.count,
            answers.kafka.instance_type,
            answers.kafka.heap,
            answers.kafka.cpu,
            answers.kafka.memory
        );
        println!(
            "  Storage:  {} ({})",
            answers.storage.storage_type,
            storage::hardware_storage_summary(&answers.storage)
        );
    }
    println!(
        "  Workers:  {}x {} (heap: {}, cpu: {}, mem: {})",
        answers.workers.count,
        answers.workers.instance_type,
        answers.workers.heap,
        answers.workers.cpu,
        answers.workers.memory
    );
    println!(
        "  Infra:    1x {} (default-pool: coordinator + Grafana + Prometheus)",
        answers.coordinator.infra_instance_type
    );
    println!(
        "  Coord:    heap: {}, cpu: {}, mem: {}",
        answers.coordinator.heap, answers.coordinator.cpu, answers.coordinator.memory
    );
}

#[allow(clippy::too_many_arguments)]
fn run_delete(
    answers_path: Option<String>,
    name: Option<String>,
    project: Option<String>,
    region: Option<String>,
    zone: Option<String>,
    keep_registry: bool,
    dry_run: bool,
    generate_script: bool,
    yes: bool,
) -> Result<()> {
    let opts = RunOptions::from_flags(dry_run, generate_script, yes);

    let (dimster_name, gcp_project, gcp_region, gcp_zone, keep_reg) =
        if let Some(ref path) = answers_path {
            if opts.mode != Mode::GenerateScript {
                println!("Loading answers from {}...\n", path);
            }
            let a = answers::Answers::load(path)?;
            let keep = keep_registry || a.gcp.existing_image.is_some();
            (a.gcp.dimster_name, a.gcp.project, a.gcp.region, a.gcp.zone, keep)
        } else {
            match (name, project, region, zone) {
                (Some(n), Some(p), Some(r), Some(z)) => (n, p, r, z, keep_registry),
                _ => bail!(
                    "Either --answers or all of --name, --project, --region, --zone are required"
                ),
            }
        };

    if opts.mode != Mode::GenerateScript {
        println!("Dimster GKE Delete");
        println!("==================\n");
        println!("  Project:       {}", gcp_project);
        println!("  Cluster:       {}-benchmark", dimster_name);
        println!("  Location:      {} ({})", gcp_region, gcp_zone);
        println!(
            "  Keep Registry: {}",
            if keep_reg { "yes" } else { "no" }
        );
    }

    steps::delete_all(
        &dimster_name,
        &gcp_project,
        &gcp_region,
        &gcp_zone,
        keep_reg,
        opts,
    )
}
