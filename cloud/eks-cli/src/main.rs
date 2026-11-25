//! Entry point for `eks-setup` — interactive EKS cluster setup CLI for Dimster.

mod answers;
mod instances;
mod questions;
mod steps;

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use cloud_common::exec::{Mode, RunOptions};
use cloud_common::sizing::resolve_instance_types_path;

#[derive(Parser)]
#[command(
    name = "eks-setup",
    about = "Interactive EKS cluster setup for Dimster benchmarks"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new EKS cluster for benchmarking
    Create {
        /// Path to answers YAML file (replay mode — skips interactive prompts)
        #[arg(long)]
        answers: Option<String>,

        /// Path to instance types YAML file
        #[arg(long, default_value = "cloud/eks-cli/instance-types.yaml")]
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

    /// Delete an EKS cluster and associated resources
    Delete {
        /// Path to answers YAML file (used to determine cluster name and region)
        #[arg(long)]
        answers: Option<String>,

        /// Dimster name (alternative to --answers, derives cluster name as {name}-benchmark)
        #[arg(long, required_unless_present = "answers")]
        name: Option<String>,

        /// AWS region (required with --name)
        #[arg(long, required_unless_present = "answers")]
        region: Option<String>,

        /// Keep the ECR repository (only delete the EKS cluster)
        #[arg(long)]
        keep_ecr: bool,

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
            region,
            keep_ecr,
            dry_run,
            generate_script,
            yes,
        } => run_delete(answers, name, region, keep_ecr, dry_run, generate_script, yes),
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
        println!("Dimster EKS Setup");
        println!("=================\n");
    }

    let resolved = resolve_instance_types_path(
        &instance_types,
        &[
            "cloud/eks-cli/instance-types.yaml",
            "eks-cli/instance-types.yaml",
            "instance-types.yaml",
        ],
    );
    let instance_types = instances::InstanceTypes::load(&resolved)
        .with_context(|| format!("Failed to load instance types from {:?}", resolved))?;

    let answers = if let Some(ref path) = answers_path {
        if opts.mode != Mode::GenerateScript {
            println!("Loading answers from {}...\n", path);
        }
        answers::Answers::load(path)?
    } else {
        questions::ask_all(&instance_types)?
    };

    if opts.mode != Mode::GenerateScript {
        println!("\n=== Configuration Summary ===\n");
        println!("  Env key:  {}", answers.env_name);
        println!("  Cluster:  {}-benchmark", answers.aws.dimster_name);
        println!("  Region:   {} ({})", answers.aws.region, answers.aws.az);
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
                match answers.storage.performance.as_deref() {
                    Some("provisioned") => format!(
                        "{}, {} IOPS, {} MB/s",
                        answers.storage.size,
                        answers.storage.iops.unwrap_or(0),
                        answers.storage.throughput_mbs.unwrap_or(0)
                    ),
                    _ => answers.storage.size.clone(),
                }
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
            "  Infra:    1x {} (coordinator + Grafana + Prometheus)",
            answers.coordinator.infra_instance_type
        );
        println!(
            "  Coord:    heap: {}, cpu: {}, mem: {}",
            answers.coordinator.heap, answers.coordinator.cpu, answers.coordinator.memory
        );
    }

    if opts.mode == Mode::Execute && answers_path.is_none() && !opts.auto_accept {
        let proceed = dialoguer::Confirm::new()
            .with_prompt("\nProceed with setup?")
            .default(true)
            .interact()?;

        if !proceed {
            println!("Aborted.");
            return Ok(());
        }
    }

    steps::run_all(&answers, opts)
}

fn run_delete(
    answers_path: Option<String>,
    name: Option<String>,
    region: Option<String>,
    keep_ecr: bool,
    dry_run: bool,
    generate_script: bool,
    yes: bool,
) -> Result<()> {
    let opts = RunOptions::from_flags(dry_run, generate_script, yes);

    let (dimster_name, aws_region) = if let Some(ref path) = answers_path {
        if opts.mode != Mode::GenerateScript {
            println!("Loading answers from {}...\n", path);
        }
        let answers = answers::Answers::load(path)?;
        (answers.aws.dimster_name, answers.aws.region)
    } else {
        match (name, region) {
            (Some(n), Some(r)) => (n, r),
            _ => bail!("Either --answers or both --name and --region are required"),
        }
    };

    let cluster_name = format!("{}-benchmark", dimster_name);

    if opts.mode != Mode::GenerateScript {
        println!("Dimster EKS Delete");
        println!("==================\n");
        println!("  Cluster:  {}", cluster_name);
        println!("  Region:   {}", aws_region);
        println!("  Keep ECR: {}", if keep_ecr { "yes" } else { "no" });
    }

    steps::delete_all(&dimster_name, &aws_region, keep_ecr, opts)
}
