//! EKS provisioning, teardown, and shell-script generation steps.
//!
//! Orchestrates `aws`, `eksctl`, and `kubectl` while delegating shared script
//! formatting, Dimster config rendering/insertion, and subprocess execution to
//! `cloud-common`.

use std::process::Command;

use anyhow::{Result, bail};
use cloud_common::config_gen::{
    CoordinatorRenderSpec, DimsterEnvSpec, KafkaRenderSpec, MonitoringRenderSpec,
    WorkersRenderSpec, save_or_insert_config,
};
use cloud_common::exec::{
    Mode, RunOptions, confirm_and_apply, confirm_and_run, confirm_and_run_inherit,
    delete_cluster_pvcs, emit_pvc_cleanup_script, verify_nodes,
};
use cloud_common::script::Script;

use crate::answers::Answers;

pub fn run_all(answers: &Answers, opts: RunOptions) -> Result<()> {
    if opts.mode == Mode::GenerateScript {
        let script = generate_script(answers)?;
        print!("{}", script);
        return Ok(());
    }

    step_1_ecr(answers, opts)?;
    step_2_eks(answers, opts)?;
    step_3_ebs_csi(answers, opts)?;

    if answers.is_external() {
        println!("\n=== Step 4/5: Create StorageClass — skipped (external) ===\n");
    } else {
        step_4_storage(answers, opts)?;
    }
    let inserted = step_5_generate(answers, opts)?;

    println!("\n=== Done! ===\n");
    let name = &answers.aws.dimster_name;
    let region = &answers.aws.region;
    let env_name = &answers.env_name;

    let account_id = aws_account_id().unwrap_or_else(|| "<ACCOUNT_ID>".to_string());

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
    println!(
        "    {}. Log in to ECR (token expires every 12 hours):",
        step
    );
    println!("       aws ecr get-login-password --region {} | \\", region);
    println!(
        "         docker login --username AWS --password-stdin {}.dkr.ecr.{}.amazonaws.com",
        account_id, region
    );
    step += 1;
    println!("    {}. Build and push: ./dimster build {}", step, env_name);
    step += 1;
    println!("    {}. Deploy: ./dimster deploy {}", step, env_name);

    Ok(())
}

fn aws_account_id() -> Option<String> {
    let out = Command::new("aws")
        .args([
            "sts",
            "get-caller-identity",
            "--query",
            "Account",
            "--output",
            "text",
        ])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if id.is_empty() { None } else { Some(id) }
}

fn eks_storage_class_name(answers: &Answers) -> &'static str {
    if answers.storage.storage_type == "instance-store" {
        "local-storage"
    } else if answers.storage.performance.as_deref() == Some("provisioned") {
        "gp3-fast"
    } else {
        "gp3"
    }
}

/// Build the shared `DimsterEnvSpec` so standalone files, `--generate-script`,
/// and in-place `dimster-config.yaml` insertion emit identical keys.
pub fn build_dimster_env_spec(
    answers: &Answers,
    image: String,
    context: String,
) -> DimsterEnvSpec {
    let hardware = if answers.is_external() {
        format!(
            "external, workers={}x{}, region={}, az={}, eks={}",
            answers.workers.count,
            answers.workers.instance_type,
            answers.aws.region,
            answers.aws.az,
            answers.aws.eks_version
        )
    } else {
        let storage_desc = if answers.storage.performance.as_deref() == Some("provisioned") {
            format!(
                "{}/{}/provisioned/{}iops/{}MBs",
                answers.storage.storage_type,
                answers.storage.size,
                answers.storage.iops.unwrap_or(0),
                answers.storage.throughput_mbs.unwrap_or(0)
            )
        } else {
            format!("{}/{}", answers.storage.storage_type, answers.storage.size)
        };
        format!(
            "kafka={}x{}, workers={}x{}, storage={}, region={}, az={}, eks={}",
            answers.kafka.count,
            answers.kafka.instance_type,
            answers.workers.count,
            answers.workers.instance_type,
            storage_desc,
            answers.aws.region,
            answers.aws.az,
            answers.aws.eks_version
        )
    };

    let kafka = if answers.is_external() {
        None
    } else {
        Some(KafkaRenderSpec {
            replicas: answers.kafka.count,
            version: answers.kafka.version.clone(),
            pvc_size: answers.storage.size.clone(),
            pvc_storage_class: eks_storage_class_name(answers).to_string(),
            heap: answers.kafka.heap.clone(),
            cpu: answers.kafka.cpu.clone(),
            memory: answers.kafka.memory.clone(),
            broker_config: answers.kafka.broker_config.clone(),
            extra_config: answers.kafka.extra_config.clone(),
        })
    };

    let system = if answers.is_external() {
        None
    } else {
        Some(format!("Apache Kafka {}", answers.kafka.version))
    };

    DimsterEnvSpec {
        env_name: answers.env_name.clone(),
        image,
        context,
        hardware,
        system,
        kafka_client_version: answers.kafka_client_version.clone(),
        deploy_mode: answers.deploy_mode.clone(),
        bootstrap_servers: answers.bootstrap_servers.clone(),
        cpu_manager: None,
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
            node_label: Some("role=infra".into()),
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

fn second_az_for(az: &str) -> String {
    let suffix = az.chars().last().unwrap_or('a');
    let other = if suffix == 'a' { 'b' } else { 'a' };
    format!("{}{}", &az[..az.len().saturating_sub(1)], other)
}

fn render_ebs_storage_class(answers: &Answers, cluster_name: &str) -> (&'static str, String) {
    let is_provisioned = answers.storage.performance.as_deref() == Some("provisioned");
    let tag_params = format!(
        "  tagSpecification_1: \"dimster-cluster={}\"\n  tagSpecification_2: \"dimster-name={}\"",
        cluster_name, answers.aws.dimster_name
    );

    let (sc_name, params) = if is_provisioned {
        let iops = answers.storage.iops.unwrap_or(16000);
        let throughput = answers.storage.throughput_mbs.unwrap_or(1000);
        (
            "gp3-fast",
            format!(
                "  type: gp3\n  fsType: xfs\n  iops: \"{}\"\n  throughput: \"{}\"\n{}",
                iops, throughput, tag_params
            ),
        )
    } else {
        ("gp3", format!("  type: gp3\n  fsType: xfs\n{}", tag_params))
    };

    let manifest = format!(
        "apiVersion: storage.k8s.io/v1\n\
         kind: StorageClass\n\
         metadata:\n\
         \x20\x20name: {sc_name}\n\
         provisioner: ebs.csi.aws.com\n\
         parameters:\n\
         {params}\n\
         reclaimPolicy: Delete\n\
         volumeBindingMode: WaitForFirstConsumer\n\
         allowVolumeExpansion: true\n"
    );
    (sc_name, manifest)
}

fn render_nvme_daemonset() -> String {
    format!(
        r#"apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: nvme-format
  namespace: kube-system
spec:
  selector:
    matchLabels:
      app: nvme-format
  template:
    metadata:
      labels:
        app: nvme-format
    spec:
      nodeSelector:
        role: kafka
      containers:
        - name: format
          image: amazonlinux:2023
          securityContext:
            privileged: true
          command:
            - /bin/bash
            - -c
            - |
{script}
          volumeMounts:
            - name: dev
              mountPath: /dev
            - name: mnt
              mountPath: /mnt
              mountPropagation: Bidirectional
      volumes:
        - name: dev
          hostPath:
            path: /dev
        - name: mnt
          hostPath:
            path: /mnt
      tolerations:
        - operator: Exists
"#,
        script = nvme_setup_script()
    )
}

// ---------------------------------------------------------------------------
// Script generation
// ---------------------------------------------------------------------------

pub fn generate_script(answers: &Answers) -> Result<String> {
    let mut s = Script::new();
    let name = &answers.aws.dimster_name;
    let region = &answers.aws.region;
    let cluster_name = format!("{}-benchmark", name);
    let second_az = second_az_for(&answers.aws.az);

    s.cmd("#!/usr/bin/env bash");
    s.cmd("set -euo pipefail");
    s.blank();
    s.comment(&format!("EKS setup script for '{}'", name));
    s.comment("Generated by eks-setup. Review and edit before running.");

    s.section("Variables");
    s.cmd(&format!("DIMSTER_NAME=\"{}\"", name));
    s.cmd(&format!("AWS_REGION=\"{}\"", region));
    s.cmd(&format!("AWS_AZ=\"{}\"", answers.aws.az));
    s.cmd(&format!("SECOND_AZ=\"{}\"", second_az));
    s.cmd(&format!("CLUSTER_NAME=\"{}\"", cluster_name));
    s.cmd("AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)");

    // Step 1: ECR
    s.section("Step 1: Create ECR Repository");
    s.cmd("if ! aws ecr describe-repositories --repository-names \"$DIMSTER_NAME\" --region \"$AWS_REGION\" &>/dev/null; then");
    s.cmd("  aws ecr create-repository --repository-name \"$DIMSTER_NAME\" --region \"$AWS_REGION\"");
    s.cmd("  echo \"ECR repository created\"");
    s.cmd("else");
    s.cmd("  echo \"ECR repository already exists\"");
    s.cmd("fi");

    // Step 2: EKS cluster
    s.section("Step 2: Create EKS Cluster");
    let eksctl_config = render_eksctl_config(answers, "${CLUSTER_NAME}", "${AWS_REGION}", "${AWS_AZ}", "${SECOND_AZ}");
    let eksctl_path = format!("/tmp/eks-cluster-{}.yaml", name);

    s.cmd("if ! eksctl get cluster --name \"$CLUSTER_NAME\" --region \"$AWS_REGION\" &>/dev/null; then");
    s.cmd(&format!("  cat > {} <<EKSCTL_EOF", eksctl_path));
    for line in eksctl_config.lines() {
        s.cmd(line);
    }
    s.cmd("EKSCTL_EOF");
    s.blank();
    s.cmd("  echo \"Creating EKS cluster (this takes 15-20 minutes)...\"");
    s.cmd(&format!("  eksctl create cluster -f {}", eksctl_path));
    s.cmd("  echo \"EKS cluster created\"");
    s.cmd("else");
    s.cmd("  echo \"EKS cluster already exists\"");
    s.cmd("fi");
    s.blank();
    s.cmd("kubectl get nodes -o custom-columns=NAME:.metadata.name,ZONE:.metadata.labels.topology\\\\.kubernetes\\\\.io/zone,ROLE:.metadata.labels.role");

    // Step 3: EBS CSI driver
    s.section("Step 3: Install EBS CSI Driver");
    s.cmd("if ! kubectl get pods -n kube-system -l app.kubernetes.io/name=aws-ebs-csi-driver --no-headers 2>/dev/null | grep -q .; then");
    s.cmd("  eksctl utils associate-iam-oidc-provider --region \"$AWS_REGION\" --cluster \"$CLUSTER_NAME\" --approve");
    s.cmd("  eksctl create iamserviceaccount \\");
    s.cmd("    --cluster \"$CLUSTER_NAME\" \\");
    s.cmd("    --region \"$AWS_REGION\" \\");
    s.cmd("    --name ebs-csi-controller-sa \\");
    s.cmd("    --namespace kube-system \\");
    s.cmd("    --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \\");
    s.cmd("    --approve \\");
    s.cmd("    --override-existing-serviceaccounts");
    s.cmd("  EBS_CSI_ROLE_ARN=$(kubectl get sa ebs-csi-controller-sa -n kube-system \\");
    s.cmd("    -o jsonpath='{.metadata.annotations.eks\\.amazonaws\\.com/role-arn}')");
    s.cmd("  eksctl create addon \\");
    s.cmd("    --cluster \"$CLUSTER_NAME\" \\");
    s.cmd("    --region \"$AWS_REGION\" \\");
    s.cmd("    --name aws-ebs-csi-driver \\");
    s.cmd("    --service-account-role-arn \"$EBS_CSI_ROLE_ARN\" \\");
    s.cmd("    --force");
    s.cmd("  echo \"EBS CSI driver installed\"");
    s.cmd("else");
    s.cmd("  echo \"EBS CSI driver already installed\"");
    s.cmd("fi");
    s.blank();
    s.cmd("if ! kubectl get sc -o jsonpath='{.items[?(@.metadata.annotations.storageclass\\.kubernetes\\.io/is-default-class==\"true\")].metadata.name}' | grep -q .; then");
    s.cmd("  kubectl annotate storageclass gp2 storageclass.kubernetes.io/is-default-class=true --overwrite");
    s.cmd("fi");

    // Step 4: StorageClass
    s.section("Step 4: Create StorageClass");
    if answers.is_external() {
        s.comment("Skipped — external mode (no Kafka storage needed)");
    } else if answers.storage.storage_type == "instance-store" {
        let sc = "apiVersion: storage.k8s.io/v1\nkind: StorageClass\nmetadata:\n  name: local-storage\nprovisioner: kubernetes.io/no-provisioner\nvolumeBindingMode: WaitForFirstConsumer\n";
        s.heredoc("kubectl apply -f -", sc);
        s.blank();
        s.heredoc("kubectl apply -f -", &render_nvme_daemonset());
        s.blank();
        s.cmd("kubectl rollout status daemonset/nvme-format -n kube-system --timeout=120s || true");
        s.blank();
        s.cmd("KAFKA_NODES=$(kubectl get nodes -l role=kafka -o jsonpath='{.items[*].metadata.name}')");
        s.cmd("INDEX=0");
        s.cmd("for NODE in $KAFKA_NODES; do");
        s.cmd(&format!(
            "  kubectl apply -f - <<PV_EOF\n\
             apiVersion: v1\n\
             kind: PersistentVolume\n\
             metadata:\n\
             \x20\x20name: kafka-nvme-$INDEX\n\
             spec:\n\
             \x20\x20capacity:\n\
             \x20\x20\x20\x20storage: {size}\n\
             \x20\x20accessModes: [\"ReadWriteOnce\"]\n\
             \x20\x20persistentVolumeReclaimPolicy: Retain\n\
             \x20\x20storageClassName: local-storage\n\
             \x20\x20local:\n\
             \x20\x20\x20\x20path: /mnt/nvme0\n\
             \x20\x20nodeAffinity:\n\
             \x20\x20\x20\x20required:\n\
             \x20\x20\x20\x20\x20\x20nodeSelectorTerms:\n\
             \x20\x20\x20\x20\x20\x20\x20\x20- matchExpressions:\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20- key: kubernetes.io/hostname\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20operator: In\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20values: [\"$NODE\"]\n\
             PV_EOF",
            size = answers.storage.size
        ));
        s.cmd("  INDEX=$((INDEX + 1))");
        s.cmd("done");
    } else {
        let (_, sc) = render_ebs_storage_class(answers, &cluster_name);
        s.heredoc("kubectl apply -f -", &sc);
    }

    // Step 5: Generate config file (unquoted heredoc so AWS_ACCOUNT_ID and context expand)
    s.section("Step 5: Generate Configuration Files");
    let spec = build_dimster_env_spec(
        answers,
        "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${DIMSTER_NAME}".into(),
        "$(kubectl config current-context)".into(),
    );
    let config_yaml = spec.render_standalone_config("eks-setup", name)?;
    let config_path = format!("dimster-config-{}.yaml", name);
    s.heredoc_with_tag(
        &format!("cat > \"{}\"", config_path),
        "CONFIG_EOF",
        false,
        &config_yaml,
    );
    s.cmd(&format!("echo \"Wrote {}\"", config_path));
    s.blank();
    s.cmd("echo \"Done! Next steps:\"");
    s.cmd(&format!("echo \"  1. Review {}\"", config_path));
    s.cmd(&format!(
        "echo \"  2. Copy: cp {} dimster-config.yaml\"",
        config_path
    ));
    s.cmd("echo \"  3. Log in to ECR (token expires every 12 hours):\"");
    s.cmd("echo \"     aws ecr get-login-password --region $AWS_REGION | \\\\\"");
    s.cmd("echo \"       docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com\"");
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

fn render_eksctl_config(
    answers: &Answers,
    cluster_name: &str,
    region: &str,
    az: &str,
    second_az: &str,
) -> String {
    let kafka_node_group = if answers.is_external() {
        String::new()
    } else {
        format!(
            "  - name: kafka\n\
             \x20\x20\x20\x20instanceType: {kafka_type}\n\
             \x20\x20\x20\x20desiredCapacity: {kafka_count}\n\
             \x20\x20\x20\x20minSize: {kafka_count}\n\
             \x20\x20\x20\x20maxSize: {kafka_count}\n\
             \x20\x20\x20\x20availabilityZones: [\"{az}\"]\n\
             \x20\x20\x20\x20labels:\n\
             \x20\x20\x20\x20\x20\x20role: kafka\n\n",
            kafka_type = answers.kafka.instance_type,
            kafka_count = answers.kafka.count,
            az = az,
        )
    };

    format!(
        "apiVersion: eksctl.io/v1alpha5\n\
         kind: ClusterConfig\n\n\
         metadata:\n\
         \x20\x20name: {cluster_name}\n\
         \x20\x20region: {region}\n\
         \x20\x20version: \"{eks_version}\"\n\n\
         availabilityZones: [\"{az}\", \"{second_az}\"]\n\n\
         managedNodeGroups:\n\
         {kafka_node_group}\
         \x20\x20- name: workers\n\
         \x20\x20\x20\x20instanceType: {worker_type}\n\
         \x20\x20\x20\x20desiredCapacity: {worker_count}\n\
         \x20\x20\x20\x20minSize: {worker_min}\n\
         \x20\x20\x20\x20maxSize: {worker_max}\n\
         \x20\x20\x20\x20availabilityZones: [\"{az}\"]\n\
         \x20\x20\x20\x20labels:\n\
         \x20\x20\x20\x20\x20\x20role: worker\n\n\
         \x20\x20- name: infra\n\
         \x20\x20\x20\x20instanceType: {infra_type}\n\
         \x20\x20\x20\x20desiredCapacity: 1\n\
         \x20\x20\x20\x20minSize: 1\n\
         \x20\x20\x20\x20maxSize: 1\n\
         \x20\x20\x20\x20availabilityZones: [\"{az}\"]\n\
         \x20\x20\x20\x20labels:\n\
         \x20\x20\x20\x20\x20\x20role: infra\n",
        cluster_name = cluster_name,
        region = region,
        eks_version = answers.aws.eks_version,
        az = az,
        second_az = second_az,
        kafka_node_group = kafka_node_group,
        worker_type = answers.workers.instance_type,
        worker_count = answers.workers.count,
        worker_min = answers.workers.min,
        worker_max = answers.workers.max,
        infra_type = answers.coordinator.infra_instance_type,
    )
}

// ---------------------------------------------------------------------------
// Interactive execution
// ---------------------------------------------------------------------------

fn step_1_ecr(answers: &Answers, opts: RunOptions) -> Result<()> {
    println!("\n=== Step 1/5: Create ECR Repository ===\n");

    if opts.mode == Mode::DryRun {
        println!(
            "  [dry-run] aws ecr create-repository --repository-name {} --region {}",
            answers.aws.dimster_name, answers.aws.region
        );
        return Ok(());
    }

    let check = Command::new("aws")
        .args([
            "ecr",
            "describe-repositories",
            "--repository-names",
            &answers.aws.dimster_name,
            "--region",
            &answers.aws.region,
        ])
        .output()?;

    if check.status.success() {
        println!("  ✓ ECR repository already exists");
        return Ok(());
    }

    confirm_and_run(
        opts,
        "aws",
        &[
            "ecr",
            "create-repository",
            "--repository-name",
            &answers.aws.dimster_name,
            "--region",
            &answers.aws.region,
        ],
    )?;
    println!("  ✓ ECR repository created");
    Ok(())
}

fn step_2_eks(answers: &Answers, opts: RunOptions) -> Result<()> {
    println!("\n=== Step 2/5: Create EKS Cluster ===\n");

    let cluster_name = format!("{}-benchmark", answers.aws.dimster_name);
    let second_az = second_az_for(&answers.aws.az);
    let eksctl_config = render_eksctl_config(
        answers,
        &cluster_name,
        &answers.aws.region,
        &answers.aws.az,
        &second_az,
    );
    let config_path = format!("/tmp/eks-cluster-{}.yaml", answers.aws.dimster_name);

    if opts.mode == Mode::DryRun {
        println!("  [dry-run] Would write eksctl config to {}", config_path);
        println!("  [dry-run] eksctl create cluster -f {}", config_path);
        println!("\n  eksctl config:\n");
        for line in eksctl_config.lines() {
            println!("    {}", line);
        }
        return Ok(());
    }

    let check = Command::new("eksctl")
        .args([
            "get",
            "cluster",
            "--name",
            &cluster_name,
            "--region",
            &answers.aws.region,
        ])
        .output()?;

    if check.status.success() {
        println!("  ✓ EKS cluster '{}' already exists", cluster_name);
        println!("  Updating kubeconfig...");
        let update = Command::new("aws")
            .args([
                "eks",
                "update-kubeconfig",
                "--name",
                &cluster_name,
                "--region",
                &answers.aws.region,
            ])
            .output()?;
        if !update.status.success() {
            let stderr = String::from_utf8_lossy(&update.stderr);
            bail!("Failed to update kubeconfig: {}", stderr);
        }
        println!("  ✓ Kubeconfig updated");

        ensure_node_groups(answers, &cluster_name, &config_path, &eksctl_config, opts)?;
        verify_nodes(None)?;
        return Ok(());
    }

    std::fs::write(&config_path, &eksctl_config)?;
    println!("  Wrote eksctl config to {}", config_path);

    confirm_and_run_inherit(opts, "eksctl", &["create", "cluster", "-f", &config_path])?;
    println!("  ✓ EKS cluster created");
    verify_nodes(None)?;
    Ok(())
}

fn ensure_node_groups(
    answers: &Answers,
    cluster_name: &str,
    config_path: &str,
    eksctl_config: &str,
    opts: RunOptions,
) -> Result<()> {
    let output = Command::new("eksctl")
        .args([
            "get",
            "nodegroup",
            "--cluster",
            cluster_name,
            "--region",
            &answers.aws.region,
            "-o",
            "json",
        ])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("Failed to list node groups: {}", stderr);
    }

    let json_str = String::from_utf8_lossy(&output.stdout);
    let nodegroups: serde_json::Value =
        serde_json::from_str(&json_str).unwrap_or_else(|_| serde_json::Value::Array(vec![]));

    let existing: Vec<String> = nodegroups
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|ng| ng.get("Name").and_then(|n| n.as_str()).map(str::to_string))
        .collect();

    let mut expected = vec!["workers", "infra"];
    if !answers.is_external() {
        expected.push("kafka");
    }

    let missing: Vec<&&str> = expected
        .iter()
        .filter(|name| !existing.contains(&name.to_string()))
        .collect();

    if missing.is_empty() {
        println!("  ✓ All required node groups present: {}", existing.join(", "));
        return Ok(());
    }

    println!("  Existing node groups: {}", existing.join(", "));
    println!(
        "  Missing node groups: {}",
        missing.iter().map(|s| **s).collect::<Vec<_>>().join(", ")
    );

    std::fs::write(config_path, eksctl_config)?;
    for ng in &missing {
        println!("\n  Creating node group '{}'...", ng);
        confirm_and_run_inherit(
            opts,
            "eksctl",
            &["create", "nodegroup", "--config-file", config_path, "--include", ng],
        )?;
        println!("  ✓ Node group '{}' created", ng);
    }

    Ok(())
}

fn step_3_ebs_csi(answers: &Answers, opts: RunOptions) -> Result<()> {
    println!("\n=== Step 3/5: Install EBS CSI Driver ===\n");

    let cluster_name = format!("{}-benchmark", answers.aws.dimster_name);
    let region = &answers.aws.region;

    if opts.mode == Mode::DryRun {
        println!(
            "  [dry-run] eksctl utils associate-iam-oidc-provider --cluster {} --region {} --approve",
            cluster_name, region
        );
        println!(
            "  [dry-run] eksctl create iamserviceaccount --cluster {} --name ebs-csi-controller-sa --namespace kube-system ...",
            cluster_name
        );
        println!(
            "  [dry-run] eksctl create addon --cluster {} --name aws-ebs-csi-driver ...",
            cluster_name
        );
        return Ok(());
    }

    let check = Command::new("kubectl")
        .args([
            "get",
            "pods",
            "-n",
            "kube-system",
            "-l",
            "app.kubernetes.io/name=aws-ebs-csi-driver",
            "--no-headers",
        ])
        .output()?;

    if check.status.success() && !String::from_utf8_lossy(&check.stdout).trim().is_empty() {
        println!("  ✓ EBS CSI driver already installed");
        mark_default_storage_class()?;
        return Ok(());
    }

    confirm_and_run_inherit(
        opts,
        "eksctl",
        &[
            "utils",
            "associate-iam-oidc-provider",
            "--region",
            region,
            "--cluster",
            &cluster_name,
            "--approve",
        ],
    )?;

    confirm_and_run_inherit(
        opts,
        "eksctl",
        &[
            "create",
            "iamserviceaccount",
            "--cluster",
            &cluster_name,
            "--region",
            region,
            "--name",
            "ebs-csi-controller-sa",
            "--namespace",
            "kube-system",
            "--attach-policy-arn",
            "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy",
            "--approve",
            "--override-existing-serviceaccounts",
        ],
    )?;

    let arn_output = Command::new("kubectl")
        .args([
            "get",
            "sa",
            "ebs-csi-controller-sa",
            "-n",
            "kube-system",
            "-o",
            "jsonpath={.metadata.annotations.eks\\.amazonaws\\.com/role-arn}",
        ])
        .output()?;

    let role_arn = String::from_utf8_lossy(&arn_output.stdout).trim().to_string();
    if role_arn.is_empty() {
        bail!("Could not get EBS CSI role ARN");
    }
    println!("  Role ARN: {}", role_arn);

    confirm_and_run_inherit(
        opts,
        "eksctl",
        &[
            "create",
            "addon",
            "--cluster",
            &cluster_name,
            "--region",
            region,
            "--name",
            "aws-ebs-csi-driver",
            "--service-account-role-arn",
            &role_arn,
            "--force",
        ],
    )?;

    println!("  ✓ EBS CSI driver installed");
    mark_default_storage_class()?;
    Ok(())
}

fn mark_default_storage_class() -> Result<()> {
    let output = Command::new("kubectl")
        .args([
            "get",
            "sc",
            "-o",
            "jsonpath={.items[?(@.metadata.annotations.storageclass\\.kubernetes\\.io/is-default-class==\"true\")].metadata.name}",
        ])
        .output()?;

    let defaults = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !defaults.is_empty() {
        println!("  ✓ Default StorageClass already set: {}", defaults);
        return Ok(());
    }

    let status = Command::new("kubectl")
        .args([
            "annotate",
            "storageclass",
            "gp2",
            "storageclass.kubernetes.io/is-default-class=true",
            "--overwrite",
        ])
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()?;

    if status.success() {
        println!("  ✓ Marked gp2 as default StorageClass");
    } else {
        println!("  ⚠ Could not mark gp2 as default StorageClass — coordinator PVC may not bind");
    }
    Ok(())
}

fn step_4_storage(answers: &Answers, opts: RunOptions) -> Result<()> {
    println!("\n=== Step 4/5: Create StorageClass ===\n");

    if answers.storage.storage_type == "instance-store" {
        setup_instance_store(answers, opts)
    } else {
        let cluster_name = format!("{}-benchmark", answers.aws.dimster_name);
        let (sc_name, manifest) = render_ebs_storage_class(answers, &cluster_name);

        if opts.mode == Mode::DryRun {
            println!("  [dry-run] kubectl apply StorageClass '{}':", sc_name);
            for line in manifest.lines() {
                println!("    {}", line);
            }
            return Ok(());
        }

        confirm_and_apply(opts, None, "Create StorageClass", &manifest)?;
        println!("  ✓ StorageClass '{}' created", sc_name);
        Ok(())
    }
}

fn setup_instance_store(answers: &Answers, opts: RunOptions) -> Result<()> {
    let sc_manifest = "apiVersion: storage.k8s.io/v1\nkind: StorageClass\nmetadata:\n  name: local-storage\nprovisioner: kubernetes.io/no-provisioner\nvolumeBindingMode: WaitForFirstConsumer\n";
    let daemonset_manifest = render_nvme_daemonset();

    if opts.mode == Mode::DryRun {
        println!("  [dry-run] kubectl apply StorageClass 'local-storage'");
        println!("  [dry-run] kubectl apply DaemonSet 'nvme-format' (formats NVMe on kafka nodes)");
        println!("  [dry-run] kubectl get nodes -l role=kafka → create PVs");
        println!("  PV capacity: {}", answers.storage.size);
        return Ok(());
    }

    confirm_and_apply(opts, None, "Create local-storage StorageClass", sc_manifest)?;
    confirm_and_apply(opts, None, "Deploy NVMe format DaemonSet", &daemonset_manifest)?;

    println!("  Waiting for NVMe format to complete on all kafka nodes...");
    let _ = Command::new("kubectl")
        .args([
            "rollout",
            "status",
            "daemonset/nvme-format",
            "-n",
            "kube-system",
            "--timeout=120s",
        ])
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status();

    let output = Command::new("kubectl")
        .args([
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
        let pv_manifest = format!(
            "apiVersion: v1\n\
             kind: PersistentVolume\n\
             metadata:\n\
             \x20\x20name: kafka-nvme-{i}\n\
             spec:\n\
             \x20\x20capacity:\n\
             \x20\x20\x20\x20storage: {size}\n\
             \x20\x20accessModes: [\"ReadWriteOnce\"]\n\
             \x20\x20persistentVolumeReclaimPolicy: Retain\n\
             \x20\x20storageClassName: local-storage\n\
             \x20\x20local:\n\
             \x20\x20\x20\x20path: /mnt/nvme0\n\
             \x20\x20nodeAffinity:\n\
             \x20\x20\x20\x20required:\n\
             \x20\x20\x20\x20\x20\x20nodeSelectorTerms:\n\
             \x20\x20\x20\x20\x20\x20\x20\x20- matchExpressions:\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20- key: kubernetes.io/hostname\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20operator: In\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20values: [\"{node}\"]\n",
            i = i,
            size = answers.storage.size,
            node = node,
        );
        confirm_and_apply(
            opts,
            None,
            &format!("Create PV kafka-nvme-{} on {}", i, node),
            &pv_manifest,
        )?;
    }

    println!("  ✓ Local storage configured ({} PVs)", nodes.len());
    println!("  ℹ NVMe format DaemonSet left running to maintain mount propagation");
    Ok(())
}

fn step_5_generate(answers: &Answers, opts: RunOptions) -> Result<bool> {
    println!("\n=== Step 5/5: Generate Configuration ===\n");

    let name = &answers.aws.dimster_name;

    let account_id = if opts.mode == Mode::DryRun {
        "<ACCOUNT_ID>".to_string()
    } else {
        aws_account_id().unwrap_or_else(|| "<ACCOUNT_ID>".to_string())
    };

    let context = if opts.mode == Mode::DryRun {
        "<KUBECTL_CONTEXT>".to_string()
    } else {
        let output = Command::new("kubectl")
            .args(["config", "current-context"])
            .output()?;
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    };

    let image = format!(
        "{}.dkr.ecr.{}.amazonaws.com/{}",
        account_id, answers.aws.region, name
    );
    let spec = build_dimster_env_spec(answers, image, context);
    let answers_path = format!("eks-answers-{}.yaml", name);

    let inserted = save_or_insert_config(&spec, "eks-setup", name, &answers_path, opts)?;

    if opts.mode != Mode::DryRun {
        answers.save(&answers_path)?;
        println!("  ✓ Wrote {}", answers_path);
    }

    Ok(inserted)
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

pub fn delete_all(
    dimster_name: &str,
    region: &str,
    keep_ecr: bool,
    opts: RunOptions,
) -> Result<()> {
    if opts.mode == Mode::GenerateScript {
        let script = generate_delete_script(dimster_name, region, keep_ecr);
        print!("{}", script);
        return Ok(());
    }

    let cluster_name = format!("{}-benchmark", dimster_name);

    // Pin kubectl delete pvc to the cluster's matching context so we never
    // accidentally wipe PVCs in a different active context.
    let eks_context = find_eks_kube_context(&cluster_name, region);

    println!("\n=== Step 1/4: Delete PVCs (graceful) ===\n");
    if let Some(ref ctx) = eks_context {
        let _ = delete_cluster_pvcs(
            opts,
            Some(ctx),
            &format!("EKS cluster '{}' in {}", cluster_name, region),
        )?;
    } else if opts.mode == Mode::DryRun {
        let _ = delete_cluster_pvcs(
            opts,
            None,
            &format!("EKS cluster '{}' in {}", cluster_name, region),
        )?;
    } else {
        println!(
            "  ✓ No kubeconfig context matching '{}' found — skipping graceful PVC delete",
            cluster_name
        );
        println!("    (orphaned volumes will be swept in Step 3)");
    }

    println!("\n=== Step 2/4: Delete EKS Cluster ===\n");
    if opts.mode == Mode::DryRun {
        println!(
            "  [dry-run] eksctl delete cluster --name {} --region {}",
            cluster_name, region
        );
    } else {
        let check = Command::new("eksctl")
            .args(["get", "cluster", "--name", &cluster_name, "--region", region])
            .output()?;
        if !check.status.success() {
            println!(
                "  ✓ EKS cluster '{}' does not exist (already deleted)",
                cluster_name
            );
        } else {
            confirm_and_run_inherit(
                opts,
                "eksctl",
                &["delete", "cluster", "--name", &cluster_name, "--region", region],
            )?;
            println!("  ✓ EKS cluster '{}' deleted", cluster_name);
        }
    }

    println!("\n=== Step 3/4: Sweep Orphaned EBS Volumes ===\n");
    sweep_orphaned_volumes(&cluster_name, region, opts)?;

    println!("\n=== Step 4/4: Delete ECR Repository ===\n");
    if keep_ecr {
        println!("  Skipped (--keep-ecr)");
    } else if opts.mode == Mode::DryRun {
        println!(
            "  [dry-run] aws ecr delete-repository --repository-name {} --region {} --force",
            dimster_name, region
        );
    } else {
        let check = Command::new("aws")
            .args([
                "ecr",
                "describe-repositories",
                "--repository-names",
                dimster_name,
                "--region",
                region,
            ])
            .output()?;
        if !check.status.success() {
            println!(
                "  ✓ ECR repository '{}' does not exist (already deleted)",
                dimster_name
            );
        } else {
            confirm_and_run(
                opts,
                "aws",
                &[
                    "ecr",
                    "delete-repository",
                    "--repository-name",
                    dimster_name,
                    "--region",
                    region,
                    "--force",
                ],
            )?;
            println!("  ✓ ECR repository '{}' deleted", dimster_name);
        }
    }

    println!("\n=== Done! ===\n");
    println!(
        "  Note: Generated config files (dimster-config-{}.yaml, eks-answers-{}.yaml)",
        dimster_name, dimster_name
    );
    println!("  were left on disk. Delete them manually if no longer needed.");

    Ok(())
}

/// Find a local kubeconfig context referencing this EKS cluster + region.
fn find_eks_kube_context(cluster_name: &str, region: &str) -> Option<String> {
    let out = Command::new("kubectl")
        .args(["config", "get-contexts", "-o", "name"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let suffix = format!(":cluster/{}", cluster_name);
    let eksctl_pattern = format!("@{}.{}.eksctl.io", cluster_name, region);
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .find(|ctx| {
            (ctx.contains(region) && ctx.ends_with(&suffix)) || ctx.ends_with(&eksctl_pattern)
        })
        .map(str::to_string)
}

fn sweep_orphaned_volumes(cluster_name: &str, region: &str, opts: RunOptions) -> Result<()> {
    if opts.mode == Mode::DryRun {
        println!("  [dry-run] aws ec2 describe-volumes --region {} \\", region);
        println!(
            "              --filters Name=tag:dimster-cluster,Values={} \\",
            cluster_name
        );
        println!("                        Name=status,Values=available");
        println!("  [dry-run] for each volume: aws ec2 delete-volume --volume-id <id>");
        return Ok(());
    }

    let output = Command::new("aws")
        .args([
            "ec2",
            "describe-volumes",
            "--region",
            region,
            "--filters",
            &format!("Name=tag:dimster-cluster,Values={}", cluster_name),
            "Name=status,Values=available",
            "--query",
            "Volumes[].VolumeId",
            "--output",
            "text",
        ])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("aws ec2 describe-volumes failed: {}", stderr);
    }

    let raw = String::from_utf8_lossy(&output.stdout);
    let volume_ids: Vec<&str> = raw.split_whitespace().collect();

    if volume_ids.is_empty() {
        println!(
            "  ✓ No orphaned EBS volumes found for cluster '{}'",
            cluster_name
        );
        return Ok(());
    }

    println!(
        "  Found {} orphaned EBS volume(s) tagged dimster-cluster={}:",
        volume_ids.len(),
        cluster_name
    );
    for id in &volume_ids {
        println!("    - {}", id);
    }

    for id in &volume_ids {
        confirm_and_run(
            opts,
            "aws",
            &["ec2", "delete-volume", "--region", region, "--volume-id", id],
        )?;
        println!("  ✓ Deleted volume {}", id);
    }
    Ok(())
}

pub fn generate_delete_script(dimster_name: &str, region: &str, keep_ecr: bool) -> String {
    let mut s = Script::new();
    let cluster_name = format!("{}-benchmark", dimster_name);

    s.cmd("#!/usr/bin/env bash");
    s.cmd("set -euo pipefail");
    s.blank();
    s.comment(&format!("EKS teardown script for '{}'", dimster_name));
    s.comment("Generated by eks-setup. Review before running.");

    s.section("Variables");
    s.cmd(&format!("DIMSTER_NAME=\"{}\"", dimster_name));
    s.cmd(&format!("AWS_REGION=\"{}\"", region));
    s.cmd(&format!("CLUSTER_NAME=\"{}\"", cluster_name));

    s.section("Step 1: Delete PVCs (graceful)");
    s.cmd("EKS_CONTEXT=$(kubectl config get-contexts -o name | grep -E \"${AWS_REGION}.*:cluster/${CLUSTER_NAME}$|@${CLUSTER_NAME}\\.${AWS_REGION}\\.eksctl\\.io$\" | head -n1 || true)");
    s.cmd("if [ -n \"$EKS_CONTEXT\" ] && kubectl --context \"$EKS_CONTEXT\" get ns --request-timeout=10s &>/dev/null; then");
    emit_pvc_cleanup_script(&mut s, "$EKS_CONTEXT", "EBS CSI driver", "volumes");
    s.cmd("else");
    s.cmd("  echo \"kubectl cannot reach cluster $CLUSTER_NAME — skipping graceful PVC delete\"");
    s.cmd("fi");

    s.section("Step 2: Delete EKS Cluster");
    s.cmd("if eksctl get cluster --name \"$CLUSTER_NAME\" --region \"$AWS_REGION\" &>/dev/null; then");
    s.cmd("  echo \"Deleting EKS cluster $CLUSTER_NAME (this takes 10-15 minutes)...\"");
    s.cmd("  eksctl delete cluster --name \"$CLUSTER_NAME\" --region \"$AWS_REGION\"");
    s.cmd("  echo \"EKS cluster deleted\"");
    s.cmd("else");
    s.cmd("  echo \"EKS cluster $CLUSTER_NAME does not exist\"");
    s.cmd("fi");

    s.section("Step 3: Sweep Orphaned EBS Volumes");
    s.cmd("ORPHANED_VOLUMES=$(aws ec2 describe-volumes --region \"$AWS_REGION\" \\");
    s.cmd("  --filters \"Name=tag:dimster-cluster,Values=$CLUSTER_NAME\" \\");
    s.cmd("            \"Name=status,Values=available\" \\");
    s.cmd("  --query 'Volumes[].VolumeId' --output text)");
    s.cmd("if [ -z \"$ORPHANED_VOLUMES\" ]; then");
    s.cmd("  echo \"No orphaned EBS volumes found for $CLUSTER_NAME\"");
    s.cmd("else");
    s.cmd("  for VOL in $ORPHANED_VOLUMES; do");
    s.cmd("    aws ec2 delete-volume --region \"$AWS_REGION\" --volume-id \"$VOL\"");
    s.cmd("    echo \"Deleted $VOL\"");
    s.cmd("  done");
    s.cmd("fi");

    if keep_ecr {
        s.section("Step 4: Delete ECR Repository — skipped (--keep-ecr)");
    } else {
        s.section("Step 4: Delete ECR Repository");
        s.cmd("if aws ecr describe-repositories --repository-names \"$DIMSTER_NAME\" --region \"$AWS_REGION\" &>/dev/null; then");
        s.cmd("  aws ecr delete-repository --repository-name \"$DIMSTER_NAME\" --region \"$AWS_REGION\" --force");
        s.cmd("  echo \"ECR repository deleted\"");
        s.cmd("else");
        s.cmd("  echo \"ECR repository $DIMSTER_NAME does not exist\"");
        s.cmd("fi");
    }

    s.blank();
    s.cmd("echo \"Done! Cluster and resources have been deleted.\"");
    s.cmd(&format!(
        "echo \"Note: Config files (dimster-config-{}.yaml, eks-answers-{}.yaml) left on disk.\"",
        dimster_name, dimster_name
    ));
    s.render()
}

fn nvme_setup_script() -> &'static str {
    "              MOUNT=/mnt/nvme0
              if mountpoint -q \"$MOUNT\"; then
                echo \"$MOUNT already mounted\"
                sleep infinity
              fi
              yum install -y xfsprogs mdadm > /dev/null 2>&1
              # Find all NVMe instance-store devices (exclude root EBS nvme0n1)
              DEVICES=()
              for dev in /dev/nvme*n1; do
                [ -b \"$dev\" ] || continue
                [ \"$dev\" = \"/dev/nvme0n1\" ] && continue
                DEVICES+=(\"$dev\")
              done
              if [ ${#DEVICES[@]} -eq 0 ]; then
                echo \"No NVMe instance-store devices found\"
                sleep infinity
              fi
              echo \"Found ${#DEVICES[@]} NVMe device(s): ${DEVICES[*]}\"
              if [ ${#DEVICES[@]} -eq 1 ]; then
                DEVICE=${DEVICES[0]}
                echo \"Single device, formatting $DEVICE as xfs...\"
                mkfs.xfs -f \"$DEVICE\"
                mkdir -p \"$MOUNT\"
                mount \"$DEVICE\" \"$MOUNT\"
              else
                echo \"Multiple devices, creating RAID 0...\"
                mdadm --create /dev/md0 --level=0 --raid-devices=${#DEVICES[@]} ${DEVICES[*]} --force
                echo \"Formatting /dev/md0 as xfs...\"
                mkfs.xfs -f /dev/md0
                mkdir -p \"$MOUNT\"
                mount /dev/md0 \"$MOUNT\"
              fi
              chmod 777 \"$MOUNT\"
              echo \"$MOUNT mounted successfully\"
              sleep infinity"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::answers::*;

    fn sample_answers() -> Answers {
        Answers {
            env_name: "aws".into(),
            aws: AwsAnswers {
                dimster_name: "demo".into(),
                region: "us-west-2".into(),
                az: "us-west-2b".into(),
                eks_version: "1.32".into(),
            },
            deploy_mode: "full".into(),
            bootstrap_servers: None,
            kafka_client_version: None,
            kafka: KafkaAnswers {
                version: "4.2.0".into(),
                instance_type: "m6i.2xlarge".into(),
                count: 3,
                heap: "-Xms8G -Xmx8G".into(),
                cpu: "6".into(),
                memory: "26Gi".into(),
                broker_config: Some("run/broker-configs/default.yaml".into()),
                extra_config: Some("num.network.threads=8".into()),
            },
            storage: StorageAnswers {
                storage_type: "ebs".into(),
                size: "500Gi".into(),
                performance: Some("provisioned".into()),
                iops: Some(16000),
                throughput_mbs: Some(1000),
            },
            workers: WorkerAnswers {
                instance_type: "m6i.xlarge".into(),
                count: 2,
                min: 1,
                max: 4,
                heap: "-Xms2G -Xmx8G".into(),
                cpu: "3".into(),
                memory: "12Gi".into(),
            },
            coordinator: CoordinatorAnswers {
                infra_instance_type: "t3.medium".into(),
                heap: "-Xms512M -Xmx512M".into(),
                cpu: "500m".into(),
                memory: "1Gi".into(),
            },
            monitoring: MonitoringAnswers {
                prometheus_cpu: "500m".into(),
                prometheus_memory: "1Gi".into(),
                grafana_cpu: "500m".into(),
                grafana_memory: "512Mi".into(),
            },
        }
    }

    #[test]
    fn generate_script_includes_pvc_size_broker_config_and_expanding_heredoc() {
        let answers = sample_answers();
        let script = generate_script(&answers).unwrap();
        assert!(script.contains("pvcSize: 500Gi"));
        assert!(!script.contains("\n      storage: 500Gi"));
        assert!(script.contains("brokerConfig: run/broker-configs/default.yaml"));
        assert!(script.contains("extraConfig: num.network.threads=8"));
        assert!(script.contains("cgroupMetrics: true"));
        assert!(script.contains("cat > \"dimster-config-demo.yaml\" <<CONFIG_EOF"));
        assert!(script.contains("cat > /tmp/eks-cluster-demo.yaml <<EKSCTL_EOF"));
    }

    #[test]
    fn delete_script_pins_eks_context() {
        let script = generate_delete_script("demo", "us-west-2", false);
        assert!(script.contains("kubectl --context \"$EKS_CONTEXT\" delete statefulset,deployment,job --all -n \"$NS\""));
        assert!(script.contains("kubectl --context \"$EKS_CONTEXT\" delete pvc --all -n \"$NS\""));
        assert!(script.contains("aws ecr delete-repository"));
    }
}
