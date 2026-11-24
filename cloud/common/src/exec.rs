//! Stateless subprocess and `kubectl` helpers shared by cloud setup CLIs.
//!
//! Takes an explicit `RunOptions` value instead of mutating global state,
//! keeping execution predictable and easy to test.

use std::collections::BTreeSet;
use std::io::Write;
use std::process::Command;

use anyhow::{Result, bail};
use dialoguer::Confirm;

use crate::script::Script;

/// Execution mode for `create` and `delete` commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Execute,
    DryRun,
    GenerateScript,
}

/// Stateless runtime options passed into execution steps.
#[derive(Debug, Clone, Copy)]
pub struct RunOptions {
    pub mode: Mode,
    pub auto_accept: bool,
}

impl RunOptions {
    pub fn from_flags(dry_run: bool, generate_script: bool, yes: bool) -> Self {
        let mode = if generate_script {
            Mode::GenerateScript
        } else if dry_run {
            Mode::DryRun
        } else {
            Mode::Execute
        };
        Self {
            mode,
            auto_accept: yes,
        }
    }
}

/// Print the command, optionally ask for confirmation, and run it (capturing output).
pub fn confirm_and_run(opts: RunOptions, program: &str, args: &[&str]) -> Result<()> {
    println!("  → {} {}", program, args.join(" "));

    if !opts.auto_accept {
        let proceed = Confirm::new()
            .with_prompt("  Run this command?")
            .default(true)
            .interact()?;

        if !proceed {
            bail!("Aborted by user");
        }
    }

    let output = Command::new(program).args(args).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{} failed: {}", program, stderr.trim());
    }
    Ok(())
}

/// Print the command, optionally ask for confirmation, and run it inheriting stdio.
pub fn confirm_and_run_inherit(opts: RunOptions, program: &str, args: &[&str]) -> Result<()> {
    println!("  → {} {}", program, args.join(" "));

    if !opts.auto_accept {
        let proceed = Confirm::new()
            .with_prompt("  Run this command?")
            .default(true)
            .interact()?;

        if !proceed {
            bail!("Aborted by user");
        }
    }

    let status = Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()?;

    if !status.success() {
        bail!("{} {} failed", program, args.first().unwrap_or(&""));
    }
    Ok(())
}

/// Pipe a YAML manifest to `kubectl apply -f -` (optionally pinned to a context).
pub fn confirm_and_apply(
    opts: RunOptions,
    context: Option<&str>,
    description: &str,
    manifest: &str,
) -> Result<()> {
    println!("  → kubectl apply: {}", description);

    if !opts.auto_accept {
        let proceed = Confirm::new()
            .with_prompt("  Apply this manifest?")
            .default(true)
            .interact()?;

        if !proceed {
            bail!("Aborted by user");
        }
    }

    let mut cmd = Command::new("kubectl");
    if let Some(ctx) = context {
        cmd.args(["--context", ctx]);
    }
    let mut child = cmd
        .args(["apply", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(manifest.as_bytes())?;
    }

    let status = child.wait()?;
    if !status.success() {
        bail!("kubectl apply failed ({})", description);
    }
    Ok(())
}

/// Display cluster nodes along with their zone and Dimster `role` label.
pub fn verify_nodes(context: Option<&str>) -> Result<()> {
    println!("\n  Verifying nodes:");
    let mut cmd = Command::new("kubectl");
    if let Some(ctx) = context {
        cmd.args(["--context", ctx]);
    }
    let _ = cmd
        .args([
            "get",
            "nodes",
            "-o",
            "custom-columns=NAME:.metadata.name,ZONE:.metadata.labels.topology\\.kubernetes\\.io/zone,ROLE:.metadata.labels.role",
        ])
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status();
    Ok(())
}

/// Return true for Kubernetes/GKE control-plane namespaces whose workloads and
/// PVCs must never be touched during benchmark teardown.
fn is_system_namespace(ns: &str) -> bool {
    matches!(
        ns,
        "kube-system" | "kube-public" | "kube-node-lease" | "gmp-system" | "gmp-public"
    ) || ns.starts_with("gke-")
}

/// Extract sorted, deduplicated non-system namespaces from newline/whitespace
/// separated `kubectl get pvc --all-namespaces` output.
pub fn pvc_user_namespaces(raw: &str) -> Vec<String> {
    raw.split_whitespace()
        .filter(|ns| !ns.is_empty() && !is_system_namespace(ns))
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Append the bash body that stops PVC-holding workloads (so Kubernetes drops
/// `pvc-protection` finalizers) and deletes PVCs in each non-system namespace.
pub fn emit_pvc_cleanup_script(
    s: &mut Script,
    ctx_var: &str,
    csi_label: &str,
    resource_noun: &str,
) {
    s.cmd(&format!(
        "  PVC_NAMESPACES=$(kubectl --context \"{}\" get pvc --all-namespaces -o jsonpath='{{range .items[*]}}{{.metadata.namespace}}{{\"\\n\"}}{{end}}' | sort -u | grep -Ev '^(kube-system|kube-public|kube-node-lease|gmp-system|gmp-public|gke-.*)$' || true)",
        ctx_var
    ));
    s.cmd("  if [ -z \"$PVC_NAMESPACES\" ]; then");
    s.cmd("    echo \"No user PVCs found\"");
    s.cmd("  else");
    s.cmd("    for NS in $PVC_NAMESPACES; do");
    s.cmd("      echo \"Stopping workloads in namespace $NS so PVCs can detach...\"");
    s.cmd(&format!(
        "      kubectl --context \"{}\" delete statefulset,deployment,job --all -n \"$NS\" --ignore-not-found --cascade=foreground --wait=true --timeout=90s || true",
        ctx_var
    ));
    s.cmd(&format!(
        "      echo \"Deleting PVCs in namespace $NS so the {} can free {}...\"",
        csi_label, resource_noun
    ));
    s.cmd(&format!(
        "      kubectl --context \"{}\" delete pvc --all -n \"$NS\" --wait=true --timeout=120s || \\",
        ctx_var
    ));
    s.cmd(&format!(
        "        echo \"PVC delete in $NS did not complete cleanly — Step 3 will sweep remaining {}\"",
        resource_noun
    ));
    s.cmd("    done");
    s.cmd("  fi");
}

/// Best-effort deletion of user workloads and PVCs so the CSI driver frees
/// cloud disks before the cluster itself is destroyed.
///
/// Stopping StatefulSets/Deployments/Jobs in PVC-owning namespaces first is
/// required because Kubernetes `pvc-protection` finalizers block PVC deletion
/// until every pod mounting the PVC has terminated. Always targets an explicit
/// kubeconfig `context` when provided so teardown can never wipe another cluster.
pub fn delete_cluster_pvcs(
    opts: RunOptions,
    context: Option<&str>,
    cluster_desc: &str,
) -> Result<()> {
    let ctx_prefix = match context {
        Some(ctx) => format!("kubectl --context {}", ctx),
        None => "kubectl".to_string(),
    };

    if opts.mode == Mode::DryRun {
        println!(
            "  [dry-run] for each non-system namespace with PVCs in {}:",
            cluster_desc
        );
        println!(
            "  [dry-run]   {} delete statefulset,deployment,job --all -n <ns> --ignore-not-found --cascade=foreground --wait=true --timeout=90s",
            ctx_prefix
        );
        println!(
            "  [dry-run]   {} delete pvc --all -n <ns> --wait=true --timeout=120s",
            ctx_prefix
        );
        return Ok(());
    }

    let mut probe_cmd = Command::new("kubectl");
    if let Some(ctx) = context {
        probe_cmd.args(["--context", ctx]);
    }
    let probe = probe_cmd
        .args(["get", "ns", "--request-timeout=10s"])
        .output();

    match probe {
        Ok(out) if out.status.success() => {}
        _ => {
            println!("  ✓ kubectl cannot reach cluster — skipping graceful PVC delete");
            println!("    (orphaned disks will be swept in Step 3)");
            return Ok(());
        }
    }

    let mut list_cmd = Command::new("kubectl");
    if let Some(ctx) = context {
        list_cmd.args(["--context", ctx]);
    }
    let list_out = list_cmd
        .args([
            "get",
            "pvc",
            "--all-namespaces",
            "-o",
            "jsonpath={range .items[*]}{.metadata.namespace}{\"\\n\"}{end}",
        ])
        .output();

    let namespaces = match list_out {
        Ok(out) if out.status.success() => {
            pvc_user_namespaces(&String::from_utf8_lossy(&out.stdout))
        }
        _ => {
            println!(
                "  ⚠ Could not list PVCs — Step 3 will sweep remaining cloud disks"
            );
            return Ok(());
        }
    };

    if namespaces.is_empty() {
        println!("  ✓ No user PVCs found in cluster");
        return Ok(());
    }

    let mut all_clean = true;
    for ns in &namespaces {
        println!(
            "  → {} delete statefulset,deployment,job --all -n {} --ignore-not-found --cascade=foreground --wait=true --timeout=90s",
            ctx_prefix, ns
        );
        let mut stop_cmd = Command::new("kubectl");
        if let Some(ctx) = context {
            stop_cmd.args(["--context", ctx]);
        }
        let _ = stop_cmd
            .args([
                "delete",
                "statefulset,deployment,job",
                "--all",
                "-n",
                ns,
                "--ignore-not-found",
                "--cascade=foreground",
                "--wait=true",
                "--timeout=90s",
            ])
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status();

        println!(
            "  → {} delete pvc --all -n {} --wait=true --timeout=120s",
            ctx_prefix, ns
        );
        let mut del_cmd = Command::new("kubectl");
        if let Some(ctx) = context {
            del_cmd.args(["--context", ctx]);
        }
        let status = del_cmd
            .args([
                "delete",
                "pvc",
                "--all",
                "-n",
                ns,
                "--wait=true",
                "--timeout=120s",
            ])
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status();

        if !matches!(status, Ok(s) if s.success()) {
            all_clean = false;
        }
    }

    if all_clean {
        println!("  ✓ PVCs deleted; CSI driver will release cloud disks");
    } else {
        println!(
            "  ⚠ PVC delete did not complete cleanly — Step 3 will sweep remaining disks"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pvc_user_namespaces_dedupes_and_excludes_system() {
        let raw = "dimster\ndimster\nkube-system\ngmp-system\ngke-managed-cim\nbench-a\n";
        assert_eq!(pvc_user_namespaces(raw), vec!["bench-a", "dimster"]);
    }

    #[test]
    fn emit_pvc_cleanup_script_stops_workloads_before_pvcs() {
        let mut s = Script::new();
        emit_pvc_cleanup_script(&mut s, "$KUBE_CONTEXT", "PD CSI driver", "GCE disks");
        let rendered = s.render();
        let stop_idx = rendered
            .find("delete statefulset,deployment,job --all -n \"$NS\"")
            .expect("workload stop command");
        let pvc_idx = rendered
            .find("delete pvc --all -n \"$NS\"")
            .expect("pvc delete command");
        assert!(stop_idx < pvc_idx);
        assert!(rendered.contains("gke-.*"));
    }
}
