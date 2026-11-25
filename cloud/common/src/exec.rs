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

/// PV and backing cloud-disk names extracted from `kubectl get pv -o json` for
/// dynamically provisioned (`reclaimPolicy: Delete`) user PVs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserDeletePvs {
    pub pv_names: Vec<String>,
    pub volume_names: Vec<String>,
}

/// Parse `kubectl get pv -o json` and return all `reclaimPolicy: Delete` PVs
/// claimed by non-system namespaces, along with their backing CSI volume
/// basenames (e.g. `pvc-...` on GKE or `vol-...` on EKS).
pub fn parse_user_delete_pvs(json_output: &str) -> Result<UserDeletePvs> {
    if json_output.trim().is_empty() {
        return Ok(UserDeletePvs::default());
    }
    let val: serde_json::Value = serde_json::from_str(json_output)?;
    let mut out = UserDeletePvs::default();
    let Some(items) = val.get("items").and_then(|i| i.as_array()) else {
        return Ok(out);
    };
    for pv in items {
        let reclaim = pv
            .pointer("/spec/persistentVolumeReclaimPolicy")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if reclaim != "Delete" {
            continue;
        }
        let ns = pv
            .pointer("/spec/claimRef/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if ns.is_empty() || is_system_namespace(ns) {
            continue;
        }
        if let Some(name) = pv.pointer("/metadata/name").and_then(|v| v.as_str()) {
            out.pv_names.push(name.to_string());
        }
        if let Some(handle) = pv.pointer("/spec/csi/volumeHandle").and_then(|v| v.as_str()) {
            let basename = handle.rsplit('/').next().unwrap_or(handle);
            if !basename.is_empty() {
                out.volume_names.push(basename.to_string());
            }
        }
    }
    Ok(out)
}

/// Append the bash body that stops PVC-holding workloads (so Kubernetes drops
/// `pvc-protection` finalizers), deletes PVCs in each non-system namespace, and
/// waits for the backing `Delete`-policy PVs to disappear so CSI `DeleteVolume`
/// completes before the cluster control plane is destroyed.
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
    s.cmd(&format!(
        "    USER_DELETE_PVS=$(kubectl --context \"{}\" get pv -o jsonpath='{{range .items[*]}}{{.metadata.name}} {{.spec.persistentVolumeReclaimPolicy}} {{.spec.claimRef.namespace}}{{\"\\n\"}}{{end}}' | awk '$2 == \"Delete\" && $3 != \"\" && $3 !~ /^(kube-system|kube-public|kube-node-lease|gmp-system|gmp-public|gke-.*)$/ {{print \"pv/\"$1}}' || true)",
        ctx_var
    ));
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
    s.cmd("    if [ -n \"$USER_DELETE_PVS\" ]; then");
    s.cmd(&format!(
        "      echo \"Waiting for {} to finish deleting backing PVs...\"",
        csi_label
    ));
    s.cmd(&format!(
        "      kubectl --context \"{}\" wait --for=delete $USER_DELETE_PVS --timeout=120s || true",
        ctx_var
    ));
    s.cmd("    fi");
    s.cmd("  fi");
}

/// Best-effort deletion of user workloads and PVCs so the CSI driver frees
/// cloud disks before the cluster itself is destroyed.
///
/// Stopping StatefulSets/Deployments/Jobs first drops `pvc-protection`
/// finalizers; waiting for the backing `Delete`-policy `PersistentVolume`s to
/// disappear ensures the CSI driver's async `DeleteVolume` RPC completes before
/// Step 2 tears down the control plane. Returns the captured CSI backing volume
/// names so Step 3 can sweep any leftovers if a wait times out.
pub fn delete_cluster_pvcs(
    opts: RunOptions,
    context: Option<&str>,
    cluster_desc: &str,
) -> Result<Vec<String>> {
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
        println!(
            "  [dry-run]   {} wait --for=delete pv/<name>... --timeout=120s",
            ctx_prefix
        );
        return Ok(Vec::new());
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
            return Ok(Vec::new());
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
            return Ok(Vec::new());
        }
    };

    if namespaces.is_empty() {
        println!("  ✓ No user PVCs found in cluster");
        return Ok(Vec::new());
    }

    // Snapshot Delete-policy PVs and their backing cloud disk names before deletion.
    let mut pv_cmd = Command::new("kubectl");
    if let Some(ctx) = context {
        pv_cmd.args(["--context", ctx]);
    }
    let user_pvs = pv_cmd
        .args(["get", "pv", "-o", "json"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| parse_user_delete_pvs(&String::from_utf8_lossy(&o.stdout)).ok())
        .unwrap_or_default();

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

    if !user_pvs.pv_names.is_empty() {
        let pv_targets: Vec<String> = user_pvs
            .pv_names
            .iter()
            .map(|n| format!("pv/{}", n))
            .collect();
        println!(
            "  → {} wait --for=delete {} --timeout=120s",
            ctx_prefix,
            pv_targets.join(" ")
        );
        let mut wait_cmd = Command::new("kubectl");
        if let Some(ctx) = context {
            wait_cmd.args(["--context", ctx]);
        }
        wait_cmd.args(["wait", "--for=delete"]);
        wait_cmd.args(&pv_targets);
        wait_cmd.arg("--timeout=120s");
        let wait_status = wait_cmd
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status();
        if !matches!(wait_status, Ok(s) if s.success()) {
            all_clean = false;
        }
    }

    if all_clean {
        println!("  ✓ PVCs and backing cloud disks released by CSI driver");
    } else {
        println!(
            "  ⚠ PVC/PV delete did not complete cleanly — Step 3 will sweep remaining disks"
        );
    }
    Ok(user_pvs.volume_names)
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
    fn parse_user_delete_pvs_extracts_delete_policy_user_volumes_only() {
        let json = r#"{
            "items": [
                {
                    "metadata": {"name": "pvc-1"},
                    "spec": {
                        "persistentVolumeReclaimPolicy": "Delete",
                        "claimRef": {"namespace": "dimster"},
                        "csi": {"volumeHandle": "projects/p/zones/europe-west1-b/disks/pvc-1"}
                    }
                },
                {
                    "metadata": {"name": "kafka-nvme-0"},
                    "spec": {
                        "persistentVolumeReclaimPolicy": "Retain",
                        "claimRef": {"namespace": "dimster"}
                    }
                },
                {
                    "metadata": {"name": "pvc-sys"},
                    "spec": {
                        "persistentVolumeReclaimPolicy": "Delete",
                        "claimRef": {"namespace": "kube-system"},
                        "csi": {"volumeHandle": "projects/p/zones/europe-west1-b/disks/pvc-sys"}
                    }
                }
            ]
        }"#;
        let parsed = parse_user_delete_pvs(json).unwrap();
        assert_eq!(parsed.pv_names, vec!["pvc-1"]);
        assert_eq!(parsed.volume_names, vec!["pvc-1"]);
    }

    #[test]
    fn emit_pvc_cleanup_script_stops_workloads_before_pvcs_and_waits_for_pvs() {
        let mut s = Script::new();
        emit_pvc_cleanup_script(&mut s, "$KUBE_CONTEXT", "PD CSI driver", "GCE disks");
        let rendered = s.render();
        let stop_idx = rendered
            .find("delete statefulset,deployment,job --all -n \"$NS\"")
            .expect("workload stop command");
        let pvc_idx = rendered
            .find("delete pvc --all -n \"$NS\"")
            .expect("pvc delete command");
        let pv_wait_idx = rendered
            .find("wait --for=delete $USER_DELETE_PVS")
            .expect("pv wait command");
        assert!(stop_idx < pvc_idx && pvc_idx < pv_wait_idx);
        assert!(rendered.contains("gke-.*"));
    }
}
