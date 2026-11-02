use std::path::Path;

use anyhow::Result;

use crate::commands::cadvisor;
use crate::commands::deploy;
use crate::commands::deploy::Component;
use crate::kubectl::Kubectl;

/// Tear down a dimster deployment from the env's resolved namespace.
///
/// Symmetric with `deploy::run`: builds a small temporary kustomize overlay
/// that re-templates the `Namespace` resource and sets a top-level
/// `namespace:` so kustomize rewrites every resource to the right namespace
/// before delete. Without this, `kubectl delete -k <base>` would always
/// target the literal `dimster` namespace baked into the base manifests —
/// which is wrong for any env that overrides `namespace:` (and would even
/// silently delete a sibling deploy living in `dimster`).
pub fn run(
    kubectl: &Kubectl,
    env: &str,
    k8s_dir: &Path,
    reset_local_storage_pvs: bool,
    external: bool,
    only: &[String],
) -> Result<()> {
    let ns = kubectl.namespace();
    let mut components = deploy::parse_components(only)?;
    println!(
        "==> Tearing down Dimster ({}) from namespace {}...",
        env, ns
    );

    if external && components.contains(&Component::Kafka) && !only.is_empty() {
        eprintln!(
            "Warning: ignoring 'kafka' component — environment '{}' uses external Kafka",
            env
        );
        components.remove(&Component::Kafka);
    }

    if !only.is_empty() {
        let names: Vec<&str> = only.iter().map(|s| s.as_str()).collect();
        println!("    Components: {}", names.join(", "));
    }

    // Kafka is the only Dimster component whose application state lives in
    // generated StatefulSet PVCs. Clean it through the same helper deploy uses
    // for Kafka recreates so local and remote clusters get identical PVC
    // deletion behavior.
    if !external
        && components.contains(&Component::Kafka)
        && let Err(e) = deploy::cleanup_kafka_cluster(kubectl, reset_local_storage_pvs)
    {
        eprintln!("==> Warning: Kafka cleanup step failed: {e:#}");
    }

    // Stage a temp overlay that mirrors the deploy-time overlay structure.
    // We list every component's resources so that a teardown invoked
    // against a partially-deployed env (e.g. workers only) still sweeps
    // everything that may have been left around — `--ignore-not-found`
    // makes missing resources a no-op.
    let tmp_dir = tempfile::tempdir()?;
    let tmp = tmp_dir.path();
    let base = k8s_dir.join("base");
    deploy::copy_dir_recursive(&base, &tmp.join("base"))?;
    deploy::write_namespace_yaml(&tmp.join("base/namespace.yaml"), ns)?;

    let include_namespace = only.is_empty();
    let mut resource_list =
        deploy::build_teardown_resource_list(&components, external, include_namespace);
    // Always sweep the cAdvisor ClusterRole and binding with monitoring,
    // even if `monitoring.cgroupMetrics` was switched off after deploying:
    // they are cluster-scoped, so deleting the namespace leaves them behind.
    if components.contains(&Component::Monitoring) {
        resource_list.push('\n');
        resource_list.push_str(&cadvisor::write_rbac(tmp, ns)?);
    }
    if !resource_list.trim().is_empty() {
        let kustomization = format!(
            "apiVersion: kustomize.config.k8s.io/v1beta1\n\
             kind: Kustomization\n\
             namespace: {ns}\n\
             \n\
             resources:\n\
             {resource_list}\n",
        );
        std::fs::write(tmp.join("kustomization.yaml"), &kustomization)?;

        // Surface the kustomize delete result. With --ignore-not-found, missing
        // resources are not errors, so any failure here is a real problem
        // (kustomize build error, wrong context, RBAC, etc.) and should be shown
        // rather than silently swallowed.
        if let Err(e) =
            kubectl.run_inherit(&["delete", "-k", &tmp.to_string_lossy(), "--ignore-not-found"])
        {
            eprintln!("==> kustomize delete failed: {e:#}");
            return Err(e);
        }
    }

    // Belt-and-braces cleanup of helper resources that aren't part of the
    // kustomize overlay (results-copy pod) or that may linger past the
    // namespace delete due to PVC finalizers. All scoped to the resolved
    // namespace via the Kubectl wrapper, so safe to run alongside other
    // dimster instances in different namespaces. These are best-effort:
    // surface the error to stderr but don't abort the teardown.
    let cleanup_results_helpers = only.is_empty() || components.contains(&Component::Coordinator);
    let cleanup_inspect_helper = only.is_empty()
        || components.contains(&Component::Coordinator)
        || components.contains(&Component::Monitoring);

    for args in [
        &[
            "delete",
            "pods",
            "-l",
            "app=benchmark-results-copy",
            "--ignore-not-found",
        ][..],
        // dimster-inspect is the long-lived helper pod created on demand by
        // `dimster inspect <env>` in external deploy mode. It is not part of
        // the kustomize overlay (lazy-created on first inspect) so it needs
        // explicit cleanup here to avoid lingering past teardown.
        &[
            "delete",
            "pods",
            "-l",
            "app=dimster-inspect",
            "--ignore-not-found",
        ][..],
        &["delete", "pvc", "benchmark-results", "--ignore-not-found"][..],
    ] {
        if (args.contains(&"app=benchmark-results-copy") && !cleanup_results_helpers)
            || (args.contains(&"app=dimster-inspect") && !cleanup_inspect_helper)
            || (args.contains(&"benchmark-results") && !cleanup_results_helpers)
        {
            continue;
        }
        if let Err(e) = kubectl.run(args) {
            eprintln!("==> Warning: cleanup step failed: {e:#}");
        }
    }

    println!("==> Teardown complete.");
    Ok(())
}
