//! Workload hook scripts. A workload's `engineConfig.hooks` names scripts to
//! run at points in its lifecycle (see dev.dimster.hooks.Hooks). The CLI reads
//! each referenced script from this machine and ships its contents in the run
//! request; the coordinator installs and runs only scripts sent this way, so
//! it never needs access to the user's files or knowledge of what they do.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow, bail};

/// Reference scripts shipped with Dimster, relative to the repo root. A
/// workload can name one by its file name, e.g. `script: evict-topic-cache.sh`.
pub const BUILTIN_HOOKS_DIR: &str = "run/hooks";

/// The YAML names of the hook points, matching dev.dimster.hooks.HookPoint.
const HOOK_POINTS: [&str; 5] = [
    "beforeWorkload",
    "afterWorkload",
    "beforeTestPoint",
    "afterTestPoint",
    "beforeDrain",
];

/// The `script` values under `engineConfig.hooks`, in lifecycle order, each
/// once. Unparseable YAML yields none and is left for the coordinator to report.
pub fn referenced_scripts(content: &str) -> Vec<String> {
    let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(content) else {
        return Vec::new();
    };
    let Some(hooks) = doc.get("engineConfig").and_then(|e| e.get("hooks")) else {
        return Vec::new();
    };
    let mut scripts: Vec<String> = Vec::new();
    for point in HOOK_POINTS {
        let Some(list) = hooks.get(point).and_then(|l| l.as_sequence()) else {
            continue;
        };
        for hook in list {
            if let Some(script) = hook.get("script").and_then(|s| s.as_str()) {
                if !script.trim().is_empty() && !scripts.iter().any(|s| s == script) {
                    scripts.push(script.to_string());
                }
            }
        }
    }
    scripts
}

/// Where a script reference points: absolute, else beside the workload, else
/// relative to the working directory, else a built-in reference script.
fn resolve(script: &str, workload_dir: &Path, builtin_dir: &Path) -> Option<PathBuf> {
    let p = Path::new(script);
    if p.is_absolute() {
        return p.is_file().then(|| p.to_path_buf());
    }
    [workload_dir.join(p), p.to_path_buf(), builtin_dir.join(p)]
        .into_iter()
        .find(|c| c.is_file())
}

/// Reads every script the workloads reference, keyed by the reference as
/// written (the key the coordinator looks the script up by). Fails if a
/// script is missing, or if one reference means different files in two
/// workloads, since the coordinator could only run one of them.
pub fn collect(
    workloads: &[(PathBuf, String)],
    builtin_dir: &Path,
) -> Result<BTreeMap<String, String>> {
    let mut scripts: BTreeMap<String, String> = BTreeMap::new();
    for (path, content) in workloads {
        let workload_dir = path.parent().unwrap_or(Path::new("."));
        for script in referenced_scripts(content) {
            let resolved = resolve(&script, workload_dir, builtin_dir).ok_or_else(|| {
                anyhow!(
                    "Workload '{}' references hook script '{}', which was not found beside the workload, \
                     in the current directory, or in {}",
                    path.display(),
                    script,
                    builtin_dir.display()
                )
            })?;
            let body = std::fs::read_to_string(&resolved)
                .map_err(|e| anyhow!("Cannot read hook script '{}': {}", resolved.display(), e))?;
            if let Some(existing) = scripts.get(&script) {
                if *existing != body {
                    bail!(
                        "Hook script '{}' resolves to different files across workloads; \
                         give them distinct paths",
                        script
                    );
                }
                continue;
            }
            scripts.insert(script, body);
        }
    }
    Ok(scripts)
}

/// Copies the shipped scripts into the local results directory, so a result
/// records exactly what ran against the environment.
pub fn stage(run_dir: &Path, scripts: &BTreeMap<String, String>) -> Result<()> {
    if scripts.is_empty() {
        return Ok(());
    }
    let dir = run_dir.join("configs").join("hooks");
    std::fs::create_dir_all(&dir)
        .map_err(|e| anyhow!("Failed to create '{}': {}", dir.display(), e))?;
    for (name, body) in scripts {
        let file_name: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || "._-".contains(c) {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        std::fs::write(dir.join(&file_name), body)
            .map_err(|e| anyhow!("Failed to stage hook script '{}': {}", name, e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORKLOAD: &str = "
name: x
engineConfig:
  hooks:
    beforeDrain:
      - script: evict.sh
    afterTestPoint:
      - script: local/notify.sh
      - script: evict.sh
";

    #[test]
    fn finds_each_script_once_in_lifecycle_order() {
        assert_eq!(
            referenced_scripts(WORKLOAD),
            vec!["local/notify.sh", "evict.sh"]
        );
    }

    #[test]
    fn workloads_without_hooks_reference_nothing() {
        assert!(referenced_scripts("name: x\nengineConfig:\n  cooldownMinutes: 1\n").is_empty());
        assert!(referenced_scripts("not: [valid").is_empty());
    }

    #[test]
    fn resolves_beside_workload_before_builtins() {
        let tmp = tempfile::tempdir().unwrap();
        let wl_dir = tmp.path().join("workloads");
        let builtin = tmp.path().join("builtin");
        std::fs::create_dir_all(wl_dir.join("local")).unwrap();
        std::fs::create_dir_all(&builtin).unwrap();
        std::fs::write(wl_dir.join("local/notify.sh"), "notify").unwrap();
        std::fs::write(wl_dir.join("evict.sh"), "workload evict").unwrap();
        std::fs::write(builtin.join("evict.sh"), "builtin evict").unwrap();

        let scripts = collect(&[(wl_dir.join("w.yaml"), WORKLOAD.to_string())], &builtin).unwrap();

        assert_eq!(scripts["evict.sh"], "workload evict");
        assert_eq!(scripts["local/notify.sh"], "notify");
    }

    #[test]
    fn falls_back_to_builtin_scripts() {
        let tmp = tempfile::tempdir().unwrap();
        let builtin = tmp.path().join("builtin");
        std::fs::create_dir_all(&builtin).unwrap();
        std::fs::write(builtin.join("evict.sh"), "builtin evict").unwrap();
        let wl = "engineConfig:\n  hooks:\n    beforeDrain:\n      - script: evict.sh\n";

        let scripts = collect(&[(tmp.path().join("w.yaml"), wl.to_string())], &builtin).unwrap();

        assert_eq!(scripts["evict.sh"], "builtin evict");
    }

    #[test]
    fn missing_script_is_an_error_naming_it() {
        let tmp = tempfile::tempdir().unwrap();
        let err = collect(
            &[(tmp.path().join("w.yaml"), WORKLOAD.to_string())],
            tmp.path(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("local/notify.sh"), "{}", err);
    }

    #[test]
    fn same_reference_to_different_files_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        for (dir, body) in [("a", "one"), ("b", "two")] {
            std::fs::create_dir_all(tmp.path().join(dir)).unwrap();
            std::fs::write(tmp.path().join(dir).join("evict.sh"), body).unwrap();
        }
        let wl =
            "engineConfig:\n  hooks:\n    beforeDrain:\n      - script: evict.sh\n".to_string();
        let workloads = vec![
            (tmp.path().join("a/w1.yaml"), wl.clone()),
            (tmp.path().join("b/w2.yaml"), wl),
        ];

        assert!(collect(&workloads, tmp.path()).is_err());
    }

    #[test]
    fn stages_scripts_with_safe_names() {
        let tmp = tempfile::tempdir().unwrap();
        let scripts = BTreeMap::from([("local/notify.sh".to_string(), "notify".to_string())]);

        stage(tmp.path(), &scripts).unwrap();

        let staged = tmp.path().join("configs/hooks/local_notify.sh");
        assert_eq!(std::fs::read_to_string(staged).unwrap(), "notify");
    }
}
