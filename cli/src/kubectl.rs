use std::io::Write;
use std::process::{Child, Command, Stdio};

use anyhow::{Context, Result, bail};

/// Wrapper around `kubectl` that handles context and namespace.
pub struct Kubectl {
    context: Option<String>,
    namespace: String,
}

/// RAII guard that kills a child process on drop.
pub struct ChildGuard(Option<Child>);

impl ChildGuard {
    pub fn new(child: Child) -> Self {
        Self(Some(child))
    }

    /// Take the child out of the guard (caller takes ownership).
    #[allow(dead_code)]
    pub fn take(&mut self) -> Option<Child> {
        self.0.take()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Kubectl {
    pub fn new(context: Option<String>, namespace: String) -> Self {
        Self { context, namespace }
    }

    fn base_cmd(&self) -> Command {
        let mut cmd = Command::new("kubectl");
        if let Some(ctx) = &self.context {
            cmd.args(["--context", ctx]);
        }
        cmd
    }

    /// Run kubectl with namespace, capture stdout. Fails on non-zero exit.
    pub fn run(&self, args: &[&str]) -> Result<String> {
        let output = self
            .base_cmd()
            .args(["-n", &self.namespace])
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("Failed to execute kubectl")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("kubectl failed: {}", stderr.trim());
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// Run kubectl without namespace prefix (e.g., apply -k, delete -k).
    pub fn run_no_ns(&self, args: &[&str]) -> Result<String> {
        let output = self
            .base_cmd()
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("Failed to execute kubectl")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("kubectl failed: {}", stderr.trim());
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// Run kubectl with namespace, inheriting stdin/stdout/stderr.
    pub fn run_inherit(&self, args: &[&str]) -> Result<()> {
        let status = self
            .base_cmd()
            .args(["-n", &self.namespace])
            .args(args)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .context("Failed to execute kubectl")?;

        if !status.success() {
            bail!("kubectl exited with status {}", status);
        }
        Ok(())
    }

    /// Run kubectl with namespace, piping `stdin_data` to the child's stdin
    /// while inheriting stdout/stderr. Used by `inspect` (external mode) to
    /// stream a Java Properties payload into `kubectl exec -i ... --props -`
    /// without an intermediate temp file or kubectl-cp roundtrip.
    pub fn run_inherit_with_stdin(&self, stdin_data: &str, args: &[&str]) -> Result<()> {
        let mut child = self
            .base_cmd()
            .args(["-n", &self.namespace])
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .context("Failed to spawn kubectl")?;

        // Scoped block so the BorrowedFd to the child's stdin is closed before
        // we wait — the child reads to EOF and only then exits.
        {
            let stdin = child
                .stdin
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("failed to open kubectl stdin"))?;
            stdin
                .write_all(stdin_data.as_bytes())
                .context("Failed to write to kubectl stdin")?;
        }
        // Explicitly drop the stdin handle so the child sees EOF.
        drop(child.stdin.take());

        let status = child.wait().context("Failed to wait on kubectl")?;
        if !status.success() {
            bail!("kubectl exited with status {}", status);
        }
        Ok(())
    }

    /// Spawn kubectl with namespace as a background process (stdout piped).
    pub fn spawn(&self, args: &[&str]) -> Result<Child> {
        self.base_cmd()
            .args(["-n", &self.namespace])
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .context("Failed to spawn kubectl")
    }

    /// Spawn kubectl with namespace, inheriting stdio (for port-forward, etc.).
    pub fn spawn_inherit(&self, args: &[&str]) -> Result<Child> {
        self.base_cmd()
            .args(["-n", &self.namespace])
            .args(args)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .context("Failed to spawn kubectl")
    }

    /// Execute curl inside a pod via kubectl exec.
    pub fn exec_curl(
        &self,
        pod: &str,
        method: &str,
        url: &str,
        body_file: Option<&str>,
    ) -> Result<String> {
        let mut args = vec!["exec", pod, "--", "curl", "-sf", "-X", method];
        let data_arg;
        if let Some(file) = body_file {
            args.push("-H");
            args.push("Content-Type: application/json");
            data_arg = format!("@{}", file);
            args.push("-d");
            args.push(&data_arg);
        }
        args.push(url);
        self.run(&args)
    }

    /// Copy a local file to a pod.
    pub fn cp_to_pod(&self, local_path: &str, pod: &str, remote_path: &str) -> Result<()> {
        let dest = format!("{}:{}", pod, remote_path);
        let output = self
            .base_cmd()
            .args(["-n", &self.namespace, "cp", local_path, &dest])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("Failed to kubectl cp")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("kubectl cp failed: {}", stderr.trim());
        }
        Ok(())
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The same cluster (kubectl context) in another namespace, e.g. the
    /// namespace of the env whose Kafka an `external-env` env uses.
    pub fn for_namespace(&self, namespace: &str) -> Kubectl {
        Kubectl::new(self.context.clone(), namespace.to_string())
    }

    /// Copy a file from a pod to a local path using a streaming tar pipe with
    /// gzip compression. This is more reliable than `kubectl cp` for large files
    /// because it avoids the intermediate temp-file dance and kubectl cp's own
    /// flakiness — the data streams directly from the pod to the local filesystem.
    /// Falls back to `kubectl cp` if the tar pipe fails. Returns true on success.
    pub fn copy_file_from_pod(&self, pod: &str, remote_path: &str, local_path: &str) -> bool {
        let remote_dir = std::path::Path::new(remote_path)
            .parent()
            .unwrap_or(std::path::Path::new("/"))
            .to_string_lossy();
        let remote_filename = std::path::Path::new(remote_path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy();
        let local_dir = std::path::Path::new(local_path)
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .to_string_lossy();

        // Primary: stream via tar pipe with gzip compression.
        // kubectl exec tar czf on the pod, pipe stdout to local tar xzf.
        if self.copy_via_tar_pipe(pod, &remote_dir, &remote_filename, &local_dir) {
            return true;
        }

        // Fallback: direct kubectl cp (uses tar internally but has its own retry logic)
        let src = format!("{}/{}:{}", self.namespace, pod, remote_path);
        self.run_no_ns(&[
            "-n",
            &self.namespace,
            "cp",
            &src,
            local_path,
            "--retries",
            "3",
        ])
        .is_ok()
    }

    /// Stream a file from a pod via a kubectl exec tar pipe:
    ///   kubectl exec <pod> -- tar czf - -C <remote_dir> <filename> | tar xzf - -C <local_dir>
    /// No temp files, no cleanup — a single streaming operation.
    fn copy_via_tar_pipe(
        &self,
        pod: &str,
        remote_dir: &str,
        remote_filename: &str,
        local_dir: &str,
    ) -> bool {
        let remote_proc = self
            .base_cmd()
            .args([
                "-n",
                &self.namespace,
                "exec",
                pod,
                "--",
                "tar",
                "czf",
                "-",
                "-C",
                remote_dir,
                remote_filename,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();

        let mut remote_proc = match remote_proc {
            Ok(p) => p,
            Err(_) => return false,
        };

        // Pipe kubectl's stdout directly into local tar's stdin
        let kubectl_stdout = match remote_proc.stdout.take() {
            Some(s) => s,
            None => {
                let _ = remote_proc.kill();
                let _ = remote_proc.wait();
                return false;
            }
        };

        let local_proc = Command::new("tar")
            .args(["xzf", "-", "-C", local_dir])
            .stdin(kubectl_stdout)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();

        let mut local_proc = match local_proc {
            Ok(p) => p,
            Err(_) => {
                let _ = remote_proc.kill();
                let _ = remote_proc.wait();
                return false;
            }
        };

        let local_status = local_proc.wait();
        let remote_status = remote_proc.wait();

        matches!(
            (remote_status, local_status),
            (Ok(r), Ok(l)) if r.success() && l.success()
        )
    }
}
