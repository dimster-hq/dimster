// `dimster pivot` — generate a pivot HTML view from a single run or
// drain-backlog result file.
//
// `pivot` slices a single file along the *scenario* axis: each array index
// (test label) becomes its own HTML section, and scenarios within that index
// are the comparison series. This is useful when one workload sweeps a
// dimension (e.g. fetch.max.wait.ms) across many scenarios that differ on
// scalar fields like consumer count or key distribution. `compare --pivot`
// draws the same layout over several result files.
//
// This Rust handler is a thin shim that locates the input file, resolves the
// output directory, and shells out to charts/generate_pivot_charts.py — the
// same pattern used by commands::compare.

use std::path::Path;
use std::process::Command;

use anyhow::{Result, bail};

pub fn run(
    file: &str,
    output: Option<&str>,
    scenarios: Option<&str>,
    script_dir: &Path,
) -> Result<()> {
    if !Path::new(file).is_file() {
        bail!("File not found: {}", file);
    }

    // Default output dir = parent of the input file (mirrors compare command).
    let output_dir = match output {
        Some(d) => d.to_string(),
        None => {
            let parent = Path::new(file).parent().unwrap_or(Path::new("."));
            parent.to_string_lossy().to_string()
        }
    };

    std::fs::create_dir_all(&output_dir)?;

    let script = script_dir
        .join("charts/generate_pivot_charts.py")
        .to_string_lossy()
        .to_string();

    let basename = Path::new(file)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| file.to_string());
    println!("==> Pivoting {}...", basename);

    let mut cmd = Command::new("python3");
    cmd.arg(&script)
        .args(["--input", file])
        .args(["--output", &output_dir]);
    if let Some(s) = scenarios {
        cmd.args(["--scenarios", s]);
    }

    let status = cmd.status()?;

    if !status.success() {
        bail!("Pivot chart generation failed");
    }

    println!("==> Pivot charts saved to {}/", output_dir);
    Ok(())
}
