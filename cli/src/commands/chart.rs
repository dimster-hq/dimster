use std::path::Path;
use std::process::Command;

use anyhow::{Result, bail};

pub fn run(results_dir: &str, script_dir: &Path) -> Result<()> {
    let results_path = Path::new(results_dir);
    if !results_path.is_dir() {
        bail!("No results directory found: {}", results_dir);
    }

    if !has_json_files(results_path) {
        bail!("No JSON result files found in {}", results_dir);
    }

    let output_dir = results_path.join("charts");
    std::fs::create_dir_all(&output_dir)?;

    let charts_dir = script_dir.join("charts");

    println!("==> Generating charts from {}...", results_dir);

    // Each script silently exits if no matching result files are found
    // Availability charts are included in generate_run_charts.py when availability data is present
    let generators = [
        "generate_run_charts.py",
        "generate_explore_charts.py",
        "generate_drain_backlog_charts.py",
        "generate_correctness_charts.py",
        // Needs resource-metrics.json from the metrics export
        "generate_resource_charts.py",
    ];

    for script in generators {
        let status = Command::new("python3")
            .args([
                &charts_dir.join(script).to_string_lossy().to_string(),
                "--results",
                results_dir,
                "--output",
                &output_dir.to_string_lossy(),
            ])
            .status()?;

        if !status.success() {
            let name = script
                .trim_start_matches("generate_")
                .trim_end_matches("_charts.py");
            eprintln!("Warning: {} chart generation failed", name);
        }
    }

    println!("==> Charts saved to {}/", output_dir.display());
    Ok(())
}

fn has_json_files(dir: &Path) -> bool {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if has_json_files(&path) {
                    return true;
                }
            } else if path.extension().is_some_and(|ext| ext == "json") {
                return true;
            }
        }
    }
    false
}
