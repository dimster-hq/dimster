use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

use crate::commands::csv;

#[derive(Debug)]
struct CompareEntry {
    label: String,
    file: PathBuf,
}

pub fn run(
    files: &[String],
    directory: Option<&str>,
    output: Option<&str>,
    name: Option<&str>,
    script_dir: &Path,
) -> Result<()> {
    let entries = compare_entries(files, directory)?;
    validate_entry_count(&entries)?;
    // Resolved before anything is written, so a name too long for the
    // filesystem fails the command up front rather than midway through.
    let name = compare_name(&entries, name)?;

    let output_dir = match output {
        Some(d) => d.to_string(),
        None => {
            if let Some(directory) = directory {
                directory.to_string()
            } else {
                entries[0]
                    .file
                    .parent()
                    .unwrap_or(Path::new("."))
                    .to_string_lossy()
                    .to_string()
            }
        }
    };

    std::fs::create_dir_all(&output_dir)?;

    let labels: Vec<&str> = entries.iter().map(|e| e.label.as_str()).collect();
    println!("==> Comparing {}...", labels.join(" vs "));

    let status = Command::new("python3")
        .args(script_args(
            script_dir,
            COMPARE_SCRIPT,
            &entries,
            &name,
            &output_dir,
        ))
        .status()?;
    if !status.success() {
        bail!("Comparison chart generation failed");
    }

    let csv_path = write_combined_csv(&entries, &name, &output_dir)?;

    println!("==> Comparison charts saved to {}/", output_dir);
    println!("==> Comparison CSV saved to {}", csv_path.display());

    // Writes nothing unless at least two entries have resource data; the
    // comparison above stands either way.
    match Command::new("python3")
        .args(script_args(
            script_dir,
            RESOURCES_SCRIPT,
            &entries,
            &name,
            &output_dir,
        ))
        .status()
    {
        Ok(status) if status.success() => {}
        Ok(status) => eprintln!("==> Warning: resource comparison failed ({status})"),
        Err(e) => eprintln!("==> Warning: could not run the resource comparison: {e}"),
    }
    Ok(())
}

/// The run comparison's charts.
const COMPARE_SCRIPT: &str = "charts/generate_compare_charts.py";
/// The comparison of the entries' resources report data, where they have it.
const RESOURCES_SCRIPT: &str = "charts/generate_compare_resource_charts.py";

/// The most bytes a single file name may have: NAME_MAX on Linux and the
/// APFS limit on macOS (which counts characters, so bytes is the safe side).
const MAX_FILE_NAME_BYTES: usize = 255;

/// The longest suffix any comparison output file adds after `compare-NAME`
/// (the resources page from RESOURCES_SCRIPT).
const LONGEST_OUTPUT_SUFFIX: &str = "-resources.html";

/// The NAME in the comparison's output file names (`compare-NAME.html`,
/// `compare-NAME.csv`, `compare-NAME-resources.html`, ...). It is the
/// `--name` given, else the entries' labels joined by `-vs-`. The CLI decides
/// it and hands it to every script, so all output files share one name.
///
/// Fails when the longest output file name would exceed the filesystem
/// limit, which a default name over many entries easily does.
fn compare_name(entries: &[CompareEntry], name: Option<&str>) -> Result<String> {
    let resolved = match name {
        Some(n) => {
            if n.is_empty() {
                bail!("--name must not be empty");
            }
            if n.contains(['/', '\\']) {
                bail!(
                    "--name '{n}' must not contain a path separator; set the directory with --output"
                );
            }
            n.to_string()
        }
        None => entries
            .iter()
            .map(|e| e.label.as_str())
            .collect::<Vec<_>>()
            .join("-vs-"),
    };

    let longest = output_file_name(&resolved, LONGEST_OUTPUT_SUFFIX);
    if longest.len() > MAX_FILE_NAME_BYTES {
        // The over-long name itself is left out: with many entries it would
        // bury the advice.
        let given = if name.is_some() {
            "The --name given is too long".to_string()
        } else {
            format!(
                "The default comparison name, the {} labels joined by '-vs-', is too long",
                entries.len()
            )
        };
        bail!(
            "{given}: output file names would be up to {} bytes, over the {MAX_FILE_NAME_BYTES}-byte file name limit. \
             Use --name NAME to set a shorter name (at most {} bytes); the output files are then compare-NAME.html, compare-NAME.csv, etc.",
            longest.len(),
            MAX_FILE_NAME_BYTES - output_file_name("", LONGEST_OUTPUT_SUFFIX).len(),
        );
    }
    Ok(resolved)
}

/// An output file name of the comparison called `name`.
fn output_file_name(name: &str, suffix: &str) -> String {
    format!("compare-{name}{suffix}")
}

/// python3 arguments for a comparison script: the script, then one
/// `--entry LABEL=FILE` per entry, the comparison name and the output
/// directory.
fn script_args(
    script_dir: &Path,
    script: &str,
    entries: &[CompareEntry],
    name: &str,
    output_dir: &str,
) -> Vec<String> {
    let mut args = vec![script_dir.join(script).to_string_lossy().to_string()];
    for entry in entries {
        args.push("--entry".to_string());
        args.push(format!("{}={}", entry.label, entry.file.to_string_lossy()));
    }
    args.push("--name".to_string());
    args.push(name.to_string());
    args.push("--output".to_string());
    args.push(output_dir.to_string());
    args
}

fn write_combined_csv(entries: &[CompareEntry], name: &str, output_dir: &str) -> Result<PathBuf> {
    let out_path = Path::new(output_dir).join(output_file_name(name, ".csv"));

    let mut header: Option<String> = None;
    let mut rows = Vec::new();
    for entry in entries {
        let doc = csv::convert_file(&entry.file)?.ok_or_else(|| {
            anyhow::anyhow!(
                "Cannot convert compare file to CSV: {}",
                entry.file.display()
            )
        })?;
        match header.as_ref() {
            Some(existing) if existing != &doc.header => bail!(
                "Cannot concatenate compare CSV rows because '{}' produced a different header",
                entry.file.display()
            ),
            None => header = Some(doc.header),
            _ => {}
        }
        rows.extend(doc.rows);
    }

    let Some(header) = header else {
        bail!("No compare CSV rows generated");
    };
    let csv = csv::CsvDocument { header, rows }.to_csv();
    std::fs::write(&out_path, csv)
        .with_context(|| format!("Failed to write comparison CSV: {}", out_path.display()))?;
    Ok(out_path)
}

fn compare_entries(files: &[String], directory: Option<&str>) -> Result<Vec<CompareEntry>> {
    match (directory, files.is_empty()) {
        (Some(_), false) => bail!("Use either --directory or LABEL=FILE arguments, not both"),
        (Some(directory), true) => entries_from_directory(Path::new(directory)),
        (None, false) => files.iter().map(|f| parse_labeled_file(f)).collect(),
        (None, true) => {
            bail!("At least 2 labeled files required, or use --directory DIR")
        }
    }
}

fn validate_entry_count(entries: &[CompareEntry]) -> Result<()> {
    if entries.len() < 2 {
        bail!(
            "At least 2 compare entries required, found {}",
            entries.len()
        );
    }
    Ok(())
}

fn parse_labeled_file(arg: &str) -> Result<CompareEntry> {
    let Some((label, file)) = arg.split_once('=') else {
        bail!(
            "Invalid format '{}': expected LABEL=FILE (e.g. t1=results.json)",
            arg
        );
    };
    if label.is_empty() {
        bail!("Empty label in '{}'", arg);
    }
    if !Path::new(file).is_file() {
        bail!("File not found: {}", file);
    }
    Ok(CompareEntry {
        label: label.to_string(),
        file: PathBuf::from(file),
    })
}

fn entries_from_directory(directory: &Path) -> Result<Vec<CompareEntry>> {
    if !directory.is_dir() {
        bail!("Compare directory not found: {}", directory.display());
    }

    let mut entries = Vec::new();
    let mut untyped_json_dirs = Vec::new();
    let mut typed_json_dirs = Vec::new();
    collect_entries_from_tree(
        directory,
        &mut entries,
        &mut untyped_json_dirs,
        &mut typed_json_dirs,
    )?;

    if entries.is_empty() {
        if let Some(dir) = untyped_json_dirs.first() {
            bail!(
                "No JSON files in {} contain a fileType field. Re-run the workload with a newer Dimster build, or compare explicit LABEL=FILE arguments.",
                dir.display()
            );
        }
        if let Some(dir) = typed_json_dirs.first() {
            bail!(
                "No supported result JSON with fileType='dimster-result' and mode 'run', 'drain-backlog' or 'correctness' found in {}",
                dir.display()
            );
        }
        bail!(
            "No JSON files found under compare directory: {}",
            directory.display()
        );
    }
    Ok(entries)
}

fn collect_entries_from_tree(
    directory: &Path,
    entries: &mut Vec<CompareEntry>,
    untyped_json_dirs: &mut Vec<PathBuf>,
    typed_json_dirs: &mut Vec<PathBuf>,
) -> Result<()> {
    let json_files = direct_json_files(directory)?;
    if !json_files.is_empty() {
        match entry_from_json_files(directory, json_files)? {
            RunDirScan::Entry(entry) => entries.push(entry),
            RunDirScan::UntypedJson => untyped_json_dirs.push(directory.to_path_buf()),
            RunDirScan::TypedButNoRunResult => typed_json_dirs.push(directory.to_path_buf()),
        }
    }

    for subdir in direct_subdirs(directory)? {
        collect_entries_from_tree(&subdir, entries, untyped_json_dirs, typed_json_dirs)?;
    }
    Ok(())
}

fn direct_subdirs(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut subdirs = Vec::new();
    for entry in std::fs::read_dir(directory)
        .with_context(|| format!("Failed to read compare directory: {}", directory.display()))?
    {
        let path = entry?.path();
        if path.is_dir() {
            subdirs.push(path);
        }
    }
    subdirs.sort();
    Ok(subdirs)
}

enum RunDirScan {
    Entry(CompareEntry),
    UntypedJson,
    TypedButNoRunResult,
}

fn entry_from_json_files(directory: &Path, files: Vec<PathBuf>) -> Result<RunDirScan> {
    let mut candidates = Vec::new();
    let mut typed_file_count = 0;
    for file in files {
        if json_file_has_type(&file)? {
            typed_file_count += 1;
        }
        if let Some(label) = compare_result_label(&file)? {
            candidates.push(CompareEntry { label, file });
        }
    }

    match candidates.len() {
        1 => Ok(RunDirScan::Entry(candidates.remove(0))),
        0 => {
            if typed_file_count == 0 {
                Ok(RunDirScan::UntypedJson)
            } else {
                Ok(RunDirScan::TypedButNoRunResult)
            }
        }
        _ => {
            let files: Vec<String> = candidates
                .iter()
                .map(|e| e.file.display().to_string())
                .collect();
            bail!(
                "Expected exactly one run result JSON in {}, found {}: {}",
                directory.display(),
                files.len(),
                files.join(", ")
            );
        }
    }
}

fn direct_json_files(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(directory).with_context(|| {
        format!(
            "Failed to read result subdirectory: {}",
            directory.display()
        )
    })? {
        let path = entry?.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

/// Return the comparison label for a supported result file.
///
/// Run comparisons intentionally require the explicit experiment label used
/// by their existing chart layout. Correctness campaigns commonly vary the
/// workload name instead, so they fall back to metadata.name when no label is
/// present. Helper JSON files and unsupported result modes remain invisible to
/// recursive directory discovery.
fn compare_result_label(file: &Path) -> Result<Option<String>> {
    let content = std::fs::read_to_string(file)
        .with_context(|| format!("Failed to read result JSON: {}", file.display()))?;
    let value: serde_json::Value = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse result JSON: {}", file.display()))?;

    if value.get("fileType").and_then(|v| v.as_str()) != Some("dimster-result")
        || !value.get("scenarioResults").is_some_and(|v| v.is_array())
    {
        return Ok(None);
    }

    let mode = value.get("mode").and_then(|v| v.as_str());
    let metadata = value.get("metadata");
    let label = match mode {
        // Results of one workload share its name, so runs and drains need
        // the explicit label (`--metadata label=...`) to tell them apart.
        Some(m @ ("run" | "drain-backlog")) => metadata
            .and_then(|v| v.get("label"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "{} result JSON is missing string metadata.label (set it with --metadata label=...): {}",
                    if m == "run" { "Run" } else { "Drain-backlog" },
                    file.display()
                )
            })?,
        Some("correctness") => metadata
            .and_then(|v| v.get("label"))
            .and_then(|v| v.as_str())
            .or_else(|| {
                metadata
                    .and_then(|v| v.get("name"))
                    .and_then(|v| v.as_str())
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Correctness result JSON is missing string metadata.label or metadata.name: {}",
                    file.display()
                )
            })?,
        _ => return Ok(None),
    };
    if label.is_empty() {
        bail!(
            "Result JSON has an empty comparison label in metadata: {}",
            file.display()
        );
    }
    Ok(Some(label.to_string()))
}

fn json_file_has_type(file: &Path) -> Result<bool> {
    let content = std::fs::read_to_string(file)
        .with_context(|| format!("Failed to read JSON file: {}", file.display()))?;
    let value: serde_json::Value = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse JSON file: {}", file.display()))?;
    Ok(value.get("fileType").is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    fn run_result(label: &str) -> String {
        format!(
            r#"{{
  "fileType": "dimster-result",
  "mode": "run",
  "metadata": {{
    "label": "{label}"
  }},
  "scenarioResults": [
    {{
      "name": "default",
      "tests": [
        {{
          "label": "default",
          "workloadConfig": {{}},
          "results": {{}}
        }}
      ]
    }}
  ]
}}"#
        )
    }

    fn correctness_result(name: &str) -> String {
        format!(
            r#"{{
  "fileType": "dimster-result",
  "mode": "correctness",
  "metadata": {{
    "name": "{name}"
  }},
  "scenarioResults": [
    {{
      "name": "default",
      "tests": [
        {{
          "label": "default",
          "workloadConfig": {{"messageDistributor": "PINNED_PARTITIONS"}},
          "correctness": {{}}
        }}
      ]
    }}
  ]
}}"#
        )
    }

    #[test]
    fn entries_from_directory_uses_metadata_label_and_ignores_helper_json() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let first = root.join("2026-07-08-13-46-21");
        let second = root.join("2026-07-08-13-54-32");
        write(
            &first.join("run-request.json"),
            r#"{"fileType":"dimster-run-request","mode":"run","metadata":{"label":"nope"}}"#,
        );
        write(
            &first.join("topologies.json"),
            r#"{"fileType":"dimster-topology","workloadName":"topology"}"#,
        );
        write(&first.join("result.json"), &run_result("tls-none"));
        write(&second.join("result.json"), &run_result("tls-client"));

        let entries = entries_from_directory(root).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].label, "tls-none");
        assert_eq!(entries[0].file, first.join("result.json"));
        assert_eq!(entries[1].label, "tls-client");
        assert_eq!(entries[1].file, second.join("result.json"));
    }

    #[test]
    fn entries_from_directory_walks_series_instance_timestamp_layout() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let first = root
            .join("tls-permutations")
            .join("tls-1783511127")
            .join("2026-07-08-13-46-21");
        let second = root
            .join("tls-permutations")
            .join("tls-1783511127")
            .join("2026-07-08-13-54-32");
        write(&first.join("result.json"), &run_result("tls-none"));
        write(&second.join("result.json"), &run_result("tls-client"));

        let entries = entries_from_directory(&root.join("tls-permutations")).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].label, "tls-none");
        assert_eq!(entries[1].label, "tls-client");
    }

    #[test]
    fn entries_from_directory_accepts_correctness_results_using_workload_name() {
        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("2026-07-21-11-33-29");
        let second = tmp.path().join("2026-07-21-11-51-05");
        write(&first.join("result.json"), &correctness_result("consumer"));
        write(
            &second.join("result.json"),
            &correctness_result("cooperative-sticky"),
        );

        let entries = entries_from_directory(tmp.path()).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].label, "consumer");
        assert_eq!(entries[1].label, "cooperative-sticky");
    }

    #[test]
    fn entries_from_directory_rejects_missing_metadata_label() {
        let tmp = tempfile::tempdir().unwrap();
        let subdir = tmp.path().join("2026-07-08-13-46-21");
        write(
            &subdir.join("result.json"),
            r#"{"fileType":"dimster-result","mode":"run","metadata":{},"scenarioResults":[]}"#,
        );

        let err = entries_from_directory(tmp.path()).unwrap_err();

        assert!(
            err.to_string()
                .contains("Run result JSON is missing string metadata.label")
        );
    }

    #[test]
    fn entries_from_directory_accepts_drain_backlog_results_by_label() {
        let tmp = tempfile::tempdir().unwrap();
        let drain = |label: &str| {
            format!(
                r#"{{"fileType":"dimster-result","mode":"drain-backlog","metadata":{{"name":"w","label":"{label}"}},"scenarioResults":[]}}"#
            )
        };
        write(&tmp.path().join("a/result.json"), &drain("16kb"));
        write(&tmp.path().join("b/result.json"), &drain("64kb"));

        let labels: Vec<String> = entries_from_directory(tmp.path())
            .unwrap()
            .into_iter()
            .map(|e| e.label)
            .collect();

        assert_eq!(labels, vec!["16kb", "64kb"]);
    }

    #[test]
    fn drain_backlog_results_need_a_metadata_label() {
        let tmp = tempfile::tempdir().unwrap();
        write(
            &tmp.path().join("a/result.json"),
            r#"{"fileType":"dimster-result","mode":"drain-backlog","metadata":{"name":"w"},"scenarioResults":[]}"#,
        );

        let err = entries_from_directory(tmp.path()).unwrap_err().to_string();

        assert!(
            err.contains("Drain-backlog result JSON is missing string metadata.label"),
            "{err}"
        );
    }

    #[test]
    fn entries_from_directory_rejects_json_files_without_file_type() {
        let tmp = tempfile::tempdir().unwrap();
        let subdir = tmp.path().join("2026-07-08-13-46-21");
        write(
            &subdir.join("result.json"),
            r#"{"mode":"run","metadata":{"label":"legacy"},"scenarioResults":[]}"#,
        );
        write(
            &subdir.join("run-request.json"),
            r#"{"mode":"run","metadata":{"label":"request"}}"#,
        );

        let err = entries_from_directory(tmp.path()).unwrap_err();

        assert!(err.to_string().contains("contain a fileType field"));
    }

    #[test]
    fn write_combined_csv_concatenates_compare_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("first.json");
        let second = tmp.path().join("second.json");
        write(&first, &run_result("tls-none"));
        write(&second, &run_result("tls-all"));

        let output_dir = tmp.path().join("out");
        std::fs::create_dir_all(&output_dir).unwrap();
        let entries = vec![
            CompareEntry {
                label: "tls-none".to_string(),
                file: first,
            },
            CompareEntry {
                label: "tls-all".to_string(),
                file: second,
            },
        ];

        let csv_path = write_combined_csv(
            &entries,
            "tls-none-vs-tls-all",
            &output_dir.to_string_lossy(),
        )
        .unwrap();
        let csv = std::fs::read_to_string(csv_path).unwrap();

        assert!(csv.contains("metadata_label"));
        assert_eq!(csv.lines().count(), 3);
        assert!(csv.contains("tls-none"));
        assert!(csv.contains("tls-all"));
    }

    #[test]
    fn write_combined_csv_supports_correctness_results() {
        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("first.json");
        let second = tmp.path().join("second.json");
        write(&first, &correctness_result("consumer"));
        write(&second, &correctness_result("classic"));

        let output_dir = tmp.path().join("out");
        std::fs::create_dir_all(&output_dir).unwrap();
        let entries = vec![
            CompareEntry {
                label: "consumer".to_string(),
                file: first,
            },
            CompareEntry {
                label: "classic".to_string(),
                file: second,
            },
        ];

        let csv_path = write_combined_csv(
            &entries,
            "tls-none-vs-tls-all",
            &output_dir.to_string_lossy(),
        )
        .unwrap();
        let csv = std::fs::read_to_string(csv_path).unwrap();

        assert!(csv.contains("lost_messages,duplicate_count,out_of_order_count"));
        assert_eq!(csv.lines().count(), 3);
        assert!(csv.contains("consumer"));
        assert!(csv.contains("classic"));
        assert!(csv.contains("PINNED_PARTITIONS"));
    }

    #[test]
    fn validate_entry_count_allows_more_than_four_entries() {
        let entries: Vec<CompareEntry> = (0..5)
            .map(|i| CompareEntry {
                label: format!("run-{i}"),
                file: PathBuf::from(format!("run-{i}.json")),
            })
            .collect();

        validate_entry_count(&entries).unwrap();
    }

    #[test]
    fn validate_entry_count_still_requires_at_least_two_entries() {
        let entries = vec![CompareEntry {
            label: "baseline".to_string(),
            file: PathBuf::from("baseline.json"),
        }];

        let err = validate_entry_count(&entries).unwrap_err();

        assert!(
            err.to_string()
                .contains("At least 2 compare entries required")
        );
    }

    #[test]
    fn compare_entries_rejects_directory_and_positional_files_together() {
        let err = compare_entries(&["a=b.json".to_string()], Some("results")).unwrap_err();
        assert!(
            err.to_string()
                .contains("Use either --directory or LABEL=FILE arguments")
        );
    }

    fn entries_labelled(labels: &[&str]) -> Vec<CompareEntry> {
        labels
            .iter()
            .map(|label| CompareEntry {
                label: label.to_string(),
                file: PathBuf::from(format!("{label}.json")),
            })
            .collect()
    }

    #[test]
    fn compare_name_defaults_to_the_labels_joined() {
        let entries = entries_labelled(&["tls-none", "tls-client", "tls-all"]);
        assert_eq!(
            compare_name(&entries, None).unwrap(),
            "tls-none-vs-tls-client-vs-tls-all"
        );
    }

    #[test]
    fn compare_name_uses_the_given_name_however_many_entries() {
        let labels: Vec<String> = (0..24).map(|i| format!("columnar-read-T{i}")).collect();
        let labels: Vec<&str> = labels.iter().map(String::as_str).collect();
        assert_eq!(
            compare_name(&entries_labelled(&labels), Some("read-strategies")).unwrap(),
            "read-strategies"
        );
    }

    #[test]
    fn compare_name_rejects_a_default_name_too_long_and_suggests_name() {
        let labels: Vec<String> = (0..24).map(|i| format!("columnar-read-T{i}")).collect();
        let labels: Vec<&str> = labels.iter().map(String::as_str).collect();

        let err = compare_name(&entries_labelled(&labels), None)
            .unwrap_err()
            .to_string();

        assert!(err.contains("the 24 labels joined"), "{err}");
        assert!(err.contains("over the 255-byte file name limit"), "{err}");
        assert!(err.contains("Use --name NAME"), "{err}");
    }

    #[test]
    fn compare_name_limit_counts_the_longest_output_file() {
        // compare- (8) + name + -resources.html (15) may total 255 bytes.
        let fits = "x".repeat(232);
        assert_eq!(
            compare_name(&entries_labelled(&["a", "b"]), Some(&fits)).unwrap(),
            fits
        );

        let err = compare_name(&entries_labelled(&["a", "b"]), Some(&"x".repeat(233)))
            .unwrap_err()
            .to_string();
        assert!(err.contains("The --name given is too long"), "{err}");
        assert!(err.contains("256 bytes"), "{err}");
        assert!(err.contains("at most 232 bytes"), "{err}");
    }

    #[test]
    fn compare_name_rejects_an_empty_name_or_a_path() {
        let entries = entries_labelled(&["a", "b"]);
        assert!(compare_name(&entries, Some("")).is_err());
        let err = compare_name(&entries, Some("out/x"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("path separator"), "{err}");
    }

    #[test]
    fn write_combined_csv_is_named_after_the_comparison() {
        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("first.json");
        let second = tmp.path().join("second.json");
        write(&first, &run_result("a"));
        write(&second, &run_result("b"));
        let entries = vec![
            CompareEntry {
                label: "a".into(),
                file: first,
            },
            CompareEntry {
                label: "b".into(),
                file: second,
            },
        ];

        let csv_path =
            write_combined_csv(&entries, "my-sweep", &tmp.path().to_string_lossy()).unwrap();

        assert_eq!(csv_path, tmp.path().join("compare-my-sweep.csv"));
        assert!(csv_path.is_file());
    }

    #[test]
    fn script_args_pass_every_entry_and_the_output() {
        let entries = vec![
            CompareEntry {
                label: "A".into(),
                file: PathBuf::from("r/a.json"),
            },
            CompareEntry {
                label: "B".into(),
                file: PathBuf::from("r/b.json"),
            },
        ];
        assert_eq!(
            script_args(
                Path::new("/repo"),
                RESOURCES_SCRIPT,
                &entries,
                "A-vs-B",
                "out"
            ),
            vec![
                "/repo/charts/generate_compare_resource_charts.py",
                "--entry",
                "A=r/a.json",
                "--entry",
                "B=r/b.json",
                "--name",
                "A-vs-B",
                "--output",
                "out",
            ]
        );
    }
}
