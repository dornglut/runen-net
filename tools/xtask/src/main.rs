mod documentation;

use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn main() {
    let mut arguments = env::args().skip(1);
    match (arguments.next().as_deref(), arguments.next()) {
        (Some("validate"), None) => {
            if let Err(error) = validate() {
                eprintln!("validation failed: {error}");
                std::process::exit(1);
            }
        }
        _ => {
            eprintln!("usage: cargo validate");
            std::process::exit(2);
        }
    }
}

fn validate() -> Result<(), String> {
    let root = repository_root()?;
    let before = repository_status(&root)?;

    run_captured(
        &root,
        "Cargo metadata",
        "cargo",
        &["metadata", "--format-version", "1", "--locked", "--no-deps"],
    )?;
    documentation::validate(&root)?;
    run(
        &root,
        "formatting",
        "cargo",
        &["fmt", "--all", "--", "--check"],
    )?;
    run(
        &root,
        "locked workspace tests",
        "cargo",
        &["test", "--workspace", "--all-targets", "--locked"],
    )?;
    run(
        &root,
        "Clippy",
        "cargo",
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ],
    )?;
    run(&root, "diff hygiene", "git", &["diff", "--check"])?;

    let after = repository_status(&root)?;
    if after != before {
        return Err(format!(
            "validation changed repository state\nbefore:\n{}\nafter:\n{}",
            String::from_utf8_lossy(&before),
            String::from_utf8_lossy(&after)
        ));
    }

    println!("repository validation passed");
    Ok(())
}

fn repository_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| "xtask manifest must live at <repository>/tools/xtask".to_owned())
}

fn repository_status(root: &Path) -> Result<Vec<u8>, String> {
    Ok(run_captured(
        root,
        "repository status",
        "git",
        &["status", "--porcelain=v1", "--untracked-files=all"],
    )?
    .stdout)
}

fn run(root: &Path, label: &str, program: &str, arguments: &[&str]) -> Result<(), String> {
    let status = Command::new(program)
        .args(arguments)
        .current_dir(root)
        .status()
        .map_err(|error| format!("failed to start {label}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "{label} failed with status {}",
            status
                .code()
                .map_or_else(|| "signal".to_owned(), |code| code.to_string())
        ))
    }
}

fn run_captured(
    root: &Path,
    label: &str,
    program: &str,
    arguments: &[&str],
) -> Result<Output, String> {
    let output = Command::new(program)
        .args(arguments)
        .current_dir(root)
        .output()
        .map_err(|error| format!("failed to start {label}: {error}"))?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(format!(
            "{label} failed with status {}\n{}",
            output
                .status
                .code()
                .map_or_else(|| "signal".to_owned(), |code| code.to_string()),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}
