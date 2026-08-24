use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn validate(root: &Path) -> Result<(), String> {
    let mut files = Vec::new();
    collect_markdown_files(root, &mut files)?;

    let spec_root = fs::canonicalize(root.join("spec"))
        .map_err(|error| format!("failed to resolve spec directory: {error}"))?;

    for file in files {
        let content = fs::read_to_string(&file)
            .map_err(|error| format!("failed to read {}: {error}", file.display()))?;
        let is_spec = file.starts_with(root.join("spec"));

        if is_spec {
            validate_spec_content(root, &file, &content)?;
        }

        let reference_targets = reference_style_local_targets(&content);
        if let Some(target) = reference_targets.first() {
            return Err(format!(
                "repository-local Markdown links must use inline relative syntax in {}: {target}",
                relative(root, &file).display()
            ));
        }

        for target in markdown_link_targets(&content) {
            let Some(local_target) = local_markdown_target(&target) else {
                continue;
            };
            let parent = file
                .parent()
                .ok_or_else(|| format!("{} has no parent directory", file.display()))?;
            let candidate = parent.join(local_target);
            if !candidate.exists() {
                return Err(format!(
                    "broken Markdown link in {}: {target}",
                    relative(root, &file).display()
                ));
            }

            if is_spec {
                let resolved = fs::canonicalize(&candidate).map_err(|error| {
                    format!(
                        "failed to resolve Markdown link {target} in {}: {error}",
                        relative(root, &file).display()
                    )
                })?;
                if !resolved.starts_with(&spec_root) {
                    return Err(format!(
                        "normative spec link escapes spec/: {} -> {target}",
                        relative(root, &file).display()
                    ));
                }
            }
        }
    }

    println!("documentation validation passed");
    Ok(())
}

fn validate_spec_content(root: &Path, file: &Path, content: &str) -> Result<(), String> {
    const FORBIDDEN: &[&str] = &[
        "ROADMAP.md",
        "CONTRIBUTING.md",
        "AGENTS.md",
        "TESTING.md",
        "ARCHITECTURE.md",
        "docs/",
        "crates/",
        "tools/",
        "cargo validate",
        "Runenwerk source",
        "engine_net",
        "engine_net_quic",
    ];

    for marker in FORBIDDEN {
        if content.contains(marker) {
            return Err(format!(
                "normative spec contains repository/implementation marker {marker:?}: {}",
                relative(root, file).display()
            ));
        }
    }

    Ok(())
}

fn collect_markdown_files(directory: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    let mut entries = fs::read_dir(directory)
        .map_err(|error| format!("failed to read {}: {error}", directory.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to enumerate {}: {error}", directory.display()))?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?;
        if file_type.is_dir() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name != ".git" && name != "target" {
                collect_markdown_files(&path, files)?;
            }
        } else if file_type.is_file()
            && path.extension().and_then(|extension| extension.to_str()) == Some("md")
        {
            files.push(path);
        }
    }

    Ok(())
}

fn markdown_link_targets(content: &str) -> Vec<String> {
    markdown_lines_outside_fences(content)
        .flat_map(inline_link_targets)
        .collect()
}

fn reference_style_local_targets(content: &str) -> Vec<String> {
    markdown_lines_outside_fences(content)
        .filter_map(reference_definition_target)
        .filter(|target| local_markdown_target(target).is_some())
        .collect()
}

fn markdown_lines_outside_fences(content: &str) -> impl Iterator<Item = &str> {
    let mut active_fence: Option<&'static str> = None;

    content.lines().filter(move |line| {
        let trimmed = line.trim_start();
        let marker = if trimmed.starts_with("```") {
            Some("```")
        } else if trimmed.starts_with("~~~") {
            Some("~~~")
        } else {
            None
        };

        if let Some(marker) = marker {
            match active_fence {
                None => active_fence = Some(marker),
                Some(active) if active == marker => active_fence = None,
                Some(_) => {}
            }
            return false;
        }

        active_fence.is_none()
    })
}

fn inline_link_targets(line: &str) -> Vec<String> {
    let mut targets = Vec::new();
    let mut cursor = 0;

    while let Some(relative_start) = line[cursor..].find("](") {
        let start = cursor + relative_start + 2;
        let Some(relative_end) = line[start..].find(')') else {
            break;
        };
        let end = start + relative_end;
        if let Some(target) = normalized_markdown_target(&line[start..end]) {
            targets.push(target.to_owned());
        }
        cursor = end + 1;
    }

    targets
}

fn reference_definition_target(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let rest = trimmed.strip_prefix('[')?;
    if rest.starts_with('^') {
        return None;
    }
    let marker_end = rest.find("]:")?;
    normalized_markdown_target(&rest[marker_end + 2..]).map(str::to_owned)
}

fn normalized_markdown_target(raw: &str) -> Option<&str> {
    let raw = raw.trim();
    let target = if raw.starts_with('<') {
        raw.strip_prefix('<')?.split_once('>')?.0
    } else {
        raw.split_whitespace().next()?
    };
    (!target.is_empty()).then_some(target)
}

fn local_markdown_target(target: &str) -> Option<&str> {
    if target.starts_with('#')
        || target.starts_with("http://")
        || target.starts_with("https://")
        || target.starts_with("mailto:")
        || target.starts_with("data:")
    {
        return None;
    }

    let path = target.split('#').next().unwrap_or("");
    (!path.is_empty() && !Path::new(path).is_absolute()).then_some(path)
}

fn relative<'a>(root: &'a Path, path: &'a Path) -> &'a Path {
    path.strip_prefix(root).unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::{local_markdown_target, markdown_link_targets, reference_style_local_targets};

    #[test]
    fn extracts_inline_markdown_links_outside_fences() {
        let content = "[one](a.md)\n```md\n[ignored](missing.md)\n```\n[two](../b.md#section)";
        assert_eq!(
            markdown_link_targets(content),
            vec!["a.md", "../b.md#section"]
        );
    }

    #[test]
    fn finds_reference_style_local_targets() {
        let content = "[local]: ./local.md\n[web]: https://example.com";
        assert_eq!(reference_style_local_targets(content), vec!["./local.md"]);
    }

    #[test]
    fn classifies_local_targets() {
        assert_eq!(local_markdown_target("a.md#section"), Some("a.md"));
        assert_eq!(local_markdown_target("#section"), None);
        assert_eq!(local_markdown_target("https://example.com"), None);
    }
}
