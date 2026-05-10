use std::fs;
use std::path::Path;

#[test]
fn services_crate_remains_ui_free() {
    let manifest = fs::read_to_string("Cargo.toml").expect("read zqlz-services Cargo.toml");
    assert!(
        !manifest
            .lines()
            .any(|line| line.trim_start().starts_with("gpui")),
        "zqlz-services must not depend on gpui"
    );

    let source_files = rust_source_files(Path::new("src"));
    assert!(!source_files.is_empty(), "expected service source files");
    for source_file in source_files {
        let source = fs::read_to_string(&source_file).expect("read service source");
        assert!(
            !source.contains("gpui::") && !source.contains("use gpui"),
            "{} imports GPUI; move UI concerns to zqlz-app",
            source_file.display()
        );
    }
}

fn rust_source_files(root: &Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];

    while let Some(path) = pending.pop() {
        let entries = fs::read_dir(&path).unwrap_or_else(|error| {
            panic!("read directory {}: {error}", path.display());
        });
        for entry in entries {
            let entry = entry.expect("read directory entry");
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    files
}
