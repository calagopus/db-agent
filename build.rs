use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=database/migrations");

    let target_arch =
        std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_else(|_| "unknown".to_string());
    let target_env =
        std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_else(|_| "unknown".to_string());

    println!("cargo:rustc-env=CARGO_TARGET={target_arch}-{target_env}");

    handle_git_info();
}

fn handle_git_info() {
    let is_git_repo = Command::new("git")
        .args(["rev-parse", "--is-inside-work-tree"])
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false);

    let mut git_hash = "unknown".to_string();

    if is_git_repo {
        println!("cargo:rerun-if-changed=.git/HEAD");

        if let Ok(head) = std::fs::read_to_string(".git/HEAD")
            && head.starts_with("ref: ")
        {
            let head_ref = head.trim_start_matches("ref: ").trim();
            println!("cargo:rerun-if-changed=.git/{head_ref}");
            println!(
                "cargo:rustc-env=CARGO_GIT_BRANCH={}",
                head_ref.rsplit('/').next().unwrap_or("unknown")
            );
        } else {
            println!("cargo:rustc-env=CARGO_GIT_BRANCH=unknown");
        }
        println!("cargo:rerun-if-changed=.git/index");

        if let Ok(output) = Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            && output.status.success()
        {
            git_hash = String::from_utf8_lossy(&output.stdout).trim().to_string();
        }
    }
    println!("cargo:rustc-env=CARGO_GIT_COMMIT={git_hash}");
}
