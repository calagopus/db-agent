use std::{io::Write, path::Path, process::Command};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=database/migrations");
    println!("cargo:rerun-if-env-changed=DB_AGENT_UPGRADE_PROVIDER");
    println!("cargo:rerun-if-env-changed=DB_AGENT_UPGRADE_REPOSITORY");
    println!("cargo:rerun-if-env-changed=DB_AGENT_UPGRADE_API_URL");
    println!("cargo:rerun-if-env-changed=RELAY_BINARY_PATH");
    println!("cargo:rerun-if-changed=relay/src");
    println!("cargo:rerun-if-changed=relay/Cargo.toml");

    let target_arch =
        std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_else(|_| "unknown".to_string());
    let target_env =
        std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_else(|_| "unknown".to_string());

    println!("cargo:rustc-env=CARGO_TARGET={target_arch}-{target_env}");

    handle_git_info();
    handle_upgrade_source();
    handle_relay(&target_arch);
}

#[allow(clippy::panic)]
fn handle_relay(target_arch: &str) {
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR is set by cargo");
    let out_path = Path::new(&out_dir).join("relay.gz");
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("set by cargo");

    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux") {
        std::fs::write(&out_path, []).expect("failed to write relay placeholder");
        return;
    }

    let binary_path = match std::env::var("RELAY_BINARY_PATH") {
        Ok(path) if !path.is_empty() => Path::new(&manifest_dir).join(path),
        _ => {
            let musl_arch = match target_arch {
                "riscv64" => "riscv64gc",
                "powerpc64" => "powerpc64le",
                arch => arch,
            };
            let target = format!("{musl_arch}-unknown-linux-musl");
            let target_dir = Path::new(&manifest_dir).join("target").join("relay");

            let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
                .args(["build", "-p", "relay", "--release", "--target", &target])
                .env("CARGO_TARGET_DIR", &target_dir)
                .env("RUSTFLAGS", "-C target-feature=+crt-static")
                .env_remove("CARGO_ENCODED_RUSTFLAGS")
                .status();
            if !status.is_ok_and(|status| status.success()) {
                panic!(
                    "failed to build the relay for {target}, install the target or set RELAY_BINARY_PATH to a prebuilt static relay"
                );
            }

            target_dir.join(target).join("release").join("relay")
        }
    };

    let binary = std::fs::read(&binary_path).unwrap_or_else(|err| {
        panic!(
            "failed to read relay binary {}: {err}",
            binary_path.display()
        )
    });
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    encoder
        .write_all(&binary)
        .expect("failed to compress relay");
    std::fs::write(
        &out_path,
        encoder.finish().expect("failed to compress relay"),
    )
    .expect("failed to write compressed relay");
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

#[allow(clippy::panic)]
fn handle_upgrade_source() {
    let read = |name: &str, default: &str| {
        let value = std::env::var(name).unwrap_or_default();
        if value.is_empty() {
            default.to_string()
        } else {
            value
        }
    };

    let provider = read("DB_AGENT_UPGRADE_PROVIDER", "github");
    let repository = read("DB_AGENT_UPGRADE_REPOSITORY", "calagopus/db-agent");
    let api_url = read("DB_AGENT_UPGRADE_API_URL", "https://api.github.com");

    if !matches!(provider.as_str(), "github") {
        panic!("unsupported DB_AGENT_UPGRADE_PROVIDER {provider:?}, expected one of: github");
    }
    if repository.split('/').count() != 2 || repository.split('/').any(str::is_empty) {
        panic!("DB_AGENT_UPGRADE_REPOSITORY {repository:?} must be in the form owner/repo");
    }
    if !api_url.starts_with("https://") && !api_url.starts_with("http://") {
        panic!("DB_AGENT_UPGRADE_API_URL {api_url:?} must be an http(s) url");
    }

    println!("cargo:rustc-env=DB_AGENT_UPGRADE_PROVIDER={provider}");
    println!("cargo:rustc-env=DB_AGENT_UPGRADE_REPOSITORY={repository}");
    println!(
        "cargo:rustc-env=DB_AGENT_UPGRADE_API_URL={}",
        api_url.trim_end_matches('/')
    );
}
