use clap::{Args, FromArgMatches};
use colored::Colorize;
use dialoguer::{Confirm, theme::ColorfulTheme};
use serde::Deserialize;
use std::fmt::Write;

#[derive(Args)]
pub struct DiagnosticsArgs {
    #[arg(
        short = 'l',
        long = "log-lines",
        help = "number of log lines to include in the report",
        default_value_t = 100
    )]
    pub log_lines: usize,
}

pub struct DiagnosticsCommand;

impl crate::commands::CliCommand<DiagnosticsArgs> for DiagnosticsCommand {
    fn get_command(&self, command: clap::Command) -> clap::Command {
        command
    }

    fn get_executor(self) -> Box<crate::commands::ExecutorFunc> {
        Box::new(|_config, arg_matches| {
            Box::pin(async move {
                let args = DiagnosticsArgs::from_arg_matches(&arg_matches)?;

                let config_path = arg_matches
                    .get_one::<String>("config")
                    .expect("config path is required")
                    .to_string();

                let config = match crate::config::Config::open(&config_path) {
                    Ok(config) => config,
                    Err(err) => {
                        eprintln!("{}: {err:#}", "failed to load config".red());
                        return Ok(1);
                    }
                };
                let config = config.load();

                let include_endpoints = Confirm::with_theme(&ColorfulTheme::default())
                    .with_prompt(
                        "do you want to include endpoints (i.e. the bind address of your api)?",
                    )
                    .default(false)
                    .interact()?;

                let review_before_upload = Confirm::with_theme(&ColorfulTheme::default())
                    .with_prompt(
                        "do you want to review the collected data before uploading to pastes.dev?",
                    )
                    .default(true)
                    .interact()?;

                let versions = match bollard::Docker::connect_with_defaults() {
                    Ok(client) => client.version().await.unwrap_or_default(),
                    Err(_) => Default::default(),
                };

                let mut output = String::with_capacity(1024);
                writeln!(output, "db-agent - diagnostics report")?;

                write_header(&mut output, "versions")?;
                write_line(&mut output, "db-agent", &crate::full_version())?;
                write_line(
                    &mut output,
                    "docker",
                    &versions.version.unwrap_or_else(|| "unknown".to_string()),
                )?;
                write_line(
                    &mut output,
                    "kernel",
                    &sysinfo::System::kernel_long_version(),
                )?;
                write_line(
                    &mut output,
                    "os",
                    &versions.os.unwrap_or_else(|| "unknown".to_string()),
                )?;

                write_header(&mut output, "configuration")?;
                write_line(&mut output, "api", &config.api.bind)?;
                write_line(
                    &mut output,
                    "api ssl enabled",
                    &config.api.ssl.enabled.to_string(),
                )?;
                writeln!(output)?;
                write_line(&mut output, "socket directory", &config.socket_dir)?;
                write_line(&mut output, "data directory", &config.data_dir)?;
                write_line(&mut output, "log directory", &config.log_dir)?;
                writeln!(output)?;
                for (name, subsystem) in [
                    ("postgres", config.postgres.enabled),
                    ("mariadb", config.mariadb.enabled),
                    ("mongodb", config.mongodb.enabled),
                    ("redis", config.redis.enabled),
                ] {
                    write_line(
                        &mut output,
                        name,
                        if subsystem { "enabled" } else { "disabled" },
                    )?;
                }
                writeln!(output)?;
                write_line(
                    &mut output,
                    "server time",
                    &chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                )?;
                write_line(
                    &mut output,
                    "timezone",
                    &format!("{}", chrono::Local::now().offset()),
                )?;
                write_line(&mut output, "debug mode", &config.debug.to_string())?;

                write_header(&mut output, "latest db-agent logs")?;
                match latest_log_lines(&config.log_dir, args.log_lines).await {
                    Ok(lines) => output.push_str(&lines),
                    Err(err) => writeln!(output, "failed to read log directory: {err}")?,
                }

                if !include_endpoints {
                    output = output
                        .replace(&config.api.bind, "{redacted}")
                        .replace(&config.api.token, "{redacted}");

                    if !config.api.ssl.cert.is_empty() {
                        output = output.replace(&config.api.ssl.cert, "{redacted}");
                    }
                    if !config.api.ssl.key.is_empty() {
                        output = output.replace(&config.api.ssl.key, "{redacted}");
                    }
                    if !config.database.url.is_empty() {
                        output = output.replace(&config.database.url, "{redacted}");
                    }
                }

                if review_before_upload {
                    println!("{output}");
                    let confirm = Confirm::with_theme(&ColorfulTheme::default())
                        .with_prompt("do you want to upload the diagnostics report to pastes.dev?")
                        .default(true)
                        .interact()?;

                    if !confirm {
                        return Ok(0);
                    }
                }

                let client = reqwest::Client::new();
                let response = match client
                    .post("https://api.pastes.dev/post")
                    .header(
                        "User-Agent",
                        format!("db-agent diagnostics/v{}", crate::VERSION),
                    )
                    .header("Content-Type", "text/plain")
                    .header("Accept", "application/json")
                    .body(output)
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        eprintln!("{}: {err}", "failed to upload diagnostics report".red());
                        return Ok(1);
                    }
                };

                #[derive(Deserialize)]
                struct Response {
                    key: String,
                }

                let response: Response = match response.json().await {
                    Ok(response) => response,
                    Err(err) => {
                        eprintln!(
                            "{}: {err}",
                            "failed to parse response from pastes.dev".red()
                        );
                        return Ok(1);
                    }
                };

                println!(
                    "uploaded diagnostics report to https://pastes.dev/{}",
                    response.key
                );

                Ok(0)
            })
        })
    }
}

async fn latest_log_lines(log_dir: &str, count: usize) -> anyhow::Result<String> {
    let mut newest: Option<(std::time::SystemTime, std::path::PathBuf)> = None;

    let mut entries = tokio::fs::read_dir(log_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("db-agent") || !name.ends_with("log") {
            continue;
        }

        let modified = entry.metadata().await?.modified()?;
        if newest.as_ref().is_none_or(|(t, _)| modified > *t) {
            newest = Some((modified, entry.path()));
        }
    }

    let Some((_, path)) = newest else {
        return Ok("no log files found\n".to_string());
    };

    let contents = tokio::fs::read_to_string(path).await?;
    let lines: Vec<&str> = contents.lines().collect();
    let start = lines.len().saturating_sub(count);

    let mut out = String::new();
    for line in &lines[start..] {
        out.push_str(&strip_ansi(line));
        out.push('\n');
    }

    Ok(out)
}

fn strip_ansi(line: &str) -> String {
    let mut result = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            result.push(c);
        }
    }

    result
}

#[inline]
fn write_header(output: &mut String, name: &str) -> Result<(), std::fmt::Error> {
    writeln!(output, "\n|\n| {name}")?;
    writeln!(output, "| ------------------------------")
}

#[inline]
fn write_line(output: &mut String, name: &str, value: &str) -> Result<(), std::fmt::Error> {
    writeln!(output, "{name:>20}: {value}")
}
