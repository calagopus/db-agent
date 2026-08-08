use base64::{Engine, engine::general_purpose::STANDARD as B64};
use clap::{Args, FromArgMatches};
use colored::Colorize;
use dialoguer::{Confirm, Input, theme::ColorfulTheme};
use std::sync::Arc;

#[derive(Args)]
pub struct ConfigureArgs {
    #[arg(
        short = 'o',
        long = "override",
        help = "override the current configuration if it exists"
    )]
    pub r#override: bool,

    #[arg(long = "join-data", help = "base64 encoded join data from the panel")]
    pub join_data: Option<String>,

    #[arg(long = "token", help = "the API token clients authenticate with")]
    pub token: Option<String>,
}

fn apply(
    config: Option<&Arc<crate::config::Config>>,
    patch: serde_json::Value,
) -> anyhow::Result<crate::config::InnerConfig> {
    let mut inner = match config {
        Some(config) => serde_json::to_value(&**config.load())?,
        None => serde_json::to_value(crate::config::InnerConfig::default())?,
    };

    json_patch::merge(&mut inner, &patch);

    Ok(serde_json::from_value(inner)?)
}

pub struct ConfigureCommand;

impl crate::commands::CliCommand<ConfigureArgs> for ConfigureCommand {
    fn get_command(&self, command: clap::Command) -> clap::Command {
        command
    }

    fn get_executor(self) -> Box<crate::commands::ExecutorFunc> {
        Box::new(|config, arg_matches| {
            Box::pin(async move {
                let args = ConfigureArgs::from_arg_matches(&arg_matches)?;

                let exists = config.exists();
                let config = match config {
                    crate::commands::ConfigState::Loaded(config) => Some(config),
                    crate::commands::ConfigState::Unparseable(err) => {
                        eprintln!(
                            "{}: {err}",
                            "the existing configuration could not be read, its values will not be kept"
                                .yellow()
                        );

                        None
                    }
                    crate::commands::ConfigState::Missing => None,
                };

                let config_path = match config.as_ref() {
                    Some(config) => config.path.clone(),
                    None => arg_matches
                        .get_one::<String>("config")
                        .cloned()
                        .or_else(|| crate::config::Config::find().map(String::from))
                        .unwrap_or_else(|| crate::config::Config::DEFAULT_PATH.to_string()),
                };

                if exists && !args.r#override {
                    let confirm = Confirm::with_theme(&ColorfulTheme::default())
                        .with_prompt(format!(
                            "do you want to {} the configuration at {config_path}?",
                            if config.is_some() {
                                "update"
                            } else {
                                "override"
                            }
                        ))
                        .default(false)
                        .interact()?;

                    if !confirm {
                        return Ok(1);
                    }
                }

                let patch = if let Some(join_data) = args.join_data {
                    let decoded = match B64.decode(&join_data) {
                        Ok(decoded) => decoded,
                        Err(_) => {
                            eprintln!("{}", "failed to decode join data!".red());
                            return Ok(1);
                        }
                    };

                    match serde_norway::from_slice(&decoded) {
                        Ok(patch) => patch,
                        Err(_) => {
                            eprintln!("{}", "failed to decode join data payload!".red());
                            return Ok(1);
                        }
                    }
                } else {
                    let token = match args.token {
                        Some(token) => token,
                        None => Input::with_theme(&ColorfulTheme::default())
                            .with_prompt("api token")
                            .with_initial_text(
                                config
                                    .as_ref()
                                    .map(|config| config.load().api.token.clone())
                                    .unwrap_or_default(),
                            )
                            .interact_text()?,
                    };

                    if token.is_empty() {
                        eprintln!("{}", "api token cannot be empty".red());
                        return Ok(1);
                    }

                    serde_json::json!({ "api": { "token": token } })
                };

                let inner = match apply(config.as_ref(), patch) {
                    Ok(inner) => inner,
                    Err(err) => {
                        eprintln!("{} {err:#}", "failed to apply configuration:".red());
                        return Ok(1);
                    }
                };

                crate::config::Config::save_new(&config_path, inner)?;

                println!(
                    "{}",
                    format!("successfully configured db-agent in {config_path}.").green()
                );

                Ok(0)
            })
        })
    }
}
