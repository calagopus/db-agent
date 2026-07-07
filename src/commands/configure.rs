use anyhow::Context;
use clap::{Args, FromArgMatches};
use colored::Colorize;
use dialoguer::{Input, theme::ColorfulTheme};

#[derive(Args)]
pub struct ConfigureArgs {
    #[arg(long = "token", help = "the API token clients authenticate with")]
    pub token: Option<String>,
}

pub struct ConfigureCommand;

impl crate::commands::CliCommand<ConfigureArgs> for ConfigureCommand {
    fn get_command(&self, command: clap::Command) -> clap::Command {
        command
    }

    fn get_executor(self) -> Box<crate::commands::ExecutorFunc> {
        Box::new(|_config, arg_matches| {
            Box::pin(async move {
                let args = ConfigureArgs::from_arg_matches(&arg_matches)?;

                let config_path = arg_matches
                    .get_one::<String>("config")
                    .expect("config path is required")
                    .to_string();

                let mut inner = load_or_default(&config_path)?;

                let token = match args.token {
                    Some(token) => token,
                    None => Input::with_theme(&ColorfulTheme::default())
                        .with_prompt("api token")
                        .with_initial_text(inner.api.token.clone())
                        .interact_text()?,
                };

                if token.is_empty() {
                    eprintln!("{}", "api token cannot be empty".red());
                    return Ok(1);
                }

                inner.api.token = token;
                crate::config::Config::save_new(&config_path, &inner)?;

                println!(
                    "{}",
                    format!("wrote configuration to {config_path}").green()
                );

                Ok(0)
            })
        })
    }
}

fn load_or_default(path: &str) -> anyhow::Result<crate::config::InnerConfig> {
    if std::path::Path::new(path).exists() {
        let file =
            std::fs::File::open(path).context(format!("failed to open config file {path}"))?;
        serde_norway::from_reader(std::io::BufReader::new(file))
            .context(format!("failed to parse config file {path}"))
    } else {
        Ok(crate::config::InnerConfig::default())
    }
}
