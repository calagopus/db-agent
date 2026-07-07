use chrono::Datelike;
use clap::Args;

#[derive(Args)]
pub struct VersionArgs;

pub struct VersionCommand;

impl crate::commands::CliCommand<VersionArgs> for VersionCommand {
    fn get_command(&self, command: clap::Command) -> clap::Command {
        command
    }

    fn get_executor(self) -> Box<crate::commands::ExecutorFunc> {
        Box::new(|_config, _arg_matches| {
            Box::pin(async move {
                println!(
                    "github.com/calagopus/db-agent {} ({})",
                    crate::full_version(),
                    crate::TARGET
                );
                println!(
                    "copyright © 2025 - {} Calagopus & Contributors",
                    chrono::Local::now().year()
                );

                Ok(0)
            })
        })
    }
}
