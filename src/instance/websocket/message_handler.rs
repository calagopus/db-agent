use super::{WebsocketEvent, WebsocketMessage};
use futures_util::StreamExt;
use std::sync::Arc;

pub async fn handle_message(
    instance: &crate::instance::Instance,
    websocket_handler: &Arc<super::InstanceWebsocketHandler>,
    message: WebsocketMessage,
) -> Result<(), anyhow::Error> {
    match message.event {
        WebsocketEvent::SendStats => {
            websocket_handler
                .send_message(
                    WebsocketMessage::builder(WebsocketEvent::InstanceStats)
                        .structured_arg(instance.resource_usage())
                        .build(),
                )
                .await;
        }
        WebsocketEvent::SendStatus => {
            websocket_handler
                .send_message(
                    WebsocketMessage::builder(WebsocketEvent::InstanceStatus)
                        .arg(instance.resource_usage().state.to_str())
                        .build(),
                )
                .await;
        }
        WebsocketEvent::SendLogs => {
            let mut log_stream = instance
                .logs_lines(Some(instance.app_state.config.load().websocket_log_count))
                .await;

            while let Some(Ok(line)) = log_stream.next().await {
                websocket_handler
                    .send_message(
                        WebsocketMessage::builder(WebsocketEvent::InstanceConsoleOutput)
                            .arg(line.trim())
                            .build(),
                    )
                    .await;
            }
        }
        WebsocketEvent::SetState => {
            let Some(action) = message.args.first() else {
                return Ok(());
            };

            let result = match action.as_str() {
                "start" => instance.start().await,
                "stop" => instance.stop().await,
                "kill" => instance.kill().await,
                "restart" => match instance.stop().await {
                    Ok(()) => instance.start().await,
                    Err(err) => Err(err),
                },
                _ => {
                    tracing::debug!(instance = %instance.uuid, "received unknown power action: {action}");

                    return Ok(());
                }
            };

            if let Err(err) = result {
                tracing::error!(instance = %instance.uuid, "failed to run power action: {err}");

                websocket_handler
                    .send_error(&format!("failed to run power action: {err}"))
                    .await;
            }
        }
        _ => {
            tracing::debug!(
                instance = %instance.uuid,
                "received websocket message that will not be handled: {message:?}"
            );
        }
    }

    Ok(())
}
