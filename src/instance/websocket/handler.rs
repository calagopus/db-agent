use axum::{
    body::Bytes,
    extract::{Extension, WebSocketUpgrade, ws::Message},
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Mutex, broadcast::error::RecvError};

const MAX_MISSED_PONGS: usize = 2;

pub async fn handle_ws(
    ws: WebSocketUpgrade,
    Extension(instance): Extension<crate::instance::Instance>,
) -> Response {
    ws.on_upgrade(move |socket| async move {
        let (sender, mut receiver) = socket.split();
        let sender = Arc::new(Mutex::new(sender));
        let missed_pongs = Arc::new(AtomicUsize::new(0));

        let websocket_handler = Arc::new(super::InstanceWebsocketHandler::new(Arc::clone(&sender)));

        let reader = {
            let websocket_handler = Arc::clone(&websocket_handler);
            let missed_pongs = Arc::clone(&missed_pongs);
            let instance = instance.clone();

            async move {
                loop {
                    let data = match receiver.next().await {
                        Some(Ok(data)) => {
                            missed_pongs.store(0, Ordering::Relaxed);
                            data
                        }
                        Some(Err(err)) => {
                            tracing::debug!(
                                instance = %instance.uuid,
                                "error receiving websocket message: {err}"
                            );
                            break;
                        }
                        None => break,
                    };

                    let payload = match data {
                        Message::Close(_) => break,
                        Message::Text(payload) => payload,
                        _ => continue,
                    };

                    if payload.len() > crate::BUFFER_SIZE {
                        tracing::warn!(
                            instance = %instance.uuid,
                            "got massive websocket message from client, {} bytes",
                            payload.len()
                        );
                        continue;
                    }

                    let message = match serde_json::from_str::<super::WebsocketMessage>(&payload) {
                        Ok(message) => message,
                        Err(err) => {
                            tracing::debug!(
                                instance = %instance.uuid,
                                "received unparsable websocket message: {err}"
                            );
                            continue;
                        }
                    };

                    if let Err(err) = super::message_handler::handle_message(
                        &instance,
                        &websocket_handler,
                        message,
                    )
                    .await
                    {
                        tracing::error!(
                            instance = %instance.uuid,
                            "error handling websocket message: {err}"
                        );
                    }
                }
            }
        };

        let futures: [Pin<Box<dyn Future<Output = ()> + Send>>; 2] = [
            // Instance listener
            {
                let websocket_handler = Arc::clone(&websocket_handler);
                let mut receiver = instance.websocket.subscribe();
                let uuid = instance.uuid;

                Box::pin(async move {
                    loop {
                        match receiver.recv().await {
                            Ok(message) => websocket_handler.send_message(message).await,
                            Err(RecvError::Closed) => break,
                            Err(RecvError::Lagged(_)) => {
                                tracing::debug!(
                                    instance = %uuid,
                                    "websocket lagged behind, messages dropped"
                                );
                            }
                        }
                    }
                })
            },
            // Resource usage listener
            {
                let websocket_handler = Arc::clone(&websocket_handler);
                let mut resource_usage = instance.subscribe_resource_usage();

                Box::pin(async move {
                    let mut last_state = None;

                    while resource_usage.changed().await.is_ok() {
                        let usage = *resource_usage.borrow_and_update();

                        if last_state != Some(usage.state) {
                            last_state = Some(usage.state);

                            websocket_handler
                                .send_message(
                                    super::WebsocketMessage::builder(
                                        super::WebsocketEvent::InstanceStatus,
                                    )
                                    .arg(usage.state.to_str())
                                    .build(),
                                )
                                .await;
                        }

                        websocket_handler
                            .send_message(
                                super::WebsocketMessage::builder(
                                    super::WebsocketEvent::InstanceStats,
                                )
                                .structured_arg(usage)
                                .build(),
                            )
                            .await;

                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                })
            },
        ];

        let pinger = {
            let uuid = instance.uuid;

            async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;

                    if missed_pongs.fetch_add(1, Ordering::Relaxed) >= MAX_MISSED_PONGS {
                        tracing::debug!(
                            instance = %uuid,
                            "websocket ping timeout, closing dead connection"
                        );
                        break;
                    }

                    let ping = sender
                        .lock()
                        .await
                        .send(Message::Ping(Bytes::from_static(&[1, 2, 3])))
                        .await;

                    if ping.is_err() {
                        tracing::debug!(
                            instance = %uuid,
                            "websocket ping failed, closing dead connection"
                        );
                        break;
                    }
                }
            }
        };

        tokio::select! {
            _ = reader => {
                tracing::debug!(instance = %instance.uuid, "websocket reader finished");
            }
            _ = futures_util::future::join_all(futures) => {
                tracing::debug!(instance = %instance.uuid, "websocket handles finished");
            }
            _ = pinger => {
                tracing::debug!(instance = %instance.uuid, "websocket pinger finished");
            }
        }
    })
}
