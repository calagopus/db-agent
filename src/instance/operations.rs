use super::websocket::{WebsocketEvent, WebsocketMessage};
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};
use tokio::sync::{RwLock, RwLockReadGuard};
use utoipa::ToSchema;

fn serialize_arc<S>(value: &Arc<AtomicU64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_u64(value.load(std::sync::atomic::Ordering::Relaxed))
}

#[derive(Clone, ToSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum DatabaseOperation {
    RemoteImport {
        source_host: String,
        source_db: Option<String>,
        db: Option<String>,
        wipe: bool,

        start_time: chrono::DateTime<chrono::Utc>,
        #[serde(serialize_with = "serialize_arc")]
        #[schema(value_type = u64)]
        bytes_processed: Arc<AtomicU64>,
    },
}

pub struct Operation {
    pub database_operation: DatabaseOperation,
    abort_sender: tokio::sync::oneshot::Sender<()>,
}

pub struct OperationManager {
    operations: Arc<RwLock<HashMap<uuid::Uuid, Operation>>>,
    sender: tokio::sync::broadcast::Sender<WebsocketMessage>,
}

impl OperationManager {
    pub fn new(sender: tokio::sync::broadcast::Sender<WebsocketMessage>) -> Self {
        Self {
            operations: Arc::new(RwLock::new(HashMap::new())),
            sender,
        }
    }

    #[inline]
    pub async fn operations(&self) -> RwLockReadGuard<'_, HashMap<uuid::Uuid, Operation>> {
        self.operations.read().await
    }

    pub async fn add_operation<
        T: Send + 'static,
        F: Future<Output = Result<T, anyhow::Error>> + Send + 'static,
    >(
        &self,
        operation: DatabaseOperation,
        f: F,
    ) -> (
        uuid::Uuid,
        tokio::task::JoinHandle<Option<Result<T, anyhow::Error>>>,
    ) {
        let operation_uuid = uuid::Uuid::new_v4();
        let (abort_sender, abort_receiver) = tokio::sync::oneshot::channel();

        self.operations.write().await.insert(
            operation_uuid,
            Operation {
                database_operation: operation.clone(),
                abort_sender,
            },
        );

        let handle = tokio::spawn({
            let operations = Arc::clone(&self.operations);
            let sender = self.sender.clone();

            async move {
                let progress_task = async {
                    loop {
                        sender
                            .send(
                                WebsocketMessage::builder(WebsocketEvent::OperationProgress)
                                    .arg(operation_uuid.to_string())
                                    .structured_arg(&operation)
                                    .build(),
                            )
                            .ok();

                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                };

                let result = tokio::select! {
                    result = f => Some(result),
                    _ = progress_task => None,
                    _ = abort_receiver => None,
                };

                operations.write().await.remove(&operation_uuid);

                if result.is_none() {
                    sender
                        .send(
                            WebsocketMessage::builder(WebsocketEvent::OperationAborted)
                                .arg(operation_uuid.to_string())
                                .build(),
                        )
                        .ok();
                } else if let Some(Err(err)) = result.as_ref() {
                    let message = if let Some(err) =
                        err.downcast_ref::<crate::response::DisplayError>()
                    {
                        err.message.to_string()
                    } else if let Some(err) = err.downcast_ref::<&str>() {
                        err.to_string()
                    } else if let Some(err) = err.downcast_ref::<String>() {
                        err.to_string()
                    } else if let Some(err) = err.downcast_ref::<std::io::Error>() {
                        err.to_string()
                    } else {
                        tracing::error!(operation = %operation_uuid, "unknown operation error: {err:?}");

                        String::from("unknown error")
                    };

                    sender
                        .send(
                            WebsocketMessage::builder(WebsocketEvent::OperationError)
                                .arg(operation_uuid.to_string())
                                .arg(message)
                                .build(),
                        )
                        .ok();
                } else {
                    sender
                        .send(
                            WebsocketMessage::builder(WebsocketEvent::OperationCompleted)
                                .arg(operation_uuid.to_string())
                                .build(),
                        )
                        .ok();
                }

                result
            }
        });

        (operation_uuid, handle)
    }

    pub async fn abort_operation(&self, operation_uuid: uuid::Uuid) -> bool {
        if let Some(operation) = self.operations.write().await.remove(&operation_uuid) {
            operation.abort_sender.send(()).ok();
            return true;
        }

        false
    }
}
