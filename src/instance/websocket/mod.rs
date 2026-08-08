use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, stream::SplitSink};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{SeqAccess, Visitor},
    ser::SerializeSeq,
};
use std::{error::Error, marker::PhantomData, sync::Arc};
use tokio::sync::Mutex;

pub mod handler;
mod message_handler;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum WebsocketEvent {
    #[serde(rename = "send stats")]
    SendStats,
    #[serde(rename = "send status")]
    SendStatus,
    #[serde(rename = "send logs")]
    SendLogs,
    #[serde(rename = "set state")]
    SetState,

    #[serde(rename = "stats")]
    InstanceStats,
    #[serde(rename = "status")]
    InstanceStatus,
    #[serde(rename = "console output")]
    InstanceConsoleOutput,
    #[serde(rename = "image pull progress")]
    InstanceImagePullProgress,
    #[serde(rename = "image pull completed")]
    InstanceImagePullCompleted,
    #[serde(rename = "daemon error")]
    InstanceDaemonError,
    #[serde(rename = "daemon message")]
    InstanceDaemonMessage,

    #[serde(rename = "operation progress")]
    OperationProgress,
    #[serde(rename = "operation completed")]
    OperationCompleted,
    #[serde(rename = "operation error")]
    OperationError,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebsocketMessage {
    pub event: WebsocketEvent,

    #[serde(default)]
    #[serde(deserialize_with = "string_vec_or_empty")]
    #[serde(serialize_with = "arc_vec")]
    pub args: Arc<[String]>,
}

impl WebsocketMessage {
    #[inline]
    pub fn builder(event: WebsocketEvent) -> WebsocketMessageBuilder {
        WebsocketMessageBuilder::new(event)
    }
}

pub struct WebsocketMessageBuilder {
    event: WebsocketEvent,
    args: Vec<String>,
}

impl WebsocketMessageBuilder {
    pub fn new(event: WebsocketEvent) -> Self {
        Self {
            event,
            args: Vec::new(),
        }
    }

    pub fn structured_arg(mut self, arg: impl Serialize) -> Self {
        match serde_json::to_string(&arg) {
            Ok(arg) => self.args.push(arg),
            Err(err) => tracing::warn!("failed to serialize websocket message argument: {err:?}"),
        }

        self
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn build(self) -> WebsocketMessage {
        WebsocketMessage {
            event: self.event,
            args: Arc::from(self.args),
        }
    }
}

fn string_vec_or_empty<'de, D>(deserializer: D) -> Result<Arc<[String]>, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringVecVisitor(PhantomData<[String]>);

    impl<'de> Visitor<'de> for StringVecVisitor {
        type Value = Arc<[String]>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string array or null")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut vec = Vec::new();
            while let Some(element) = seq.next_element::<Option<String>>()? {
                if let Some(value) = element {
                    vec.push(value);
                }
            }

            Ok(Arc::from(vec))
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Arc::new([]))
        }
    }

    deserializer.deserialize_any(StringVecVisitor(PhantomData))
}

fn arc_vec<S>(vec: &Arc<[String]>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let mut seq = serializer.serialize_seq(Some(vec.len()))?;
    for item in vec.iter() {
        seq.serialize_element(item)?;
    }

    seq.end()
}

pub struct InstanceWebsocketHandler {
    sender: Arc<Mutex<SplitSink<WebSocket, Message>>>,
}

impl InstanceWebsocketHandler {
    fn new(sender: Arc<Mutex<SplitSink<WebSocket, Message>>>) -> Self {
        Self { sender }
    }

    pub async fn send_message(&self, message: WebsocketMessage) {
        let message = match serde_json::to_string(&message) {
            Ok(message) => message,
            Err(err) => {
                tracing::error!("failed to serialize websocket message: {err:?}");
                return;
            }
        };

        if let Err(err) = self
            .sender
            .lock()
            .await
            .send(Message::Text(message.into()))
            .await
            && err
                .source()
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .is_none_or(|source| !crate::utils::is_silent_error(source))
        {
            tracing::error!("failed to send websocket message: {err:?}");
        }
    }

    async fn send_error(&self, message: &str) {
        self.send_message(
            WebsocketMessage::builder(WebsocketEvent::InstanceDaemonError)
                .arg(message)
                .build(),
        )
        .await;
    }
}
