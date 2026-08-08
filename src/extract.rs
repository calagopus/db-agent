use crate::response::ApiResponse;
use axum::{
    extract::{FromRequestParts, path::ErrorKind, rejection},
    http::request::Parts,
    response::IntoResponse,
};
use serde::de::DeserializeOwned;

pub struct ExtractRejection(String);

impl IntoResponse for ExtractRejection {
    fn into_response(self) -> axum::response::Response {
        ApiResponse::error(&self.0)
            .with_status(axum::http::StatusCode::BAD_REQUEST)
            .into_response()
    }
}

pub struct Path<T>(pub T);

impl<T: DeserializeOwned + Send, S: Send + Sync> FromRequestParts<S> for Path<T> {
    type Rejection = ExtractRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match axum::extract::Path::<T>::from_request_parts(parts, state).await {
            Ok(axum::extract::Path(value)) => Ok(Self(value)),
            Err(rejection) => Err(ExtractRejection(path_message(rejection))),
        }
    }
}

pub struct Query<T>(pub T);

impl<T: DeserializeOwned, S: Send + Sync> FromRequestParts<S> for Query<T> {
    type Rejection = ExtractRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match axum::extract::Query::<T>::from_request_parts(parts, state).await {
            Ok(axum::extract::Query(value)) => Ok(Self(value)),
            Err(rejection) => {
                let cause = std::error::Error::source(&rejection)
                    .map_or_else(|| rejection.to_string(), |err| err.to_string());

                Err(ExtractRejection(format!("invalid query string: {cause}")))
            }
        }
    }
}

fn path_message(rejection: rejection::PathRejection) -> String {
    let rejection::PathRejection::FailedToDeserializePathParams(err) = rejection else {
        return "invalid path".to_string();
    };

    match err.into_kind() {
        ErrorKind::DeserializeError { key, .. }
        | ErrorKind::ParseErrorAtKey { key, .. }
        | ErrorKind::InvalidUtf8InPathParam { key } => format!("invalid {key} uuid"),
        _ => "invalid path".to_string(),
    }
}
