use super::State;
use axum::routing::any;
use utoipa_axum::router::OpenApiRouter;

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .route("/", any(crate::instance::websocket::handler::handle_ws))
        .with_state(state.clone())
}
