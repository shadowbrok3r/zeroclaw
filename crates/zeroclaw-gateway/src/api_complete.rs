//! `POST /api/complete`: one model call through a `[[model_routes]]` hint, with no agent, session or memory.

use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use zeroclaw_config::schema::{Config, ModelRouteConfig};

use super::AppState;
use super::api::require_auth;

/// Hint used when a request names none.
const DEFAULT_HINT: &str = "quick";
/// Largest system plus prompt text accepted, in bytes.
const INPUT_CAP: usize = 64 * 1024;

#[derive(Debug, Deserialize)]
pub struct CompleteRequest {
    /// A `[[model_routes]]` hint; `quick` when absent.
    #[serde(default)]
    pub hint: Option<String>,
    #[serde(default)]
    pub system: Option<String>,
    pub prompt: String,
    #[serde(default)]
    pub temperature: Option<f64>,
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(serde_json::json!({ "error": message.into() }))).into_response()
}

/// The route a request's hint names.
fn route_for<'a>(config: &'a Config, hint: &str) -> Result<&'a ModelRouteConfig, String> {
    config
        .model_routes
        .iter()
        .find(|route| route.hint == hint)
        .ok_or_else(|| format!("no [[model_routes]] entry has hint `{hint}`"))
}

/// Rejects an empty prompt, an oversized input or an out-of-range temperature.
fn validate(request: &CompleteRequest) -> Result<(), String> {
    if request.prompt.trim().is_empty() {
        return Err("prompt is empty".into());
    }
    let size = request.prompt.len() + request.system.as_deref().map_or(0, str::len);
    if size > INPUT_CAP {
        return Err(format!("input is {size} bytes; the cap is {INPUT_CAP}"));
    }
    if request.temperature.is_some_and(|t| !(0.0..=2.0).contains(&t)) {
        return Err("temperature must be between 0.0 and 2.0".into());
    }
    Ok(())
}

pub async fn handle_complete(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Json<CompleteRequest>, JsonRejection>,
) -> Response {
    if let Err(e) = require_auth(&state, &headers) {
        return e.into_response();
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(e) => return error(StatusCode::BAD_REQUEST, format!("invalid body: {e}")),
    };
    if let Err(why) = validate(&request) {
        return error(StatusCode::BAD_REQUEST, why);
    }
    let config = state.config.read().clone();
    let hint = request
        .hint
        .as_deref()
        .map(str::trim)
        .filter(|hint| !hint.is_empty())
        .unwrap_or(DEFAULT_HINT)
        .to_string();
    let route = match route_for(&config, &hint) {
        Ok(route) => route.clone(),
        Err(why) => return error(StatusCode::BAD_REQUEST, why),
    };
    let resolved = match zeroclaw_providers::create_model_provider_from_ref_with_model(
        &config,
        &route.model_provider,
    ) {
        Ok(resolved) => resolved,
        Err(e) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("model_provider `{}` failed to build: {e}", route.model_provider),
            );
        }
    };
    let model = Some(route.model.trim())
        .filter(|model| !model.is_empty())
        .map(str::to_string)
        .or(resolved.model)
        .unwrap_or_default();
    match resolved
        .provider
        .chat_with_system(request.system.as_deref(), &request.prompt, &model, request.temperature)
        .await
    {
        Ok(response) => Json(serde_json::json!({ "response": response, "model": model, "hint": hint }))
            .into_response(),
        Err(e) => {
            let sanitized = zeroclaw_providers::sanitize_api_error(&e.to_string());
            ::zeroclaw_log::record!(
                ERROR,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail)
                    .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                    .with_attrs(::serde_json::json!({ "hint": hint, "model": model, "error": sanitized })),
                "complete model_provider error"
            );
            error(StatusCode::BAD_GATEWAY, sanitized)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(prompt: &str) -> CompleteRequest {
        CompleteRequest { hint: None, system: None, prompt: prompt.into(), temperature: None }
    }

    #[test]
    fn a_hint_resolves_only_to_a_configured_route() {
        let mut config = Config::default();
        config.model_routes.push(ModelRouteConfig {
            hint: "quick".into(),
            model_provider: "ollama.quick".into(),
            model: "zc-quick".into(),
            ..Default::default()
        });
        let route = route_for(&config, "quick").expect("configured");
        assert_eq!((route.model_provider.as_str(), route.model.as_str()), ("ollama.quick", "zc-quick"));
        assert!(route_for(&config, "openrouter.flash").is_err());
        assert!(route_for(&config, "").is_err());
    }

    #[test]
    fn validation_rejects_empty_oversized_and_out_of_range_input() {
        assert!(validate(&request("ls -la")).is_ok());
        assert!(validate(&request("   ")).is_err());
        assert!(validate(&request(&"x".repeat(INPUT_CAP + 1))).is_err());
        let mut hot = request("ls");
        hot.temperature = Some(3.0);
        assert!(validate(&hot).is_err());
        let mut big_system = request("ls");
        big_system.system = Some("s".repeat(INPUT_CAP));
        assert!(validate(&big_system).is_err());
    }
}
