//! Authenticated bridge to the existing, agent-scoped web tools.
use super::AppState;
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use zeroclaw_runtime::agent::{scrub_credentials_value, web_tools};

pub fn routes() -> axum::Router<AppState> {
    axum::Router::new()
        .route("/api/agents/{agent}/web-tools", axum::routing::get(list))
        .route(
            "/api/agents/{agent}/web-tools/{tool}",
            axum::routing::post(call),
        )
}

fn error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(scrub_credentials_value(json!({"error":message}))),
    )
        .into_response()
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Option<Response> {
    // This opt-in surface always needs a paired token, even if the legacy gateway
    // endpoints are configured to permit anonymous access.
    let token = super::api::extract_bearer_token(headers).unwrap_or("");
    if !state.pairing.require_pairing()
        || token.is_empty()
        || !state.pairing.is_authenticated(token)
    {
        return Some(error(
            StatusCode::UNAUTHORIZED,
            "A paired bearer token is required",
        ));
    }
    if !state.config.read().gateway.web_tools_enabled {
        return Some(error(StatusCode::NOT_FOUND, "Web tool bridge is disabled"));
    }
    None
}

pub async fn list(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(agent): Path<String>,
) -> Response {
    if let Some(response) = authorize(&state, &headers) {
        return response;
    }
    let config = state.config.read().clone();
    match web_tools::registry(&config, &agent, &state.rate_limiter.web_tools).await {
        Ok(tools) => {
            Json(json!({"agent":agent,"tools":tools.iter().map(|t| t.spec()).collect::<Vec<_>>()}))
                .into_response()
        }
        Err(e) => error(StatusCode::FORBIDDEN, &e.to_string()),
    }
}

pub async fn call(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((agent, tool)): Path<(String, String)>,
    Json(arguments): Json<Value>,
) -> Response {
    if let Some(response) = authorize(&state, &headers) {
        return response;
    }
    if !web_tools::NAMES.contains(&tool.as_str()) || !arguments.is_object() {
        return error(
            StatusCode::BAD_REQUEST,
            "Only web_search_tool and web_fetch with object arguments are supported",
        );
    }
    let config = state.config.read().clone();
    if !config.agents.get(&agent).is_some_and(|a| a.enabled) {
        return error(StatusCode::NOT_FOUND, "Agent is unknown or disabled");
    }
    if !state
        .rate_limiter
        .allow_webhook(&format!("web-tools:{agent}"))
    {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "Web tool request rate limit exceeded",
        );
    }
    match web_tools::execute(
        &config,
        &agent,
        &tool,
        arguments,
        &state.rate_limiter.web_tools,
        state.observer.as_ref(),
    )
    .await
    {
        Ok(result) => {
            // Fetch limits can be large for a local agent. Bound what crosses the bridge,
            // preserving Unicode and explicitly reporting any truncation to Codex.
            let mut output: String = result.output.chars().take(32_000).collect();
            let truncated = output.len() < result.output.len();
            if truncated {
                output.push_str("\n[Page content truncated by the web tool bridge]");
            }
            Json(scrub_credentials_value(
                json!({"success":result.success,"output":output,"error":result.error_reason,
                "agent":agent,"tool":tool,"truncated":truncated}),
            ))
            .into_response()
        }
        Err(e) => error(StatusCode::FORBIDDEN, &e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;
    use zeroclaw_config::schema::{AliasedAgentConfig, RiskProfileConfig, RuntimeProfileConfig};

    async fn fixture() -> (tempfile::TempDir, AppState, String) {
        let tmp = tempfile::tempdir().unwrap();
        let state = crate::tests::admin_paircode_state(&tmp, true, false);
        {
            let mut config = state.config.write();
            config.gateway.web_tools_enabled = true;
            config.web_search.enabled = true;
            config.web_fetch.enabled = true;
            config.web_fetch.allowed_domains = vec!["*".into()];
            config.web_fetch.firecrawl.enabled = false;
            config.risk_profiles.insert(
                "bridge".into(),
                RiskProfileConfig {
                    auto_approve: web_tools::NAMES.iter().map(|s| s.to_string()).collect(),
                    always_ask: vec![],
                    ..RiskProfileConfig::default()
                },
            );
            config.runtime_profiles.insert(
                "bridge".into(),
                RuntimeProfileConfig {
                    max_actions_per_hour: 1,
                    ..RuntimeProfileConfig::default()
                },
            );
            config.agents.insert(
                "research".into(),
                AliasedAgentConfig {
                    enabled: true,
                    risk_profile: "bridge".into(),
                    runtime_profile: "bridge".into(),
                    ..AliasedAgentConfig::default()
                },
            );
        }
        let code = state.pairing.generate_new_pairing_code().unwrap();
        let token = state
            .pairing
            .try_pair(&code, "web-test")
            .await
            .unwrap()
            .unwrap();
        (tmp, state, token)
    }

    async fn request(
        state: &AppState,
        token: &str,
        method: &str,
        suffix: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(format!("/api/agents/research/web-tools{suffix}"))
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = routes()
            .with_state(state.clone())
            .oneshot(request)
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 256 * 1024).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn bridge_is_opt_in_and_never_accepts_anonymous_access() {
        let (_tmp, state, token) = fixture().await;
        assert_eq!(
            request(&state, "wrong", "GET", "", Value::Null).await.0,
            StatusCode::UNAUTHORIZED
        );
        state.config.write().gateway.web_tools_enabled = false;
        assert_eq!(
            request(&state, &token, "GET", "", Value::Null).await.0,
            StatusCode::NOT_FOUND
        );
        let tmp = tempfile::tempdir().unwrap();
        let anonymous = crate::tests::admin_paircode_state(&tmp, false, false);
        anonymous.config.write().gateway.web_tools_enabled = true;
        assert_eq!(
            request(&anonymous, "anything", "GET", "", Value::Null)
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn catalog_and_execution_obey_live_agent_policy() {
        let (_tmp, state, token) = fixture().await;
        let (status, body) = request(&state, &token, "GET", "", Value::Null).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["tools"].as_array().unwrap().len(), 2);
        state
            .config
            .write()
            .risk_profiles
            .get_mut("bridge")
            .unwrap()
            .excluded_tools = vec!["web_fetch".into()];
        let (_, body) = request(&state, &token, "GET", "", Value::Null).await;
        assert_eq!(body["tools"].as_array().unwrap().len(), 1);
        assert_eq!(body["tools"][0]["name"], "web_search_tool");
        assert_eq!(
            request(
                &state,
                &token,
                "POST",
                "/web_fetch",
                json!({"url":"https://example.com"})
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
        state
            .config
            .write()
            .risk_profiles
            .get_mut("bridge")
            .unwrap()
            .always_ask = vec!["web_search_tool".into()];
        assert_eq!(
            request(&state, &token, "GET", "", Value::Null).await.1["tools"],
            json!([])
        );
        assert_eq!(
            request(&state, &token, "POST", "/shell", json!({"command":"true"}))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
        state
            .config
            .write()
            .agents
            .get_mut("research")
            .unwrap()
            .enabled = false;
        assert_eq!(
            request(
                &state,
                &token,
                "POST",
                "/web_search_tool",
                json!({"query":"hello"})
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn real_search_tool_round_trip_keeps_sources_and_rate_limits_across_requests() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path, query_param},
        };
        let (_tmp, state, token) = fixture().await;
        let server = MockServer::start().await;
        Mock::given(method("GET")).and(path("/search")).and(query_param("q","Rust releases"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"results":[
                {"title":"Rust releases","url":"https://blog.rust-lang.org/","content":"Release notes"}
            ]}))).expect(1).mount(&server).await;
        {
            let mut config = state.config.write();
            config.web_search.search_provider = "searxng".into();
            config.web_search.searxng_instance_url = Some(server.uri());
            std::fs::write(
                &config.config_path,
                format!("[web_search]\nsearxng_instance_url = {:?}\n", server.uri()),
            )
            .unwrap();
        }
        let args = json!({"query":"Rust releases"});
        let (status, first) =
            request(&state, &token, "POST", "/web_search_tool", args.clone()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first["success"], true, "{first}");
        assert!(
            first["output"]
                .as_str()
                .unwrap()
                .contains("https://blog.rust-lang.org/")
        );
        let (_, second) = request(&state, &token, "POST", "/web_search_tool", args).await;
        assert_eq!(second["success"], false, "{second}");
        assert!(second["output"].as_str().unwrap().contains("Rate limit"));
    }

    #[tokio::test]
    async fn transport_failure_redacts_reflected_credentials_in_both_result_fields() {
        let (_tmp, state, token) = fixture().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/api_key=fixture-search-secret",
            listener.local_addr().unwrap()
        );
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket);
        });
        {
            let mut config = state.config.write();
            config.web_search.search_provider = "searxng".into();
            config.web_search.searxng_instance_url = Some(url.clone());
            std::fs::write(
                &config.config_path,
                format!("[web_search]\nsearxng_instance_url = {url:?}\n"),
            )
            .unwrap();
        }
        let (status, body) = request(
            &state,
            &token,
            "POST",
            "/web_search_tool",
            json!({"query":"Rust"}),
        )
        .await;
        server.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["success"], false, "{body}");
        for field in ["output", "error"] {
            let text = body[field].as_str().unwrap();
            assert!(text.contains("[REDACTED]"), "{field}: {text}");
            assert!(!text.contains("fixture-search-secret"), "{field}: {text}");
        }
    }

    #[tokio::test]
    async fn page_fetch_keeps_the_existing_private_network_guard() {
        let (_tmp, state, token) = fixture().await;
        let (_, body) = request(
            &state,
            &token,
            "POST",
            "/web_fetch",
            json!({"url":"http://127.0.0.1:9/"}),
        )
        .await;
        assert_eq!(body["success"], false, "{body}");
        let text = body.to_string().to_lowercase();
        assert!(
            text.contains("private") || text.contains("blocked") || text.contains("local"),
            "{body}"
        );
    }
}
