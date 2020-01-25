use svc_agent::mqtt::AgentConfig;

pub(crate) fn build_agent_config() -> AgentConfig {
    serde_json::from_str::<AgentConfig>(r#"{"uri": "0.0.0.0:1883"}"#)
        .expect("Failed to parse agent config")
}

pub(crate) mod ping_service;
