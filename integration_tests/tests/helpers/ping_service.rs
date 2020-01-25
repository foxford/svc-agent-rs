use std::sync::mpsc;

use chrono::Utc;
use serde_json::{json, Value as JsonValue};
use svc_agent::{
    mqtt::{
        compat, AgentBuilder, ConnectionMode, Notification, OutgoingResponse, QoS, ResponseStatus,
        ShortTermTimingProperties,
    },
    AccountId, AgentId, SharedGroup, Subscription,
};

use super::build_agent_config;

#[allow(dead_code)]
pub(crate) const API_VERSION: &str = "v1";

#[allow(dead_code)]
pub(crate) fn run(init_tx: mpsc::Sender<()>) {
    // Create agent.
    let account_id = AccountId::new("ping-service", "test.svc.example.org");
    let agent_id = AgentId::new("instance01", account_id.clone());
    let builder = AgentBuilder::new(agent_id, API_VERSION).connection_mode(ConnectionMode::Service);

    let (mut agent, rx) = builder
        .start(&build_agent_config())
        .expect("Failed to start ping service");

    // Subscribe to the multicast requests topic.
    agent
        .subscribe(
            &Subscription::multicast_requests(Some(API_VERSION)),
            QoS::AtLeastOnce,
            Some(&SharedGroup::new("loadbalancer", account_id)),
        )
        .expect("Error subscribing to multicast requests");

    // Notifying that the service is intiialized.
    init_tx.send(()).expect("Failed to notify about init");

    // Message handling loop.
    while let Ok(Notification::Publish(message)) = rx.recv() {
        let bytes = message.payload.as_slice();

        let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(bytes)
            .expect("Failed to parse incoming message");

        // Handle request.
        match compat::into_request::<JsonValue>(envelope) {
            Ok(request) => {
                assert_eq!(request.properties().method(), "ping");
                assert_eq!(request.payload()["message"].as_str(), Some("ping"));

                let props = request.properties().to_response(
                    ResponseStatus::CREATED,
                    ShortTermTimingProperties::new(Utc::now()),
                );

                let response = OutgoingResponse::unicast(
                    json!({"message": "pong"}),
                    props,
                    request.properties(),
                    API_VERSION,
                );

                agent
                    .publish(Box::new(response))
                    .expect("Failed to publish response");
            }
            Err(err) => panic!(err),
        }
    }
}
