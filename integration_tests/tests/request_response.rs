//! Tests simple request-response messaging pattern.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use chrono::Utc;
use serde_json::{json, Value as JsonValue};
use svc_agent::{
    mqtt::{
        compat, AgentBuilder, AgentConfig, ConnectionMode, Notification, OutgoingRequest,
        OutgoingRequestProperties, OutgoingResponse, QoS, ResponseStatus,
        ShortTermTimingProperties, SubscriptionTopic,
    },
    AccountId, AgentId, SharedGroup, Subscription,
};

const API_VERSION: &str = "v1";
const CORRELATION_DATA: &str = "12345";

fn build_agent_config() -> AgentConfig {
    serde_json::from_str::<AgentConfig>(r#"{"uri": "0.0.0.0:1883"}"#)
        .expect("Failed to parse agent config")
}

fn run_ping_service(init_tx: mpsc::Sender<()>) {
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

#[test]
fn request_response() {
    // Start service.
    let (init_tx, init_rx) = mpsc::channel::<()>();
    thread::spawn(move || run_ping_service(init_tx));
    init_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("Failed to init");

    // Create client agent.
    let account_id = AccountId::new("ping-client", "test.usr.example.org");
    let agent_id = AgentId::new("test", account_id);

    let builder =
        AgentBuilder::new(agent_id.clone(), API_VERSION).connection_mode(ConnectionMode::Service);

    let (mut agent, rx) = builder
        .start(&build_agent_config())
        .expect("Failed to start ping client");

    // Subscribe to the unicast responses topic.
    let service_account_id = AccountId::new("ping-service", "test.svc.example.org");
    let subscription = Subscription::unicast_responses_from(&service_account_id);

    agent
        .subscribe(&subscription, QoS::AtLeastOnce, None)
        .expect("Error subscribing to unicast responses");

    // Publish request.
    let response_topic = subscription
        .subscription_topic(&agent_id, API_VERSION)
        .expect("Failed to build response topic");

    let reqp = OutgoingRequestProperties::new(
        "ping",
        &response_topic,
        CORRELATION_DATA,
        ShortTermTimingProperties::new(Utc::now()),
    );

    let payload = json!({"message": "ping"});
    let request = OutgoingRequest::multicast(payload, reqp, &service_account_id);

    agent
        .publish(Box::new(request))
        .expect("Failed to publish request");

    // Receive response.
    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(Notification::Publish(message)) => {
            let bytes = message.payload.as_slice();

            let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(bytes)
                .expect("Failed to parse incoming message");

            // Handle response.
            match compat::into_response::<JsonValue>(envelope) {
                Ok(response) => {
                    assert_eq!(response.properties().status(), ResponseStatus::CREATED);
                    assert_eq!(response.properties().correlation_data(), CORRELATION_DATA);
                    assert_eq!(response.payload()["message"].as_str(), Some("pong"));
                }
                Err(err) => panic!("Failed to parse response: {}", err),
            }
        }
        Ok(other) => panic!("Expected to receive publish notification, got {:?}", other),
        Err(err) => panic!("Failed to receive response: {}", err),
    }
}
