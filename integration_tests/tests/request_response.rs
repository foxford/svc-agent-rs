//! Tests simple request-response messaging pattern.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use chrono::Utc;
use serde_json::{json, Value as JsonValue};
use svc_agent::{
    mqtt::{
        AgentBuilder, AgentNotification, ConnectionMode, IncomingMessage, OutgoingRequest,
        OutgoingRequestProperties, QoS, ResponseStatus, ShortTermTimingProperties,
        SubscriptionTopic,
    },
    AccountId, AgentId, Subscription,
};

mod helpers;

const API_VERSION: &str = "v1";
const CORRELATION_DATA: &str = "12345";

#[test]
fn request_response() {
    // Start service.
    let (init_tx, init_rx) = mpsc::channel::<()>();
    thread::spawn(move || helpers::ping_service::run(init_tx));

    init_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("Failed to init");

    // Create client agent.
    let account_id = AccountId::new("ping-client", "test.usr.example.org");
    let agent_id = AgentId::new("test", account_id);

    let builder =
        AgentBuilder::new(agent_id.clone(), API_VERSION).connection_mode(ConnectionMode::Service);

    let (mut agent, rx) = builder
        .start(&helpers::build_agent_config())
        .expect("Failed to start ping client");

    // Subscribe to the unicast responses topic.
    let service_account_id = AccountId::new("ping-service", "test.svc.example.org");
    let subscription = Subscription::unicast_responses_from(&service_account_id);

    agent
        .subscribe(&subscription, QoS::AtLeastOnce, None)
        .expect("Error subscribing to unicast responses");

    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(AgentNotification::Suback(_)) => (),
        Ok(other) => panic!("Expected to receive suback notification, got {:?}", other),
        Err(err) => panic!("Failed to receive suback notification: {}", err),
    }

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
    let request = OutgoingRequest::multicast(payload, reqp, &service_account_id, "v1");

    agent.publish(request).expect("Failed to publish request");

    // Receive response.
    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(AgentNotification::Message(Ok(IncomingMessage::Response(response)), _)) => {
            // Handle response.
            assert_eq!(response.properties().status(), ResponseStatus::CREATED);
            assert_eq!(response.properties().correlation_data(), CORRELATION_DATA);

            let content =
                serde_json::from_str::<JsonValue>(response.payload()).expect("Couldnt parse json");
            assert_eq!(content["message"].as_str(), Some("pong"));
        }
        Ok(other) => panic!("Expected to receive publish notification, got {:?}", other),
        Err(err) => panic!("Failed to receive response: {}", err),
    }
}
