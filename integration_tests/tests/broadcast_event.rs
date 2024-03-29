//! Tests broadcast event publishing/subscription.

use std::time::Duration;

use chrono::Utc;
use serde_json::{json, Value as JsonValue};
use svc_agent::{
    mqtt::{
        AgentBuilder, AgentNotification, ConnectionMode, IncomingEvent, IncomingMessage,
        OutgoingEvent, OutgoingEventProperties, QoS, ShortTermTimingProperties,
    },
    AccountId, AgentId, Subscription,
};

mod helpers;

const API_VERSION: &str = "v1";
const URI: &str = "rooms/123/events";

fn run_event_service() {
    // Create agent.
    let account_id = AccountId::new("event-service", "test.svc.example.org");
    let agent_id = AgentId::new("instance01", account_id.clone());
    let builder = AgentBuilder::new(agent_id, API_VERSION).connection_mode(ConnectionMode::Service);

    let (mut agent, _rx) = builder
        .start(&helpers::build_agent_config())
        .expect("Failed to start event service");

    // Sending broadcast event.
    let evp = OutgoingEventProperties::new("hello", ShortTermTimingProperties::new(Utc::now()));
    let payload = json!({"foo": "bar"});
    let event = OutgoingEvent::broadcast(payload, evp, URI);
    agent.publish(event).expect("Failed to publish event");
}

#[test]
fn broadcast_event() {
    // Create client agent.
    let account_id = AccountId::new("event-client", "test.usr.example.org");
    let agent_id = AgentId::new("test", account_id);

    let builder =
        AgentBuilder::new(agent_id.clone(), API_VERSION).connection_mode(ConnectionMode::Default);

    let (mut agent, rx) = builder
        .start(&helpers::build_agent_config())
        .expect("Failed to start event client");

    // Subscribe to the broadcast events topic.
    let service_account_id = AccountId::new("event-service", "test.svc.example.org");
    let subscription = Subscription::broadcast_events(&service_account_id, API_VERSION, URI);

    agent
        .subscribe(&subscription, QoS::AtLeastOnce, None)
        .expect("Error subscribing to unicast responses");

    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(AgentNotification::Connack(_)) => (),
        Ok(other) => panic!("Expected to receive connack notification, got {:?}", other),
        Err(err) => panic!("Failed to receive connack notification: {}", err),
    }

    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(AgentNotification::Suback(_)) => (),
        Ok(other) => panic!("Expected to receive suback notification, got {:?}", other),
        Err(err) => panic!("Failed to receive suback notification: {}", err),
    }

    // Start event service which broadcasts a single event.
    run_event_service();

    // Receive event.
    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(AgentNotification::Message(Ok(message), _)) => {
            match message {
                IncomingMessage::Event(event) => {
                    // Handle response.
                    match IncomingEvent::convert::<JsonValue>(event) {
                        Ok(response) => {
                            assert_eq!(response.properties().label(), Some("hello"));
                            assert_eq!(response.payload()["foo"].as_str(), Some("bar"));
                        }
                        Err(err) => panic!("Failed to parse event: {}", err),
                    }
                }
                other => panic!("Unexpected message: {:?}", other),
            }
        }
        Ok(other) => panic!("Expected to receive publish notification, got {:?}", other),
        Err(err) => panic!("Failed to receive event: {}", err),
    }
}
