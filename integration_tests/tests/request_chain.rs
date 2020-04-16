//! Tests request chain, timing & tracking.
//! Client makes a request to service A which in part makes another request to service B.
//! Then B responds to A and A responds to the client.

use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde_derive::Deserialize;
use serde_json::{json, Value as JsonValue};
use svc_agent::{
    mqtt::{
        compat, AgentBuilder, ConnectionMode, Notification, OutgoingRequest,
        OutgoingRequestProperties, OutgoingResponse, OutgoingResponseProperties, QoS,
        ResponseStatus, ShortTermTimingProperties, SubscriptionTopic,
    },
    AccountId, AgentId, SharedGroup, Subscription,
};

mod helpers;

#[derive(Deserialize)]
struct Timings {
    cumulative_processing_time: String,
    cumulative_authorization_time: String,
}

const A_API_VERSION: &str = "v1";
const B_API_VERSION: &str = "v2";
const CORRELATION_DATA_LENGTH: usize = 16;

fn generate_correlation_data() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(CORRELATION_DATA_LENGTH)
        .collect()
}

fn run_service_a(init_tx: mpsc::Sender<()>) {
    // Create agent.
    let account_id = AccountId::new("a-service", "test.svc.example.org");
    let agent_id = AgentId::new("instance01", account_id.clone());

    let (mut agent, rx) = AgentBuilder::new(agent_id.clone(), A_API_VERSION)
        .connection_mode(ConnectionMode::Service)
        .start(&helpers::build_agent_config())
        .expect("Failed to start service A");

    // Subscribe to the multicast requests topic (from clients).
    let req_subscription = Subscription::multicast_requests(Some(A_API_VERSION));
    let group = SharedGroup::new("loadbalancer", account_id);

    agent
        .subscribe(&req_subscription, QoS::AtLeastOnce, Some(&group))
        .expect("Error subscribing to multicast requests in service A");

    // Subscribe to unicast responses topic (from service B).
    let b_service_account_id = AccountId::new("b-service", "test.svc.example.org");
    let resp_subscription = Subscription::unicast_responses_from(&b_service_account_id);

    agent
        .subscribe(&resp_subscription, QoS::AtLeastOnce, None)
        .expect("Error subscribing to unicast responses in service A");

    let response_topic = resp_subscription
        .subscription_topic(&agent_id, A_API_VERSION)
        .expect("Failed to build response topic");

    // Initialize state.
    // A->B request corr. data => (agent id, Client->A request corr.data)
    let mut state: HashMap<String, (AgentId, String)> = HashMap::new();

    // Notifying that the service is initialized.
    init_tx
        .send(())
        .expect("Failed to notify about init in service A");

    // Message handling loop.
    while let Ok(Notification::Publish(message)) = rx.recv() {
        let bytes = message.payload.as_slice();

        let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(bytes)
            .expect("Failed to parse incoming message in service A");

        match envelope.properties() {
            // Handle request: Client->A.
            compat::IncomingEnvelopeProperties::Request(_) => {
                match compat::into_request::<JsonValue>(envelope) {
                    Ok(request) => {
                        // Register to state.
                        let correlation_data = generate_correlation_data();

                        state.insert(
                            correlation_data.clone(),
                            (
                                request.properties().to_connection().agent_id().to_owned(),
                                request.properties().correlation_data().to_owned(),
                            ),
                        );

                        // Make A->B request.
                        let mut short_term_timing = ShortTermTimingProperties::new(Utc::now());
                        short_term_timing.set_processing_time(ChronoDuration::seconds(10));
                        short_term_timing.set_authorization_time(ChronoDuration::seconds(5));

                        let reqp = request.properties().to_request(
                            "b",
                            &response_topic,
                            &correlation_data,
                            short_term_timing,
                        );

                        let b_request = OutgoingRequest::multicast(
                            json!({}),
                            reqp,
                            &AccountId::new("b-service", "test.svc.example.org"),
                        );

                        agent
                            .publish(Box::new(b_request))
                            .expect("Failed to make request to service B");
                    }
                    Err(err) => panic!(err),
                }
            }
            // Handle response: B->A.
            compat::IncomingEnvelopeProperties::Response(_) => {
                match compat::into_response::<JsonValue>(envelope) {
                    Ok(response) => {
                        // Find record in state.
                        let (agent_id, correlation_data) = state
                            .get(response.properties().correlation_data())
                            .expect("State record not found in service A");

                        let mut short_term_timing = ShortTermTimingProperties::new(Utc::now());
                        short_term_timing.set_processing_time(ChronoDuration::seconds(10));
                        short_term_timing.set_authorization_time(ChronoDuration::seconds(5));

                        let long_term_timing = response
                            .properties()
                            .long_term_timing()
                            .to_owned()
                            .update_cumulative_timings(&short_term_timing);

                        let props = OutgoingResponseProperties::new(
                            ResponseStatus::OK,
                            &correlation_data,
                            long_term_timing,
                            short_term_timing,
                            response.properties().tracking().to_owned(),
                        );

                        let response =
                            OutgoingResponse::unicast(json!({}), props, agent_id, A_API_VERSION);

                        agent
                            .publish(Box::new(response))
                            .expect("Failed to make request to service B");
                    }
                    Err(err) => panic!(err),
                }
            }
            compat::IncomingEnvelopeProperties::Event(_) => {
                panic!("Unexpected incoming event in service A");
            }
        }
    }
}

fn run_service_b(init_tx: mpsc::Sender<()>) {
    // Create agent.
    let account_id = AccountId::new("b-service", "test.svc.example.org");
    let agent_id = AgentId::new("instance01", account_id.clone());

    let (mut agent, rx) = AgentBuilder::new(agent_id.clone(), B_API_VERSION)
        .connection_mode(ConnectionMode::Service)
        .start(&helpers::build_agent_config())
        .expect("Failed to start service B");

    // Subscribe to the multicast requests topic.
    let subscription = Subscription::multicast_requests(Some(A_API_VERSION));
    let group = SharedGroup::new("loadbalancer", account_id);

    agent
        .subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
        .expect("Error subscribing to multicast requests in service B");

    // Notifying that the service is initialized.
    init_tx
        .send(())
        .expect("Failed to notify about init in service B");

    // Message handling loop.
    while let Ok(Notification::Publish(message)) = rx.recv() {
        let bytes = message.payload.as_slice();

        let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(bytes)
            .expect("Failed to parse incoming message");

        // Handle request.
        match compat::into_request::<JsonValue>(envelope) {
            Ok(request) => {
                let props = request.properties().to_response(
                    ResponseStatus::OK,
                    ShortTermTimingProperties::new(Utc::now()),
                );

                let response = OutgoingResponse::unicast(
                    json!({}),
                    props,
                    request.properties(),
                    A_API_VERSION,
                );

                agent
                    .publish(Box::new(response))
                    .expect("Failed to publish response in service B");
            }
            Err(err) => panic!(err),
        }
    }
}

#[test]
fn request_chain() {
    // Start services.
    let (init_tx_a, init_rx) = mpsc::channel::<()>();
    let init_tx_b = init_tx_a.clone();

    thread::spawn(move || run_service_a(init_tx_a));

    init_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("Failed to init service A");

    thread::spawn(move || run_service_b(init_tx_b));

    init_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("Failed to init service B");

    // Create client agent.
    let account_id = AccountId::new("a-client", "test.usr.example.org");
    let agent_id = AgentId::new("test", account_id);

    let builder =
        AgentBuilder::new(agent_id.clone(), A_API_VERSION).connection_mode(ConnectionMode::Service);

    let (mut agent, rx) = builder
        .start(&helpers::build_agent_config())
        .expect("Failed to start ping client");

    // Subscribe to the unicast responses topic.
    let service_a_account_id = AccountId::new("a-service", "test.svc.example.org");
    let subscription = Subscription::unicast_responses_from(&service_a_account_id);

    agent
        .subscribe(&subscription, QoS::AtLeastOnce, None)
        .expect("Error subscribing to unicast responses");

    // Publish request.
    let response_topic = subscription
        .subscription_topic(&agent_id, A_API_VERSION)
        .expect("Failed to build response topic");

    let correlation_data = generate_correlation_data();

    let reqp = OutgoingRequestProperties::new(
        "a",
        &response_topic,
        &correlation_data,
        ShortTermTimingProperties::new(Utc::now()),
    );

    let request = OutgoingRequest::multicast(json!({}), reqp, &service_a_account_id);

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
                    assert_eq!(response.properties().status(), ResponseStatus::OK);
                    assert_eq!(response.properties().correlation_data(), &correlation_data);

                    // Assert timings.
                    let long_term_timings = response.properties().long_term_timing();

                    let long_term_timings_value =
                        serde_json::to_value(long_term_timings).expect("Failed to dump timings");

                    let timings = serde_json::from_value::<Timings>(long_term_timings_value)
                        .expect("Failed to parse timings");

                    let proc_time = timings.cumulative_processing_time;
                    assert!(proc_time.parse::<u64>().unwrap() >= 20000);

                    let authz_time = timings.cumulative_authorization_time;
                    assert!(authz_time.parse::<u64>().unwrap() >= 10000);

                    // Assert tracking.
                    let tracking = serde_json::to_value(response.properties().tracking())
                        .expect("Failed to parse timings");

                    let session_tracking_label = tracking
                        .get("session_tracking_label")
                        .expect("Missing session_tracking_label")
                        .as_str()
                        .expect("session_tracking_label is not a string");

                    let ids: Vec<String> = session_tracking_label
                        .split_whitespace()
                        .map(|s| s.to_owned())
                        .collect();

                    assert_eq!(ids.len(), 3);
                }
                Err(err) => panic!("Failed to parse response: {}", err),
            }
        }
        Ok(other) => panic!("Expected to receive publish notification, got {:?}", other),
        Err(err) => panic!("Failed to receive response: {}", err),
    }
}
