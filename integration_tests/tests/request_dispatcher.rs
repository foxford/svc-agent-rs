use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use chrono::Utc;
use futures::executor::ThreadPool;
use serde_derive::Deserialize;
use serde_json::{json, Value as JsonValue};
use svc_agent::{
    mqtt::{
        AgentBuilder, AgentNotification, ConnectionMode, IncomingMessage, IncomingResponse,
        OutgoingMessage, OutgoingRequest, OutgoingRequestProperties, QoS,
        ShortTermTimingProperties, SubscriptionTopic,
    },
    request::Dispatcher,
    AccountId, AgentId, Subscription,
};

mod helpers;

const API_VERSION: &str = "v1";

#[derive(Deserialize)]
struct PingResponse {
    message: String,
}

#[test]
fn request_dispatcher() {
    // Thread pool for futures.
    let pool = ThreadPool::new().expect("Failed to build thread pool");

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

    // Create request dispatcher.
    let dispatcher = Arc::new(Dispatcher::new(&agent));
    let dispatcher_clone = dispatcher.clone();

    // ASynchronous message handling loop.
    pool.spawn_ok(async move {
        while let Ok(AgentNotification::Message(Ok(IncomingMessage::Response(resp)), _)) = rx.recv() {
            // Handle response.
            let message =
                IncomingResponse::convert::<JsonValue>(resp).expect("Couldnt convert message");

            dispatcher_clone
                .response(message)
                .await
                .expect("Failed to dispatch response")
        }
    });

    // Go async to be able to `.await` and publish two requests.
    let (resp_tx, resp_rx) = mpsc::channel::<()>();

    let response_topic = subscription
        .subscription_topic(&agent_id, API_VERSION)
        .expect("Failed to build response topic");

    pool.spawn_ok(async move {
        // The first request is to ensure that the dispatcher is able to handle multiple requests.
        let _future1 = publish_request(&dispatcher, &service_account_id, &response_topic, "req1");
        let future2 = publish_request(&dispatcher, &service_account_id, &response_topic, "req2");
        let response = future2.await;

        assert_eq!(response.payload().message, "pong");
        assert_eq!(response.properties().correlation_data(), "req2");

        // Notify the main thread that we're done.
        resp_tx
            .send(())
            .expect("Failed to notify about received response");
    });

    // Block to synchronize with the above async part.
    resp_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("Failed to await response");
}

async fn publish_request(
    dispatcher: &Dispatcher,
    to: &AccountId,
    response_topic: &str,
    correlation_data: &str,
) -> IncomingResponse<PingResponse> {
    // Build request.
    let reqp = OutgoingRequestProperties::new(
        "ping",
        response_topic,
        correlation_data,
        ShortTermTimingProperties::new(Utc::now()),
    );

    let payload = json!({"message": "ping"});
    let msg = OutgoingRequest::multicast(payload, reqp, to);

    match msg {
        OutgoingMessage::Request(request) => {
            // Send request and wait for the response.
            dispatcher
                .request(request)
                .await
                .expect("Failed to dispatch request")
        }
        _ => panic!("wrong message type"),
    }
}
