use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use log::error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::mqtt::ExtraTags;
use crate::mqtt::{IncomingMessage, PublishableMessage};

struct QueueCounter {
    cmd_rx: UnboundedReceiver<TimestampedCommand>,
    counters: HashMap<ExtraTags, QueuesCounterInstant>,
}

#[derive(Debug, Default, Clone)]
pub struct QueuesCounter {
    pub incoming_requests: u64,
    pub incoming_responses: u64,
    pub incoming_events: u64,
    pub outgoing_requests: u64,
    pub outgoing_responses: u64,
    pub outgoing_events: u64,
    pub incoming_bytes: u64,
}

#[derive(Clone)]
pub struct QueuesCounterInstant {
    pub result: QueuesCounter,
    updated_at: Instant,
}

impl Default for QueuesCounterInstant {
    fn default() -> Self {
        Self {
            updated_at: Instant::now(),
            result: Default::default(),
        }
    }
}

#[derive(Debug)]
enum Command {
    IncomingRequest(ExtraTags, u64),
    IncomingResponse(ExtraTags),
    IncomingEvent(ExtraTags),
    OutgoingRequest(ExtraTags),
    OutgoingResponse(ExtraTags),
    OutgoingEvent(ExtraTags),
    GetThroughput(oneshot::Sender<HashMap<ExtraTags, QueuesCounter>>),
}

#[derive(Debug)]
struct TimestampedCommand {
    command: Command,
    timestamp: DateTime<Utc>,
}

#[derive(Clone)]
pub struct QueueCounterHandle {
    cmd_tx: UnboundedSender<TimestampedCommand>,
}

impl QueueCounterHandle {
    pub(crate) fn start() -> Self {
        let (cmd_tx, cmd_rx) = unbounded_channel();

        let mut counter = QueueCounter {
            cmd_rx,
            counters: HashMap::new(),
        };
        tokio::spawn(async move { counter.start_loop().await });
        Self { cmd_tx }
    }

    pub(crate) fn add_incoming_message(&self, msg: &IncomingMessage<String>) {
        let command = match msg {
            IncomingMessage::Event(ev) => {
                let tags = ev.properties().tags().to_owned();
                Command::IncomingEvent(tags)
            }
            IncomingMessage::Request(req) => {
                let method = req.properties().method().to_owned();
                let mut tags = req.properties().tags().to_owned();
                tags.set_method(&method);
                let bytes = req.payload().len() as u64;
                Command::IncomingRequest(tags, bytes)
            }
            IncomingMessage::Response(resp) => {
                let tags = resp.properties().tags().to_owned();
                Command::IncomingResponse(tags)
            }
        };

        self.send_command(command);
    }

    pub(crate) fn add_outgoing_message(&self, dump: &PublishableMessage) {
        let command = match dump {
            PublishableMessage::Event(ev) => {
                let tags = ev.tags().to_owned();
                Command::OutgoingEvent(tags)
            }
            PublishableMessage::Request(req) => {
                let tags = req.tags().to_owned();
                Command::OutgoingRequest(tags)
            }
            PublishableMessage::Response(resp) => {
                let tags = resp.tags().to_owned();
                Command::OutgoingResponse(tags)
            }
        };
        self.send_command(command);
    }

    fn send_command(&self, command: Command) {
        if let Err(e) = self.cmd_tx.send(TimestampedCommand {
            timestamp: Utc::now(),
            command,
        }) {
            error!("Failed to send command, reason = {:?}", e);
        }
    }

    pub async fn get_stats(&self) -> Result<HashMap<ExtraTags, QueuesCounter>, String> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let command = Command::GetThroughput(resp_tx);

        self.send_command(command);

        resp_rx
            .await
            .map_err(|e| format!("get_stats went wrong: {:?}", e))
    }
}

const EVICTION_PERIOD: Duration = Duration::from_secs(600);
const EVICTION_CHECK_PERIOD: Duration = Duration::from_secs(5);

impl QueueCounter {
    async fn start_loop(&mut self) {
        let mut last_checked = Instant::now();

        while let Some(c) = self.cmd_rx.recv().await {
            if last_checked.elapsed() > EVICTION_CHECK_PERIOD {
                self.evict_old_counters();
                last_checked = Instant::now();
            }

            match c.command {
                Command::GetThroughput(resp_tx) => {
                    if resp_tx.send(self.fold()).is_err() {
                        error!("The receiving end was dropped before this was called");
                    }
                }
                Command::IncomingRequest(tags, bytes) => {
                    let c = self.counters.entry(tags).or_default();
                    c.result.incoming_requests += 1;
                    c.result.incoming_bytes += bytes;
                    c.updated_at = Instant::now();
                }
                Command::IncomingResponse(tags) => {
                    let c = self.counters.entry(tags).or_default();
                    c.result.incoming_responses += 1;
                    c.updated_at = Instant::now();
                }
                Command::IncomingEvent(tags) => {
                    let c = self.counters.entry(tags).or_default();
                    c.result.incoming_events += 1;
                    c.updated_at = Instant::now();
                }
                Command::OutgoingRequest(tags) => {
                    let c = self.counters.entry(tags).or_default();
                    c.result.outgoing_requests += 1;
                    c.updated_at = Instant::now();
                }
                Command::OutgoingResponse(tags) => {
                    let c = self.counters.entry(tags).or_default();
                    c.result.outgoing_responses += 1;
                    c.updated_at = Instant::now();
                }
                Command::OutgoingEvent(tags) => {
                    let c = self.counters.entry(tags).or_default();
                    c.result.outgoing_events += 1;
                    c.updated_at = Instant::now();
                }
            }
        }
    }

    fn evict_old_counters(&mut self) {
        self.counters
            .retain(|_k, v| v.updated_at.elapsed() < EVICTION_PERIOD)
    }

    fn fold(&self) -> HashMap<ExtraTags, QueuesCounter> {
        self.counters
            .iter()
            .map(|(k, v)| (k.clone(), v.result.clone()))
            .collect()
    }
}
