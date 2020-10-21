use std::collections::HashMap;

use chrono::{DateTime, Utc};
use crossbeam_channel::{Receiver, Sender};
use log::error;

use crate::mqtt::ExtraTags;
use crate::mqtt::{IncomingMessage, PublishableMessage};

struct QueueCounter {
    cmd_rx: Receiver<TimestampedCommand>,
    counters: HashMap<ExtraTags, QueuesCounterResult>,
}

#[derive(Debug, Default, Clone)]
pub struct QueuesCounterResult {
    pub incoming_requests: u64,
    pub incoming_responses: u64,
    pub incoming_events: u64,
    pub outgoing_requests: u64,
    pub outgoing_responses: u64,
    pub outgoing_events: u64,
}

#[derive(Debug)]
enum Command {
    IncomingRequest(ExtraTags),
    IncomingResponse(ExtraTags),
    IncomingEvent(ExtraTags),
    OutgoingRequest(ExtraTags),
    OutgoingResponse(ExtraTags),
    OutgoingEvent(ExtraTags),
    GetThroughput(crossbeam_channel::Sender<HashMap<ExtraTags, QueuesCounterResult>>),
}

#[derive(Debug)]
struct TimestampedCommand {
    command: Command,
    timestamp: DateTime<Utc>,
}

#[derive(Clone)]
pub struct QueueCounterHandle {
    cmd_tx: Sender<TimestampedCommand>,
}

impl QueueCounterHandle {
    pub(crate) fn start() -> Self {
        let (cmd_tx, cmd_rx) = crossbeam_channel::unbounded::<TimestampedCommand>();

        let mut counter = QueueCounter {
            cmd_rx,
            counters: HashMap::new(),
        };

        let builder = std::thread::Builder::new().name("qc-command-loop".into());
        builder
            .spawn(move || {
                counter.start_loop();
            })
            .expect("Failed to start qc-command-loop thread");
        Self { cmd_tx }
    }

    pub(crate) fn add_incoming_message<T>(&self, msg: &IncomingMessage<T>) {
        let command = match msg {
            IncomingMessage::Event(ev) => {
                let tags = ev.properties().tags().to_owned();
                Command::IncomingEvent(tags)
            }
            IncomingMessage::Request(req) => {
                let method = req.properties().method().to_owned();
                let mut tags = req.properties().tags().to_owned();
                tags.set_method(&method);
                Command::IncomingRequest(tags)
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

    pub fn get_stats(&self) -> Result<HashMap<ExtraTags, QueuesCounterResult>, String> {
        let (resp_tx, resp_rx) = crossbeam_channel::bounded::<_>(1);
        let command = Command::GetThroughput(resp_tx);

        self.send_command(command);

        resp_rx
            .recv()
            .map_err(|e| format!("get_stats went wrong: {:?}", e))
    }
}

impl QueueCounter {
    fn start_loop(&mut self) {
        while let Ok(c) = self.cmd_rx.recv() {
            match c.command {
                Command::GetThroughput(resp_tx) => {
                    if resp_tx.send(self.fold()).is_err() {
                        error!("The receiving end was dropped before this was called");
                    }
                }
                Command::IncomingRequest(tags) => {
                    let c = self.counters.entry(tags).or_default();
                    c.incoming_requests += 1;
                }
                Command::IncomingResponse(tags) => {
                    let c = self.counters.entry(tags).or_default();
                    c.incoming_responses += 1;
                }
                Command::IncomingEvent(tags) => {
                    let c = self.counters.entry(tags).or_default();
                    c.incoming_events += 1;
                }
                Command::OutgoingRequest(tags) => {
                    let c = self.counters.entry(tags).or_default();
                    c.outgoing_requests += 1;
                }
                Command::OutgoingResponse(tags) => {
                    let c = self.counters.entry(tags).or_default();
                    c.outgoing_responses += 1;
                }
                Command::OutgoingEvent(tags) => {
                    let c = self.counters.entry(tags).or_default();
                    c.outgoing_events += 1;
                }
            }
        }
    }

    fn fold(&self) -> HashMap<ExtraTags, QueuesCounterResult> {
        self.counters.clone()
    }
}
