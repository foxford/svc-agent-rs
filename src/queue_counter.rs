use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use crossbeam_channel::{Receiver, Sender};
use log::error;

use crate::mqtt::ExtraTags;
use crate::mqtt::{IncomingMessage, PublishableMessage};

const QUEUE_TIMELIMIT: i64 = 30;

// Tagged point in time when message got into Agent
struct MessageDatapoint {
    dt: DateTime<Utc>,
    tags: ExtraTags,
}

struct QueueCounter {
    cmd_rx: Receiver<TimestampedCommand>,
    incoming_requests: VecDeque<MessageDatapoint>,
    incoming_responses: VecDeque<MessageDatapoint>,
    incoming_events: VecDeque<MessageDatapoint>,
    outgoing_requests: VecDeque<MessageDatapoint>,
    outgoing_responses: VecDeque<MessageDatapoint>,
    outgoing_events: VecDeque<MessageDatapoint>,
}

#[derive(Debug, Default)]
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
    GetThroughput(
        u64,
        crossbeam_channel::Sender<HashMap<ExtraTags, QueuesCounterResult>>,
    ),
}

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
            incoming_requests: VecDeque::with_capacity(100),
            incoming_responses: VecDeque::with_capacity(100),
            incoming_events: VecDeque::with_capacity(100),
            outgoing_requests: VecDeque::with_capacity(100),
            outgoing_responses: VecDeque::with_capacity(100),
            outgoing_events: VecDeque::with_capacity(100),
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

    pub fn get_stats(
        &self,
        seconds: u64,
    ) -> Result<HashMap<ExtraTags, QueuesCounterResult>, String> {
        let (resp_tx, resp_rx) = crossbeam_channel::bounded::<_>(1);
        let command = Command::GetThroughput(seconds, resp_tx);

        self.send_command(command);

        resp_rx
            .recv()
            .map_err(|e| format!("get_stats went wrong: {:?}", e))
    }
}

impl QueueCounter {
    fn start_loop(&mut self) {
        while let Ok(c) = self.cmd_rx.recv() {
            self.clear_outdated_entries(c.timestamp - chrono::Duration::seconds(QUEUE_TIMELIMIT));

            match c.command {
                Command::GetThroughput(secs, resp_tx) => {
                    self.clear_outdated_entries(
                        c.timestamp - chrono::Duration::seconds(secs as i64),
                    );

                    if resp_tx.send(self.fold()).is_err() {
                        error!("The receiving end was dropped before this was called");
                    }
                }
                Command::IncomingRequest(tags) => {
                    let p = MessageDatapoint {
                        dt: c.timestamp,
                        tags: tags,
                    };
                    self.incoming_requests.push_back(p);
                }
                Command::IncomingResponse(tags) => {
                    let p = MessageDatapoint {
                        dt: c.timestamp,
                        tags: tags,
                    };
                    self.incoming_responses.push_back(p);
                }
                Command::IncomingEvent(tags) => {
                    let p = MessageDatapoint {
                        dt: c.timestamp,
                        tags: tags,
                    };
                    self.incoming_responses.push_back(p);
                }
                Command::OutgoingRequest(tags) => {
                    let p = MessageDatapoint {
                        dt: c.timestamp,
                        tags: tags,
                    };
                    self.outgoing_requests.push_back(p);
                }
                Command::OutgoingResponse(tags) => {
                    let p = MessageDatapoint {
                        dt: c.timestamp,
                        tags: tags,
                    };
                    self.outgoing_responses.push_back(p);
                }
                Command::OutgoingEvent(tags) => {
                    let p = MessageDatapoint {
                        dt: c.timestamp,
                        tags: tags,
                    };
                    self.outgoing_responses.push_back(p);
                }
            }
        }
    }

    fn clear_outdated_entries(&mut self, timestamp: DateTime<Utc>) {
        Self::clear_outdated_entries_from_queue(&mut self.incoming_requests, timestamp);
        Self::clear_outdated_entries_from_queue(&mut self.incoming_responses, timestamp);
        Self::clear_outdated_entries_from_queue(&mut self.incoming_events, timestamp);
        Self::clear_outdated_entries_from_queue(&mut self.outgoing_requests, timestamp);
        Self::clear_outdated_entries_from_queue(&mut self.outgoing_responses, timestamp);
        Self::clear_outdated_entries_from_queue(&mut self.outgoing_events, timestamp);
    }

    fn clear_outdated_entries_from_queue(
        queue: &mut VecDeque<MessageDatapoint>,
        timestamp: DateTime<Utc>,
    ) {
        loop {
            if let Some(v) = queue.front() {
                if v.dt < timestamp {
                    queue.pop_front();
                    continue;
                }
            }
            break;
        }
    }

    fn fold(&self) -> HashMap<ExtraTags, QueuesCounterResult> {
        let mut hashmap = HashMap::new();

        self.incoming_events.iter().fold(&mut hashmap, |acc, x| {
            if !acc.contains_key(&x.tags) {
                acc.insert(x.tags.clone(), QueuesCounterResult::default());
            }
            acc.get_mut(&x.tags).unwrap().incoming_events += 1;
            acc
        });

        self.incoming_requests.iter().fold(&mut hashmap, |acc, x| {
            if !acc.contains_key(&x.tags) {
                acc.insert(x.tags.clone(), QueuesCounterResult::default());
            }
            acc.get_mut(&x.tags).unwrap().incoming_requests += 1;
            acc
        });

        self.incoming_responses.iter().fold(&mut hashmap, |acc, x| {
            if !acc.contains_key(&x.tags) {
                acc.insert(x.tags.clone(), QueuesCounterResult::default());
            }
            acc.get_mut(&x.tags).unwrap().incoming_responses += 1;
            acc
        });

        self.outgoing_events.iter().fold(&mut hashmap, |acc, x| {
            if !acc.contains_key(&x.tags) {
                acc.insert(x.tags.clone(), QueuesCounterResult::default());
            }
            acc.get_mut(&x.tags).unwrap().outgoing_events += 1;
            acc
        });

        self.outgoing_requests.iter().fold(&mut hashmap, |acc, x| {
            if !acc.contains_key(&x.tags) {
                acc.insert(x.tags.clone(), QueuesCounterResult::default());
            }
            acc.get_mut(&x.tags).unwrap().outgoing_requests += 1;
            acc
        });

        self.outgoing_responses.iter().fold(&mut hashmap, |acc, x| {
            if !acc.contains_key(&x.tags) {
                acc.insert(x.tags.clone(), QueuesCounterResult::default());
            }
            acc.get_mut(&x.tags).unwrap().outgoing_responses += 1;
            acc
        });

        hashmap
    }
}
