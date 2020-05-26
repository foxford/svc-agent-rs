use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use crossbeam_channel::{Receiver, Sender};
use log::error;

use crate::mqtt::{IncomingMessage, PublishableMessage};

const QUEUE_TIMELIMIT: i64 = 30;

struct QueueCounter {
    cmd_rx: Receiver<TimestampedCommand>,
    incoming_requests: VecDeque<DateTime<Utc>>,
    incoming_responses: VecDeque<DateTime<Utc>>,
    incoming_events: VecDeque<DateTime<Utc>>,
    outgoing_requests: VecDeque<DateTime<Utc>>,
    outgoing_responses: VecDeque<DateTime<Utc>>,
    outgoing_events: VecDeque<DateTime<Utc>>,
}

#[derive(Debug)]
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
    IncomingRequest,
    IncomingResponse,
    IncomingEvent,
    OutgoingRequest,
    OutgoingResponse,
    OutgoingEvent,
    GetThroughput(u64, crossbeam_channel::Sender<QueuesCounterResult>),
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

        std::thread::spawn(move || {
            counter.start_loop();
        });
        Self { cmd_tx }
    }

    pub(crate) fn add_incoming_message<T>(&self, msg: &IncomingMessage<T>) {
        let command = match msg {
            IncomingMessage::Event(_) => Command::IncomingEvent,
            IncomingMessage::Request(_) => Command::IncomingRequest,
            IncomingMessage::Response(_) => Command::IncomingResponse,
        };

        self.send_command(command);
    }

    pub(crate) fn add_outgoing_message(&self, dump: &PublishableMessage) {
        let command = match dump {
            PublishableMessage::Event(_) => Command::OutgoingEvent,
            PublishableMessage::Request(_) => Command::OutgoingRequest,
            PublishableMessage::Response(_) => Command::OutgoingResponse,
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

    pub fn get_stats(&self, seconds: u64) -> Result<QueuesCounterResult, String> {
        let (resp_tx, resp_rx) = crossbeam_channel::bounded::<QueuesCounterResult>(1);
        let command = Command::GetThroughput(seconds, resp_tx);

        self.send_command(command);

        resp_rx
            .recv()
            .map_err(|e| format!("get_stats went wrong: {}", e))
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

                    let r = QueuesCounterResult {
                        incoming_requests: self.incoming_requests.len() as u64,
                        incoming_responses: self.incoming_responses.len() as u64,
                        incoming_events: self.incoming_events.len() as u64,
                        outgoing_requests: self.outgoing_requests.len() as u64,
                        outgoing_responses: self.outgoing_responses.len() as u64,
                        outgoing_events: self.outgoing_events.len() as u64,
                    };
                    if resp_tx.send(r).is_err() {
                        error!("The receiving end was dropped before this was called");
                    }
                }
                Command::IncomingRequest => {
                    self.incoming_requests.push_back(c.timestamp);
                }
                Command::IncomingResponse => {
                    self.incoming_responses.push_back(c.timestamp);
                }
                Command::IncomingEvent => {
                    self.incoming_responses.push_back(c.timestamp);
                }
                Command::OutgoingRequest => {
                    self.outgoing_requests.push_back(c.timestamp);
                }
                Command::OutgoingResponse => {
                    self.outgoing_responses.push_back(c.timestamp);
                }
                Command::OutgoingEvent => {
                    self.outgoing_responses.push_back(c.timestamp);
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
        queue: &mut VecDeque<DateTime<Utc>>,
        timestamp: DateTime<Utc>,
    ) {
        loop {
            if let Some(v) = queue.front() {
                if *v < timestamp {
                    queue.pop_front();
                    continue;
                }
            }
            break;
        }
    }
}
