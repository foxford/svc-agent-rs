use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use crossbeam_channel::{Receiver, Sender};
use log::error;

const QUEUE_TIMELIMIT: i64 = 30;

struct QueueCounter {
    cmd_rx: Receiver<TimestampedCommand>,
    resp_tx: Sender<QueuesCounterResult>,
    incoming_requests: VecDeque<DateTime<Utc>>,
    incoming_responses: VecDeque<DateTime<Utc>>,
    incoming_events: VecDeque<DateTime<Utc>>,
    outgoing_messages: VecDeque<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct QueueCounterHandle {
    cmd_tx: Sender<TimestampedCommand>,
    resp_rx: Receiver<QueuesCounterResult>
}

#[derive(Debug)]
pub struct QueuesCounterResult {
    pub incoming_requests: u64,
    pub incoming_responses: u64,
    pub incoming_events: u64,
    pub outgoing_messages: u64,
}

#[derive(Debug)]
enum Command {
    IncomingRequest,
    IncomingResponse,
    IncomingEvent,
    OutgoingMessage,
    GetThroughput(u64),
}

struct TimestampedCommand {
    command: Command,
    timestamp: DateTime<Utc>,
}

impl QueueCounterHandle {
    pub fn start() -> Self {
        let (cmd_tx, cmd_rx) = crossbeam_channel::unbounded::<TimestampedCommand>();
        let (resp_tx, resp_rx) = crossbeam_channel::unbounded::<QueuesCounterResult>();
        let mut counter = QueueCounter {
            cmd_rx,
            resp_tx,
            incoming_requests: VecDeque::with_capacity(100),
            incoming_responses: VecDeque::with_capacity(100),
            incoming_events: VecDeque::with_capacity(100),
            outgoing_messages: VecDeque::with_capacity(100),
        };
        std::thread::spawn(move || {
            counter.start_loop();
        });
        Self { cmd_tx, resp_rx }
    }

    pub fn add_incoming_request(&self) {
        self.send_command(Command::IncomingRequest);
    }

    pub fn add_incoming_response(&self) {
        self.send_command(Command::IncomingResponse);
    }

    pub fn add_incoming_event(&self) {
        self.send_command(Command::IncomingEvent);
    }

    pub fn add_outgoing_message(&self) {
        self.send_command(Command::OutgoingMessage);
    }

    fn send_command(&self, command: Command) {
        if let Err(e)  = self.cmd_tx
            .send(TimestampedCommand { timestamp: Utc::now(), command }) {
                error!("Failed to send command, reason = {:?}", e);
            }
    }

    pub fn get_stats(
        &self,
        seconds: u64,
    ) -> Result<QueuesCounterResult, String> {
        let command = Command::GetThroughput(seconds);

        self.send_command(command);

        self.resp_rx
            .recv()
            .map_err(|e| format!("get_stats went wrong: {}", e))
    }
}

impl QueueCounter {
    fn start_loop(&mut self) {
        while let Ok(c) = self.cmd_rx.recv() {
            self.clear_outdated_entries(c.timestamp - chrono::Duration::seconds(QUEUE_TIMELIMIT));

            match c.command {
                Command::GetThroughput(secs) => {
                    self.clear_outdated_entries(
                        c.timestamp - chrono::Duration::seconds(secs as i64),
                    );

                    let r = QueuesCounterResult {
                        incoming_requests: self.incoming_requests.len() as u64,
                        incoming_responses: self.incoming_responses.len() as u64,
                        incoming_events: self.incoming_events.len() as u64,
                        outgoing_messages: self.outgoing_messages.len() as u64,
                    };
                    if self.resp_tx.send(r).is_err() {
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
                Command::OutgoingMessage => {
                    self.outgoing_messages.push_back(c.timestamp);
                }
            }
        }
    }

    fn clear_outdated_entries(&mut self, timestamp: DateTime<Utc>) {
        Self::clear_outdated_entries_from_queue(&mut self.incoming_requests, timestamp);
        Self::clear_outdated_entries_from_queue(&mut self.incoming_responses, timestamp);
        Self::clear_outdated_entries_from_queue(&mut self.incoming_events, timestamp);
        Self::clear_outdated_entries_from_queue(&mut self.outgoing_messages, timestamp);
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
