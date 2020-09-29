use std::fmt;
use std::str::FromStr;

use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{serde::session_ids_list, Error};

/// Tracking session ID.
#[derive(Clone, Debug)]
pub struct SessionId {
    agent_session_label: Uuid,
    broker_session_label: Uuid,
}

impl FromStr for SessionId {
    type Err = Error;

    /// Parses a [SessionId](struct.SessionId.html) from a string of two UUIDs
    /// separated by a dot.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let components = s.splitn(2, '.').collect::<Vec<&str>>();

        match components[..] {
            [agent_session_label_str, broker_session_label_str] => {
                let agent_session_label =
                    Uuid::parse_str(agent_session_label_str).map_err(|err| {
                        let msg = format!("Failed to parse agent session label UUID: {}", err);
                        Error::new(&msg)
                    })?;

                let broker_session_label =
                    Uuid::parse_str(broker_session_label_str).map_err(|err| {
                        let msg = format!("Failed to parse broker session label UUID: {}", err);
                        Error::new(&msg)
                    })?;

                Ok(Self {
                    agent_session_label,
                    broker_session_label,
                })
            }
            _ => Err(Error::new(
                "Failed to parse SessionId. Expected 2 UUIDs separated by .",
            )),
        }
    }
}

impl fmt::Display for SessionId {
    /// Dumps a [SessionId](struct.SessionId.html) to a string of two UUIDs separated by a dot.
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}.{}",
            self.agent_session_label, self.broker_session_label
        )
    }
}

/// Message chain ID.
#[derive(Clone, Debug)]
pub struct TrackingId {
    label: Uuid,
    session_id: SessionId,
}

impl FromStr for TrackingId {
    type Err = Error;

    /// Parses a [TrackingId](struct.TrackingId.html) from a string of three UUIDs
    /// separated by a dot.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let components = s.splitn(2, '.').collect::<Vec<&str>>();

        match components[..] {
            [label_str, session_id_str] => {
                let label = Uuid::parse_str(label_str).map_err(|err| {
                    let msg = format!("Failed to parse tracking id label UUID: {}", err);
                    Error::new(&msg)
                })?;

                Ok(Self {
                    label,
                    session_id: SessionId::from_str(session_id_str)?,
                })
            }
            _ => Err(Error::new(
                "Failed to parse TrackingId. Expected 3 UUIDs separated by .",
            )),
        }
    }
}

impl fmt::Display for TrackingId {
    /// Dumps a [TrackingId](struct.TrackingId.html) to a string of three UUIDs separated by a
    /// dot.
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}.{}", self.label, self.session_id)
    }
}

/// Message tracking properties.
///
/// Apart from [LongTermTimingProperties](struct.LongTermTimingProperties.html) and
/// [OutgoingShortTermTimingProperties](struct.OutgoingShortTermTimingProperties.html) there are also
/// tracking properties. They get assigned by the broker but since the decision on whether to
/// continue the chain by the next message either start a new chain is up to the agent,
/// tracking properties needs to be proxied in the former case.
///
/// Proxying is performed by [to_response](type.IncomingRequest.html#method.to_response) and
/// the like methods.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackingProperties {
    tracking_id: TrackingId,
    #[serde(with = "session_ids_list")]
    session_tracking_label: Vec<SessionId>,
}
