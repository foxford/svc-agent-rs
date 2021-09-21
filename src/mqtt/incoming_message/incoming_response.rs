use serde::{Deserialize, Serialize};

use super::super::*;
use crate::{AccountId, Addressable, AgentId, Authenticable};

/// Properties of an incoming response.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IncomingResponseProperties {
    #[serde(with = "crate::serde::HttpStatusCodeRef")]
    status: ResponseStatus,
    correlation_data: String,
    #[serde(flatten)]
    conn: ConnectionProperties,
    #[serde(flatten)]
    long_term_timing: LongTermTimingProperties,
    #[serde(flatten)]
    short_term_timing: IncomingShortTermTimingProperties,
    #[serde(flatten)]
    tracking: TrackingProperties,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_tracking_label: Option<String>,
    #[serde(flatten)]
    tags: ExtraTags,
}

impl IncomingResponseProperties {
    pub fn status(&self) -> ResponseStatus {
        self.status
    }

    pub fn correlation_data(&self) -> &str {
        &self.correlation_data
    }

    pub fn long_term_timing(&self) -> &LongTermTimingProperties {
        &self.long_term_timing
    }

    pub fn short_term_timing(&self) -> &IncomingShortTermTimingProperties {
        &self.short_term_timing
    }

    pub fn tracking(&self) -> &TrackingProperties {
        &self.tracking
    }

    pub fn local_tracking_label(&self) -> &Option<String> {
        &self.local_tracking_label
    }

    pub fn to_connection(&self) -> Connection {
        self.conn.to_connection()
    }

    pub fn tags(&self) -> &ExtraTags {
        &self.tags
    }
}

impl Authenticable for IncomingResponseProperties {
    fn as_account_id(&self) -> &AccountId {
        self.conn.as_account_id()
    }
}

impl Addressable for IncomingResponseProperties {
    fn as_agent_id(&self) -> &AgentId {
        self.conn.as_agent_id()
    }
}

pub type IncomingResponse<T> = IncomingMessageContent<T, IncomingResponseProperties>;

impl<String: std::ops::Deref<Target = str>> IncomingResponse<String> {
    pub fn convert_payload<T>(message: &IncomingResponse<String>) -> Result<T, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = serde_json::from_str::<T>(message.payload()).map_err(|e| {
            Error::new(&format!(
                "error deserializing payload of an envelope, {}",
                &e
            ))
        })?;
        Ok(payload)
    }

    pub fn convert<T>(message: IncomingResponse<String>) -> Result<IncomingResponse<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let props = message.properties().to_owned();
        let payload = serde_json::from_str::<T>(message.payload()).map_err(|e| {
            Error::new(&format!(
                "error deserializing payload of an envelope, {}",
                &e
            ))
        })?;
        Ok(IncomingResponse::new(payload, props))
    }
}
