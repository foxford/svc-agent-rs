use serde_derive::{Deserialize, Serialize};

use super::super::*;
use crate::{AccountId, Addressable, AgentId, Authenticable};

/// Properties of an incoming event message.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IncomingEventProperties {
    #[serde(flatten)]
    conn: ConnectionProperties,
    label: Option<String>,
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

impl IncomingEventProperties {
    pub fn to_connection(&self) -> Connection {
        self.conn.to_connection()
    }

    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
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

    pub fn tags(&self) -> &ExtraTags {
        &self.tags
    }

    /// Builds [OutgoingEventProperties](struct.OutgoingEventProperties.html) based on the
    /// [IncomingEventProperties](struct.IncomingEventProperties.html).
    ///
    /// Use it to dispatch an event as a reaction on another event.
    ///
    /// # Arguments
    ///
    /// * `label` – outgoing event label.
    /// * `short_term_timing` – outgoing event's short term timing properties.
    ///
    /// # Example
    ///
    /// ```
    /// let short_term_timing = OutgoingShortTermTimingProperties::until_now(start_timestamp);
    /// let out_props = in_props.to_event("agent.enter", short_term_timing);
    /// ```
    pub fn to_event(
        &self,
        label: &'static str,
        short_term_timing: OutgoingShortTermTimingProperties,
    ) -> OutgoingEventProperties {
        let long_term_timing = self.update_long_term_timing(&short_term_timing);
        let mut props = OutgoingEventProperties::new(label, short_term_timing);
        props.set_long_term_timing(long_term_timing);
        props.set_tracking(self.tracking.clone());
        props.set_tags(self.tags.clone());
        if let Some(ref label) = self.local_tracking_label {
            props.set_local_tracking_label(label.to_owned());
        }
        props
    }

    fn update_long_term_timing(
        &self,
        short_term_timing: &OutgoingShortTermTimingProperties,
    ) -> LongTermTimingProperties {
        self.long_term_timing
            .clone()
            .update_cumulative_timings(short_term_timing)
    }
}

impl Authenticable for IncomingEventProperties {
    fn as_account_id(&self) -> &AccountId {
        &self.conn.as_account_id()
    }
}

impl Addressable for IncomingEventProperties {
    fn as_agent_id(&self) -> &AgentId {
        &self.conn.as_agent_id()
    }
}

pub type IncomingEvent<T> = IncomingMessageContent<T, IncomingEventProperties>;

impl<String: std::ops::Deref<Target = str>> IncomingEvent<String> {
    pub fn convert_payload<T>(message: &IncomingEvent<String>) -> Result<T, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = serde_json::from_str::<T>(&message.payload()).map_err(|e| {
            Error::new(&format!(
                "error deserializing payload of an envelope, {}",
                &e
            ))
        })?;
        Ok(payload)
    }

    pub fn convert<T>(message: IncomingEvent<String>) -> Result<IncomingEvent<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let props = message.properties().to_owned();
        let payload = serde_json::from_str::<T>(&message.payload()).map_err(|e| {
            Error::new(&format!(
                "error deserializing payload of an envelope, {}",
                &e
            ))
        })?;
        Ok(IncomingEvent::new(payload, props))
    }
}
