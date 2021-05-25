use serde::{Deserialize, Serialize};

use crate::{
    Addressable, Authenticable, Destination, Error, EventSubscription, RequestSubscription,
    ResponseSubscription, Source,
};

/// HTTP status code.
pub type ResponseStatus = http::StatusCode;

////////////////////////////////////////////////////////////////////////////////

pub trait SubscriptionTopic {
    /// Returns a topic to subscribe to as string.
    ///
    /// # Arguments
    ///
    /// * `agent_id` – current agent's [AgentId](../struct.AgentId.html).
    /// * `me_version` – current agent's API version.
    ///
    /// # Example
    ///
    /// ```
    /// let me = AgentId::new("instance01", AccountId::new("me_name", "svc.example.org"));
    /// let agent = AgentId::new("instance01", AccountId::new("service_name", "svc.example.org"));
    /// let subscription = Subscription::broadcast_events(&agent, "v1", "rooms/123/events");
    /// let topic = subscription.subscription_topic(me, "v1")?;
    /// ```
    fn subscription_topic<A>(&self, agent_id: &A, me_version: &str) -> Result<String, Error>
    where
        A: Addressable;
}

impl SubscriptionTopic for &'static str {
    fn subscription_topic<A>(&self, _me: &A, _me_version: &str) -> Result<String, Error>
    where
        A: Addressable,
    {
        Ok((*self).to_string())
    }
}

impl<'a> SubscriptionTopic for EventSubscription<'a> {
    fn subscription_topic<A>(&self, _me: &A, _me_version: &str) -> Result<String, Error>
    where
        A: Addressable,
    {
        match self.source {
            Source::Broadcast(ref from_account_id, ref version, ref uri) => Ok(format!(
                "apps/{app}/api/{version}/{uri}",
                app = from_account_id,
                version = version,
                uri = uri,
            )),
            _ => Err(Error::new(&format!(
                "source = '{:?}' is incompatible with event subscription",
                self.source,
            ))),
        }
    }
}

impl<'a> SubscriptionTopic for RequestSubscription<'a> {
    fn subscription_topic<A>(&self, me: &A, me_version: &str) -> Result<String, Error>
    where
        A: Addressable,
    {
        match self.source {
            Source::Multicast(Some(ref from_agent_id), ver) => Ok(format!(
                "agents/{agent_id}/api/{version}/out/{app}",
                agent_id = from_agent_id,
                version = ver.unwrap_or("+"),
                app = me.as_account_id(),
            )),
            Source::Multicast(None, ver) => Ok(format!(
                "agents/+/api/{version}/out/{app}",
                version = ver.unwrap_or("+"),
                app = me.as_account_id(),
            )),
            Source::Unicast(Some(ref from_account_id)) => Ok(format!(
                "agents/{agent_id}/api/{version}/in/{app}",
                agent_id = me.as_agent_id(),
                version = me_version,
                app = from_account_id,
            )),
            Source::Unicast(None) => Ok(format!(
                "agents/{agent_id}/api/{version}/in/+",
                agent_id = me.as_agent_id(),
                version = me_version,
            )),
            _ => Err(Error::new(&format!(
                "source = '{:?}' is incompatible with request subscription",
                self.source,
            ))),
        }
    }
}

impl<'a> SubscriptionTopic for ResponseSubscription<'a> {
    fn subscription_topic<A>(&self, me: &A, me_version: &str) -> Result<String, Error>
    where
        A: Addressable,
    {
        match self.source {
            Source::Unicast(Some(ref from_account_id)) => Ok(format!(
                "agents/{agent_id}/api/{version}/in/{app}",
                agent_id = me.as_agent_id(),
                version = me_version,
                app = from_account_id,
            )),
            Source::Unicast(None) => Ok(format!(
                "agents/{agent_id}/api/{version}/in/+",
                agent_id = me.as_agent_id(),
                version = me_version,
            )),
            _ => Err(Error::new(&format!(
                "source = '{:?}' is incompatible with response subscription",
                self.source,
            ))),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, Hash, Eq, PartialEq)]
pub struct ExtraTags {
    app_label: Option<String>,
    app_version: Option<String>,
    app_audience: Option<String>,
    scope: Option<String>,
    #[serde(skip_deserializing)]
    request_method: Option<String>,
}

impl ExtraTags {
    pub fn set_method(&mut self, method: &str) {
        self.request_method = Some(method.to_owned());
    }
}

/// Quality of service that defines delivery guarantee level.
///
/// MQTT protocol defines three quality of service levels:
///
/// * 0 – at least once; no delivery guarantee.
/// * 1 – at most once; guaranteed to deliver but duplicates may arrive.
/// * 2 – exactly once; guaranteed to deliver only once.
///
/// The more the level – the more the performance overhead.
///
/// svc-agent sets QoS = 0 for outgoing events and responses and QoS = 1 for outgoing requests.
/// This means that only requests are guaranteed to be delivered to the broker but duplicates
/// are possible so maintaining request idempotency is up to agents.
pub use rumqttc::QoS;

pub use agent::Address;
pub use agent::Agent;

pub use incoming_message::*;
pub use outgoing_message::*;

pub use publishable::*;
pub use timing_properties::*;
pub use tracking_properties::*;

pub use agent::*;

pub mod agent;
pub mod compat;
pub mod publishable;

mod incoming_message;
mod outgoing_message;

mod timing_properties;
mod tracking_properties;
