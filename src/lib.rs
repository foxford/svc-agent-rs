//! # Overview
//!
//! svc-agent is a library implementing common MQTT agent messaging pattern conventions and
//! abstracting out the protocol's specifics to enable building microservices with full-duplex
//! communication.
//!
//! On the broker side its counterpart is
//! [mqtt-gateway](https://github.com/netology-group/mqtt-gateway) plugin for
//! [VerneMQ](https://vernemq.com/).
//!
//! # Key concepts
//!
//! svc-agent is about exchanging messages between agents using pub-sub model.
//!
//! An agent is a service or end user who can publish and [subscribe](struct.Subscription.html)
//! to messages. Each agent has a unique [AgentId](struct.Agent.Id).
//!
//! Message can be of three types:
//!
//! 1. **Requests** that end users send to services. Services may call other services too.
//! 2. **Responses** that services send back.
//! 3. **Events** that just may happen in services and it pushes a notification to subscribers.
//!
//! Each outgoing message has a [Destination](enum.Destination.html) and each incoming message has a
//! [Source](enum.Source.html) which can also be of three types:
//!
//! 1. **Broadcast** that is being received by each of the subscribed agents.
//! 2. **Multicast** that is being received by only one agent of a
//! [SharedGroup](struct.SharedGroup.html) of subscribers.
//! 3. **Unicast** that is intended for a specific agent.

#[cfg_attr(feature = "diesel", macro_use)]
#[cfg(feature = "diesel")]
extern crate diesel;

use std::fmt;
use std::str::FromStr;

////////////////////////////////////////////////////////////////////////////////

/// Something that can be addressed as agent.
pub trait Addressable: Authenticable {
    /// Returns the [AgentId](struct.AgentId.html) reference of the addressable object.
    fn as_agent_id(&self) -> &AgentId;
}

////////////////////////////////////////////////////////////////////////////////

/// Agent identifier.
///
/// It consists of a string `label` and [AccountId](struct.AccountId.html) and must be unique.
///
/// Multiple agents may use the same [AccountId](struct.AccountId.html), e.g. multiple instances
/// of the same service or multiple devices or browser tabs of an end user, but the `label`
/// must be different across them. An agent identifier has to be unique, otherwise it gets
/// disconnected by the broker. You can safely use the same `label` if
/// [AccountId](struct.AccountId.html) is different.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "diesel", derive(FromSqlRow, AsExpression))]
#[cfg_attr(feature = "diesel", sql_type = "sql::Agent_id")]
pub struct AgentId {
    account_id: AccountId,
    label: String,
}

#[cfg(feature = "sqlx")]
impl sqlx::encode::Encode<'_, sqlx::Postgres> for AgentId
where
    AccountId: for<'q> sqlx::encode::Encode<'q, sqlx::Postgres>,
    AccountId: sqlx::types::Type<sqlx::Postgres>,
    String: for<'q> sqlx::encode::Encode<'q, sqlx::Postgres>,
    String: sqlx::types::Type<sqlx::Postgres>,
{
    fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
        let mut encoder = sqlx::postgres::types::PgRecordEncoder::new(buf);
        encoder.encode(&self.account_id);
        encoder.encode(&self.label);
        encoder.finish();
        sqlx::encode::IsNull::No
    }
    fn size_hint(&self) -> usize {
        2usize * (4 + 4)
            + <AccountId as sqlx::encode::Encode<sqlx::Postgres>>::size_hint(&self.account_id)
            + <String as sqlx::encode::Encode<sqlx::Postgres>>::size_hint(&self.label)
    }
}

// This is what `derive(sqlx::Type)` expands to but with fixed lifetime.
// https://github.com/launchbadge/sqlx/issues/672
#[cfg(feature = "sqlx")]
impl<'r> sqlx::decode::Decode<'r, sqlx::Postgres> for AgentId
where
    // Originally it was `AccountId: sqlx::decode::Decode<'r, sqlx::Postgres>,`
    AccountId: for<'q> sqlx::decode::Decode<'q, sqlx::Postgres>,
    AccountId: sqlx::types::Type<sqlx::Postgres>,
    String: sqlx::decode::Decode<'r, sqlx::Postgres>,
    String: sqlx::types::Type<sqlx::Postgres>,
{
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> std::result::Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let mut decoder = sqlx::postgres::types::PgRecordDecoder::new(value)?;
        let account_id = decoder.try_decode::<AccountId>()?;
        let label = decoder.try_decode::<String>()?;
        Ok(AgentId { account_id, label })
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for AgentId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("agent_id")
    }
}

impl AgentId {
    /// Builds an [AgentId](struct.AgentId.html).
    ///
    /// # Arguments
    ///
    /// * `label` – a unique string to identify the particular agent.
    /// For example the name of a service instance or a user device.
    ///
    /// * `account_id` – the account identifier of an agent.
    ///
    /// # Example
    ///
    /// ```
    /// let agent_id1 = AgentId::new("instance01", AccountId::new("service_name", "svc.example.org"));
    /// let agent_id2 = AgentId::new("web", AccountId::new("user_name", "usr.example.org"));
    /// ```
    pub fn new(label: &str, account_id: AccountId) -> Self {
        Self {
            label: label.to_owned(),
            account_id,
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }
}

impl fmt::Display for AgentId {
    /// Formats [AgentId](struct.AgentId.html) as `LABEL.ACCOUNT_ID`.
    ///
    /// # Example
    ///
    /// ```
    /// let agent_id = AgentId::new("instance01", AccountId::new("service_name", "svc.example.org"));
    /// format!("{}", agent_id); // => "instance01.service_name.svc.example.org"
    /// ```
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}.{}", self.label(), self.account_id)
    }
}

impl FromStr for AgentId {
    type Err = Error;

    /// Parses [AgentId](struct.AgentId.html) from `LABEL.ACCOUNT_ID` format.
    ///
    /// # Example
    ///
    /// ```
    /// let agent_id = AgentId::from_str("instance01.service_name.svc.example.org"));
    /// ```
    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(2, '.').collect();
        match parts[..] {
            [ref label, ref rest] => {
                let account_id = rest.parse::<AccountId>().map_err(|e| {
                    Error::new(&format!(
                        "error deserializing shared group from a string, {}",
                        &e
                    ))
                })?;
                Ok(Self::new(label, account_id))
            }
            _ => Err(Error::new(&format!(
                "invalid value for the agent id: {}",
                val
            ))),
        }
    }
}

impl Authenticable for AgentId {
    fn as_account_id(&self) -> &AccountId {
        &self.account_id
    }
}

impl Addressable for AgentId {
    fn as_agent_id(&self) -> &Self {
        self
    }
}

////////////////////////////////////////////////////////////////////////////////

/// A group of agents which [shares a subscription](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901250).
/// Commonly used for balancing requests over a group of instances of some service.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SharedGroup {
    label: String,
    account_id: AccountId,
}

impl SharedGroup {
    /// Builds a [SharedGroup](struct.SharedGroup).
    ///
    /// # Arguments
    ///
    /// * `label` – shared group label to distinct it from the others if there are any.
    /// * `account_id` – service account id. All the group's participant agents must have the same.
    ///
    /// # Example
    ///
    /// ```
    /// let_account_id = AccountId::new("service_name", "svc.example.org");
    /// let shared_group = SharedGroup::new("loadbalancer", account_id);
    /// ```
    pub fn new(label: &str, account_id: AccountId) -> Self {
        Self {
            label: label.to_owned(),
            account_id,
        }
    }
}

impl fmt::Display for SharedGroup {
    /// Formats [SharedGroup](struct.SharedGroup.html) as `LABEL.ACCOUNT_ID`.
    ///
    /// # Example
    ///
    /// ```
    /// let_account_id = AccountId::new("service_name", "svc.example.org");
    /// let shared_group = SharedGroup::new("loadbalancer", account_id);
    /// format!("{}", shared_group); // => "loadbalancer.service_name.svc.example.org"
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.label, self.account_id)
    }
}

impl FromStr for SharedGroup {
    type Err = Error;

    /// Parses [SharedGroup](struct.SharedGroup.html) from `LABEL.ACCOUNT_ID` format.
    ///
    /// # Example
    ///
    /// ```
    /// let shared_group = SharedGroup::from_str("loadbalancer.service_name.svc.example.org"));
    /// ```
    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(2, '.').collect();
        match parts[..] {
            [ref label, ref rest] => {
                let account_id = rest.parse::<AccountId>().map_err(|e| {
                    Error::new(&format!(
                        "error deserializing shared group from a string, {}",
                        &e
                    ))
                })?;
                Ok(Self::new(label, account_id))
            }
            _ => Err(Error::new(&format!(
                "invalid value for the application group: {}",
                val
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Message destination.
///
/// This is an abstraction over MQTT topic pattern to determine outgoing message's publish topic.
///
/// Understanding message routing is the key thing to use svc-agent.
/// Make sure that you understand the patterns below and their counterparts described in
/// [Source](enum.Source.html).
///
/// ### MQTT topic variables:
///
/// * `MY_ACCOUNT_ID` – [AccountId](struct.AccountId) of the current agent that sends the message.
/// * `MY_VER` – API version string of the current agent. For example: `v1`.
/// * `MY_BROADCAST_URI`– current agent's API specific path to some resource divided by `/`.
/// For example: `/rooms/ROOM_ID/events`. If you will want to change its structure in the future
/// you must also bump `VER(ME)`.
/// * `ACCOUNT_ID` – destination [AccountId](struct.AccountId) (no specific agent).
/// * `AGENT_ID` – destination [AgentId](struct.AgentId).
/// * `VER` – destination agent version.
#[derive(Debug)]
pub enum Destination {
    /// Publish a message to each of the topic subscribers.
    ///
    /// Typically being used for publishing notification events from a service.
    ///
    /// The string supplied is `MY_BROADCAST_URI`.
    ///
    /// ### Patterns
    ///
    /// | Type  | Pattern    | MQTT topic                                           |
    /// |-------|------------|------------------------------------------------------|
    /// | event | app-to-any | apps/`MY_ACCOUNT_ID`/api/`MY_VER`/`MY_BROADCAST_URI` |
    Broadcast(String),
    /// Publish a message to any single of [SharedGroup](struct.SharedGroup.html) agents.
    ///
    /// Typically being used for sending requests to services which may have multiple instances.
    ///
    /// [AccountId](struct.AccountId.html) is being supplied since we don't care which specific
    /// instance will process the message.
    ///
    /// ### Patterns
    ///
    /// | Type     | Pattern     | MQTT topic                                         |
    /// |----------|-------------|----------------------------------------------------|
    /// | request  | one-to_app  | agents/`MY_AGENT_ID`/api/`MY_VER`/out/`ACCOUNT_ID` |
    Multicast(AccountId, String),
    /// Publish a message to the specific instance with known `AGENT_ID`.
    ///
    /// Typically being used for responding to requests.
    /// Also used for making a request to a specific instance of a stateful service.
    ///
    /// Values supplied are `AGENT_ID` and `VER`.
    ///
    /// ### Patterns
    ///
    /// | Type     | Pattern     | MQTT topic                                     |
    /// |----------|-------------|------------------------------------------------|
    /// | request  | one-to-one  | agents/`AGENT_ID`/api/`VER`/in/`MY_ACCOUNT_ID` |
    /// | response | one-to_one  | agents/`AGENT_ID`/api/`VER`/in/`MY_ACCOUNT_ID` |
    Unicast(AgentId, String),
}

////////////////////////////////////////////////////////////////////////////////

/// Message source.
///
/// This is an abstraction over MQTT topic pattern to determine the subscription topic to receive
/// messages.
///
/// If you want to subscribe to a topic consider using [Subscription](struct.Subscription.html)
/// builder or building [RequestSubscription](struct.RequestSubscription.html),
/// [ResponseSubscription](struct.ResponseSubscription.html) or
/// [EventSubscription](struct.EventSubscription.html) directly when you need something special.
///
/// Understanding message routing is the key thing to use svc-agent.
/// Make sure that you understand the patterns below and their counterparts described in
/// [Destination](enum.Destination.html).
///
/// ### MQTT topic variables:
///
/// * `MY_ACCOUNT_ID` – [AccountId](struct.AccountId) of the current agent that send the message.
/// * `MY_VER` – API version string of the current agent. For example: `v1`.
/// * `ACCOUNT_ID` – source [AccountId](struct.AccountId) (no specific agent).
/// * `AGENT_ID` – source [AgentId](struct.AgentId).
/// * `VER` – source agent version.
/// * `BROADCAST_URI` source agent's API specific path to some resource divided by `/`.
/// For example: `/rooms/ROOM_ID/events`. Use `+` as single-level wildcard like `/room/+/events`
/// to subscribe to events in all rooms and `#` as multi-level  wildcard like `/rooms/#` to
/// subscribe to all rooms and their nested resources.
#[derive(Debug)]
pub enum Source<'a> {
    /// Receive a message along with other subscribers.
    ///
    /// Typically used for receiving notification events from a service.
    ///
    /// Value supplied are `ACCOUNT_ID`, `VER` and `BROADCAST_URI`.
    ///
    /// ### Patterns
    ///
    /// | Type  | Pattern      | MQTT topic                                  |
    /// |-------|--------------|---------------------------------------------|
    /// | event | any-from-app | apps/`ACCOUNT_ID`/api/`VER`/`BROADCAST_URI` |
    Broadcast(&'a AccountId, &'a str, &'a str),
    /// Receive a message from any single of [SharedGroup](struct.SharedGroup.html) agents.
    ///
    /// Typically used for receiving requests by services which may have multiple instances.
    ///
    /// [AccountId](struct.AccountId.html) is being supplied since we don't care which specific
    /// instance will process the message.
    ///
    /// Optional values supplied are `AGENT_ID` and `VER`. If `None` is specified for either of
    /// the two then wildcard is being used to receive messages from any agent or its API version.
    ///
    /// ### Patterns
    ///
    /// | Type     | Pattern      | MQTT topic                                    |
    /// |----------|--------------|-----------------------------------------------|
    /// | request  | app-from-any | agents/+/api/+/out/`MY_ACCOUNT_ID`            |
    /// | request  | app-from-any | agents/+/api/VER/out/`MY_ACCOUNT_ID`          |
    /// | request  | app-from-any | agents/`AGENT_ID`/api/+/out/`MY_ACCOUNT_ID`   |
    /// | request  | app-from-any | agents/`AGENT_ID`/api/VER/out/`MY_ACCOUNT_ID` |
    Multicast(Option<&'a AgentId>, Option<&'a str>),
    /// Receive a message sent specifically to the current agent by its `AGENT_ID`.
    ///
    /// Typically being used for receiving responses for requests.
    /// Also used for receiving a request by a specific instance of a stateful service.
    ///
    /// Optional `ACCOUNT_ID` may be supplied to specify an [AccountId](struct.AccountId.html)
    /// to subscribe to. If `None` is specified then wildcard is being used to receive unicast
    /// messages from any account.
    ///
    /// ### Patterns
    ///
    /// | Type     | Pattern      | MQTT topic                                        |
    /// |----------|--------------|---------------------------------------------------|
    /// | request  | one-from-one | agents/`MY_AGENT_ID`/api/`MY_VER`/in/`ACCOUNT_ID` |
    /// | request  | one-from-any | agents/`MY_AGENT_ID`/api/`MY_VER`/in/+            |
    /// | response | one-from-one | agents/`MY_AGENT_ID`/api/`MY_VER`/in/`ACCOUNT_ID` |
    /// | response | one-from-any | agents/`MY_AGENT_ID`/api/`MY_VER`/in/+            |
    Unicast(Option<&'a AccountId>),
}

////////////////////////////////////////////////////////////////////////////////

/// Messages subscription builder.
pub struct Subscription {}

impl Subscription {
    /// Builds an [EventSubscription](struct.EventSubscription) for
    /// [broadcast events](struct.Source.html#variant.Broadcast).
    ///
    /// Use it to subscribe to events from some service,
    ///
    /// # Arguments
    ///
    /// * `from` – anything [Addressable](trait.Addressable) to receive events from.
    /// For example service [AgentId](struct.AgentId).
    /// * `version` – API version string of the `from` agent. Example: `v1`.
    /// * `uri` – resource path divided by `/` to receive events on. Example: `room/ROOM_ID/events`.
    ///
    /// # Example
    ///
    /// ```
    /// let agent = AgentId::new("instance01", AccountId::new("service_name", "svc.example.org"));
    /// let subscription = Subscription::broadcast_events(&agent, "v1", "rooms/123/events");
    /// ```
    pub fn broadcast_events<'a, A>(
        from: &'a A,
        version: &'a str,
        uri: &'a str,
    ) -> EventSubscription<'a>
    where
        A: Authenticable,
    {
        EventSubscription::new(Source::Broadcast(from.as_account_id(), version, uri))
    }

    /// Builds a [RequestSubscription](struct.RequestSubscription) for
    /// [multicast requests](struct.Source#variant.Multicast) from any agent.
    ///
    /// Use it to subscribe a stateless service endpoint to its consumers' requests.
    ///
    /// # Arguments
    ///
    /// * `version` – API version string of the `from` agent. Example: `v1`.
    ///
    /// # Example
    ///
    /// ```
    /// let subscription = Subscription::multicast_requests("v1");
    /// ```
    pub fn multicast_requests(version: Option<&str>) -> RequestSubscription {
        RequestSubscription::new(Source::Multicast(None, version))
    }

    /// Builds a [RequestSubscription](struct.RequestSubscription) for
    /// [multicast requests](struct.Source#variant.Multicast) from a specific agent.
    ///
    /// This is the same as [multicast_requests](struct.Subscription.html#method.multicast_requests)
    /// but subscribes only from requests from a specific agent.
    ///
    /// # Arguments
    ///
    /// * `from` – anything [Addressable](trait.Addressable) to receive requests from.
    /// For example service [AgentId](struct.AgentId).
    /// * `version` – API version string of the `from` agent. Example: `v1`.
    ///
    /// # Example
    ///
    /// ```
    /// let agent = AgentId::new("instance01", AccountId::new("service_name", "svc.example.org"));
    /// let subscription = Subscription::multicast_requests_from(&agent, "v1");
    /// ```
    pub fn multicast_requests_from<'a, A>(
        from: &'a A,
        version: Option<&'a str>,
    ) -> RequestSubscription<'a>
    where
        A: Addressable,
    {
        RequestSubscription::new(Source::Multicast(Some(from.as_agent_id()), version))
    }

    /// Builds a [RequestSubscription](struct.RequestSubscription) for
    /// [unicast requests](struct.Source#variant.Unicast) from any agent.
    ///
    /// Use it to subscribe a stateful service endpoint to its consumers' requests.
    ///
    /// # Example
    ///
    /// ```
    /// let subscription = Subscription::unicast_requests();
    /// ```
    pub fn unicast_requests<'a>() -> RequestSubscription<'a> {
        RequestSubscription::new(Source::Unicast(None))
    }

    /// Builds a [RequestSubscription](struct.RequestSubscription) for
    /// [unicast requests](struct.Source#variant.Unicast) from a specific agent.
    ///
    /// This is the same as [unicast_requests](struct.Subscription.html#method.unicast_requests)
    /// but subscribes only from requests from a specific agent.
    ///
    /// # Arguments
    ///
    /// * `from` – anything [Addressable](trait.Addressable) to receive requests from.
    /// For example service [AgentId](struct.AgentId).
    ///
    /// # Example
    ///
    /// ```
    /// let agent = AgentId::new("instance01", AccountId::new("service_name", "svc.example.org"));
    /// let subscription = Subscription::unicast_requests(&agent);
    /// ```
    pub fn unicast_requests_from<A>(from: &A) -> RequestSubscription
    where
        A: Authenticable,
    {
        RequestSubscription::new(Source::Unicast(Some(from.as_account_id())))
    }

    /// Builds a [ResponseSubscription](struct.ResponseSubscription) for
    /// [unicast requests](struct.Source#variant.Unicast) from any agent.
    ///
    /// Use it to subscribe to responses from all services.
    ///
    /// # Example
    ///
    /// ```
    /// let subscription = Subscription::unicast_responses();
    /// ```
    pub fn unicast_responses<'a>() -> ResponseSubscription<'a> {
        ResponseSubscription::new(Source::Unicast(None))
    }

    /// Builds a [ResponseSubscription](struct.ResponseSubscription) for
    /// [unicast requests](struct.Source#variant.Unicast) from a specific agent.
    ///
    /// This is the same as [unicast_responses](struct.Subscription.html#method.unicast_responses)
    /// but subscribes only from requests from a specific agent.
    ///
    /// # Example
    ///
    /// ```
    /// let agent = AgentId::new("instance01", AccountId::new("service_name", "svc.example.org"));
    /// let subscription = Subscription::unicast_responses_from(&agent);
    /// ```
    pub fn unicast_responses_from<A>(from: &A) -> ResponseSubscription
    where
        A: Authenticable,
    {
        ResponseSubscription::new(Source::Unicast(Some(from.as_account_id())))
    }
}

pub struct EventSubscription<'a> {
    source: Source<'a>,
}

impl<'a> EventSubscription<'a> {
    /// Builds an [EventSubscription](struct.EventSubscription).
    ///
    /// # Arguments
    ///
    /// * `source` – events source.
    ///
    /// # Example
    ///
    /// ```
    /// let account_id = AccountId::new("service_name", "svc.example.org");
    /// let source = Source::Broadcast(&account_id, "v1", "rooms/+/events");
    /// let subscription = EventSubscription::new(source);
    /// ```
    pub fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

pub struct RequestSubscription<'a> {
    source: Source<'a>,
}

impl<'a> RequestSubscription<'a> {
    /// Builds a [RequestSubscription](struct.RequestSubscription).
    ///
    /// # Arguments
    ///
    /// * `source` – requests source.
    ///
    /// # Example
    ///
    /// ```
    /// let subscription = RequestSubscription::new(Source::Multicast(None, "v1"));
    /// ```
    pub fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

pub struct ResponseSubscription<'a> {
    source: Source<'a>,
}

impl<'a> ResponseSubscription<'a> {
    /// Builds a [ResponseSubscription](struct.ResponseSubscription).
    ///
    /// # Arguments
    ///
    /// * `source` – responses source.
    ///
    /// # Example
    ///
    /// ```
    /// let account_id = AccountId::new("service_name", "svc.example.org");
    /// let subscription = RequestSubscription::new(Source::Unicast(&account_id));
    /// ```
    pub fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Integration with [diesel](https://crates.io/crates/diesel).
///
/// It implements a diesel type for [AgentId](struct.AgentId) so you can store them in a database.
///
/// Compile with `diesel` feature enabled to make use of it.
#[cfg(feature = "diesel")]
pub mod sql {
    use super::{AccountId, AgentId};

    use diesel::deserialize::{self, FromSql};
    use diesel::pg::Pg;
    use diesel::serialize::{self, Output, ToSql, WriteTuple};
    use diesel::sql_types::{Record, Text};
    use std::io::Write;

    #[derive(SqlType, QueryId)]
    #[postgres(type_name = "agent_id")]
    #[allow(non_camel_case_types)]
    pub struct Agent_id;

    impl ToSql<Agent_id, Pg> for AgentId {
        fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
            WriteTuple::<(Account_id, Text)>::write_tuple(&(&self.account_id, &self.label), out)
        }
    }

    impl FromSql<Agent_id, Pg> for AgentId {
        fn from_sql(bytes: Option<&[u8]>) -> deserialize::Result<Self> {
            let (account_id, label): (AccountId, String) =
                FromSql::<Record<(Account_id, Text)>, Pg>::from_sql(bytes)?;
            Ok(AgentId::new(&label, account_id))
        }
    }

    pub use svc_authn::sql::Account_id;
}

////////////////////////////////////////////////////////////////////////////////

pub use svc_authn::{AccountId, Authenticable};

pub use self::error::Error;
pub mod error;
pub mod mqtt;
#[cfg(feature = "queue-counter")]
pub mod queue_counter;
pub mod request;
pub(crate) mod serde;
