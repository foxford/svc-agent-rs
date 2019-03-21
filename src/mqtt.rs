use serde_derive::{Deserialize, Serialize};
use std::fmt;

use crate::{
    AccountId, Addressable, AgentId, Authenticable, Destination, Error, EventSubscription,
    RequestSubscription, ResponseSubscription, SharedGroup, Source,
};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum ConnectionMode {
    Default,
    Service,
    Bridge,
}

impl fmt::Display for ConnectionMode {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}",
            match self {
                ConnectionMode::Default => "agents",
                ConnectionMode::Service => "service-agents",
                ConnectionMode::Bridge => "bridge-agents",
            }
        )
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct AgentConfig {
    uri: String,
    clean_session: Option<bool>,
    keep_alive_interval: Option<u64>,
    reconnect_interval: Option<u64>,
    outgoing_message_queue_size: Option<usize>,
    incomming_message_queue_size: Option<usize>,
}

#[derive(Debug)]
pub struct AgentBuilder {
    agent_id: AgentId,
    version: String,
    mode: ConnectionMode,
}

impl AgentBuilder {
    pub fn new(agent_id: AgentId) -> Self {
        Self {
            agent_id,
            version: String::from("v1.mqtt3"),
            mode: ConnectionMode::Default,
        }
    }

    pub fn version(self, version: &str) -> Self {
        Self {
            version: version.to_owned(),
            ..self
        }
    }

    pub fn mode(self, mode: ConnectionMode) -> Self {
        Self { mode, ..self }
    }

    pub fn start(
        self,
        config: &AgentConfig,
    ) -> Result<(Agent, rumqtt::Receiver<rumqtt::Notification>), Error> {
        let options = Self::mqtt_options(&self.mqtt_client_id(), &config)?;
        let (tx, rx) = rumqtt::MqttClient::start(options)
            .map_err(|e| Error::new(&format!("error starting MQTT client, {}", e)))?;

        let agent = Agent::new(self.agent_id, tx);
        Ok((agent, rx))
    }

    fn mqtt_client_id(&self) -> String {
        format!(
            "{version}/{mode}/{agent_id}",
            version = self.version,
            mode = self.mode,
            agent_id = self.agent_id,
        )
    }

    fn mqtt_options(client_id: &str, config: &AgentConfig) -> Result<rumqtt::MqttOptions, Error> {
        let uri = config
            .uri
            .parse::<http::Uri>()
            .map_err(|e| Error::new(&format!("error parsing MQTT connection URL, {}", e)))?;
        let host = uri.host().ok_or_else(|| Error::new("missing MQTT host"))?;
        let port = uri
            .port_part()
            .ok_or_else(|| Error::new("missing MQTT port"))?;

        let mut opts = rumqtt::MqttOptions::new();
        opts.set_client_id(client_id);
        opts.set_host(host);
        opts.set_port(port.as_u16());
        if let Some(value) = config.clean_session {
            opts.set_clean_session(value);
        };
        if let Some(value) = config.keep_alive_interval {
            opts.set_keep_alive(value);
        };
        if let Some(value) = config.reconnect_interval {
            opts.set_reconnect_opts(rumqtt::ReconnectOptions::Always(value));
        };
        if let Some(value) = config.incomming_message_queue_size {
            opts.set_notification_channel_capacity(value);
        };
        if let Some(value) = config.outgoing_message_queue_size {
            opts.set_outgoing_queuelimit(value, std::time::Duration::from_secs(5));
        };

        Ok(opts)
    }
}

#[derive(Clone)]
pub struct Agent {
    id: AgentId,
    tx: rumqtt::MqttClient,
}

impl Agent {
    fn new(id: AgentId, tx: rumqtt::MqttClient) -> Self {
        Self { id, tx }
    }

    pub fn id(&self) -> &AgentId {
        &self.id
    }

    pub fn publish<M>(&mut self, message: &M) -> Result<(), Error>
    where
        M: Publishable,
    {
        let topic = message.destination_topic(&self.id)?;
        let bytes = message.to_bytes()?;

        self.tx
            .publish(topic, QoS::AtLeastOnce, false, bytes)
            .map_err(|e| Error::new(&format!("error publishing MQTT message, {}", &e)))
    }

    pub fn subscribe<S>(
        &mut self,
        subscription: &S,
        qos: QoS,
        maybe_group: Option<&SharedGroup>,
    ) -> Result<(), Error>
    where
        S: SubscriptionTopic,
    {
        let mut topic = subscription.subscription_topic(&self.id)?;
        if let Some(ref group) = maybe_group {
            topic = format!("$share/{group}/{topic}", group = group, topic = topic);
        };

        self.tx
            .subscribe(topic, qos)
            .map_err(|e| Error::new(&format!("error creating MQTT subscription, {}", e)))?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct AuthnProperties {
    agent_id: AgentId,
}

impl Authenticable for AuthnProperties {
    fn as_account_id(&self) -> &AccountId {
        &self.agent_id.as_account_id()
    }
}

impl Addressable for AuthnProperties {
    fn as_agent_id(&self) -> &AgentId {
        &self.agent_id
    }
}

impl From<AgentId> for AuthnProperties {
    fn from(agent_id: AgentId) -> Self {
        Self { agent_id }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct IncomingEventProperties {
    #[serde(flatten)]
    authn: AuthnProperties,
}

impl Authenticable for IncomingEventProperties {
    fn as_account_id(&self) -> &AccountId {
        &self.authn.as_account_id()
    }
}

impl Addressable for IncomingEventProperties {
    fn as_agent_id(&self) -> &AgentId {
        &self.authn.as_agent_id()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IncomingRequestProperties {
    method: String,
    correlation_data: String,
    response_topic: String,
    #[serde(flatten)]
    authn: AuthnProperties,
}

impl IncomingRequestProperties {
    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn correlation_data(&self) -> &str {
        &self.correlation_data
    }

    pub fn to_response(&self, status: ResponseStatus) -> OutgoingResponseProperties {
        OutgoingResponseProperties::new(status, &self.correlation_data, Some(&self.response_topic))
    }
}

impl Authenticable for IncomingRequestProperties {
    fn as_account_id(&self) -> &AccountId {
        &self.authn.as_account_id()
    }
}

impl Addressable for IncomingRequestProperties {
    fn as_agent_id(&self) -> &AgentId {
        &self.authn.as_agent_id()
    }
}

#[derive(Debug, Deserialize)]
pub struct IncomingResponseProperties {
    #[serde(with = "crate::serde::HttpStatusCodeRef")]
    status: ResponseStatus,
    correlation_data: String,
    #[serde(flatten)]
    authn: AuthnProperties,
}

impl IncomingResponseProperties {
    pub fn status(&self) -> ResponseStatus {
        self.status
    }

    pub fn correlation_data(&self) -> &str {
        &self.correlation_data
    }
}

impl Authenticable for IncomingResponseProperties {
    fn as_account_id(&self) -> &AccountId {
        &self.authn.as_account_id()
    }
}

impl Addressable for IncomingResponseProperties {
    fn as_agent_id(&self) -> &AgentId {
        &self.authn.as_agent_id()
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct IncomingMessage<T, P>
where
    P: Addressable,
{
    payload: T,
    properties: P,
}

impl<T, P> IncomingMessage<T, P>
where
    P: Addressable,
{
    pub fn new(payload: T, properties: P) -> Self {
        Self {
            payload,
            properties,
        }
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }

    pub fn properties(&self) -> &P {
        &self.properties
    }
}

impl<T> IncomingRequest<T> {
    pub fn to_response<R>(&self, data: R, status: ResponseStatus) -> OutgoingResponse<R>
    where
        R: serde::Serialize,
    {
        OutgoingMessage::new(
            data,
            self.properties.to_response(status),
            Destination::Unicast(self.properties().as_agent_id().clone()),
        )
    }
}

pub type IncomingEvent<T> = IncomingMessage<T, IncomingEventProperties>;
pub type IncomingRequest<T> = IncomingMessage<T, IncomingRequestProperties>;
pub type IncomingResponse<T> = IncomingMessage<T, IncomingResponseProperties>;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub struct OutgoingEventProperties {
    label: &'static str,
}

impl OutgoingEventProperties {
    pub fn new(label: &'static str) -> Self {
        Self { label }
    }
}

#[derive(Debug, Serialize)]
pub struct OutgoingRequestProperties {
    method: String,
    correlation_data: String,
    response_topic: String,
    #[serde(flatten)]
    authn: Option<AuthnProperties>,
}

impl OutgoingRequestProperties {
    pub fn new(method: &str, response_topic: &str, correlation_data: &str) -> Self {
        Self {
            method: method.to_owned(),
            response_topic: response_topic.to_owned(),
            correlation_data: correlation_data.to_owned(),
            authn: None,
        }
    }

    pub fn set_authn(&mut self, authn: AuthnProperties) -> &mut Self {
        self.authn = Some(authn);
        self
    }

    pub fn correlation_data(&self) -> &str {
        &self.correlation_data
    }
}

#[derive(Debug, Serialize)]
pub struct OutgoingResponseProperties {
    #[serde(with = "crate::serde::HttpStatusCodeRef")]
    status: ResponseStatus,
    correlation_data: String,
    #[serde(skip)]
    response_topic: Option<String>,
}

impl OutgoingResponseProperties {
    pub fn new(
        status: ResponseStatus,
        correlation_data: &str,
        response_topic: Option<&str>,
    ) -> Self {
        Self {
            status,
            correlation_data: correlation_data.to_owned(),
            response_topic: response_topic.map(ToOwned::to_owned),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub type ResponseStatus = http::StatusCode;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct OutgoingMessage<T, P>
where
    T: serde::Serialize,
{
    payload: T,
    properties: P,
    destination: Destination,
}

impl<T, P> OutgoingMessage<T, P>
where
    T: serde::Serialize,
{
    fn new(payload: T, properties: P, destination: Destination) -> Self {
        Self {
            payload,
            properties,
            destination,
        }
    }
}

impl<T> OutgoingEvent<T>
where
    T: serde::Serialize,
{
    pub fn broadcast(payload: T, properties: OutgoingEventProperties, to_uri: &str) -> Self {
        OutgoingMessage::new(
            payload,
            properties,
            Destination::Broadcast(to_uri.to_owned()),
        )
    }
}

impl<T> OutgoingRequest<T>
where
    T: serde::Serialize,
{
    pub fn multicast<A>(payload: T, properties: OutgoingRequestProperties, to: &A) -> Self
    where
        A: Authenticable,
    {
        OutgoingMessage::new(
            payload,
            properties,
            Destination::Multicast(to.as_account_id().clone()),
        )
    }

    pub fn unicast<A>(payload: T, properties: OutgoingRequestProperties, to: &A) -> Self
    where
        A: Addressable,
    {
        OutgoingMessage::new(
            payload,
            properties,
            Destination::Unicast(to.as_agent_id().clone()),
        )
    }
}

impl<T> OutgoingResponse<T>
where
    T: serde::Serialize,
{
    pub fn unicast<A>(payload: T, properties: OutgoingResponseProperties, to: &A) -> Self
    where
        A: Addressable,
    {
        OutgoingMessage::new(
            payload,
            properties,
            Destination::Unicast(to.as_agent_id().clone()),
        )
    }
}

pub type OutgoingEvent<T> = OutgoingMessage<T, OutgoingEventProperties>;
pub type OutgoingRequest<T> = OutgoingMessage<T, OutgoingRequestProperties>;
pub type OutgoingResponse<T> = OutgoingMessage<T, OutgoingResponseProperties>;

impl<T> compat::IntoEnvelope for OutgoingEvent<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<compat::OutgoingEnvelope, Error> {
        let payload = serde_json::to_string(&self.payload)
            .map_err(|e| Error::new(&format!("error serializing payload of an envelope, {}", e)))?;
        let envelope = compat::OutgoingEnvelope::new(
            &payload,
            compat::OutgoingEnvelopeProperties::Event(self.properties),
            self.destination,
        );
        Ok(envelope)
    }
}

impl<T> compat::IntoEnvelope for OutgoingRequest<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<compat::OutgoingEnvelope, Error> {
        let payload = serde_json::to_string(&self.payload)
            .map_err(|e| Error::new(&format!("error serializing payload of an envelope, {}", e)))?;
        let envelope = compat::OutgoingEnvelope::new(
            &payload,
            compat::OutgoingEnvelopeProperties::Request(self.properties),
            self.destination,
        );
        Ok(envelope)
    }
}

impl<T> compat::IntoEnvelope for OutgoingResponse<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<compat::OutgoingEnvelope, Error> {
        let payload = serde_json::to_string(&self.payload)
            .map_err(|e| Error::new(&format!("error serializing payload of an envelope, {}", e)))?;
        let envelope = compat::OutgoingEnvelope::new(
            &payload,
            compat::OutgoingEnvelopeProperties::Response(self.properties),
            self.destination,
        );
        Ok(envelope)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub trait Publishable {
    fn destination_topic<A>(&self, me: &A) -> Result<String, Error>
    where
        A: Addressable;

    fn to_bytes(&self) -> Result<String, Error>;
}

////////////////////////////////////////////////////////////////////////////////

pub trait Publish {
    fn publish(&self, tx: &mut Agent) -> Result<(), Error>;
}

impl<T> Publish for T
where
    T: Publishable,
{
    fn publish(&self, tx: &mut Agent) -> Result<(), Error> {
        tx.publish(self)?;
        Ok(())
    }
}

impl<T1, T2> Publish for (T1, T2)
where
    T1: Publishable,
    T2: Publishable,
{
    fn publish(&self, tx: &mut Agent) -> Result<(), Error> {
        tx.publish(&self.0)?;
        tx.publish(&self.1)?;
        Ok(())
    }
}

impl<T> Publish for Vec<T>
where
    T: Publishable,
{
    fn publish(&self, tx: &mut Agent) -> Result<(), Error> {
        for msg in self {
            tx.publish(msg)?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////

trait DestinationTopic {
    fn destination_topic<A>(&self, me: &A, dest: &Destination) -> Result<String, Error>
    where
        A: Addressable;
}

impl DestinationTopic for OutgoingEventProperties {
    fn destination_topic<A>(&self, me: &A, dest: &Destination) -> Result<String, Error>
    where
        A: Addressable,
    {
        match dest {
            Destination::Broadcast(ref uri) => Ok(format!(
                "apps/{app}/api/v1/{uri}",
                app = me.as_account_id(),
                uri = uri,
            )),
            _ => Err(Error::new(&format!(
                "destination = '{:?}' is incompatible with event message type",
                dest,
            ))),
        }
    }
}

impl DestinationTopic for OutgoingRequestProperties {
    fn destination_topic<A>(&self, me: &A, dest: &Destination) -> Result<String, Error>
    where
        A: Addressable,
    {
        match dest {
            Destination::Unicast(ref agent_id) => Ok(format!(
                "agents/{agent_id}/api/v1/in/{app}",
                agent_id = agent_id,
                app = me.as_account_id(),
            )),
            Destination::Multicast(ref account_id) => Ok(format!(
                "agents/{agent_id}/api/v1/out/{app}",
                agent_id = me.as_agent_id(),
                app = account_id,
            )),
            _ => Err(Error::new(&format!(
                "destination = '{:?}' is incompatible with request message type",
                dest,
            ))),
        }
    }
}

impl DestinationTopic for OutgoingResponseProperties {
    fn destination_topic<A>(&self, me: &A, dest: &Destination) -> Result<String, Error>
    where
        A: Addressable,
    {
        match &self.response_topic {
            Some(ref val) => Ok(val.to_owned()),
            None => match dest {
                Destination::Unicast(ref agent_id) => Ok(format!(
                    "agents/{agent_id}/api/v1/in/{app}",
                    agent_id = agent_id,
                    app = me.as_account_id(),
                )),
                _ => Err(Error::new(&format!(
                    "destination = '{:?}' is incompatible with response message type",
                    dest,
                ))),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub trait SubscriptionTopic {
    fn subscription_topic<A>(&self, agent_id: &A) -> Result<String, Error>
    where
        A: Addressable;
}

impl<'a> SubscriptionTopic for EventSubscription<'a> {
    fn subscription_topic<A>(&self, _me: &A) -> Result<String, Error>
    where
        A: Addressable,
    {
        match self.source {
            Source::Broadcast(ref from_account_id, ref uri) => Ok(format!(
                "apps/{app}/api/v1/{uri}",
                app = from_account_id,
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
    fn subscription_topic<A>(&self, me: &A) -> Result<String, Error>
    where
        A: Addressable,
    {
        match self.source {
            Source::Multicast(Some(ref from_agent_id)) => Ok(format!(
                "agents/{agent_id}/api/v1/out/{app}",
                agent_id = from_agent_id,
                app = me.as_account_id(),
            )),
            Source::Multicast(None) => Ok(format!(
                "agents/+/api/v1/out/{app}",
                app = me.as_account_id(),
            )),
            Source::Unicast(Some(ref from_account_id)) => Ok(format!(
                "agents/{agent_id}/api/v1/in/{app}",
                agent_id = me.as_agent_id(),
                app = from_account_id,
            )),
            Source::Unicast(None) => Ok(format!(
                "agents/{agent_id}/api/v1/in/+",
                agent_id = me.as_agent_id(),
            )),
            _ => Err(Error::new(&format!(
                "source = '{:?}' is incompatible with request subscription",
                self.source,
            ))),
        }
    }
}

impl<'a> SubscriptionTopic for ResponseSubscription<'a> {
    fn subscription_topic<A>(&self, me: &A) -> Result<String, Error>
    where
        A: Addressable,
    {
        match self.source {
            Source::Unicast(Some(ref from_account_id)) => Ok(format!(
                "agents/{agent_id}/api/v1/in/{app}",
                agent_id = me.as_agent_id(),
                app = from_account_id,
            )),
            Source::Unicast(None) => Ok(format!(
                "agents/{agent_id}/api/v1/in/+",
                agent_id = me.as_agent_id(),
            )),
            _ => Err(Error::new(&format!(
                "source = '{:?}' is incompatible with response subscription",
                self.source,
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub mod compat {

    use serde_derive::{Deserialize, Serialize};

    use super::{
        Destination, DestinationTopic, IncomingEvent, IncomingEventProperties, IncomingMessage,
        IncomingRequest, IncomingRequestProperties, IncomingResponse, IncomingResponseProperties,
        OutgoingEventProperties, OutgoingRequestProperties, OutgoingResponseProperties,
        Publishable,
    };
    use crate::Addressable;
    use crate::Error;

    ////////////////////////////////////////////////////////////////////////////////

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "lowercase")]
    #[serde(tag = "type")]
    pub enum IncomingEnvelopeProperties {
        Event(IncomingEventProperties),
        Request(IncomingRequestProperties),
        Response(IncomingResponseProperties),
    }

    #[derive(Debug, Deserialize)]
    pub struct IncomingEnvelope {
        payload: String,
        properties: IncomingEnvelopeProperties,
    }

    impl IncomingEnvelope {
        pub fn properties(&self) -> &IncomingEnvelopeProperties {
            &self.properties
        }

        pub fn payload<T>(&self) -> Result<T, Error>
        where
            T: serde::de::DeserializeOwned,
        {
            let payload = serde_json::from_str::<T>(&self.payload).map_err(|e| {
                Error::new(&format!(
                    "error deserializing payload of an envelope, {}",
                    &e
                ))
            })?;
            Ok(payload)
        }
    }

    pub fn into_event<T>(envelope: IncomingEnvelope) -> Result<IncomingEvent<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        match envelope.properties {
            IncomingEnvelopeProperties::Event(props) => Ok(IncomingMessage::new(payload, props)),
            _ => Err(Error::new("error serializing an envelope into event")),
        }
    }

    pub fn into_request<T>(envelope: IncomingEnvelope) -> Result<IncomingRequest<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        match envelope.properties {
            IncomingEnvelopeProperties::Request(props) => Ok(IncomingMessage::new(payload, props)),
            _ => Err(Error::new("error serializing an envelope into request")),
        }
    }

    pub fn into_response<T>(envelope: IncomingEnvelope) -> Result<IncomingResponse<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        match envelope.properties {
            IncomingEnvelopeProperties::Response(props) => Ok(IncomingMessage::new(payload, props)),
            _ => Err(Error::new("error serializing an envelope into response")),
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "lowercase")]
    #[serde(tag = "type")]
    pub enum OutgoingEnvelopeProperties {
        Event(OutgoingEventProperties),
        Request(OutgoingRequestProperties),
        Response(OutgoingResponseProperties),
    }

    #[derive(Debug, Serialize)]
    pub struct OutgoingEnvelope {
        payload: String,
        properties: OutgoingEnvelopeProperties,
        #[serde(skip)]
        destination: Destination,
    }

    impl OutgoingEnvelope {
        pub fn new(
            payload: &str,
            properties: OutgoingEnvelopeProperties,
            destination: Destination,
        ) -> Self {
            Self {
                payload: payload.to_owned(),
                properties,
                destination,
            }
        }
    }

    impl DestinationTopic for OutgoingEnvelopeProperties {
        fn destination_topic<A>(&self, me: &A, dest: &Destination) -> Result<String, Error>
        where
            A: Addressable,
        {
            match self {
                OutgoingEnvelopeProperties::Event(val) => val.destination_topic(me, dest),
                OutgoingEnvelopeProperties::Request(val) => val.destination_topic(me, dest),
                OutgoingEnvelopeProperties::Response(val) => val.destination_topic(me, dest),
            }
        }
    }

    impl<'a> Publishable for OutgoingEnvelope {
        fn destination_topic<A>(&self, me: &A) -> Result<String, Error>
        where
            A: Addressable,
        {
            self.properties.destination_topic(me, &self.destination)
        }

        fn to_bytes(&self) -> Result<String, Error> {
            Ok(serde_json::to_string(&self).map_err(|e| {
                Error::new(&format!("error serializing an envelope to bytes, {}", &e))
            })?)
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    pub trait IntoEnvelope {
        fn into_envelope(self) -> Result<OutgoingEnvelope, Error>;
    }
}

////////////////////////////////////////////////////////////////////////////////

pub use rumqtt::client::Notification;
pub use rumqtt::QoS;
