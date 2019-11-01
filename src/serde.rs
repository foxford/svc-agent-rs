use serde::{de, ser};
use serde_derive::{Deserialize, Serialize};
use std::fmt;

use crate::{
    mqtt::{AuthnProperties, BrokerProperties, Connection, ConnectionMode},
    AccountId, Addressable, AgentId, Authenticable, SharedGroup,
};

////////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Serialize)]
#[serde(remote = "http::StatusCode")]
pub(crate) struct HttpStatusCodeRef(#[serde(getter = "http_status_code_to_string")] String);

fn http_status_code_to_string(status_code: &http::StatusCode) -> String {
    status_code.as_u16().to_string()
}

impl From<HttpStatusCodeRef> for http::StatusCode {
    fn from(value: HttpStatusCodeRef) -> http::StatusCode {
        use std::str::FromStr;

        http::StatusCode::from_str(&value.0).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////

impl ser::Serialize for AgentId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> de::Deserialize<'de> for AgentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct AgentIdVisitor;

        impl<'de> de::Visitor<'de> for AgentIdVisitor {
            type Value = AgentId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct AgentId")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                use std::str::FromStr;

                AgentId::from_str(v)
                    .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(v), &self))
            }
        }

        deserializer.deserialize_str(AgentIdVisitor)
    }
}

////////////////////////////////////////////////////////////////////////////////

impl ser::Serialize for SharedGroup {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> de::Deserialize<'de> for SharedGroup {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct SharedGroupVisitor;

        impl<'de> de::Visitor<'de> for SharedGroupVisitor {
            type Value = SharedGroup;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct SharedGroup")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                use std::str::FromStr;

                SharedGroup::from_str(v)
                    .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(v), &self))
            }
        }

        deserializer.deserialize_str(SharedGroupVisitor)
    }
}

////////////////////////////////////////////////////////////////////////////////

impl ser::Serialize for ConnectionMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> de::Deserialize<'de> for ConnectionMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct ConnectionModeVisitor;

        impl<'de> de::Visitor<'de> for ConnectionModeVisitor {
            type Value = ConnectionMode;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("enum ConnectionMode")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                use std::str::FromStr;

                ConnectionMode::from_str(v)
                    .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(v), &self))
            }
        }

        deserializer.deserialize_str(ConnectionModeVisitor)
    }
}

////////////////////////////////////////////////////////////////////////////////

impl ser::Serialize for Connection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> de::Deserialize<'de> for Connection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct ConnectionVisitor;

        impl<'de> de::Visitor<'de> for ConnectionVisitor {
            type Value = Connection;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Connection")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                use std::str::FromStr;

                Connection::from_str(v)
                    .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(v), &self))
            }
        }

        deserializer.deserialize_str(ConnectionVisitor)
    }
}

////////////////////////////////////////////////////////////////////////////////

impl ser::Serialize for AuthnProperties {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("AuthnProperties", 3)?;
        state.serialize_field("agent_label", self.as_agent_id().label())?;
        state.serialize_field("account_label", self.as_account_id().label())?;
        state.serialize_field("audience", self.as_account_id().audience())?;
        state.end()
    }
}

impl<'de> de::Deserialize<'de> for AuthnProperties {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        enum Field {
            AgentLabel,
            AccountLabel,
            Audience,
        };

        impl<'de> de::Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: de::Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> de::Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`agent_label` or `account_label` or `audience`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "agent_label" => Ok(Field::AgentLabel),
                            "account_label" => Ok(Field::AccountLabel),
                            "audience" => Ok(Field::Audience),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct AuthnPropertiesVisitor;

        impl<'de> de::Visitor<'de> for AuthnPropertiesVisitor {
            type Value = AuthnProperties;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct AuthnProperties")
            }

            fn visit_map<V>(self, mut map: V) -> Result<AuthnProperties, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut agent_label = None;
                let mut account_label = None;
                let mut audience = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::AgentLabel => {
                            if agent_label.is_some() {
                                return Err(de::Error::duplicate_field("agent_label"));
                            }
                            agent_label = Some(map.next_value()?);
                        }
                        Field::AccountLabel => {
                            if account_label.is_some() {
                                return Err(de::Error::duplicate_field("account_label"));
                            }
                            account_label = Some(map.next_value()?);
                        }
                        Field::Audience => {
                            if audience.is_some() {
                                return Err(de::Error::duplicate_field("audience"));
                            }
                            audience = Some(map.next_value()?);
                        }
                    }
                }
                let agent_label =
                    agent_label.ok_or_else(|| de::Error::missing_field("agent_label"))?;
                let account_label =
                    account_label.ok_or_else(|| de::Error::missing_field("account_label"))?;
                let audience = audience.ok_or_else(|| de::Error::missing_field("audience"))?;

                let account_id = AccountId::new(account_label, audience);
                let agent_id = AgentId::new(agent_label, account_id);
                Ok(AuthnProperties::from(agent_id))
            }
        }

        const FIELDS: &[&str] = &["agent_label", "account_label", "audience"];
        deserializer.deserialize_struct("AuthnProperties", FIELDS, AuthnPropertiesVisitor)
    }
}

///////////////////////////////////////////////////////////////////////////////

impl ser::Serialize for BrokerProperties {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("BrokerProperties", 3)?;
        state.serialize_field("broker_agent_label", self.as_agent_id().label())?;
        state.serialize_field("broker_account_label", self.as_account_id().label())?;
        state.serialize_field("broker_audience", self.as_account_id().audience())?;
        state.end()
    }
}

impl<'de> de::Deserialize<'de> for BrokerProperties {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        enum Field {
            BrokerAgentLabel,
            BrokerAccountLabel,
            BrokerAudience,
        };

        impl<'de> de::Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: de::Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> de::Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str(
                            "`broker_agent_label` or `broker_account_label` or `broker_audience`",
                        )
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "broker_agent_label" => Ok(Field::BrokerAgentLabel),
                            "broker_account_label" => Ok(Field::BrokerAccountLabel),
                            "broker_audience" => Ok(Field::BrokerAudience),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct BrokerPropertiesVisitor;

        impl<'de> de::Visitor<'de> for BrokerPropertiesVisitor {
            type Value = BrokerProperties;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct BrokerProperties")
            }

            fn visit_map<V>(self, mut map: V) -> Result<BrokerProperties, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut broker_agent_label = None;
                let mut broker_account_label = None;
                let mut broker_audience = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::BrokerAgentLabel => {
                            if broker_agent_label.is_some() {
                                return Err(de::Error::duplicate_field("broker_agent_label"));
                            }
                            broker_agent_label = Some(map.next_value()?);
                        }
                        Field::BrokerAccountLabel => {
                            if broker_account_label.is_some() {
                                return Err(de::Error::duplicate_field("broker_account_label"));
                            }
                            broker_account_label = Some(map.next_value()?);
                        }
                        Field::BrokerAudience => {
                            if broker_audience.is_some() {
                                return Err(de::Error::duplicate_field("broker_audience"));
                            }
                            broker_audience = Some(map.next_value()?);
                        }
                    }
                }
                let broker_agent_label = broker_agent_label
                    .ok_or_else(|| de::Error::missing_field("broker_agent_label"))?;
                let broker_account_label = broker_account_label
                    .ok_or_else(|| de::Error::missing_field("broker_account_label"))?;
                let broker_audience =
                    broker_audience.ok_or_else(|| de::Error::missing_field("broker_audience"))?;

                let broker_account_id = AccountId::new(broker_account_label, broker_audience);
                let broker_agent_id = AgentId::new(broker_agent_label, broker_account_id);
                Ok(BrokerProperties::from(broker_agent_id))
            }
        }

        const FIELDS: &[&str] = &[
            "broker_agent_label",
            "broker_account_label",
            "broker_audience",
        ];
        deserializer.deserialize_struct("BrokerProperties", FIELDS, BrokerPropertiesVisitor)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) mod ts_milliseconds_string {
    use std::fmt;

    use chrono::{offset::TimeZone, DateTime, LocalResult, Utc};
    use serde::{de, ser};

    pub(crate) fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&dt.timestamp_millis().to_string())
    }

    pub fn deserialize<'de, D>(d: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_any(TimestampMillisecondsVisitor)
    }

    pub struct TimestampMillisecondsVisitor;

    impl<'de> de::Visitor<'de> for TimestampMillisecondsVisitor {
        type Value = DateTime<Utc>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("unix time (milliseconds as string)")
        }

        fn visit_str<E>(self, value_str: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match value_str.parse::<u64>() {
                Ok(value) => {
                    let local_result = Utc
                        .timestamp_opt((value / 1000) as i64, ((value % 1000) * 1_000_000) as u32);

                    match local_result {
                        LocalResult::Single(ms) => Ok(ms),
                        _ => Err(E::custom(format!(
                            "failed to parse milliseconds: {}",
                            value
                        ))),
                    }
                }
                Err(err) => Err(E::custom(format!(
                    "failed to parse integer from string: {}",
                    err
                ))),
            }
        }
    }
}

pub(crate) mod ts_milliseconds_string_option {
    use std::fmt;

    use chrono::{DateTime, Utc};
    use serde::{de, ser};

    use super::ts_milliseconds_string;

    pub(crate) fn serialize<S>(
        option: &Option<DateTime<Utc>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match option {
            Some(value) => ts_milliseconds_string::serialize(value, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_option(TimestampMillisecondsOptionVisitor)
    }

    pub struct TimestampMillisecondsOptionVisitor;

    impl<'de> de::Visitor<'de> for TimestampMillisecondsOptionVisitor {
        type Value = Option<DateTime<Utc>>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("optional unix time (milliseconds as string)")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
        where
            D: de::Deserializer<'de>,
        {
            let value = ts_milliseconds_string::deserialize(d)?;
            Ok(Some(value))
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) mod duration_milliseconds_string {
    use std::fmt;
    use std::time::Duration;

    use serde::{de, ser};

    pub(crate) fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&duration.as_millis().to_string())
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Duration, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_any(DurationMillisecondsVisitor)
    }

    pub struct DurationMillisecondsVisitor;

    impl<'de> de::Visitor<'de> for DurationMillisecondsVisitor {
        type Value = Duration;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("duration (milliseconds as string)")
        }

        fn visit_str<E>(self, value_str: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match value_str.parse::<u64>() {
                Ok(value) => Ok(Duration::from_millis(value)),
                Err(err) => Err(E::custom(format!(
                    "failed to parse integer from string: {}",
                    err
                ))),
            }
        }
    }
}

pub(crate) mod duration_milliseconds_string_option {
    use std::fmt;
    use std::time::Duration;

    use serde::{de, ser};

    use super::duration_milliseconds_string;

    pub(crate) fn serialize<S>(option: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match option {
            Some(value) => duration_milliseconds_string::serialize(value, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Option<Duration>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_option(DurationMillisecondsOptionVisitor)
    }

    pub struct DurationMillisecondsOptionVisitor;

    impl<'de> de::Visitor<'de> for DurationMillisecondsOptionVisitor {
        type Value = Option<Duration>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("optional duration (milliseconds as string)")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
        where
            D: de::Deserializer<'de>,
        {
            let value = duration_milliseconds_string::deserialize(d)?;
            Ok(Some(value))
        }
    }
}
