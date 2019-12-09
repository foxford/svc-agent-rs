use serde::{de, ser};
use serde_derive::{Deserialize, Serialize};
use std::fmt;

use crate::{
    mqtt::{Connection, ConnectionMode, TrackingId},
    AgentId, SharedGroup,
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

    use chrono::Duration;
    use serde::{de, ser};

    pub(crate) fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&duration.num_milliseconds().to_string())
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
            match value_str.parse::<i64>() {
                Ok(value) => Ok(Duration::milliseconds(value)),
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

    use chrono::Duration;
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

///////////////////////////////////////////////////////////////////////////////

pub(crate) mod session_ids_list {
    use std::fmt;
    use std::str::FromStr;

    use serde::{de, ser};

    use crate::mqtt::SessionId;

    pub(crate) fn serialize<S>(
        session_ids: &Vec<SessionId>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let session_ids_str = session_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<String>>()
            .join(" ");

        serializer.serialize_str(&session_ids_str)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Vec<SessionId>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_str(SessionIdListVisitor)
    }

    pub struct SessionIdListVisitor;

    impl<'de> de::Visitor<'de> for SessionIdListVisitor {
        type Value = Vec<SessionId>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("session id list (whitespace separated)")
        }

        fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let mut session_ids = vec![];

            for session_id_str in s.split(" ") {
                match SessionId::from_str(session_id_str) {
                    Ok(session_id) => session_ids.push(session_id),
                    Err(err) => {
                        return Err(E::custom(format!(
                            "failed to parse SessionId from string: {}",
                            err
                        )))
                    }
                }
            }

            Ok(session_ids)
        }
    }
}

impl ser::Serialize for TrackingId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> de::Deserialize<'de> for TrackingId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct TrackingIdVisitor;

        impl<'de> de::Visitor<'de> for TrackingIdVisitor {
            type Value = TrackingId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct TrackingId")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                use std::str::FromStr;

                TrackingId::from_str(v)
                    .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(v), &self))
            }
        }

        deserializer.deserialize_str(TrackingIdVisitor)
    }
}
