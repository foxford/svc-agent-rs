use serde::{de, ser};
use serde_derive::{Deserialize, Serialize};
use std::fmt;

use crate::{
    mqtt::AuthnProperties, mqtt::Connection, mqtt::ConnectionMode, AccountId, Addressable, AgentId,
    Authenticable, SharedGroup,
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
