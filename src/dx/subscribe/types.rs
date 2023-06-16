//! Subscription types module.

use crate::core::{AnyValue, Deserialize, PubNubError, ScalarValue};
use crate::dx::subscribe::result::{Envelope, EnvelopePayload, ObjectDataBody};
use crate::lib::{
    alloc::{
        boxed::Box,
        string::{String, ToString},
        vec::Vec,
    },
    collections::HashMap,
    core::fmt::Formatter,
    core::result::Result,
};

/// Known types of events / messages received from subscribe.
///
/// While subscribed to channels and groups [`PubNub`] service may deliver
/// real-time updates which can be differentiated by their type.
/// This enum contains list of known general message types.
///
/// [`PubNub`]:https://www.pubnub.com/
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize), serde(untagged))]
pub enum SubscribeMessageType {
    /// Regular messages.
    ///
    /// This type is set for events published by user using [`publish`] feature.
    ///
    /// [`publish`]: crate::dx::publish
    Message = 0,

    /// Small message.
    ///
    /// Message sent with separate endpoint as chunk of really small data.
    Signal = 1,

    /// Object related event.
    ///
    /// This type is set to the group of events which is related to the
    /// `user Id` / `channel` objects and their relationship changes.
    Object = 2,

    /// Message action related event.
    ///
    /// This type is set to the group of events which is related to the
    /// `message` associated actions changes (addition, removal).
    MessageAction = 3,

    /// File related event.
    ///
    /// This type is set to the group of events which is related to file
    /// sharing (upload / removal).
    File = 4,
}

/// Time cursor.
///
/// Cursor used by subscription loop to identify point in time after
/// which updates will be delivered.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
pub struct SubscribeCursor {
    /// PubNub high-precision timestamp.
    ///
    /// Aside of specifying exact time of receiving data / event this token used
    /// to catchup / follow on real-time updates.
    #[cfg_attr(feature = "serde", serde(rename = "t"))]
    pub timetoken: String,

    /// Data center region for which `timetoken` has been generated.
    #[cfg_attr(feature = "serde", serde(rename = "r"))]
    pub region: u32,
}

/// Subscription statuses.
#[derive(Debug, Copy, Clone)]
pub enum SubscribeStatus {
    /// Successfully connected and receiving real-time updates.
    Connected,

    /// Successfully reconnected after real-time updates received has been
    /// stopped.
    Reconnected,

    /// Real-time updates receive stopped.
    Disconnected,
}

/// Presence update information.
///
/// Enum provides [`Presence::Join`], [`Presence::Leave`], [`Presence::Timeout`],
/// [`Presence::Interval`] and [`Presence::StateChange`] variants for updates
/// listener. These variants allow listener understand how presence changes on
/// channel.
#[derive(Debug)]
pub enum Presence {
    /// Remote user `join` update.
    ///
    /// Contains information about the user which joined the channel.
    Join {
        /// Unix timestamp when the user joined the channel.
        timestamp: usize,

        /// Unique identification of the user which joined the channel.
        uuid: String,

        /// Name of channel to which user joined.
        channel: String,

        /// Current channel occupancy after user joined.
        occupancy: usize,
    },

    /// Remote user `leave` update.
    ///
    /// Contains information about the user which left the channel.
    Leave {
        /// Unix timestamp when the user left the channel.
        timestamp: usize,

        /// Name of channel which user left.
        channel: String,

        /// Current channel occupancy after user left.
        occupancy: usize,

        /// Unique identification of the user which left the channel.
        uuid: String,
    },

    /// Remote user `timeout` update.
    ///
    /// Contains information about the user which unexpectedly left the channel.
    Timeout {
        /// Unix timestamp when event has been triggered.
        timestamp: usize,

        /// Name of channel where user timeout.
        channel: String,

        /// Current channel occupancy after user timeout.
        occupancy: usize,

        /// Unique identification of the user which timeout the channel.
        uuid: String,
    },

    /// Channel `interval` presence update.
    ///
    /// Contains information about the users which joined / left / unexpectedly
    /// left the channel since previous `interval` update.
    Interval {
        /// Unix timestamp when event has been triggered.
        timestamp: usize,

        /// Name of channel where user timeout.
        channel: String,

        /// Current channel occupancy.
        occupancy: usize,

        /// The list of unique user identifiers that `joined` the channel since
        /// the last interval presence update.
        join: Option<Vec<String>>,

        /// The list of unique user identifiers that `left` the channel since
        /// the last interval presence update.
        leave: Option<Vec<String>>,

        /// The list of unique user identifiers that `timeout` the channel since
        /// the last interval presence update.
        timeout: Option<Vec<String>>,
    },

    /// Remote user `state` change update.
    ///
    /// Contains information about the user for which associated `state` has
    /// been changed on `channel`.
    StateChange {
        /// Unix timestamp when event has been triggered.
        timestamp: usize,

        /// Name of channel where user timeout.
        channel: String,

        /// Unique identification of the user for which state has been changed.
        uuid: String,

        /// The user's state associated with the channel has been updated.
        data: Option<String>,
    },
}

/// Objects update information.
///
/// Enum provides [`Object::Channel`], [`Object::Uuid`] and
/// [`Object::Membership`] variants for updates listener. These variants allow
/// listener understand how objects and their relationship changes.
#[derive(Debug)]
pub enum Object {
    /// `Channel` object update.
    Channel {
        /// The type of event that happened during the object update.
        event: Option<ObjectEvent>,

        /// Time when `channel` object has been updated.
        timestamp: Option<usize>,

        /// Given name of the channel object.
        name: Option<String>,

        /// `Channel` object additional description.
        description: Option<String>,

        /// `Channel` object type information.
        r#type: Option<String>,

        /// `Channel` object current status.
        status: Option<String>,

        /// Unique `channel` object identifier.
        id: String,

        /// Flatten `HashMap` with additional information associated with
        /// `channel` object.
        custom: Option<HashMap<String, ScalarValue>>,

        /// Recent `channel` object modification date.
        updated: String,

        /// Current `channel` object state hash.
        tag: String,
    },

    /// `UUID` object update.
    Uuid {
        /// The type of event that happened during the object update.
        event: Option<ObjectEvent>,

        /// Time when `uuid` object has been updated.
        timestamp: Option<usize>,

        /// Give `uuid` object name.
        name: Option<String>,

        /// Email address associated with `uuid` object.
        email: Option<String>,

        /// `uuid` object identifier in external systems.
        external_id: Option<String>,

        /// `uuid` object external profile URL.
        profile_url: Option<String>,

        /// `Uuid` object type information.
        r#type: Option<String>,

        /// `Uuid` object current status.
        status: Option<String>,

        /// Unique `uuid` object identifier.
        id: String,

        /// Flatten `HashMap` with additional information associated with
        /// `uuid` object.
        custom: Option<HashMap<String, ScalarValue>>,

        /// Recent `uuid` object modification date.
        updated: String,

        /// Current `uuid` object state hash.
        tag: String,
    },

    /// `Membership` object update.
    Membership {
        /// The type of event that happened during the object update.
        event: Option<ObjectEvent>,

        /// Time when `membership` object has been updated.
        timestamp: Option<usize>,

        /// `Channel` object within which `uuid` object registered as member.
        channel: Box<Object>,

        /// Flatten `HashMap` with additional information associated with
        /// `membership` object.
        custom: Option<HashMap<String, ScalarValue>>,

        /// `Membership` object current status.
        status: Option<String>,

        /// Unique identifier of `uuid` object which has relationship with
        /// `channel`.
        uuid: String,

        /// Recent `membership` object modification date.
        updated: String,

        /// Current `membership` object state hash.
        tag: String,
    },
}

/// Message's action update information.
///
/// [`MessageAction`] type provides to the updates listener message's action
/// changes information.
#[derive(Debug)]
pub struct MessageAction {
    /// The type of event that happened during the message action update.
    pub event: MessageActionEvent,

    /// Identifier of client which sent updated message's actions.
    pub sender: String,

    /// Time when message action has been changed.
    pub timestamp: usize,

    /// Name of channel where update received.
    pub channel: String,

    /// Actual name of subscription through which update has been delivered.
    pub subscription: String,

    /// Timetoken of message for which action has been added / removed.
    pub message_timetoken: String,

    /// Timetoken of message action which has been added / removed.
    pub action_timetoken: String,

    /// Message action type.
    pub r#type: String,

    /// Value associated with message action `type`.
    pub value: String,
}

/// File sharing information.
///
/// [`File`] type provides to the updates listener information about shared
/// files.
#[derive(Debug)]
#[allow(dead_code)]
pub struct File {
    /// Identifier of client which sent shared file.
    pub sender: String,

    /// Time when file has been shared.
    pub timestamp: usize,

    /// Name of channel where file update received.
    pub channel: String,

    /// Actual name of subscription through which update has been delivered.
    pub subscription: String,

    /// Message which has been associated with uploaded file.
    message: String,

    /// Unique identifier of uploaded file.
    id: String,

    /// Actual name with which file has been stored.
    name: String,
}

/// Published message / signal information.
///
/// [`Message`] type provides access to the published message / signal actual
/// payload to the updates listener.
#[derive(Debug)]
pub enum Message<D> {
    /// Custom user-provided type.
    ///
    /// This type (preferable `enum`) used to match against received real-time
    /// update and deserialize as user-type if match.
    Custom(D),

    /// General payload type.
    ///
    /// [`AnyValue`] type covers basic data types including nested collections.
    General(AnyValue),
}

/// Object update event types.
#[derive(Debug, Copy, Clone)]
pub enum ObjectEvent {
    /// Object information has been modified.
    Update,

    /// Object has been deleted.
    Delete,
}

/// Message's actions update event types.
#[derive(Debug, Copy, Clone)]
pub enum MessageActionEvent {
    /// Message's action has been modified.
    Update,

    /// Message's action has been deleted.
    Delete,
}

impl Default for SubscribeCursor {
    fn default() -> Self {
        Self {
            timetoken: "0".into(),
            region: 0,
        }
    }
}

impl TryFrom<String> for ObjectEvent {
    type Error = PubNubError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            _ => Err(PubNubError::Deserialization {
                details: "Unable deserialize: unexpected object event type".to_string(),
            }),
        }
    }
}

impl TryFrom<String> for MessageActionEvent {
    type Error = PubNubError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            _ => Err(PubNubError::Deserialization {
                details: "Unable deserialize: unexpected message action event type".to_string(),
            }),
        }
    }
}

impl From<SubscribeCursor> for HashMap<String, String> {
    fn from(value: SubscribeCursor) -> Self {
        if value.timetoken.eq(&"0") {
            HashMap::from([("tt".into(), value.timetoken)])
        } else {
            HashMap::from([
                ("tt".into(), value.timetoken.to_string()),
                ("tr".into(), value.region.to_string()),
            ])
        }
    }
}

impl core::fmt::Display for SubscribeStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Connected => write!(f, "Connected"),
            Self::Reconnected => write!(f, "Reconnected"),
            Self::Disconnected => write!(f, "Disconnected"),
        }
    }
}

impl<D> TryFrom<Envelope<D>> for Presence
where
    D: for<'response> Deserialize<'response, D>,
{
    type Error = PubNubError;

    fn try_from(value: Envelope<D>) -> Result<Self, Self::Error> {
        if let EnvelopePayload::Presence {
            action,
            timestamp,
            uuid,
            occupancy,
            data,
            join,
            leave,
            timeout,
        } = value.payload
        {
            let action = action.unwrap_or("interval".to_string());
            match action.as_str() {
                "join" => Ok(Self::Join {
                    timestamp,
                    // `join` event always has `uuid` and unwrap_or default
                    // value won't be actually used.
                    uuid: uuid.unwrap_or("".to_string()),
                    channel: value.channel,
                    occupancy: occupancy.unwrap_or(0),
                }),
                "leave" => Ok(Self::Leave {
                    timestamp,
                    // `leave` event always has `uuid` and unwrap_or default
                    // value won't be actually used.
                    uuid: uuid.unwrap_or("".to_string()),
                    channel: value.channel,
                    occupancy: occupancy.unwrap_or(0),
                }),
                "timeout" => Ok(Self::Timeout {
                    timestamp,
                    // `leave` event always has `uuid` and unwrap_or default
                    // value won't be actually used.
                    uuid: uuid.unwrap_or("".to_string()),
                    channel: value.channel,
                    occupancy: occupancy.unwrap_or(0),
                }),
                "interval" => Ok(Self::Interval {
                    timestamp,
                    channel: value.channel,
                    occupancy: occupancy.unwrap_or(0),
                    join,
                    leave,
                    timeout,
                }),
                _ => Ok(Self::StateChange {
                    timestamp,
                    // `state-change` event always has `uuid` and unwrap_or
                    // default value won't be actually used.
                    uuid: uuid.unwrap_or("".to_string()),
                    channel: value.channel,
                    data,
                }),
            }
        } else {
            Err(PubNubError::Deserialization {
                details: "Unable deserialize: unexpected payload for presence.".to_string(),
            })
        }
    }
}

impl<D> TryFrom<Envelope<D>> for Object
where
    D: for<'response> Deserialize<'response, D>,
{
    type Error = PubNubError;

    fn try_from(value: Envelope<D>) -> Result<Self, Self::Error> {
        let timestamp = value.published.timetoken.parse::<usize>();
        if let EnvelopePayload::Object {
            event,
            r#type,
            data,
            ..
        } = value.payload
        {
            let update_type = r#type;
            match data {
                ObjectDataBody::Channel {
                    name,
                    description,
                    r#type,
                    status,
                    id,
                    custom,
                    updated,
                    tag,
                } if update_type.as_str().eq("channel") => Ok(Self::Channel {
                    event: Some(event.try_into()?),
                    timestamp: timestamp.ok(),
                    name,
                    description,
                    r#type,
                    status,
                    id,
                    custom,
                    updated,
                    tag,
                }),
                ObjectDataBody::Uuid {
                    name,
                    email,
                    external_id,
                    profile_url,
                    r#type,
                    status,
                    id,
                    custom,
                    updated,
                    tag,
                } if update_type.as_str().eq("uuid") => Ok(Self::Uuid {
                    event: Some(event.try_into()?),
                    timestamp: timestamp.ok(),
                    name,
                    email,
                    external_id,
                    profile_url,
                    r#type,
                    status,
                    id,
                    custom,
                    updated,
                    tag,
                }),
                ObjectDataBody::Membership {
                    channel,
                    custom,
                    uuid,
                    status,
                    updated,
                    tag,
                } if update_type.as_str().eq("membership") => {
                    if let ObjectDataBody::Channel {
                        name,
                        description: channel_description,
                        r#type: channel_type,
                        status: channel_status,
                        id,
                        custom: channel_custom,
                        updated: channel_updated,
                        tag: channel_tag,
                    } = *channel
                    {
                        Ok(Self::Membership {
                            event: Some(event.try_into()?),
                            timestamp: timestamp.ok(),
                            channel: Box::new(Object::Channel {
                                event: None,
                                timestamp: None,
                                name,
                                description: channel_description,
                                r#type: channel_type,
                                status: channel_status,
                                id,
                                custom: channel_custom,
                                updated: channel_updated,
                                tag: channel_tag,
                            }),
                            custom,
                            status,
                            uuid,
                            updated,
                            tag,
                        })
                    } else {
                        Err(PubNubError::Deserialization {
                            details: "Unable deserialize: unknown object type.".to_string(),
                        })
                    }
                }
                _ => Err(PubNubError::Deserialization {
                    details: "Unable deserialize: unknown object type.".to_string(),
                }),
            }
        } else {
            Err(PubNubError::Deserialization {
                details: "Unable deserialize: unexpected payload for object.".to_string(),
            })
        }
    }
}

impl<D> TryFrom<Envelope<D>> for MessageAction
where
    D: for<'response> Deserialize<'response, D>,
{
    type Error = PubNubError;

    fn try_from(value: Envelope<D>) -> Result<Self, Self::Error> {
        // `Message action` event always has `timetoken` and unwrap_or default
        // value won't be actually used.
        let timestamp = value.published.timetoken.parse::<usize>().ok().unwrap_or(0);
        // `Message action` event always has `sender` and unwrap_or default
        // value won't be actually used.
        let sender = value.sender.unwrap_or("".to_string());
        if let EnvelopePayload::MessageAction { event, data, .. } = value.payload {
            Ok(Self {
                event: event.try_into()?,
                sender,
                timestamp,
                channel: value.channel,
                subscription: value.subscription,
                message_timetoken: data.message_timetoken,
                action_timetoken: data.action_timetoken,
                r#type: data.r#type,
                value: data.value,
            })
        } else {
            Err(PubNubError::Deserialization {
                details: "Unable deserialize: unexpected payload for message action.".to_string(),
            })
        }
    }
}

impl<D> TryFrom<Envelope<D>> for File
where
    D: for<'response> Deserialize<'response, D>,
{
    type Error = PubNubError;

    fn try_from(value: Envelope<D>) -> Result<Self, Self::Error> {
        // `File` event always has `timetoken` and unwrap_or default
        // value won't be actually used.
        let timestamp = value.published.timetoken.parse::<usize>().ok().unwrap_or(0);
        // `File` event always has `sender` and unwrap_or default
        // value won't be actually used.
        let sender = value.sender.unwrap_or("".to_string());
        if let EnvelopePayload::File { message, file } = value.payload {
            Ok(Self {
                sender,
                timestamp,
                channel: value.channel,
                subscription: value.subscription,
                message,
                id: file.id,
                name: file.name,
            })
        } else {
            Err(PubNubError::Deserialization {
                details: "Unable deserialize: unexpected payload for file.".to_string(),
            })
        }
    }
}

impl<D> TryFrom<Envelope<D>> for Message<D>
where
    D: for<'response> Deserialize<'response, D>,
{
    type Error = PubNubError;

    fn try_from(value: Envelope<D>) -> Result<Self, Self::Error> {
        if let EnvelopePayload::Custom(data) = value.payload {
            Ok(Self::Custom(data))
        } else if let EnvelopePayload::General(data) = value.payload {
            Ok(Self::General(data))
        } else {
            Err(PubNubError::Deserialization {
                details: "Unable deserialize: unexpected payload for message / signal.".to_string(),
            })
        }
    }
}
