//! Subscribe event engine module types.
//!
//! This module contains the [`SubscribeInput`] type, which represents
//! user-provided channels and groups for which real-time updates should be
//! retrieved from the [`PubNub`] network.
//!
//! [`PubNub`]:https://www.pubnub.com/

use crate::{
    core::PubNubError,
    lib::{
        alloc::collections::HashSet,
        core::ops::{Add, AddAssign, Sub, SubAssign},
    },
    subscribe::SubscribeCursor,
};

/// User-provided channels and groups for subscription.
///
/// Object contains information about channels and groups for which real-time
/// updates should be retrieved from the [`PubNub`] network.
///
/// [`PubNub`]:https://www.pubnub.com/
#[derive(Clone, Debug, PartialEq)]
pub struct SubscribeInput {
    /// Optional list of channels.
    ///
    /// List of channels for which real-time updates should be retrieved
    /// from the [`PubNub`] network.
    ///
    /// List is optional if there is at least one `channel_group` provided.
    ///
    /// [`PubNub`]:https://www.pubnub.com/
    pub channels: Option<HashSet<String>>,

    /// Optional list of channel groups.
    ///
    /// List of channel groups for which real-time updates should be retrieved
    /// from the [`PubNub`] network.
    ///
    /// [`PubNub`]:https://www.pubnub.com/
    pub channel_groups: Option<HashSet<String>>,

    /// Whether user input is empty or not.
    pub is_empty: bool,
}

#[allow(dead_code)]
impl SubscribeInput {
    pub fn new(channels: &Option<Vec<String>>, channel_groups: &Option<Vec<String>>) -> Self {
        let channels = channels.as_ref().map(|channels| {
            channels.iter().fold(HashSet::new(), |mut acc, channel| {
                acc.insert(channel.clone());
                acc
            })
        });
        let channel_groups = channel_groups.as_ref().map(|groups| {
            groups.iter().fold(HashSet::new(), |mut acc, group| {
                acc.insert(group.clone());
                acc
            })
        });

        let channel_groups_is_empty = channel_groups.as_ref().map_or(true, |set| set.is_empty());
        let channels_is_empty = channels.as_ref().map_or(true, |set| set.is_empty());

        Self {
            channels,
            channel_groups,
            is_empty: channel_groups_is_empty && channels_is_empty,
        }
    }

    pub fn channels(&self) -> Option<Vec<String>> {
        self.channels.clone().map(|ch| ch.into_iter().collect())
    }

    pub fn contains_channel(&self, channel: &String) -> bool {
        self.channels
            .as_ref()
            .map_or(false, |channels| channels.contains(channel))
    }

    pub fn channel_groups(&self) -> Option<Vec<String>> {
        self.channel_groups
            .clone()
            .map(|ch| ch.into_iter().collect())
    }

    pub fn contains_channel_group(&self, channel_group: &Option<String>) -> bool {
        let Some(channel_group) = channel_group else {
            return false;
        };

        self.channel_groups
            .as_ref()
            .map_or(false, |channel_groups| {
                channel_groups.contains(channel_group)
            })
    }
}

impl Add for SubscribeInput {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let channel_groups: Option<HashSet<String>> =
            match (self.channel_groups, rhs.channel_groups) {
                (Some(lhs), Some(rhs)) => Some(lhs.into_iter().chain(rhs).collect()),
                (Some(lhs), None) => Some(lhs),
                (None, Some(rhs)) => Some(rhs),
                _ => None,
            };
        let channels: Option<HashSet<String>> = match (self.channels, rhs.channels) {
            (Some(lhs), Some(rhs)) => Some(lhs.into_iter().chain(rhs).collect()),
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            _ => None,
        };

        let channel_groups_is_empty = channel_groups.as_ref().map_or(true, |set| set.is_empty());
        let channels_is_empty = channels.as_ref().map_or(true, |set| set.is_empty());

        Self {
            channels,
            channel_groups,
            is_empty: channel_groups_is_empty && channels_is_empty,
        }
    }
}

impl AddAssign for SubscribeInput {
    fn add_assign(&mut self, rhs: Self) {
        let channel_groups: Option<HashSet<String>> =
            match (self.channel_groups.clone(), rhs.channel_groups.clone()) {
                (Some(lhs), Some(rhs)) => Some(lhs.into_iter().chain(rhs).collect()),
                (Some(lhs), None) => Some(lhs),
                (None, Some(rhs)) => Some(rhs),
                _ => None,
            };
        let channels: Option<HashSet<String>> = match (self.channels.clone(), rhs.channels.clone())
        {
            (Some(lhs), Some(rhs)) => Some(lhs.into_iter().chain(rhs).collect()),
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            _ => None,
        };

        let channel_groups_is_empty = channel_groups.as_ref().map_or(true, |set| set.is_empty());
        let channels_is_empty = channels.as_ref().map_or(true, |set| set.is_empty());

        self.channels = channels;
        self.channel_groups = channel_groups;
        self.is_empty = channel_groups_is_empty && channels_is_empty;
    }
}

impl Sub for SubscribeInput {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        let channel_groups: Option<HashSet<String>> =
            match (self.channel_groups, rhs.channel_groups) {
                (Some(lhs), Some(rhs)) => Some(&lhs - &rhs),
                (Some(lhs), None) => Some(lhs),
                _ => None,
            };
        let channels: Option<HashSet<String>> = match (self.channels, rhs.channels) {
            (Some(lhs), Some(rhs)) => Some(&lhs - &rhs),
            (Some(lhs), None) => Some(lhs),
            _ => None,
        };

        let channel_groups_is_empty = channel_groups.as_ref().map_or(true, |set| set.is_empty());
        let channels_is_empty = channels.as_ref().map_or(true, |set| set.is_empty());

        Self {
            channels,
            channel_groups,
            is_empty: channel_groups_is_empty && channels_is_empty,
        }
    }
}

impl SubAssign for SubscribeInput {
    fn sub_assign(&mut self, rhs: Self) {
        let channel_groups: Option<HashSet<String>> =
            match (self.channel_groups.clone(), rhs.channel_groups.clone()) {
                (Some(lhs), Some(rhs)) => Some(&lhs - &rhs),
                (Some(lhs), None) => Some(lhs),
                _ => None,
            };
        let channels: Option<HashSet<String>> = match (self.channels.clone(), rhs.channels.clone())
        {
            (Some(lhs), Some(rhs)) => Some(&lhs - &rhs),
            (Some(lhs), None) => Some(lhs),
            _ => None,
        };

        let channel_groups_is_empty = channel_groups.as_ref().map_or(true, |set| set.is_empty());
        let channels_is_empty = channels.as_ref().map_or(true, |set| set.is_empty());

        self.channels = channels;
        self.channel_groups = channel_groups;
        self.is_empty = channel_groups_is_empty && channels_is_empty;
    }
}

#[cfg(feature = "std")]
#[derive(Clone)]
/// Subscribe event engine data.
///
/// Data objects are used by the subscribe event engine to communicate between
/// components.
pub(crate) struct SubscriptionParams<'execution> {
    /// Channels from which real-time updates should be received.
    pub channels: &'execution Option<Vec<String>>,

    /// Channel groups from which real-time updates should be received.
    pub channel_groups: &'execution Option<Vec<String>>,

    /// Time cursor.
    pub cursor: Option<&'execution SubscribeCursor>,

    /// How many consequent retry attempts has been made.
    pub attempt: u8,

    /// Reason why previous request created by subscription event engine failed.
    pub reason: Option<PubNubError>,

    /// Effect identifier.
    ///
    /// Identifier of effect which requested to create request.
    pub effect_id: &'execution str,
}
