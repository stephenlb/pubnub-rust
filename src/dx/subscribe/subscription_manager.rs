//! Subscriptions' manager.
//!
//! This module contains manager which is responsible for tracking and updating
//! active subscription streams.
use crate::{
    dx::subscribe::{
        event_engine::SubscribeEventEngine, result::Update, subscription::Subscription,
        types::SubscribeStreamEvent, SubscribeStatus,
    },
    lib::alloc::{sync::Arc, vec::Vec},
};
use spin::RwLock;

/// Active subscriptions manager.
///
/// [`PubNubClient`] allows to have multiple [`subscription`] objects which will
/// be used to deliver real-time updates on channels and groups specified during
/// [`subscribe`] method call.
///
/// [`subscription`]: crate::Subscription
/// [`PubNubClient`]: crate::PubNubClient
pub(crate) struct SubscriptionManager {
    /// Subscription event engine.
    ///
    /// State machine which is responsible for subscription loop maintenance.
    subscribe_event_engine: RwLock<SubscribeEventEngine>,

    /// List of registered subscribers.
    ///
    /// List of subscribers which will receive real-time updates.
    pub subscribers: RwLock<Vec<Arc<Subscription>>>,
}

impl SubscriptionManager {
    pub fn new(subscribe_event_engine: SubscribeEventEngine) -> Self {
        Self {
            subscribe_event_engine: RwLock::new(subscribe_event_engine),
            subscribers: Default::default(),
        }
    }

    pub fn notify_new_status(&self, status: &SubscribeStatus) {
        self.subscribers.read().iter().for_each(|subscription| {
            subscription.notify_update(SubscribeStreamEvent::Status(status.clone()));
        });
    }

    pub fn notify_new_messages(&self, messages: Vec<Update>) {
        messages.iter().for_each(|update| {
            let channel = update.channel();
            self.subscribers.read().iter().for_each(|subscription| {
                if subscription.channels.contains(&channel) {
                    subscription.notify_update(SubscribeStreamEvent::Update(update.clone()));
                }
            });
        });
    }

    pub fn register(&self, subscription: Arc<Subscription>) {
        let mut subscribers_slot = self.subscribers.write();
        subscribers_slot.push(subscription);
    }

    pub fn unregister(&self, subscription: Arc<Subscription>) {
        let mut subscribers_slot = self.subscribers.write();
        if let Some(position) = subscribers_slot
            .iter()
            .position(|val| val.id.eq(&subscription.id))
        {
            subscribers_slot.swap_remove(position);
        }
    }
}
