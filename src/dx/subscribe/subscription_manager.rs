//! Subscriptions' manager.
//!
//! This module contains manager which is responsible for tracking and updating
//! active subscription streams.

use spin::RwLock;

use crate::core::{Deserializer, Transport};
use crate::subscribe::traits::EventHandler;
use crate::{
    dx::subscribe::{
        event_engine::{
            event::SubscribeEvent, SubscribeEffectInvocation, SubscribeEventEngine,
            SubscriptionInput,
        },
        result::Update,
        ConnectionStatus, PubNubClientInstance, Subscription, SubscriptionCursor,
    },
    lib::{
        alloc::{
            sync::{Arc, Weak},
            vec::Vec,
        },
        collections::HashMap,
        core::{
            fmt::{Debug, Formatter},
            ops::{Deref, DerefMut},
        },
    },
};

#[cfg(feature = "presence")]
pub(in crate::dx::subscribe) type PresenceCall =
    dyn Fn(Option<Vec<String>>, Option<Vec<String>>, bool) + Send + Sync;

/// Active subscriptions' manager.
///
/// [`PubNubClient`] allows to have multiple [`subscription`] objects which will
/// be used to deliver real-time updates on channels and groups specified during
/// [`subscribe`] method call.
///
/// [`subscription`]: crate::Subscription
/// [`PubNubClient`]: crate::PubNubClient
#[derive(Debug)]
pub(crate) struct SubscriptionManager<T, D> {
    pub(crate) inner: Arc<SubscriptionManagerRef<T, D>>,
}

impl<T, D> SubscriptionManager<T, D> {
    pub fn new(
        event_engine: Arc<SubscribeEventEngine>,
        #[cfg(feature = "presence")] heartbeat_call: Arc<PresenceCall>,
        #[cfg(feature = "presence")] leave_call: Arc<PresenceCall>,
    ) -> Self {
        Self {
            inner: Arc::new(SubscriptionManagerRef {
                event_engine,
                event_handlers: Default::default(),
                #[cfg(feature = "presence")]
                heartbeat_call,
                #[cfg(feature = "presence")]
                leave_call,
            }),
        }
    }
}

impl<T, D> Deref for SubscriptionManager<T, D> {
    type Target = SubscriptionManagerRef<T, D>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, D> DerefMut for SubscriptionManager<T, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::get_mut(&mut self.inner).expect("Presence configuration is not unique.")
    }
}

impl<T, D> Clone for SubscriptionManager<T, D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Active subscriptions' manager reference.
///
/// This struct contains the actual subscriptions' manager state. It is wrapped
/// in an Arc by [`SubscriptionManager`] and uses internal mutability for its
/// internal state.
///
/// Not intended to be used directly. Use [`SubscriptionManager`] instead.
pub(crate) struct SubscriptionManagerRef<T, D> {
    /// Subscription event engine.
    ///
    /// State machine which is responsible for subscription loop maintenance.
    event_engine: Arc<SubscribeEventEngine>,

    /// List of registered event handlers.
    ///
    /// List of handlers which will receive real-time events and dispatch them
    /// to the listeners.
    event_handlers: RwLock<HashMap<String, Weak<dyn EventHandler<T, D> + Send + Sync>>>,

    /// Presence `join` announcement.
    ///
    /// Announces `user_id` presence on specified channels and groups.
    #[cfg(feature = "presence")]
    heartbeat_call: Arc<PresenceCall>,

    /// Presence `leave` announcement.
    ///
    /// Announces `user_id` `leave` from specified channels and groups.
    #[cfg(feature = "presence")]
    leave_call: Arc<PresenceCall>,
}

impl<T, D> SubscriptionManagerRef<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    pub fn notify_new_status(&self, status: &ConnectionStatus) {
        if let Some(client) = self.client() {
            client.handle_status(status.clone())
        }
    }

    pub fn notify_new_messages(&self, cursor: SubscriptionCursor, events: Vec<Update>) {
        if let Some(client) = self.client() {
            client.handle_events(cursor.clone(), &events)
        }

        self.event_handlers.write().retain(|_, weak_handler| {
            if let Some(handler) = weak_handler.upgrade().clone() {
                handler.handle_events(cursor.clone(), &events);
                true
            } else {
                false
            }
        });
    }

    pub fn register(
        &mut self,
        event_handler: &Weak<dyn EventHandler<T, D> + Send + Sync>,
        cursor: Option<SubscriptionCursor>,
    ) {
        let Some(upgraded_event_handler) = event_handler.upgrade().clone() else {
            return;
        };

        let event_handler_id = upgraded_event_handler.id();
        if self.event_handlers.read().contains_key(event_handler_id) {
            return;
        }

        {
            self.event_handlers
                .write()
                .insert(event_handler_id.clone(), event_handler.clone());
        }

        if let Some(cursor) = cursor {
            self.restore_subscription(cursor);
        } else {
            self.change_subscription(None);
        }
    }

    pub fn update(
        &self,
        event_handler: &Weak<dyn EventHandler<T, D> + Send + Sync>,
        removed: Option<&[Subscription<T, D>]>,
    ) {
        let Some(upgraded_event_handler) = event_handler.upgrade().clone() else {
            return;
        };

        if !self
            .event_handlers
            .read()
            .contains_key(upgraded_event_handler.id())
        {
            return;
        }

        // Handle subscriptions' set subscriptions subset which has been removed from
        // it.
        let removed = removed.map(|removed| {
            removed
                .iter()
                .filter(|subscription| subscription.entity.subscriptions_count().eq(&0))
                .fold(SubscriptionInput::default(), |mut acc, subscription| {
                    acc += subscription.subscription_input.clone();
                    acc
                })
        });

        self.change_subscription(removed.as_ref());
    }

    pub fn unregister(&mut self, event_handler: &Weak<dyn EventHandler<T, D> + Send + Sync>) {
        let Some(upgraded_event_handler) = event_handler.upgrade().clone() else {
            return;
        };

        let event_handler_id = upgraded_event_handler.id();
        if !self.event_handlers.read().contains_key(event_handler_id) {
            return;
        }

        {
            self.event_handlers.write().remove(event_handler_id);
        }

        self.change_subscription(Some(&upgraded_event_handler.subscription_input(false)));
    }

    pub fn unregister_all(&mut self) {
        let inputs = self.current_input();

        // Invalidate current event handler state (subscribed and entity usage).
        {
            let mut handlers = self.event_handlers.write();

            handlers.iter().for_each(|(_, handler)| {
                if let Some(handler) = handler.upgrade() {
                    handler.invalidate();
                }
            });
            handlers.clear();
        }

        self.change_subscription(Some(&inputs));
    }

    pub fn disconnect(&self) {
        self.event_engine.process(&SubscribeEvent::Disconnect);
    }

    pub fn reconnect(&self, cursor: Option<SubscriptionCursor>) {
        self.event_engine
            .process(&SubscribeEvent::Reconnect { cursor });
    }

    /// Returns the current subscription input.
    ///
    /// Gather subscriptions from all registered (active) event handlers.
    ///
    /// # Returns
    ///
    /// - [`SubscriptionInput`]: The sum of all subscription inputs from the
    ///   event handlers.
    pub fn current_input(&self) -> SubscriptionInput {
        self.event_handlers
            .read()
            .values()
            .filter_map(|weak_handler| weak_handler.upgrade().clone())
            .map(|handler| handler.subscription_input(false).clone())
            .sum()
    }

    /// Checks if there are any event handlers registered.
    ///
    /// # Returns
    ///
    /// Returns `true` if there are registered event handlers, `false`
    /// otherwise.
    pub fn has_handlers(&self) -> bool {
        !self.event_handlers.read().is_empty()
    }

    /// Terminate subscription manager.
    ///
    /// Gracefully terminate all ongoing tasks including detached event engine
    /// loop.
    pub fn terminate(&self) {
        self.event_engine
            .stop(SubscribeEffectInvocation::TerminateEventEngine);
    }

    fn change_subscription(&self, removed: Option<&SubscriptionInput>) {
        let mut inputs = self.current_input();

        if let Some(removed) = removed {
            inputs -= removed.clone();
        }

        let channels = inputs.channels();
        let channel_groups = inputs.channel_groups();

        #[cfg(feature = "presence")]
        {
            (!inputs.is_empty && removed.is_none()).then(|| {
                self.heartbeat_call.as_ref()(channels.clone(), channel_groups.clone(), false)
            });

            if let Some(removed) = removed {
                if !removed.is_empty {
                    self.leave_call.as_ref()(
                        removed.channels(),
                        removed.channel_groups(),
                        inputs.is_empty,
                    );
                }
            }
        }

        self.event_engine
            .process(&SubscribeEvent::SubscriptionChanged {
                channels,
                channel_groups,
            });
    }

    fn restore_subscription(&self, cursor: SubscriptionCursor) {
        let inputs = self.current_input();

        #[cfg(feature = "presence")]
        if !inputs.is_empty {
            self.heartbeat_call.as_ref()(inputs.channels(), inputs.channel_groups(), false);
        }

        self.event_engine
            .process(&SubscribeEvent::SubscriptionRestored {
                channels: inputs.channels(),
                channel_groups: inputs.channel_groups(),
                cursor,
            });
    }

    /// [`PubNubClientInstance`] associated with any of the event handlers.
    ///
    /// # Returns
    ///
    /// Reference on the underlying [`PubNubClientInstance`] instance of event
    /// handler.
    fn client(&self) -> Option<Arc<PubNubClientInstance<T, D>>> {
        let event_handlers = self.event_handlers.read();
        let mut client = None;
        if !event_handlers.is_empty() {
            if let Some((_, handler)) = event_handlers.iter().next() {
                if let Some(handler) = handler.upgrade().clone() {
                    client.clone_from(&handler.client().upgrade());
                }
            }
        }

        client
    }
}

impl<T, D> Debug for SubscriptionManagerRef<T, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SubscriptionManagerRef {{ event_engine: {:?}, event handlers: {:?} }}",
            self.event_engine, self.event_handlers
        )
    }
}

#[cfg(test)]
mod should {
    use futures::{FutureExt, StreamExt};

    use super::*;
    use crate::{
        core::RequestRetryConfiguration,
        dx::subscribe::{
            event_engine::{SubscribeEffectHandler, SubscribeState},
            result::SubscribeResult,
            types::Message,
            EventEmitter, Subscriber, Update,
        },
        lib::alloc::sync::Arc,
        providers::futures_tokio::RuntimeTokio,
        Keyset, PubNubClient, PubNubClientBuilder,
    };

    fn client() -> PubNubClient {
        PubNubClientBuilder::with_reqwest_transport()
            .with_keyset(Keyset {
                subscribe_key: "",
                publish_key: Some(""),
                secret_key: None,
            })
            .with_user_id("user_id")
            .build()
            .unwrap()
    }

    fn event_engine() -> Arc<SubscribeEventEngine> {
        let (cancel_tx, _) = async_channel::bounded(1);

        SubscribeEventEngine::new(
            SubscribeEffectHandler::new(
                Arc::new(move |_| {
                    async move {
                        Ok(SubscribeResult {
                            cursor: Default::default(),
                            messages: Default::default(),
                        })
                    }
                    .boxed()
                }),
                Arc::new(|_| {
                    // Do nothing yet
                }),
                Arc::new(Box::new(|_, _| {
                    // Do nothing yet
                })),
                RequestRetryConfiguration::None,
                cancel_tx,
            ),
            SubscribeState::Unsubscribed,
            RuntimeTokio,
        )
    }

    #[tokio::test]
    async fn register_subscription() {
        let client = client();
        let mut manager = SubscriptionManager::new(
            event_engine(),
            #[cfg(feature = "presence")]
            Arc::new(|channels, _, _| {
                assert!(channels.is_some());
                assert_eq!(channels.unwrap().len(), 1);
            }),
            #[cfg(feature = "presence")]
            Arc::new(|_, _, _| {}),
        );
        let channel = client.channel("test");
        let subscription = channel.subscription(None);
        let weak_subscription = &Arc::downgrade(&subscription.inner);
        let weak_handler: Weak<dyn EventHandler<_, _> + Send + Sync> = weak_subscription.clone();

        manager.register(&weak_handler, None);

        assert_eq!(manager.event_handlers.read().len(), 1);
    }

    #[tokio::test]
    async fn unregister_subscription() {
        let client = client();
        let mut manager = SubscriptionManager::new(
            event_engine(),
            #[cfg(feature = "presence")]
            Arc::new(|_, _, _| {}),
            #[cfg(feature = "presence")]
            Arc::new(|channels, _, _| {
                assert!(channels.is_some());
                assert_eq!(channels.unwrap().len(), 1);
            }),
        );
        let channel = client.channel("test");
        let subscription = channel.subscription(None);
        let weak_subscription = &Arc::downgrade(&subscription.inner);
        let weak_handler: Weak<dyn EventHandler<_, _> + Send + Sync> = weak_subscription.clone();

        manager.register(&weak_handler, None);
        manager.unregister(&weak_handler);

        assert_eq!(manager.event_handlers.read().len(), 0);
    }

    #[tokio::test]
    async fn notify_subscription_about_updates() {
        let client = client();
        let mut manager = SubscriptionManager::new(
            event_engine(),
            #[cfg(feature = "presence")]
            Arc::new(|_, _, _| {}),
            #[cfg(feature = "presence")]
            Arc::new(|_, _, _| {}),
        );
        let cursor: SubscriptionCursor = "15800701771129796".to_string().into();
        let channel = client.channel("test");
        let subscription = channel.subscription(None);
        let weak_subscription = Arc::downgrade(&subscription.inner);
        let weak_handler: Weak<dyn EventHandler<_, _> + Send + Sync> = weak_subscription.clone();

        // Simulate `.subscribe()` call.
        {
            let mut is_subscribed = subscription.is_subscribed.write();
            *is_subscribed = true;
        }
        manager.register(&weak_handler, Some(cursor.clone()));

        manager.notify_new_messages(
            cursor.clone(),
            vec![Update::Message(Message {
                channel: "test".into(),
                subscription: "test".into(),
                timestamp: cursor.timetoken.parse::<usize>().ok().unwrap(),
                ..Default::default()
            })],
        );

        assert!(subscription.messages_stream().next().await.is_some());
    }
}
