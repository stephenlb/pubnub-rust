//! # Subscription module.
//!
//! This module contains the [`Subscription`] type, which is used to manage
//! subscription to the specific entity and attach listeners to process
//! real-time events triggered for the `entity`.

use spin::RwLock;
use uuid::Uuid;

use crate::core::{Deserializer, Transport};
use crate::{
    core::{DataStream, PubNubEntity},
    dx::pubnub_client::PubNubClientInstance,
    lib::{
        alloc::{
            string::String,
            sync::{Arc, Weak},
            vec,
            vec::Vec,
        },
        collections::HashMap,
        core::{
            cmp::PartialEq,
            fmt::{Debug, Formatter, Result},
            ops::{Add, Deref, DerefMut, Drop},
        },
    },
    subscribe::{
        event_engine::SubscriptionInput, traits::EventHandler, AppContext, EventDispatcher,
        EventEmitter, EventSubscriber, File, Message, MessageAction, Presence, SubscribableType,
        SubscriptionCursor, SubscriptionOptions, SubscriptionSet, Update,
    },
};

/// Entity subscription.
///
/// # Example
///
/// ### Multiplexed subscription
///
/// ```rust
/// use pubnub::{
///     subscribe::{Subscriber, SubscriptionOptions}, Keyset, PubNubClient, PubNubClientBuilder,
/// };
///
/// # fn main() -> Result<(), pubnub::core::PubNubError> {
/// let pubnub = // PubNubClient
/// #     PubNubClientBuilder::with_reqwest_transport()
/// #         .with_keyset(Keyset {
/// #              subscribe_key: "demo",
/// #              publish_key: Some("demo"),
/// #              secret_key: Some("demo")
/// #          })
/// #         .with_user_id("uuid")
/// #         .build()?;
/// let channel = pubnub.channel("my_channel");
/// // Creating Subscription instance for the Channel entity to subscribe and listen
/// // for real-time events.
/// let subscription = channel.subscription(None);
/// // Subscription with presence announcements
/// let subscription_with_presence = channel.subscription(Some(vec![SubscriptionOptions::ReceivePresenceEvents]));
/// #     Ok(())
/// # }
/// ```
///
/// ### Sum of subscriptions
///
/// ```rust
/// use pubnub::{
///     subscribe::{Subscriber, Subscription, SubscriptionSet},
///     Keyset, PubNubClient, PubNubClientBuilder,
/// };
///
/// # fn main() -> Result<(), pubnub::core::PubNubError> {
/// let pubnub = // PubNubClient
/// #     PubNubClientBuilder::with_reqwest_transport()
/// #         .with_keyset(Keyset {
/// #              subscribe_key: "demo",
/// #              publish_key: Some("demo"),
/// #              secret_key: Some("demo")
/// #          })
/// #         .with_user_id("uuid")
/// #         .build()?;
/// let channels = pubnub.channels(&["my_channel_1", "my_channel_2"]);
/// // Two `Subscription` instances can be added to create `SubscriptionSet` which can be used
/// // to attach listeners and subscribe in one place for both subscriptions used in addition
/// // operation.
/// let subscription = channels[0].subscription(None) + channels[1].subscription(None);
/// #     Ok(())
/// # }
/// ```
pub struct Subscription<
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
> {
    /// Subscription reference.
    pub(super) inner: Arc<SubscriptionRef<T, D>>,

    /// Whether subscription is `Clone::clone()` method call result or not.
    is_clone: bool,
}

/// Subscription reference
///
/// This struct contains the actual subscription state.
/// It's wrapped in `Arc` by [`Subscription`] and uses interior mutability for
/// its internal state.
///
/// Not intended to be used directly. Use [`Subscription`] instead.
pub struct SubscriptionRef<T: Send + Sync, D: Send + Sync> {
    /// Unique event handler instance identifier.
    ///
    /// [`Subscription`] can be cloned, but the internal state is always bound
    /// to the same reference of [`SubscriptionState`] with the same `id`.
    instance_id: String,

    /// Subscription state.
    state: Arc<SubscriptionState<T, D>>,

    /// Real-time event dispatcher.
    event_dispatcher: Arc<EventDispatcher>,
}

/// Shared subscription state
///
/// This struct contains state shared across all [`Subscription`] clones.
/// It's wrapped in `Arc` by [`SubscriptionRef`] and uses interior mutability
/// for its internal state.
///
/// Not intended to be used directly. Use [`Subscription`] instead.
#[derive(Debug)]
pub struct SubscriptionState<T: Send + Sync, D: Send + Sync> {
    /// Unique event handler identifier.
    pub(super) id: String,

    /// [`PubNubClientInstance`] which is backing which subscription.
    pub(super) client: Weak<PubNubClientInstance<T, D>>,

    /// Subscribable entity.
    ///
    /// Entity with information that is required to receive real-time updates
    /// for it.
    pub(super) entity: PubNubEntity<T, D>,

    /// Whether set is currently subscribed and active.
    pub(super) is_subscribed: Arc<RwLock<bool>>,

    /// List of strings which represent data stream identifiers for entity
    /// real-time events.
    pub(super) subscription_input: SubscriptionInput,

    /// Subscription time cursor.
    cursor: RwLock<Option<SubscriptionCursor>>,

    /// Subscription listener options.
    ///
    /// Options used to set up listener behavior and real-time events
    /// processing.
    options: Option<Vec<SubscriptionOptions>>,

    /// The list of weak references to all [`SubscriptionRef`] clones created
    /// for this reference.
    clones: RwLock<HashMap<String, Weak<SubscriptionRef<T, D>>>>,
}

impl<T, D> Subscription<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    /// Creates a new subscription for specified entity.
    ///
    /// # Arguments
    ///
    /// * `client` - Weak reference on [`PubNubClientInstance`] to access shared
    ///   resources.
    /// * `entities` - A `PubNubEntity` representing the entity to subscribe to.
    /// * `options` - An optional list of `SubscriptionOptions` specifying the
    ///   subscription behaviour.
    ///
    /// # Returns
    ///
    /// A new `Subscription` for the given `entity` and `options`.
    pub(crate) fn new(
        client: Weak<PubNubClientInstance<T, D>>,
        entity: PubNubEntity<T, D>,
        options: Option<Vec<SubscriptionOptions>>,
    ) -> Self {
        Self {
            inner: SubscriptionRef::new(client, entity, options),
            is_clone: false,
        }
    }

    /// Creates a clone of the subscription set with an empty event dispatcher.
    ///
    /// Empty clones have the same subscription state but an empty list of
    /// real-time event listeners, which makes it possible to attach
    /// listeners specific to the context. When the cloned subscription goes out
    /// of scope, all associated listeners will be invalidated and released.
    ///
    /// # Example
    ///
    /// ```rust
    /// use pubnub::{subscribe::Subscriber, PubNubClient, PubNubClientBuilder, Keyset};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), pubnub::core::PubNubError> {
    /// let pubnub = // PubNubClient
    /// #     PubNubClientBuilder::with_reqwest_transport()
    /// #         .with_keyset(Keyset {
    /// #              subscribe_key: "demo",
    /// #              publish_key: Some("demo"),
    /// #              secret_key: Some("demo")
    /// #          })
    /// #         .with_user_id("uuid")
    /// #         .build()?;
    /// let channel = pubnub.channel("my_channel");
    /// // Creating Subscription instance for the Channel entity to subscribe and listen
    /// // for real-time events.
    /// let subscription = channel.subscription(None);
    /// // ...
    /// // We need to pass subscription into other component which would like to
    /// // have own listeners to handle real-time events.
    /// let empty_subscription = subscription.clone_empty();
    /// // self.other_component(empty_subscription);    
    /// #     Ok(())
    /// # }
    /// ```
    ///
    /// # Returns
    ///
    /// A new instance of the subscription object with an empty event
    /// dispatcher.
    pub fn clone_empty(&self) -> Self {
        Self {
            inner: self.inner.clone_empty(),
            is_clone: false,
        }
    }
}

impl<T, D> Deref for Subscription<T, D>
where
    T: Transport + Send + Sync,
    D: Deserializer + Send + Sync,
{
    type Target = SubscriptionRef<T, D>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, D> DerefMut for Subscription<T, D>
where
    T: Transport + Send + Sync,
    D: Deserializer + Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::get_mut(&mut self.inner)
            .expect("Multiple mutable references to the Subscription are not allowed")
    }
}

impl<T, D> Clone for Subscription<T, D>
where
    T: Transport + Send + Sync,
    D: Deserializer + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            is_clone: true,
        }
    }
}

impl<T, D> Drop for Subscription<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    fn drop(&mut self) {
        // Nothing should be done for regular subscription clone.
        if self.is_clone {
            return;
        }

        // Unregistering self to clean up subscriptions list if required.
        let Some(client) = self.client().upgrade().clone() else {
            return;
        };

        if let Some(manager) = client.subscription_manager(false).write().as_mut() {
            let mut clones = self.clones.write();

            if clones.len().gt(&1) {
                clones.retain(|instance_id, _| instance_id.ne(&self.instance_id));
            } else if let Some((_, handler)) = clones.iter().next() {
                let handler: Weak<dyn EventHandler<T, D> + Send + Sync> = handler.clone();
                manager.unregister(&handler);
            }
        }
    }
}

impl<T, D> Add for Subscription<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    type Output = SubscriptionSet<T, D>;

    fn add(self, rhs: Self) -> Self::Output {
        SubscriptionSet::new_with_subscriptions(
            vec![self.clone(), rhs.clone()],
            self.options.clone(),
        )
    }
}

impl<T, D> PartialEq for Subscription<T, D>
where
    T: Transport + Send + Sync,
    D: Deserializer + Send + Sync,
{
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<T, D> Debug for Subscription<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "Subscription {{ id: {}, instance_id: {}, entity: {:?}, subscription_input: {:?}, \
            is_subscribed: {}, cursor: {:?}, options: {:?}}}",
            self.id,
            self.instance_id,
            self.entity,
            self.subscription_input,
            self.is_subscribed(),
            self.cursor.read().clone(),
            self.options
        )
    }
}

impl<T, D> SubscriptionRef<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    /// Creates a new subscription reference for specified entity.
    ///
    /// # Arguments
    ///
    /// * `client` - Weak reference on [`PubNubClientInstance`] to access shared
    ///   resources.
    /// * `entities` - A `PubNubEntity` representing the entity to subscribe to.
    /// * `options` - An optional list of `SubscriptionOptions` specifying the
    ///   subscription behaviour.
    ///
    /// # Returns
    ///
    /// A new [`SubscriptionRef`] for the given `entity` and `options`.
    pub(crate) fn new(
        client: Weak<PubNubClientInstance<T, D>>,
        entity: PubNubEntity<T, D>,
        options: Option<Vec<SubscriptionOptions>>,
    ) -> Arc<Self> {
        let subscription_ref = SubscriptionState::new(client, entity, options);
        let subscription_id = Uuid::new_v4().to_string();
        let subscription = Arc::new(Self {
            instance_id: subscription_id.clone(),
            state: Arc::new(subscription_ref),
            event_dispatcher: Default::default(),
        });
        subscription.store_clone(subscription_id, Arc::downgrade(&subscription));
        subscription
    }

    /// Creates a clone of the subscription set with an empty event dispatcher.
    ///
    /// # Returns
    ///
    /// A new instance of the subscription object with an empty event
    /// dispatcher.
    pub fn clone_empty(&self) -> Arc<Self> {
        let instance_id = Uuid::new_v4().to_string();
        let instance = Arc::new(Self {
            instance_id: instance_id.clone(),
            state: Arc::clone(&self.state),
            event_dispatcher: Default::default(),
        });
        self.store_clone(instance_id, Arc::downgrade(&instance));
        instance
    }

    /// Retrieves the current timetoken value.
    ///
    /// # Returns
    ///
    /// The current timetoken value as an `usize`, or 0 if the timetoken cannot
    /// be parsed.
    pub(super) fn current_timetoken(&self) -> usize {
        self.cursor
            .read()
            .as_ref()
            .and_then(|cursor| cursor.timetoken.parse::<usize>().ok())
            .unwrap_or(0)
    }

    /// Checks if the [`Subscription`] is active or not.
    ///
    /// # Returns
    ///
    /// Returns `true` if the active, otherwise `false`.
    pub(super) fn is_subscribed(&self) -> bool {
        *self.is_subscribed.read()
    }

    /// Register `Subscription` within `SubscriptionManager`.
    ///
    /// # Arguments
    ///
    /// - `cursor` - Subscription real-time events catch up cursor.
    fn register_with_cursor(&self, cursor: Option<SubscriptionCursor>) {
        let Some(client) = self.client.upgrade().clone() else {
            return;
        };

        {
            if let Some(manager) = client.subscription_manager(true).write().as_mut() {
                // Mark entities as "in use" by subscription.
                self.entity.increase_subscriptions_count();

                if let Some((_, handler)) = self.clones.read().iter().next() {
                    let handler: Weak<dyn EventHandler<T, D> + Send + Sync> = handler.clone();
                    manager.register(&handler, cursor);
                }
            }
        }
    }

    /// Filters the given list of `Update` events based on the subscription
    /// input and the current timetoken.
    ///
    /// # Arguments
    ///
    /// * `events` - A slice of `Update` events to filter.
    ///
    /// # Returns
    ///
    /// A new `Vec<Update>` containing only the events that satisfy the
    /// following conditions:
    /// 1. The event's subscription is present in the subscription input.
    /// 2. The event's timestamp is greater than or equal to the current
    ///    timetoken.
    fn filtered_events(&self, events: &[Update]) -> Vec<Update> {
        let subscription_input = self.subscription_input(true);
        let current_timetoken = self.current_timetoken();

        events
            .iter()
            .filter(|event| {
                subscription_input.contains(&event.subscription())
                    && event.event_timestamp().ge(&current_timetoken)
            })
            .cloned()
            .collect::<Vec<Update>>()
    }
}

impl<T, D> Deref for SubscriptionRef<T, D>
where
    T: Send + Sync,
    D: Send + Sync,
{
    type Target = SubscriptionState<T, D>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<T, D> DerefMut for SubscriptionRef<T, D>
where
    T: Send + Sync,
    D: Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::get_mut(&mut self.state)
            .expect("Multiple mutable references to the SubscriptionRef are not allowed")
    }
}

impl<T, D> EventSubscriber for SubscriptionRef<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    fn subscribe(&self) {
        let mut is_subscribed = self.is_subscribed.write();
        if *is_subscribed {
            return;
        }
        *is_subscribed = true;

        self.register_with_cursor(self.cursor.read().clone());
    }

    fn subscribe_with_timetoken<SC>(&self, cursor: SC)
    where
        SC: Into<SubscriptionCursor>,
    {
        let mut is_subscribed = self.is_subscribed.write();
        if *is_subscribed {
            return;
        }
        *is_subscribed = true;

        let user_cursor = cursor.into();
        let cursor = user_cursor.is_valid().then_some(user_cursor);

        {
            if cursor.is_some() {
                let mut cursor_slot = self.cursor.write();
                if let Some(current_cursor) = cursor_slot.as_ref() {
                    let catchup_cursor = cursor.clone().unwrap_or_default();
                    catchup_cursor
                        .gt(current_cursor)
                        .then(|| *cursor_slot = Some(catchup_cursor));
                } else {
                    cursor_slot.clone_from(&cursor);
                }
            }
        }

        self.register_with_cursor(cursor);
    }

    fn unsubscribe(&self) {
        {
            let mut is_subscribed_slot = self.is_subscribed.write();
            if !*is_subscribed_slot {
                return;
            }
            *is_subscribed_slot = false;

            let mut cursor = self.cursor.write();
            *cursor = None;
        }

        if let Some(client) = self.client().upgrade().clone() {
            if let Some(manager) = client.subscription_manager(false).write().as_mut() {
                // Mark entities as "not in-use" by subscription.
                self.entity.decrease_subscriptions_count();

                if let Some((_, handler)) = self.clones.read().iter().next() {
                    let handler: Weak<dyn EventHandler<T, D> + Send + Sync> = handler.clone();
                    manager.unregister(&handler);
                }
            }
        }
    }
}

impl<T, D> EventHandler<T, D> for SubscriptionRef<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    fn handle_events(&self, cursor: SubscriptionCursor, events: &[Update]) {
        if !self.is_subscribed() {
            return;
        }

        let filtered_events = self.filtered_events(events);

        let mut cursor_slot = self.cursor.write();
        if let Some(current_cursor) = cursor_slot.as_ref() {
            cursor
                .gt(current_cursor)
                .then(|| *cursor_slot = Some(cursor));
        } else {
            *cursor_slot = Some(cursor);
        }

        // Go through subscription clones and trigger events for them.
        self.clones.write().retain(|_, handler| {
            if let Some(handler) = handler.upgrade().clone() {
                handler
                    .event_dispatcher
                    .handle_events(filtered_events.clone());
                return true;
            }
            false
        });
    }

    fn subscription_input(&self, include_inactive: bool) -> SubscriptionInput {
        if !include_inactive && self.entity.subscriptions_count().eq(&0) {
            Default::default()
        }

        self.subscription_input.clone()
    }

    fn invalidate(&self) {
        {
            let mut is_subscribed = self.is_subscribed.write();
            if !*is_subscribed {
                return;
            }
            *is_subscribed = false;
        }
        self.entity.decrease_subscriptions_count();

        // Go through subscription clones and invalidate them.
        self.clones.write().retain(|_, handler| {
            if let Some(handler) = handler.upgrade().clone() {
                handler.event_dispatcher.invalidate();
                return true;
            }
            false
        });
    }

    fn id(&self) -> &String {
        &self.id
    }

    fn client(&self) -> Weak<PubNubClientInstance<T, D>> {
        self.client.clone()
    }
}

impl<T, D> EventEmitter for SubscriptionRef<T, D>
where
    T: Send + Sync,
    D: Send + Sync,
{
    fn messages_stream(&self) -> DataStream<Message> {
        self.event_dispatcher.messages_stream()
    }

    fn signals_stream(&self) -> DataStream<Message> {
        self.event_dispatcher.signals_stream()
    }

    fn message_actions_stream(&self) -> DataStream<MessageAction> {
        self.event_dispatcher.message_actions_stream()
    }

    fn files_stream(&self) -> DataStream<File> {
        self.event_dispatcher.files_stream()
    }

    fn app_context_stream(&self) -> DataStream<AppContext> {
        self.event_dispatcher.app_context_stream()
    }

    fn presence_stream(&self) -> DataStream<Presence> {
        self.event_dispatcher.presence_stream()
    }

    fn stream(&self) -> DataStream<Update> {
        self.event_dispatcher.stream()
    }
}

impl<T, D> SubscriptionState<T, D>
where
    T: Send + Sync,
    D: Send + Sync,
{
    fn new(
        client: Weak<PubNubClientInstance<T, D>>,
        entity: PubNubEntity<T, D>,
        options: Option<Vec<SubscriptionOptions>>,
    ) -> SubscriptionState<T, D> {
        let is_channel_type = matches!(entity.r#type(), SubscribableType::Channel);
        let with_presence = if let Some(options) = &options {
            options
                .iter()
                .any(|option| matches!(option, SubscriptionOptions::ReceivePresenceEvents))
        } else {
            false
        };
        let entity_names = entity.names(with_presence);

        let input = SubscriptionInput::new(
            &is_channel_type.then(|| entity_names.clone()),
            &(!is_channel_type).then_some(entity_names),
        );

        Self {
            id: Uuid::new_v4().to_string(),
            client,
            entity,
            is_subscribed: Default::default(),
            subscription_input: input,
            cursor: Default::default(),
            options,
            clones: Default::default(),
        }
    }

    /// Store a clone of a [`SubscriptionRef`] instance with a given instance
    /// ID.
    ///
    /// # Arguments
    ///
    /// * `instance_id` - The instance ID to associate with the clone.
    /// * `instance` - The weak reference to the subscription reference to store
    ///   as a clone.
    fn store_clone(&self, instance_id: String, instance: Weak<SubscriptionRef<T, D>>) {
        let mut clones = self.clones.write();
        (!clones.contains_key(&instance_id)).then(|| clones.insert(instance_id, instance));
    }
}

#[cfg(test)]
mod it_should {
    use super::*;
    use crate::{Channel, ChannelGroup, Keyset, PubNubClient, PubNubClientBuilder};

    fn client() -> PubNubClient {
        PubNubClientBuilder::with_reqwest_transport()
            .with_keyset(Keyset {
                subscribe_key: "demo",
                publish_key: Some("demo"),
                secret_key: None,
            })
            .with_user_id("user")
            .build()
            .unwrap()
    }

    #[test]
    fn create_subscription_from_channel_entity() {
        let client = Arc::new(client());
        let channel = Channel::new(&client, "channel");
        let channel2 = Channel::new(&client, "channel2");
        let subscription = Subscription::new(
            Arc::downgrade(&client),
            PubNubEntity::Channel(channel),
            None,
        );
        let subscription2 = Subscription::new(
            Arc::downgrade(&client),
            PubNubEntity::Channel(channel2),
            None,
        );

        let _ = subscription.clone() + subscription2;

        assert!(!subscription.is_subscribed());
        assert!(subscription.subscription_input.contains_channel("channel"));
    }

    #[test]
    fn create_subscription_from_channel_entity_with_options() {
        let client = Arc::new(client());
        let channel = Channel::new(&client, "channel");
        let subscription = Subscription::new(
            Arc::downgrade(&client),
            PubNubEntity::Channel(channel),
            Some(vec![SubscriptionOptions::ReceivePresenceEvents]),
        );

        assert!(!subscription.is_subscribed());
        assert!(subscription.subscription_input.contains_channel("channel"));
        assert!(subscription
            .subscription_input
            .contains_channel("channel-pnpres"));
    }

    #[test]
    fn create_subscription_from_channel_group_entity() {
        let client = Arc::new(client());
        let channel_group = ChannelGroup::new(&client, "channel-group");
        let subscription = Subscription::new(
            Arc::downgrade(&client),
            PubNubEntity::ChannelGroup(channel_group),
            None,
        );

        assert!(!subscription.is_subscribed());
        assert!(subscription
            .subscription_input
            .contains_channel_group("channel-group"));
        assert!(!subscription
            .subscription_input
            .contains_channel_group("channel-group-pnpres"));
    }

    #[test]
    fn create_subscription_from_channel_group_entity_with_options() {
        let client = Arc::new(client());
        let channel_group = ChannelGroup::new(&client, "channel-group");
        let subscription = Subscription::new(
            Arc::downgrade(&client),
            PubNubEntity::ChannelGroup(channel_group),
            Some(vec![SubscriptionOptions::ReceivePresenceEvents]),
        );

        assert!(!subscription.is_subscribed());
        assert!(subscription
            .subscription_input
            .contains_channel_group("channel-group"));
        assert!(subscription
            .subscription_input
            .contains_channel_group("channel-group-pnpres"));
    }

    #[test]
    fn preserve_id_between_clones() {
        let client = Arc::new(client());
        let channel = Channel::new(&client, "channel");
        let subscription = Subscription::new(
            Arc::downgrade(&client),
            PubNubEntity::Channel(channel),
            None,
        );
        assert_eq!(subscription.clone().id.clone(), subscription.id.clone());
    }

    #[test]
    fn preserve_options_between_clones() {
        let client = Arc::new(client());
        let channel = Channel::new(&client, "channel");
        let subscription = Subscription::new(
            Arc::downgrade(&client),
            PubNubEntity::Channel(channel),
            Some(vec![SubscriptionOptions::ReceivePresenceEvents]),
        );
        assert_eq!(
            subscription.clone().options.clone(),
            subscription.options.clone()
        );
    }

    #[test]
    fn not_preserve_listeners_between_clones() {
        let client = Arc::new(client());
        let channel = Channel::new(&client, "channel");
        let subscription = Subscription::new(
            Arc::downgrade(&client),
            PubNubEntity::Channel(channel),
            None,
        );
        let _ = subscription.messages_stream();

        assert_eq!(
            subscription
                .event_dispatcher
                .message_streams
                .read()
                .as_ref()
                .unwrap()
                .len(),
            1
        );
        assert!(subscription
            .clone_empty()
            .event_dispatcher
            .message_streams
            .read()
            .as_ref()
            .is_none());
    }
}
