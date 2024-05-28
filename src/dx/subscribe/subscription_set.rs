//! # Subscription set module
//!
//! This module contains the [`SubscriptionSet`] type, which can be used to
//! manage subscription to the entities represented by managed subscriptions and
//! attach listeners to the specific event types.

use spin::RwLock;
use uuid::Uuid;

use crate::core::{Deserializer, Transport};
use crate::subscribe::traits::EventHandler;
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
            fmt::{Debug, Formatter, Result},
            ops::{Add, AddAssign, Deref, DerefMut, Sub, SubAssign},
        },
    },
    subscribe::{
        event_engine::SubscriptionInput, AppContext, EventDispatcher, EventEmitter,
        EventSubscriber, File, Message, MessageAction, Presence, Subscriber, Subscription,
        SubscriptionCursor, SubscriptionOptions, Update,
    },
};

/// Entities subscriptions set.
///
/// # Example
///
/// ### Multiplexed subscription
///
/// ```rust
/// use pubnub::{subscribe::SubscriptionParams, Keyset, PubNubClient, PubNubClientBuilder};
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
/// let subscription = pubnub.subscription(SubscriptionParams {
///     channels: Some(&["my_channel_1", "my_channel_2"]),
///     channel_groups:Some(&["my_group"]),
///     options:None
/// });
/// #     Ok(())
/// # }
/// ```
///
/// ### Sum of subscriptions
///
/// ```rust
/// use pubnub::{
///     subscribe::{Subscriber, Subscription},
///     Keyset, PubNubClient, PubNubClientBuilder,
/// };
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
/// let channels = pubnub.channels(&["my_channel_1", "my_channel_2"]);
/// // Two `Subscription` instances can be added to create `SubscriptionSet` which can be used
/// // to attach listeners and subscribe in one place for both subscriptions used in addition
/// // operation.
/// let subscription = channels[0].subscription(None) + channels[1].subscription(None);
/// #     Ok(())
/// # }
/// ```
pub struct SubscriptionSet<
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
> {
    /// Subscriptions set reference.
    pub(super) inner: Arc<SubscriptionSetRef<T, D>>,

    /// Whether subscription set is `Clone::clone()` method call result or not.
    is_clone: bool,
}

/// Entities subscriptions set reference.
///
/// This struct contains the actual entities subscriptions set state.
/// It's wrapped in `Arc` by [`SubscriptionSet`] and uses interior mutability
/// for its internal state.
///
/// Not intended to be used directly. Use [`SubscriptionSet`] instead.
pub struct SubscriptionSetRef<
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
> {
    /// Unique event handler instance identifier.
    ///
    /// [`SubscriptionSet`] can be cloned, but the internal state is always
    /// bound to the same reference of [`SubscriptionSetState`] with the
    /// same `id`.
    pub(super) instance_id: String,

    /// Subscriptions set reference.
    state: Arc<SubscriptionSetState<T, D>>,

    /// Real-time event dispatcher.
    event_dispatcher: EventDispatcher,
}

/// Shared entities subscriptions set state.
///
/// This struct contains the actual entities subscriptions set state.
/// It's wrapped in `Arc` by [`SubscriptionSet`] and uses interior mutability
/// for its internal state.
///
/// Not intended to be used directly. Use [`SubscriptionSet`] instead.
#[derive(Debug)]
pub struct SubscriptionSetState<
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
> {
    /// Unique event handler identifier.
    pub(super) id: String,

    /// [`PubNubClientInstance`] which is backing which subscription set.
    pub(super) client: Weak<PubNubClientInstance<T, D>>,

    /// Grouped subscriptions list.
    pub(crate) subscriptions: RwLock<Vec<Subscription<T, D>>>,

    /// Whether set is currently subscribed and active.
    pub(super) is_subscribed: Arc<RwLock<bool>>,

    /// List of strings which represent data stream identifiers for
    /// subscriptions' entity real-time events.
    pub(crate) subscription_input: RwLock<SubscriptionInput>,

    /// Subscription time cursor.
    cursor: RwLock<Option<SubscriptionCursor>>,

    /// Subscription set listener options.
    ///
    /// Options used to set up listener behavior and real-time events
    /// processing.
    options: Option<Vec<SubscriptionOptions>>,

    /// The list of weak references to all [`SubscriptionSet`] clones created
    /// for this reference.
    clones: RwLock<HashMap<String, Weak<SubscriptionSetRef<T, D>>>>,
}

impl<T, D> SubscriptionSet<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    /// Create subscription set from PubNub entities list.
    ///
    /// # Arguments
    ///
    /// * `entities` - A vector of [`PubNubEntity`] representing the entities to
    ///   subscribe to.
    /// * `options` - An optional [`SubscriptionOptions`] specifying the
    ///   subscription options.
    ///
    /// # Returns
    ///
    /// A new [`SubscriptionSet`] containing the subscriptions initialized from
    /// the given `entities` and `options`.
    pub(crate) fn new(
        entities: Vec<PubNubEntity<T, D>>,
        options: Option<Vec<SubscriptionOptions>>,
    ) -> Self {
        Self {
            inner: SubscriptionSetRef::new(entities, options),
            is_clone: false,
        }
    }

    /// Create subscription set reference from given subscriptions list.
    ///
    /// # Arguments
    ///
    /// * `subscriptions` - A vector of [`Subscription`] which should be grouped
    ///   in set.
    /// * `options` - An optional vector of [`SubscriptionOptions`] representing
    ///   the options for the subscriptions.
    ///
    /// # Returns
    ///
    /// A new [`SubscriptionSet`] containing given subscriptions and `options`.
    ///
    /// # Panics
    ///
    /// This function will panic if the `subscriptions` vector is empty.
    pub fn new_with_subscriptions(
        subscriptions: Vec<Subscription<T, D>>,
        options: Option<Vec<SubscriptionOptions>>,
    ) -> Self {
        Self {
            inner: SubscriptionSetRef::new_with_subscriptions(subscriptions, options),
            is_clone: false,
        }
    }

    /// Creates a clone of the subscription set with an empty event dispatcher.
    ///
    /// Empty clones have the same subscription set state but an empty list of
    /// real-time event listeners, which makes it possible to attach listeners
    /// specific to the context. When the cloned subscription set goes out of
    /// scope, all associated listeners will be invalidated and released.
    ///
    /// # Example
    ///
    /// ```rust
    /// use pubnub::{subscribe::SubscriptionParams, Keyset, PubNubClient, PubNubClientBuilder};
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
    /// let subscription = pubnub.subscription(SubscriptionParams {
    ///     channels: Some(&["my_channel_1", "my_channel_2"]),
    ///     channel_groups: Some(&["my_group"]),
    ///     options: None
    /// });
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

    /// Adds a list of subscriptions to the subscription set.
    ///
    /// # Arguments
    ///
    /// * `subscriptions` - A vector of `Subscription` objects to be added to
    ///   the subscription set.
    ///
    /// # Example
    ///
    /// ```rust
    /// use pubnub::{
    ///     subscribe::{Subscriber, SubscriptionParams},
    ///     Keyset, PubNubClient, PubNubClientBuilder,
    /// };
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
    /// // Create subscription set for list of channels and groups.
    /// let mut subscription = pubnub.subscription(SubscriptionParams {
    ///     channels: Some(&["my_channel_1", "my_channel_2"]),
    ///     channel_groups: Some(&["my_group"]),
    ///     options: None
    /// });
    /// let channel = pubnub.channel("my_channel_3");
    /// // Creating Subscription instance for the Channel entity to subscribe and listen
    /// // for real-time events.
    /// let channel_subscription = channel.subscription(None);
    /// // It is possible to separately add listeners to the `channel_subscription`
    /// // and call `subscribe()` or `subscribe_with_timetoken(..)`.
    ///
    /// // Add channel subscription to the set.
    /// subscription.add_subscriptions(vec![channel_subscription]);
    /// #     Ok(())
    /// # }
    /// ```
    pub fn add_subscriptions(&mut self, subscriptions: Vec<Subscription<T, D>>) {
        let unique_subscriptions =
            { Self::unique_subscriptions_from_list(Some(self), subscriptions) };

        {
            let mut subscription_input = self.subscription_input.write();
            *subscription_input += Self::subscription_input_from_list(&unique_subscriptions, true);
            self.subscriptions
                .write()
                .extend(unique_subscriptions.clone());
        }

        // Check whether subscription change required or not.
        if !self.is_subscribed() || unique_subscriptions.is_empty() {
            return;
        }

        let Some(client) = self.client().upgrade().clone() else {
            return;
        };

        if let Some(manager) = client.subscription_manager(true).write().as_mut() {
            // Mark entities as "in-use" by subscription.
            unique_subscriptions.iter().for_each(|subscription| {
                subscription.entity.increase_subscriptions_count();
            });

            // Notify manager to update its state with new subscriptions.
            if let Some((_, handler)) = self.clones.read().iter().next() {
                let handler: Weak<dyn EventHandler<T, D> + Send + Sync> = handler.clone();
                manager.update(&handler, None);
            }
        };
    }

    /// Subtracts the given subscriptions from the subscription set.
    ///
    /// # Arguments
    ///
    /// * `subscriptions` - A vector of `Subscription` objects to be removed
    ///   from the subscription set.
    ///
    /// # Example
    ///
    /// ```rust
    /// use pubnub::{
    ///     subscribe::{Subscriber, SubscriptionParams},
    ///     Keyset, PubNubClient, PubNubClientBuilder,
    /// };
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
    /// // Create subscription set for list of channels and groups.
    /// let mut subscription = pubnub.subscription(SubscriptionParams {
    ///     channels: Some(&["my_channel_1", "my_channel_2"]),
    ///     channel_groups: Some(&["my_group"]),
    ///     options: None
    /// });
    /// let channel = pubnub.channel("my_channel_3");
    /// // Creating Subscription instance for the Channel entity to subscribe and listen
    /// // for real-time events.
    /// let channel_subscription = channel.subscription(None);
    /// // It is possible to separately add listeners to the `channel_subscription`
    /// // and call `subscribe()` or `subscribe_with_timetoken(..)`.
    ///
    /// // Add channel subscription to the set.
    /// subscription.add_subscriptions(vec![channel_subscription.clone()]);
    ///
    /// // After some time it needed to remove subscriptions from the set.
    /// subscription.sub_subscriptions(vec![channel_subscription]);
    /// #     Ok(())
    /// # }
    /// ```
    pub fn sub_subscriptions(&mut self, subscriptions: Vec<Subscription<T, D>>) {
        let removed: Vec<Subscription<T, D>> = {
            let subscriptions_slot = self.subscriptions.read();
            Self::unique_subscriptions_from_list(None, subscriptions)
                .into_iter()
                .filter(|subscription| subscriptions_slot.contains(subscription))
                .collect()
        };

        {
            let mut subscription_input = self.subscription_input.write();
            *subscription_input -= Self::subscription_input_from_list(&removed, true);
            let mut subscription_slot = self.subscriptions.write();
            subscription_slot.retain(|subscription| !removed.contains(subscription));
        }

        // Check whether subscription change required or not.
        if !self.is_subscribed() || removed.is_empty() {
            return;
        }

        let Some(client) = self.client().upgrade().clone() else {
            return;
        };

        // Mark entities as "not in-use" by subscription.
        removed.iter().for_each(|subscription| {
            subscription.entity.decrease_subscriptions_count();
        });

        if let Some(manager) = client.subscription_manager(true).write().as_mut() {
            // Notify manager to update its state with removed subscriptions.
            if let Some((_, handler)) = self.clones.read().iter().next() {
                let handler: Weak<dyn EventHandler<T, D> + Send + Sync> = handler.clone();
                manager.update(&handler, Some(&removed));
            }
        };
    }

    /// Aggregate subscriptions' input.
    ///
    /// # Arguments
    ///
    /// * `subscriptions` - A slice of `Subscription<T, D>` representing a list
    ///   of subscriptions.
    /// * `include_inactive` - Whether _unused_ entities should be included into
    ///   the subscription input or not.
    ///
    /// # Returns
    ///
    /// `SubscriptionInput` which contains input from all `subscriptions`.
    fn subscription_input_from_list(
        subscriptions: &[Subscription<T, D>],
        include_inactive: bool,
    ) -> SubscriptionInput {
        let input = subscriptions
            .iter()
            .map(|subscription| {
                if !include_inactive && subscription.entity.subscriptions_count().eq(&0) {
                    return Default::default();
                }

                subscription.subscription_input.clone()
            })
            .sum();

        input
    }

    /// Filter unique subscriptions.
    ///
    /// Filter out duplicates and subscriptions which is already part of the
    /// `set`.
    ///
    /// # Arguments
    ///
    /// * `set` - An optional reference to a subscription set.
    /// * `subscriptions` - Vector of [`Subscription`] which should be filtered.
    ///
    /// # Returns
    ///
    /// Vector with unique subscriptions which is not part of the `set`.
    fn unique_subscriptions_from_list(
        set: Option<&Self>,
        subscriptions: Vec<Subscription<T, D>>,
    ) -> Vec<Subscription<T, D>> {
        let subscriptions_slot = if let Some(set) = set {
            set.subscriptions.read().clone()
        } else {
            vec![]
        };

        let mut unique_subscriptions = Vec::with_capacity(subscriptions.len());
        subscriptions.into_iter().for_each(|subscription| {
            if !unique_subscriptions.contains(&subscription)
                && !subscriptions_slot.contains(&subscription)
            {
                unique_subscriptions.push(subscription);
            }
        });

        unique_subscriptions
    }
}

impl<T, D> Deref for SubscriptionSet<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    type Target = SubscriptionSetRef<T, D>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, D> DerefMut for SubscriptionSet<T, D>
where
    T: Transport + Send + Sync,
    D: Deserializer + Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::get_mut(&mut self.inner)
            .expect("Multiple mutable references to the SubscriptionSet are not allowed")
    }
}

impl<T, D> Clone for SubscriptionSet<T, D>
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

impl<T, D> Drop for SubscriptionSet<T, D>
where
    T: Transport + Send + Sync,
    D: Deserializer + Send + Sync,
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

impl<T, D> Debug for SubscriptionSet<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "SubscriptionSet {{ id: {}, subscription_input: {:?}, is_subscribed: {}, cursor: {:?}, \
            options: {:?}, subscriptions: {:?}}}",
            self.id,
            self.subscription_input,
            self.is_subscribed(),
            self.cursor.read().clone(),
            self.options,
            self.subscriptions
        )
    }
}

impl<T, D> Add for SubscriptionSet<T, D>
where
    T: Transport + Send + Sync,
    D: Deserializer + Send + Sync,
{
    type Output = SubscriptionSet<T, D>;

    fn add(self, rhs: Self) -> Self::Output {
        let mut subscriptions = {
            let other_subscriptions = rhs.subscriptions.read();
            SubscriptionSet::unique_subscriptions_from_list(
                Some(&self),
                other_subscriptions.clone(),
            )
        };
        subscriptions.extend(self.subscriptions.read().clone());

        SubscriptionSet::new_with_subscriptions(subscriptions, None)
    }
}
impl<T, D> AddAssign for SubscriptionSet<T, D>
where
    T: Transport + Send + Sync,
    D: Deserializer + Send + Sync,
{
    fn add_assign(&mut self, rhs: Self) {
        self.add_subscriptions(rhs.subscriptions.read().clone());
    }
}
impl<T, D> Sub for SubscriptionSet<T, D>
where
    T: Transport + Send + Sync,
    D: Deserializer + Send + Sync,
{
    type Output = SubscriptionSet<T, D>;

    fn sub(self, rhs: Self) -> Self::Output {
        let removed: Vec<Subscription<T, D>> = {
            let other_subscriptions = rhs.subscriptions.read();
            let subscriptions_slot = self.subscriptions.read();
            Self::unique_subscriptions_from_list(None, other_subscriptions.clone())
                .into_iter()
                .filter(|subscription| subscriptions_slot.contains(subscription))
                .collect()
        };
        let mut subscriptions = self.subscriptions.read().clone();
        subscriptions.retain(|subscription| !removed.contains(subscription));

        SubscriptionSet::new_with_subscriptions(subscriptions, None)
    }
}
impl<T, D> SubAssign for SubscriptionSet<T, D>
where
    T: Transport + Send + Sync,
    D: Deserializer + Send + Sync,
{
    fn sub_assign(&mut self, rhs: Self) {
        self.sub_subscriptions(rhs.subscriptions.read().clone());
    }
}

impl<T, D> SubscriptionSetRef<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    /// Create subscription set reference from PubNub entities list.
    ///
    /// # Arguments
    ///
    /// * `entities` - A vector of [`PubNubEntity`] representing the entities to
    ///   subscribe to.
    /// * `options` - An optional [`SubscriptionOptions`] specifying the
    ///   subscription options.
    ///
    /// # Returns
    ///
    /// A new [`SubscriptionSetRef`] containing the subscriptions initialized
    /// from the given `entities` and `options`.
    pub(crate) fn new(
        entities: Vec<PubNubEntity<T, D>>,
        options: Option<Vec<SubscriptionOptions>>,
    ) -> Arc<Self> {
        let subscriptions = entities
            .into_iter()
            .map(|entity| entity.subscription(options.clone()))
            .collect::<Vec<Subscription<T, D>>>();

        Self::new_with_subscriptions(subscriptions, options)
    }

    /// Create subscription set reference from given subscriptions list.
    ///
    /// # Arguments
    ///
    /// * `subscriptions` - A vector of [`Subscription`] which should be grouped
    ///   in set.
    /// * `options` - An optional vector of [`SubscriptionOptions`] representing
    ///   the options for the subscriptions.
    ///
    /// # Returns
    ///
    /// A new [`SubscriptionSet`] containing given subscriptions and `options`.
    ///
    /// # Panics
    ///
    /// This function will panic if the `subscriptions` vector is empty.
    pub(crate) fn new_with_subscriptions(
        subscriptions: Vec<Subscription<T, D>>,
        options: Option<Vec<SubscriptionOptions>>,
    ) -> Arc<Self> {
        let subscription = subscriptions
            .first()
            .expect("At least one subscription expected.");
        let subscription_state =
            SubscriptionSetState::new(subscription.client(), subscriptions, options);
        let subscription_set = Arc::new(Self {
            instance_id: Uuid::new_v4().to_string(),
            state: Arc::new(subscription_state),
            event_dispatcher: Default::default(),
        });
        subscription_set.store_clone(
            subscription_set.instance_id.clone(),
            Arc::downgrade(&subscription_set),
        );
        subscription_set
    }

    /// Creates a clone of the subscription set with an empty event dispatcher.
    ///
    /// Empty clones have the same subscription set state but an empty list of
    /// real-time event listeners, which makes it possible to attach listeners
    /// specific to the context. When the cloned subscription set goes out of
    /// scope, all associated listeners will be invalidated and released.
    ///
    /// # Example
    ///
    /// ```rust
    /// use pubnub::{PubNubClient, PubNubClientBuilder, Keyset};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), pubnub::core::PubNubError> {
    /// use pubnub::subscribe::SubscriptionParams;
    /// let pubnub = // PubNubClient
    /// #     PubNubClientBuilder::with_reqwest_transport()
    /// #         .with_keyset(Keyset {
    /// #              subscribe_key: "demo",
    /// #              publish_key: Some("demo"),
    /// #              secret_key: Some("demo")
    /// #          })
    /// #         .with_user_id("uuid")
    /// #         .build()?;
    /// let subscription = pubnub.subscription(SubscriptionParams {
    ///     channels: Some(&["my_channel_1", "my_channel_2"]),
    ///     channel_groups: Some(&["my_group"]),
    ///     options: None
    /// });
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
        let cursor = self.cursor.read();
        cursor
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
        let Some(client) = self.client().upgrade().clone() else {
            return;
        };

        {
            let manager = client.subscription_manager(true);
            if let Some(manager) = manager.write().as_mut() {
                // Mark entities as "in-use" by subscription.
                self.subscriptions.read().iter().for_each(|subscription| {
                    subscription.entity.increase_subscriptions_count();
                });

                if let Some((_, handler)) = self.clones.read().iter().next() {
                    let handler: Weak<dyn EventHandler<T, D> + Send + Sync> = handler.clone();
                    manager.register(&handler, cursor);
                }
            };
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

impl<T, D> Deref for SubscriptionSetRef<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    type Target = SubscriptionSetState<T, D>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<T, D> DerefMut for SubscriptionSetRef<T, D>
where
    T: Transport + Send + Sync,
    D: Deserializer + Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::get_mut(&mut self.state)
            .expect("Multiple mutable references to the SubscriptionSetRef are not allowed")
    }
}

impl<T, D> EventSubscriber for SubscriptionSetRef<T, D>
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

        self.register_with_cursor(self.cursor.read().clone())
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

        let Some(client) = self.client().upgrade().clone() else {
            return;
        };

        {
            if let Some(manager) = client.subscription_manager(false).write().as_mut() {
                // Mark entities as "not in-use" by subscription.
                self.subscriptions.read().iter().for_each(|subscription| {
                    subscription.entity.increase_subscriptions_count();
                });

                if let Some((_, handler)) = self.clones.read().iter().next() {
                    let handler: Weak<dyn EventHandler<T, D> + Send + Sync> = handler.clone();
                    manager.unregister(&handler);
                }
            };
        }
    }
}

impl<T, D> EventHandler<T, D> for SubscriptionSetRef<T, D>
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
        SubscriptionSet::subscription_input_from_list(&self.subscriptions.read(), include_inactive)
    }

    fn invalidate(&self) {
        {
            let mut is_subscribed = self.is_subscribed.write();
            if !*is_subscribed {
                return;
            }
            *is_subscribed = false;
        }

        self.subscriptions
            .read()
            .iter()
            .for_each(|subscription| subscription.invalidate());

        self.event_dispatcher.invalidate();
    }

    fn id(&self) -> &String {
        &self.id
    }

    fn client(&self) -> Weak<PubNubClientInstance<T, D>> {
        self.client.clone()
    }
}

impl<T, D> EventEmitter for SubscriptionSetRef<T, D>
where
    T: Transport + Send + Sync,
    D: Deserializer + Send + Sync,
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

impl<T, D> SubscriptionSetState<T, D>
where
    T: Transport + Send + Sync + 'static,
    D: Deserializer + Send + Sync + 'static,
{
    fn new(
        client: Weak<PubNubClientInstance<T, D>>,
        subscriptions: Vec<Subscription<T, D>>,
        options: Option<Vec<SubscriptionOptions>>,
    ) -> SubscriptionSetState<T, D> {
        Self {
            id: Uuid::new_v4().to_string(),
            client,
            subscription_input: RwLock::new(SubscriptionSet::subscription_input_from_list(
                &subscriptions,
                true,
            )),
            is_subscribed: Default::default(),
            cursor: Default::default(),
            subscriptions: RwLock::new(SubscriptionSet::unique_subscriptions_from_list(
                None,
                subscriptions,
            )),
            options,
            clones: Default::default(),
        }
    }

    /// Store a clone of a [`SubscriptionSetRef`] instance with a given instance
    /// ID.
    ///
    /// # Arguments
    ///
    /// * `instance_id` - The instance ID to associate with the clone.
    /// * `instance` - The weak reference to the subscription set instance to
    ///   store as a clone.
    fn store_clone(&self, instance_id: String, instance: Weak<SubscriptionSetRef<T, D>>) {
        let mut clones = self.clones.write();
        (!clones.contains_key(&instance_id)).then(|| clones.insert(instance_id, instance));
    }
}

#[cfg(test)]
mod it_should {
    use super::*;
    use crate::{Channel, Keyset, PubNubClient, PubNubClientBuilder};

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
    fn create_subscription_set_from_entities() {
        let client = Arc::new(client());
        let channels = vec!["channel_1", "channel_2"]
            .into_iter()
            .map(|name| PubNubEntity::Channel(Channel::new(&client, name)))
            .collect();
        let subscription_set = SubscriptionSet::new(channels, None);

        assert!(!subscription_set.is_subscribed());
        assert!(subscription_set
            .subscription_input
            .read()
            .contains("channel_1"));
        assert!(subscription_set
            .subscription_input
            .read()
            .contains("channel_2"));
    }

    #[test]
    fn preserve_id_between_clones() {
        let client = Arc::new(client());
        let channels = vec!["channel_1", "channel_2"]
            .into_iter()
            .map(|name| PubNubEntity::Channel(Channel::new(&client, name)))
            .collect();
        let subscription_set = SubscriptionSet::new(channels, None);
        assert_eq!(
            subscription_set.clone().id.clone(),
            subscription_set.id.clone()
        );
    }

    #[test]
    fn not_preserve_listeners_between_clones() {
        let client = Arc::new(client());
        let channels = vec!["channel_1", "channel_2"]
            .into_iter()
            .map(|name| PubNubEntity::Channel(Channel::new(&client, name)))
            .collect();
        let subscription_set = SubscriptionSet::new(channels, None);
        let _ = subscription_set.messages_stream();

        assert_eq!(
            subscription_set
                .clone()
                .event_dispatcher
                .message_streams
                .read()
                .as_ref()
                .unwrap()
                .len(),
            1
        );
        assert!(subscription_set
            .clone_empty()
            .event_dispatcher
            .message_streams
            .read()
            .as_ref()
            .is_none());
    }

    #[test]
    fn concat_subscriptions() {
        let client = Arc::new(client());
        let channels_1_subscriptions = vec!["channel_1", "channel_2"]
            .into_iter()
            .map(|name| client.channel(name).subscription(None))
            .collect::<Vec<Subscription<_, _>>>();
        let channels_2_subscriptions = vec!["channel_3", "channel_4"]
            .into_iter()
            .map(|name| client.channel(name).subscription(None))
            .collect::<Vec<Subscription<_, _>>>();
        let channels_3_subscriptions = [
            channels_1_subscriptions[0].clone(),
            channels_2_subscriptions[1].clone(),
        ];
        let mut subscription_set_1 =
            channels_1_subscriptions[0].clone() + channels_1_subscriptions[1].clone();
        let subscription_set_2 =
            channels_2_subscriptions[0].clone() + channels_2_subscriptions[1].clone();
        let subscription_set_3 =
            channels_3_subscriptions[0].clone() + channels_3_subscriptions[1].clone();

        subscription_set_1 += subscription_set_2;
        assert!(subscription_set_1
            .subscription_input(true)
            .contains_channel("channel_3"));
        subscription_set_1 -= subscription_set_3;
        assert!(!subscription_set_1
            .subscription_input(true)
            .contains_channel("channel_1"));
    }
}
