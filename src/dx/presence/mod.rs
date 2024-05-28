//! # Presence module.
//!
//! The presence module allows retrieving presence information and managing the
//! state in specific channels associated with specific `uuid`.
//! The presence module contains [`SetStateRequestBuilder`] type.

#[cfg(feature = "std")]
use futures::{
    future::{ready, BoxFuture},
    {select_biased, FutureExt},
};

#[cfg(feature = "std")]
use spin::RwLock;

#[doc(inline)]
pub use builders::*;
pub mod builders;

#[doc(inline)]
pub use result::{HeartbeatResponseBody, HeartbeatResult, LeaveResponseBody, LeaveResult};
pub mod result;

#[cfg(feature = "std")]
#[doc(inline)]
pub(crate) use presence_manager::PresenceManager;
#[cfg(feature = "std")]
pub(crate) mod presence_manager;

#[cfg(feature = "std")]
#[doc(inline)]
pub(crate) use event_engine::{
    PresenceEffectHandler, PresenceEventEngine, PresenceParameters, PresenceState,
};
#[cfg(feature = "std")]
pub(crate) mod event_engine;

#[cfg(feature = "std")]
use crate::{
    core::{
        event_engine::{cancel::CancellationTask, EventEngine},
        Deserializer, PubNubError, Runtime, Transport,
    },
    lib::alloc::sync::Arc,
};

use crate::{
    core::Serialize,
    dx::pubnub_client::PubNubClientInstance,
    lib::{
        alloc::{
            string::{String, ToString},
            vec::Vec,
        },
        collections::HashMap,
    },
};

impl<T, D> PubNubClientInstance<T, D> {
    /// Create a heartbeat request builder.
    ///
    /// This method is used to announce the presence of `user_id` on the
    /// provided list of channels and/or groups.
    ///
    /// Instance of [`HeartbeatRequestsBuilder`] returned.
    ///
    /// # Example
    /// ```rust
    /// use pubnub::presence::*;
    /// # use pubnub::{core::Serialize, Keyset, PubNubClientBuilder};
    /// # use std::collections::HashMap;
    ///
    /// #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut pubnub = // PubNubClient
    /// #     PubNubClientBuilder::with_reqwest_transport()
    /// #         .with_keyset(Keyset {
    /// #             subscribe_key: "demo",
    /// #             publish_key: None,
    /// #             secret_key: None
    /// #         })
    /// #         .with_user_id("uuid")
    /// #         .build()?;
    /// pubnub
    ///     .heartbeat()
    ///     .channels(["lobby".into(), "announce".into()])
    ///     .channel_groups(["area-51".into()])
    ///     .state(HashMap::<String, Vec<u8>>::from(
    ///         [(
    ///             String::from("lobby"),
    ///             HashMap::from([("is_admin".to_string(), false)]).serialize()?
    ///         )]
    ///     ))
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn heartbeat(&self) -> HeartbeatRequestBuilder<T, D> {
        HeartbeatRequestBuilder {
            pubnub_client: Some(self.clone()),
            heartbeat: Some(self.config.presence.heartbeat_value),
            user_id: Some(self.config.user_id.clone().to_string()),
            ..Default::default()
        }
    }

    /// Create a leave request builder.
    ///
    /// This method is used to announce `leave` of `user_id` on the provided
    /// list of channels and/or groups and update state associated with
    /// `user_id` on channels.
    ///
    /// Instance of [`LeaveRequestBuilder`] returned.
    ///
    /// # Example
    /// ```rust
    /// use pubnub::presence::*;
    /// # use pubnub::{Keyset, PubNubClientBuilder};
    ///
    /// #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut pubnub = // PubNubClient
    /// #         PubNubClientBuilder::with_reqwest_transport()
    /// #             .with_keyset(Keyset {
    /// #                 subscribe_key: "demo",
    /// #                 publish_key: None,
    /// #                 secret_key: None
    /// #             })
    /// #             .with_user_id("uuid")
    /// #             .build()?;
    /// pubnub
    ///     .leave()
    ///     .channels(["lobby".into(), "announce".into()])
    ///     .channel_groups(["area-51".into()])
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn leave(&self) -> LeaveRequestBuilder<T, D> {
        LeaveRequestBuilder {
            pubnub_client: Some(self.clone()),
            user_id: Some(self.config.user_id.clone().to_string()),
            ..Default::default()
        }
    }

    /// Create a set state request builder.
    ///
    /// This method is used to update state associated with `user_id` on
    /// channels and channels registered with channel groups.
    ///
    /// Instance of [`SetStateRequestBuilder`] returned.
    ///
    /// # Example
    /// ```rust
    /// use pubnub::presence::*;
    /// # use pubnub::{Keyset, PubNubClientBuilder};
    /// # use std::collections::HashMap;
    ///
    /// #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut pubnub = // PubNubClient
    /// #         PubNubClientBuilder::with_reqwest_transport()
    /// #             .with_keyset(Keyset {
    /// #                 subscribe_key: "demo",
    /// #                 publish_key: None,
    /// #                 secret_key: None
    /// #             })
    /// #             .with_user_id("uuid")
    /// #             .build()?;
    /// pubnub
    ///     .set_presence_state(HashMap::<String, bool>::from(
    ///          [(String::from("is_admin"), false)]
    ///      ))
    ///     .channels(["lobby".into(), "announce".into()])
    ///     .channel_groups(["area-51".into()])
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "serde")]
    pub fn set_presence_state<S>(&self, state: S) -> SetStateRequestBuilder<T, D>
    where
        T: 'static,
        D: 'static,
        S: serde::Serialize,
    {
        #[cfg(feature = "std")]
        let client = self.clone();

        SetStateRequestBuilder {
            pubnub_client: Some(self.clone()),
            state: Some(serde_json::to_vec(&state).ok()),
            user_id: Some(self.config.user_id.clone().to_string()),

            #[cfg(feature = "std")]
            on_execute: Some(Arc::new(move |channels, state| {
                let Some(state) = state else {
                    return;
                };

                client.update_presence_state(channels.into_iter().fold(
                    HashMap::new(),
                    |mut acc, channel| {
                        acc.insert(channel, state.clone());
                        acc
                    },
                ))
            })),
            ..Default::default()
        }
    }

    /// Create a set state request builder.
    ///
    /// This method is used to update state associated with `user_id` on
    /// channels and channels registered with channel groups.
    ///
    /// Instance of [`SetStateRequestBuilder`] returned.
    ///
    /// # Example
    /// ```rust
    /// use pubnub::presence::*;
    /// # use pubnub::{Keyset, PubNubClientBuilder};
    /// # use std::collections::HashMap;
    ///
    /// #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut pubnub = // PubNubClient
    /// #         PubNubClientBuilder::with_reqwest_transport()
    /// #             .with_keyset(Keyset {
    /// #                 subscribe_key: "demo",
    /// #                 publish_key: None,
    /// #                 secret_key: None
    /// #             })
    /// #             .with_user_id("uuid")
    /// #             .build()?;
    /// pubnub
    ///     .set_presence_state(HashMap::<String, bool>::from(
    ///          [(String::from("is_admin"), false)]
    ///      ))
    ///     .channels(["lobby".into(), "announce".into()])
    ///     .channel_groups(["area-51".into()])
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(not(feature = "serde"))]
    pub fn set_presence_state<S>(&self, state: S) -> SetStateRequestBuilder<T, D>
    where
        T: 'static,
        D: 'static,
        S: Serialize,
    {
        #[cfg(feature = "std")]
        let client = self.clone();

        SetStateRequestBuilder {
            pubnub_client: Some(self.clone()),
            state: Some(state.serialize().ok()),
            user_id: Some(self.config.user_id.clone().to_string()),

            #[cfg(feature = "std")]
            on_execute: Some(Arc::new(move |channels, state| {
                let Some(state) = state else {
                    return;
                };

                client.update_presence_state(channels.into_iter().fold(
                    HashMap::new(),
                    |mut acc, channel| {
                        acc.insert(channel, state.clone());
                        acc
                    },
                ))
            })),
            ..Default::default()
        }
    }

    /// Create a heartbeat request builder.
    ///
    /// This method is used to update state associated with `user_id` on
    /// channels using `heartbeat` operation endpoint. State with heartbeat can
    /// be set **only** for channels.
    ///
    /// Instance of [`HeartbeatRequestsBuilder`] returned.
    ///
    /// # Example
    /// ```rust
    /// use pubnub::presence::*;
    /// # use pubnub::{Keyset, PubNubClientBuilder};
    /// # use std::collections::HashMap;
    ///
    /// #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut pubnub = // PubNubClient
    /// #         PubNubClientBuilder::with_reqwest_transport()
    /// #             .with_keyset(Keyset {
    /// #                 subscribe_key: "demo",
    /// #                 publish_key: None,
    /// #                 secret_key: None
    /// #             })
    /// #             .with_user_id("uuid")
    /// #             .build()?;
    /// pubnub
    ///     .set_presence_state_with_heartbeat(HashMap::from([
    ///          ("lobby".to_string(), HashMap::from([("key".to_string(), "value".to_string())])),
    ///          ("announce".to_string(), HashMap::from([("key".to_string(), "value".to_string())])),
    ///     ]))
    ///     .channels(["lobby".into(), "announce".into()])
    ///     .channel_groups(["area-51".into()])
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_presence_state_with_heartbeat<S>(
        &self,
        state: HashMap<String, S>,
    ) -> HeartbeatRequestBuilder<T, D>
    where
        S: Serialize,
    {
        let mapped = state
            .iter()
            .fold(HashMap::new(), |mut acc, (channel, state)| {
                if let Ok(serialized_state) = state.serialize() {
                    acc.insert(channel.clone(), serialized_state);
                }
                acc
            });

        self.update_presence_state(mapped.clone());
        self.heartbeat().state(mapped)
    }

    /// Create a get state request builder.
    ///
    /// This method is used to get state associated with `user_id` on
    /// channels and channels registered with channel groups.
    ///
    /// Instance of [`GetStateRequestBuilder`] returned.
    ///
    /// # Example
    /// ```rust
    /// use pubnub::presence::*;
    /// # use pubnub::{Keyset, PubNubClientBuilder};
    ///
    /// #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut pubnub = // PubNubClient
    /// #         PubNubClientBuilder::with_reqwest_transport()
    /// #             .with_keyset(Keyset {
    /// #                 subscribe_key: "demo",
    /// #                 publish_key: None,
    /// #                 secret_key: None
    /// #             })
    /// #             .with_user_id("uuid")
    /// #             .build()?;
    /// pubnub
    ///     .get_presence_state()
    ///     .channels(["lobby".into(), "announce".into()])
    ///     .channel_groups(["area-51".into()])
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_presence_state(&self) -> GetStateRequestBuilder<T, D> {
        GetStateRequestBuilder {
            pubnub_client: Some(self.clone()),
            user_id: Some(self.config.user_id.clone().to_string()),
            ..Default::default()
        }
    }

    /// Create a here now request builder.
    ///
    /// This method is used to get information about current occupancy of
    /// channels and channel groups.
    ///
    /// Instance of [`HereNowRequestBuilder`] returned.
    ///
    /// # Example
    /// ```rust
    /// use pubnub::presence::*;
    /// # use pubnub::{Keyset, PubNubClientBuilder};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use std::sync::Arc;
    /// let mut pubnub = // PubNubClient
    /// #         PubNubClientBuilder::with_reqwest_transport()
    /// #             .with_keyset(Keyset {
    /// #                 subscribe_key: "demo",
    /// #                 publish_key: None,
    /// #                 secret_key: None,
    /// #             })
    /// #             .with_user_id("uuid")
    /// #             .build()?;
    /// let response = pubnub.here_now()
    ///         .channels(["lobby".into()])
    ///         .include_state(true)
    ///         .include_user_id(true)
    ///         .execute()
    ///         .await?;
    ///
    /// println!("All channels data: {:?}", response);
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn here_now(&self) -> HereNowRequestBuilder<T, D> {
        HereNowRequestBuilder {
            pubnub_client: Some(self.clone()),
            ..Default::default()
        }
    }

    /// Create a where now request builder.
    ///
    /// This method is used to get information about channels where `user_id`
    /// is currently present.
    ///
    /// Instance of [`WhereNowRequestBuilder`] returned.
    ///
    /// # Example
    /// ```rust
    /// use pubnub::presence::*;
    /// # use pubnub::{Keyset, PubNubClientBuilder};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut pubnub = // PubNubClient
    /// #         PubNubClientBuilder::with_reqwest_transport()
    /// #             .with_keyset(Keyset {
    /// #                 subscribe_key: "demo",
    /// #                 publish_key: None,
    /// #                 secret_key: None,
    /// #             })
    /// #             .with_user_id("uuid")
    /// #             .build()?;
    /// let response = pubnub.where_now().user_id("user_id").execute().await?;
    ///
    /// println!("User channels: {:?}", response);
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn where_now(&self) -> WhereNowRequestBuilder<T, D> {
        WhereNowRequestBuilder {
            pubnub_client: Some(self.clone()),
            ..Default::default()
        }
    }

    /// Update presence state associated with `user_id`.
    pub(crate) fn update_presence_state(&self, accumulated_state: HashMap<String, Vec<u8>>) {
        if accumulated_state.is_empty() {
            return;
        }

        let mut current_state = self.state.write();
        accumulated_state.into_iter().for_each(|(channel, state)| {
            current_state.insert(channel, state.clone());
        });
    }
}

#[cfg(feature = "std")]
impl<T, D> PubNubClientInstance<T, D>
where
    T: Transport + Send + 'static,
    D: Deserializer + 'static,
{
    /// Announce `join` for `user_id` on provided channels and groups.
    pub(crate) fn announce_join(
        &self,
        channels: Option<Vec<String>>,
        channel_groups: Option<Vec<String>>,
    ) {
        {
            if let Some(presence) = self.presence_manager(true).read().as_ref() {
                presence.announce_join(channels, channel_groups);
            };
        };
    }

    /// Announce `leave` for `user_id` on provided channels and groups.
    pub(crate) fn announce_left(
        &self,
        channels: Option<Vec<String>>,
        channel_groups: Option<Vec<String>>,
    ) {
        {
            if let Some(presence) = self.presence_manager(false).read().as_ref() {
                presence.announce_left(channels, channel_groups);
            };
        };
    }

    /// Announce `leave` for `user_id` on all active channels and groups.
    pub(crate) fn announce_left_all(&self) {
        {
            if let Some(presence) = self.presence_manager(false).read().as_ref() {
                presence.announce_left_all();
            }
        }
    }

    /// Presence manager which maintains Presence EE.
    ///
    /// # Arguments
    ///
    /// `create` - Whether manager should be created if not initialized.
    ///
    /// # Returns
    ///
    /// Returns an [`PresenceManager`] which represents the manager.
    #[cfg(all(feature = "presence", feature = "std"))]
    pub(crate) fn presence_manager(&self, create: bool) -> Arc<RwLock<Option<PresenceManager>>> {
        if self.config.presence.heartbeat_interval.unwrap_or(0).eq(&0) {
            return self.presence.clone();
        }

        {
            let manager = self.presence.read();
            if manager.is_some() || !create {
                return self.presence.clone();
            }
        }

        {
            let mut slot = self.presence.write();
            if slot.is_none() && create {
                *slot = Some(PresenceManager::new(
                    self.presence_event_engine(),
                    self.config.presence.heartbeat_interval.unwrap_or_default(),
                    self.config.presence.suppress_leave_events,
                ));
            };
        }

        self.presence.clone()
    }

    /// Presence event engine.
    ///
    /// Prepare presence event engine instance which will be used for `user_id`
    /// presence announcement and management.
    fn presence_event_engine(&self) -> Arc<PresenceEventEngine> {
        let channel_bound = 3;
        let (cancel_tx, cancel_rx) = async_channel::bounded::<String>(channel_bound);
        let delayed_heartbeat_cancel_rx = cancel_rx.clone();
        let wait_cancel_rx = cancel_rx.clone();
        let runtime = self.runtime.clone();
        let delayed_heartbeat_call_client = self.clone();
        let heartbeat_call_client = self.clone();
        let leave_call_client = self.clone();
        let wait_call_client = self.clone();
        let request_retry = self.config.transport.retry_configuration.clone();
        let request_delayed_retry = request_retry.clone();
        let delayed_heartbeat_runtime_sleep = runtime.clone();
        let wait_runtime_sleep = runtime.clone();

        EventEngine::new(
            PresenceEffectHandler::new(
                Arc::new(move |parameters| {
                    Self::heartbeat_call(heartbeat_call_client.clone(), parameters.clone())
                }),
                Arc::new(move |parameters| {
                    let delay_in_microseconds = request_delayed_retry.retry_delay(
                        Some("/v2/presence".to_string()),
                        &parameters.attempt,
                        parameters.reason.as_ref(),
                    );
                    let inner_runtime_sleep = delayed_heartbeat_runtime_sleep.clone();

                    Self::delayed_heartbeat_call(
                        delayed_heartbeat_call_client.clone(),
                        parameters.clone(),
                        Arc::new(move || {
                            if let Some(delay) = delay_in_microseconds {
                                inner_runtime_sleep
                                    .clone()
                                    .sleep_microseconds(delay)
                                    .boxed()
                            } else {
                                ready(()).boxed()
                            }
                        }),
                        delayed_heartbeat_cancel_rx.clone(),
                    )
                }),
                Arc::new(move |parameters| {
                    Self::leave_call(leave_call_client.clone(), parameters.clone())
                }),
                Arc::new(move |effect_id| {
                    let delay_in_secs = wait_call_client.config.presence.heartbeat_interval;
                    let inner_runtime_sleep = wait_runtime_sleep.clone();

                    Self::wait_call(
                        effect_id,
                        Arc::new(move || {
                            if let Some(delay) = delay_in_secs {
                                inner_runtime_sleep.clone().sleep(delay).boxed()
                            } else {
                                ready(()).boxed()
                            }
                        }),
                        wait_cancel_rx.clone(),
                    )
                }),
                request_retry,
                cancel_tx,
            ),
            PresenceState::Inactive,
            runtime,
        )
    }

    /// Call to announce `user_id` presence.
    pub(crate) fn heartbeat_call(
        client: Self,
        params: PresenceParameters,
    ) -> BoxFuture<'static, Result<HeartbeatResult, PubNubError>> {
        let mut request = client.heartbeat_request(params);
        let state = client.state.read();
        if !state.is_empty() {
            request = request.state(state.clone());
        }

        request.execute().boxed()
    }

    /// Call delayed announce of `user_id` presence.
    pub(crate) fn delayed_heartbeat_call<F>(
        client: Self,
        params: PresenceParameters,
        delay: Arc<F>,
        cancel_rx: async_channel::Receiver<String>,
    ) -> BoxFuture<'static, Result<HeartbeatResult, PubNubError>>
    where
        F: Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        let effect_id = params.effect_id.to_owned();
        let cancel_task = CancellationTask::new(cancel_rx, effect_id);

        client
            .heartbeat_request(params)
            .execute_with_cancel_and_delay(delay, cancel_task)
            .boxed()
    }

    /// Call announce `leave` for `user_id`.
    pub(crate) fn leave_call(
        client: Self,
        params: PresenceParameters,
    ) -> BoxFuture<'static, Result<LeaveResult, PubNubError>> {
        if client.config.presence.suppress_leave_events {
            return ready(Ok(LeaveResult)).boxed();
        }

        let mut request = client.leave();

        if let Some(channels) = params.channels.clone() {
            request = request.channels(channels);
        }

        if let Some(channel_groups) = params.channel_groups.clone() {
            request = request.channel_groups(channel_groups);
        }

        request.execute().boxed()
    }

    /// Heartbeat idle.
    pub(crate) fn wait_call<F>(
        effect_id: &str,
        delay: Arc<F>,
        cancel_rx: async_channel::Receiver<String>,
    ) -> BoxFuture<'static, Result<(), PubNubError>>
    where
        F: Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        let cancel_task = CancellationTask::new(cancel_rx, effect_id.to_owned());

        async move {
            select_biased! {
                _ = cancel_task.wait_for_cancel().fuse() => {
                    Err(PubNubError::EffectCanceled)
                },
                _ = delay().fuse() => Ok(())
            }
        }
        .boxed()
    }

    pub(crate) fn heartbeat_request(
        &self,
        params: PresenceParameters,
    ) -> HeartbeatRequestBuilder<T, D> {
        let mut request = self.heartbeat();

        if let Some(channels) = params.channels.clone() {
            request = request.channels(channels);
        }

        if let Some(channel_groups) = params.channel_groups.clone() {
            request = request.channel_groups(channel_groups);
        }
        //
        // if let Some(presence) = self.presence.clone().read().as_ref() {
        //     request = request.state_serialized(presence.state.clone())
        // }

        request
    }
}

#[cfg(test)]
mod it_should {
    use super::*;
    use crate::core::{PubNubError, Transport, TransportRequest, TransportResponse};
    use crate::providers::deserialization_serde::DeserializerSerde;
    use crate::transport::middleware::PubNubMiddleware;
    use crate::{
        lib::{alloc::vec::Vec, collections::HashMap},
        Keyset, PubNubClientBuilder,
    };

    /// Requests handler function type.
    type RequestHandler = Box<dyn Fn(&TransportRequest) + Send + Sync>;

    #[derive(Default)]
    struct MockTransport {
        ///  Response which mocked transport should return.
        response: Option<TransportResponse>,

        /// Request handler function which will be called before returning
        /// response.
        ///
        /// Use function to verify request parameters.
        request_handler: Option<RequestHandler>,
    }

    #[async_trait::async_trait]
    impl Transport for MockTransport {
        async fn send(&self, req: TransportRequest) -> Result<TransportResponse, PubNubError> {
            // Calling request handler (if provided).
            if let Some(handler) = &self.request_handler {
                handler(&req);
            }

            Ok(self.response.clone().unwrap_or(transport_response(200)))
        }
    }

    /// Service response payload.
    fn transport_response(status: u16) -> TransportResponse {
        TransportResponse {
            status,
            body: Some(Vec::from(if status < 400 {
                "{\"status\": 200, \"message\": \"OK\", \"service\": \"Presence\"}"
            } else {
                "\"error\":{{\"message\":\"Overall error\",\"source\":\"test\",\"details\":[{{\"message\":\"Error\",\"location\":\"signature\",\"locationType\":\"query\"}}]}}"
            })),
            ..Default::default()
        }
    }

    /// Construct test client with mocked transport.
    fn client(
        with_subscribe_key: bool,
        transport: Option<MockTransport>,
    ) -> PubNubClientInstance<PubNubMiddleware<MockTransport>, DeserializerSerde> {
        PubNubClientBuilder::with_transport(transport.unwrap_or(MockTransport {
            response: None,
            request_handler: None,
        }))
        .with_keyset(Keyset {
            subscribe_key: if with_subscribe_key { "demo" } else { "" },
            publish_key: None,
            secret_key: None,
        })
        .with_user_id("user")
        .build()
        .unwrap()
    }

    #[test]
    fn not_heartbeat_when_subscribe_key_missing() {
        let client = client(false, None);
        let request = client.heartbeat().channels(["test".into()]).build();

        assert!(&client.config.subscribe_key.is_empty());
        assert!(request.is_err())
    }

    #[tokio::test]
    async fn send_heartbeat() {
        let client = PubNubClientBuilder::with_reqwest_transport()
            .with_keyset(Keyset {
                subscribe_key: "demo",
                publish_key: Some("demo"),
                secret_key: None,
            })
            .with_user_id("user_id")
            .build()
            .unwrap();

        let result = client
            .heartbeat()
            .state(HashMap::<String, Vec<u8>>::from([(
                String::from("hello"),
                HashMap::<String, bool>::from([(String::from("is_admin"), false)])
                    .serialize()
                    .ok()
                    .unwrap(),
            )]))
            .channels(["hello".into()])
            .user_id("my_user")
            .execute()
            .await;

        match result {
            Ok(_) => {}
            Err(err) => panic!("Request should not fail: {err}"),
        }
    }

    #[tokio::test]
    async fn include_state_in_query() {
        let transport = MockTransport {
            response: None,
            request_handler: Some(Box::new(|req| {
                assert!(req.query_parameters.contains_key("state"));
                assert!(req.query_parameters.contains_key("state"));

                let state = req.query_parameters.get("state").unwrap();
                assert!(state.contains("channel_a"));
                assert!(state.contains("channel_c"));
            })),
        };

        let _ = client(true, Some(transport))
            .heartbeat()
            .state(HashMap::<String, Vec<u8>>::from([
                (
                    String::from("channel_a"),
                    HashMap::<String, String>::from([(
                        String::from("value_a"),
                        String::from("secret_a"),
                    )])
                    .serialize()
                    .ok()
                    .unwrap(),
                ),
                (
                    String::from("channel_c"),
                    HashMap::<String, String>::from([(
                        String::from("value_c"),
                        String::from("secret_c"),
                    )])
                    .serialize()
                    .ok()
                    .unwrap(),
                ),
            ]))
            .channels(["channel_a".into(), "channel_b".into(), "channel_c".into()])
            .execute()
            .await;
    }
}
