//! # Heartbeat event engine state module.
//!
//! The module contains the [`SubscribeState`] type, which describes available
//! event engine states. The module also contains an implementation of
//! `transition` between states in response to certain events.

use crate::{
    core::{
        event_engine::{State, Transition},
        PubNubError,
    },
    dx::subscribe::{
        event_engine::{
            types::SubscriptionInput,
            SubscribeEffectInvocation::{
                self, CancelHandshake, CancelHandshakeReconnect, CancelReceive,
                CancelReceiveReconnect, EmitMessages, EmitStatus, Handshake, HandshakeReconnect,
                Receive, ReceiveReconnect,
            },
            SubscribeEvent,
        },
        result::Update,
        ConnectionStatus, SubscriptionCursor,
    },
    lib::alloc::{string::String, vec, vec::Vec},
};

/// States of subscribe state machine.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SubscribeState {
    /// Unsubscribed state.
    ///
    /// The initial state has no information about channels or groups from which
    /// events should be retrieved in real-time.
    Unsubscribed,

    /// Subscription initiation state.
    ///
    /// Retrieve the information that will be used to start the subscription
    /// loop.
    Handshaking {
        /// User input with channels and groups.
        ///
        /// Object contains list of channels and groups which will be source of
        /// real-time updates after initial subscription completion.
        input: SubscriptionInput,

        /// Custom time cursor.
        ///
        /// Custom cursor used by subscription loop to identify point in time
        /// after which updates will be delivered.
        cursor: Option<SubscriptionCursor>,
    },

    /// Subscription recover state.
    ///
    /// The system is recovering after the initial subscription attempt failed.
    HandshakeReconnecting {
        /// User input with channels and groups.
        ///
        /// Object contains list of channels and groups which has been used
        /// during recently failed initial subscription.
        input: SubscriptionInput,

        /// Custom time cursor.
        ///
        /// Custom cursor used by subscription loop to identify point in time
        /// after which updates will be delivered.
        cursor: Option<SubscriptionCursor>,

        /// Current initial subscribe retry attempt.
        ///
        /// Used to track overall number of initial subscription retry attempts.
        attempts: u8,

        /// Initial subscribe attempt failure reason.
        reason: PubNubError,
    },

    /// Initial subscription stopped state.
    HandshakeStopped {
        /// User input with channels and groups.
        ///
        /// Object contains list of channels and groups for which initial
        /// subscription stopped.
        input: SubscriptionInput,

        /// Custom time cursor.
        ///
        /// Custom cursor used by subscription loop to identify point in time
        /// after which updates will be delivered.
        cursor: Option<SubscriptionCursor>,
    },

    /// Initial subscription failure state.
    ///
    /// System wasn't able to perform successful initial subscription after
    /// fixed number of attempts.
    HandshakeFailed {
        /// User input with channels and groups.
        ///
        /// Object contains list of channels and groups which has been used
        /// during recently failed initial subscription.
        input: SubscriptionInput,

        /// Custom time cursor.
        ///
        /// Custom cursor used by subscription loop to identify point in time
        /// after which updates will be delivered.
        cursor: Option<SubscriptionCursor>,

        /// Initial subscribe attempt failure reason.
        reason: PubNubError,
    },

    /// Receiving updates state.
    ///
    /// Subscription state machine is in state where it receives real-time
    /// updates from [`PubNub`] network.
    ///
    /// [`PubNub`]:https://www.pubnub.com/
    Receiving {
        /// User input with channels and groups.
        ///
        /// Object contains list of channels and groups which real-time updates
        /// will be delivered.
        input: SubscriptionInput,

        /// Time cursor.
        ///
        /// Cursor used by subscription loop to identify point in time after
        /// which updates will be delivered.
        cursor: SubscriptionCursor,
    },

    /// Subscription recover state.
    ///
    /// The system is recovering after the updates receiving attempt failed.
    ReceiveReconnecting {
        /// User input with channels and groups.
        ///
        /// Object contains list of channels and groups which has been used
        /// during recently failed receive updates.
        input: SubscriptionInput,

        /// Time cursor.
        ///
        /// Cursor used by subscription loop to identify point in time after
        /// which updates will be delivered.
        cursor: SubscriptionCursor,

        /// Current receive retry attempt.
        ///
        /// Used to track overall number of receive updates retry attempts.
        attempts: u8,

        /// Receive updates attempt failure reason.
        reason: PubNubError,
    },

    /// Updates receiving stopped state.
    ReceiveStopped {
        /// User input with channels and groups.
        ///
        /// Object contains list of channels and groups for which updates
        /// receive stopped.
        input: SubscriptionInput,

        /// Time cursor.
        ///
        /// Cursor used by subscription loop to identify point in time after
        /// which updates will be delivered.
        cursor: SubscriptionCursor,
    },

    /// Updates receiving failure state.
    ///
    /// System wasn't able to receive updates after fixed number of attempts.
    ReceiveFailed {
        /// User input with channels and groups.
        ///
        /// Object contains list of channels and groups which has been used
        /// during recently failed receive updates.
        input: SubscriptionInput,

        /// Time cursor.
        ///
        /// Cursor used by subscription loop to identify point in time after
        /// which updates will be delivered.
        cursor: SubscriptionCursor,

        /// Receive updates attempt failure reason.
        reason: PubNubError,
    },
}

impl SubscribeState {
    /// Handle channels / groups list change event.
    fn subscription_changed_transition(
        &self,
        channels: &Option<Vec<String>>,
        channel_groups: &Option<Vec<String>>,
    ) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        match self {
            Self::Unsubscribed => Some(self.transition_to(
                Some(Self::Handshaking {
                    input: SubscriptionInput::new(channels, channel_groups),
                    cursor: None,
                }),
                None,
            )),
            Self::Handshaking { cursor, .. }
            | Self::HandshakeReconnecting { cursor, .. }
            | Self::HandshakeFailed { cursor, .. } => Some(self.transition_to(
                Some(Self::Handshaking {
                    input: SubscriptionInput::new(channels, channel_groups),
                    cursor: cursor.clone(),
                }),
                None,
            )),
            Self::HandshakeStopped { cursor, .. } => Some(self.transition_to(
                Some(Self::HandshakeStopped {
                    input: SubscriptionInput::new(channels, channel_groups),
                    cursor: cursor.clone(),
                }),
                None,
            )),
            Self::Receiving { cursor, .. } | Self::ReceiveReconnecting { cursor, .. } => {
                Some(self.transition_to(
                    Some(Self::Receiving {
                        input: SubscriptionInput::new(channels, channel_groups),
                        cursor: cursor.clone(),
                    }),
                    Some(vec![EmitStatus(ConnectionStatus::SubscriptionChanged {
                        channels: channels.clone(),
                        channel_groups: channel_groups.clone(),
                    })]),
                ))
            }
            Self::ReceiveFailed { cursor, .. } => Some(self.transition_to(
                Some(Self::Handshaking {
                    input: SubscriptionInput::new(channels, channel_groups),
                    cursor: Some(cursor.clone()),
                }),
                None,
            )),
            Self::ReceiveStopped { cursor, .. } => Some(self.transition_to(
                Some(Self::ReceiveStopped {
                    input: SubscriptionInput::new(channels, channel_groups),
                    cursor: cursor.clone(),
                }),
                None,
            )),
        }
    }

    /// Handle catchup event.
    ///
    /// Event is sent each time during attempt to subscribe with specific
    /// `cursor`.
    fn subscription_restored_transition(
        &self,
        channels: &Option<Vec<String>>,
        channel_groups: &Option<Vec<String>>,
        restore_cursor: &SubscriptionCursor,
    ) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        match self {
            Self::Unsubscribed => Some(self.transition_to(
                Some(Self::Handshaking {
                    input: SubscriptionInput::new(channels, channel_groups),
                    cursor: Some(restore_cursor.clone()),
                }),
                None,
            )),
            Self::Handshaking { .. }
            | Self::HandshakeReconnecting { .. }
            | Self::HandshakeFailed { .. } => Some(self.transition_to(
                Some(Self::Handshaking {
                    input: SubscriptionInput::new(channels, channel_groups),
                    cursor: Some(restore_cursor.clone()),
                }),
                None,
            )),
            Self::HandshakeStopped { .. } => Some(self.transition_to(
                Some(Self::HandshakeStopped {
                    input: SubscriptionInput::new(channels, channel_groups),
                    cursor: Some(restore_cursor.clone()),
                }),
                None,
            )),
            Self::Receiving { .. } | Self::ReceiveReconnecting { .. } => Some(self.transition_to(
                Some(Self::Receiving {
                    input: SubscriptionInput::new(channels, channel_groups),
                    cursor: restore_cursor.clone(),
                }),
                Some(vec![EmitStatus(ConnectionStatus::SubscriptionChanged {
                    channels: channels.clone(),
                    channel_groups: channel_groups.clone(),
                })]),
            )),
            Self::ReceiveFailed { .. } => Some(self.transition_to(
                Some(Self::Handshaking {
                    input: SubscriptionInput::new(channels, channel_groups),
                    cursor: Some(restore_cursor.clone()),
                }),
                None,
            )),
            Self::ReceiveStopped { .. } => Some(self.transition_to(
                Some(Self::ReceiveStopped {
                    input: SubscriptionInput::new(channels, channel_groups),
                    cursor: restore_cursor.clone(),
                }),
                None,
            )),
        }
    }

    /// Handle initial (reconnect) handshake success event.
    ///
    /// Event is sent when provided set of channels and groups has been used for
    /// first time.
    fn handshake_success_transition(
        &self,
        next_cursor: &SubscriptionCursor,
    ) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        match self {
            Self::Handshaking { input, cursor }
            | Self::HandshakeReconnecting { input, cursor, .. } => {
                // Merge stored cursor with service-provided.
                let mut next_cursor = next_cursor.clone();
                if let Some(cursor) = cursor {
                    next_cursor.timetoken.clone_from(&cursor.timetoken);
                }

                Some(self.transition_to(
                    Some(Self::Receiving {
                        input: input.clone(),
                        cursor: next_cursor,
                    }),
                    Some(vec![EmitStatus(ConnectionStatus::Connected)]),
                ))
            }
            _ => None,
        }
    }

    /// Handle initial handshake failure event.
    fn handshake_failure_transition(
        &self,
        reason: &PubNubError,
    ) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        // Request cancellation shouldn't cause any transition because there
        // will be another event after this.
        if matches!(reason, PubNubError::RequestCancel { .. }) {
            return None;
        }

        match self {
            Self::Handshaking { input, cursor } => Some(self.transition_to(
                Some(Self::HandshakeReconnecting {
                    input: input.clone(),
                    cursor: cursor.clone(),
                    attempts: 1,
                    reason: reason.clone(),
                }),
                None,
            )),
            _ => None,
        }
    }

    /// Handle handshake reconnect failure event.
    ///
    /// Event is sent if handshake reconnect effect failed due to any network
    /// issues.
    fn handshake_reconnect_failure_transition(
        &self,
        reason: &PubNubError,
    ) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        // Request cancellation shouldn't cause any transition because there
        // will be another event after this.
        if matches!(reason, PubNubError::RequestCancel { .. }) {
            return None;
        }

        match self {
            Self::HandshakeReconnecting {
                input,
                cursor,
                attempts,
                ..
            } => Some(self.transition_to(
                Some(Self::HandshakeReconnecting {
                    input: input.clone(),
                    cursor: cursor.clone(),
                    attempts: attempts + 1,
                    reason: reason.clone(),
                }),
                None,
            )),
            _ => None,
        }
    }

    /// Handle handshake reconnection limit event.
    ///
    /// Event is sent if handshake reconnect reached maximum number of reconnect
    /// attempts.
    fn handshake_reconnect_give_up_transition(
        &self,
        reason: &PubNubError,
    ) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        match self {
            Self::HandshakeReconnecting { input, cursor, .. } => Some(self.transition_to(
                Some(Self::HandshakeFailed {
                    input: input.clone(),
                    cursor: cursor.clone(),
                    reason: reason.clone(),
                }),
                Some(vec![EmitStatus(ConnectionStatus::ConnectionError(
                    reason.clone(),
                ))]),
            )),
            _ => None,
        }
    }

    /// Handle updates receive (reconnect) success event.
    ///
    /// Event is sent when real-time updates received for previously subscribed
    /// channels / groups.
    fn receive_success_transition(
        &self,
        cursor: &SubscriptionCursor,
        messages: &[Update],
    ) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        match self {
            Self::Receiving { input, .. } | Self::ReceiveReconnecting { input, .. } => {
                Some(self.transition_to(
                    Some(Self::Receiving {
                        input: input.clone(),
                        cursor: cursor.clone(),
                    }),
                    Some(vec![EmitMessages(messages.to_vec(), cursor.clone())]),
                ))
            }
            _ => None,
        }
    }

    /// Handle updates receive failure event.
    fn receive_failure_transition(
        &self,
        reason: &PubNubError,
    ) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        // Request cancellation shouldn't cause any transition because there
        // will be another event after this.
        if matches!(reason, PubNubError::RequestCancel { .. }) {
            return None;
        }

        match self {
            Self::Receiving { input, cursor, .. } => Some(self.transition_to(
                Some(Self::ReceiveReconnecting {
                    input: input.clone(),
                    cursor: cursor.clone(),
                    attempts: 1,
                    reason: reason.clone(),
                }),
                None,
            )),
            _ => None,
        }
    }

    /// Handle updates receive failure event.
    ///
    /// Event is sent if updates receive effect failed due to any network
    /// issues.
    fn receive_reconnect_failure_transition(
        &self,
        reason: &PubNubError,
    ) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        // Request cancellation shouldn't cause any transition because there
        // will be another event after this.
        if matches!(reason, PubNubError::RequestCancel { .. }) {
            return None;
        }

        match self {
            Self::ReceiveReconnecting {
                input,
                attempts,
                cursor,
                ..
            } => Some(self.transition_to(
                Some(Self::ReceiveReconnecting {
                    input: input.clone(),
                    cursor: cursor.clone(),
                    attempts: attempts + 1,
                    reason: reason.clone(),
                }),
                None,
            )),
            _ => None,
        }
    }

    /// Handle receive updates reconnection limit event.
    ///
    /// Event is sent if receive updates reconnect reached maximum number of
    /// reconnect attempts.
    fn receive_reconnect_give_up_transition(
        &self,
        reason: &PubNubError,
    ) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        match self {
            Self::ReceiveReconnecting { input, cursor, .. } => Some(self.transition_to(
                Some(Self::ReceiveFailed {
                    input: input.clone(),
                    cursor: cursor.clone(),
                    reason: reason.clone(),
                }),
                Some(vec![EmitStatus(
                    ConnectionStatus::DisconnectedUnexpectedly(reason.clone()),
                )]),
            )),
            _ => None,
        }
    }

    /// Handle disconnect event.
    ///
    /// Event is sent each time when client asked to unsubscribe all
    /// channels / groups or temporally stop any activity.
    fn disconnect_transition(&self) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        match self {
            Self::Handshaking { input, cursor }
            | Self::HandshakeReconnecting { input, cursor, .. } => Some(self.transition_to(
                Some(Self::HandshakeStopped {
                    input: input.clone(),
                    cursor: cursor.clone(),
                }),
                None,
            )),
            Self::Receiving { input, cursor } | Self::ReceiveReconnecting { input, cursor, .. } => {
                Some(self.transition_to(
                    Some(Self::ReceiveStopped {
                        input: input.clone(),
                        cursor: cursor.clone(),
                    }),
                    Some(vec![EmitStatus(ConnectionStatus::Disconnected)]),
                ))
            }
            _ => None,
        }
    }

    /// Handle reconnect event.
    ///
    /// Event is sent each time when client asked to restore activity for
    /// channels / groups after which previously temporally stopped or restore
    /// after reconnection failures.
    fn reconnect_transition(
        &self,
        restore_cursor: &Option<SubscriptionCursor>,
    ) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        match self {
            Self::HandshakeStopped { input, cursor }
            | Self::HandshakeFailed { input, cursor, .. } => Some(self.transition_to(
                Some(Self::Handshaking {
                    input: input.clone(),
                    cursor: if restore_cursor.is_some() {
                        restore_cursor.clone()
                    } else {
                        cursor.clone()
                    },
                }),
                None,
            )),
            Self::ReceiveStopped { input, cursor } | Self::ReceiveFailed { input, cursor, .. } => {
                Some(self.transition_to(
                    Some(Self::Handshaking {
                        input: input.clone(),
                        cursor: if restore_cursor.is_some() {
                            restore_cursor.clone()
                        } else {
                            Some(cursor.clone())
                        },
                    }),
                    None,
                ))
            }
            _ => None,
        }
    }

    /// Handle unsubscribe all event.
    fn unsubscribe_all_transition(&self) -> Option<Transition<Self, SubscribeEffectInvocation>> {
        Some(self.transition_to(
            Some(Self::Unsubscribed),
            Some(vec![EmitStatus(ConnectionStatus::Disconnected)]),
        ))
    }
}

impl State for SubscribeState {
    type State = Self;
    type Invocation = SubscribeEffectInvocation;
    type Event = SubscribeEvent;

    fn enter(&self) -> Option<Vec<Self::Invocation>> {
        match self {
            Self::Handshaking { input, cursor } => Some(vec![Handshake {
                input: input.clone(),
                cursor: cursor.clone(),
            }]),
            Self::HandshakeReconnecting {
                input,
                cursor,
                attempts,
                reason,
            } => Some(vec![HandshakeReconnect {
                input: input.clone(),
                cursor: cursor.clone(),
                attempts: *attempts,
                reason: reason.clone(),
            }]),
            Self::Receiving { input, cursor } => Some(vec![Receive {
                input: input.clone(),
                cursor: cursor.clone(),
            }]),
            Self::ReceiveReconnecting {
                input,
                cursor,
                attempts,
                reason,
            } => Some(vec![ReceiveReconnect {
                input: input.clone(),
                cursor: cursor.clone(),
                attempts: *attempts,
                reason: reason.clone(),
            }]),
            _ => None,
        }
    }

    fn exit(&self) -> Option<Vec<Self::Invocation>> {
        match self {
            Self::Handshaking { .. } => Some(vec![CancelHandshake]),
            Self::HandshakeReconnecting { .. } => Some(vec![CancelHandshakeReconnect]),
            Self::Receiving { .. } => Some(vec![CancelReceive]),
            Self::ReceiveReconnecting { .. } => Some(vec![CancelReceiveReconnect]),
            _ => None,
        }
    }

    fn transition(&self, event: &Self::Event) -> Option<Transition<Self::State, Self::Invocation>> {
        match event {
            SubscribeEvent::SubscriptionChanged {
                channels,
                channel_groups,
            } => self.subscription_changed_transition(channels, channel_groups),
            SubscribeEvent::SubscriptionRestored {
                channels,
                channel_groups,
                cursor,
            } => self.subscription_restored_transition(channels, channel_groups, cursor),
            SubscribeEvent::HandshakeSuccess { cursor }
            | SubscribeEvent::HandshakeReconnectSuccess { cursor } => {
                self.handshake_success_transition(cursor)
            }
            SubscribeEvent::HandshakeFailure { reason } => {
                self.handshake_failure_transition(reason)
            }
            SubscribeEvent::HandshakeReconnectFailure { reason, .. } => {
                self.handshake_reconnect_failure_transition(reason)
            }
            SubscribeEvent::HandshakeReconnectGiveUp { reason } => {
                self.handshake_reconnect_give_up_transition(reason)
            }
            SubscribeEvent::ReceiveSuccess { cursor, messages }
            | SubscribeEvent::ReceiveReconnectSuccess { cursor, messages } => {
                self.receive_success_transition(cursor, messages)
            }
            SubscribeEvent::ReceiveFailure { reason } => self.receive_failure_transition(reason),
            SubscribeEvent::ReceiveReconnectFailure { reason } => {
                self.receive_reconnect_failure_transition(reason)
            }
            SubscribeEvent::ReceiveReconnectGiveUp { reason } => {
                self.receive_reconnect_give_up_transition(reason)
            }
            SubscribeEvent::Disconnect => self.disconnect_transition(),
            SubscribeEvent::Reconnect { cursor } => self.reconnect_transition(cursor),
            SubscribeEvent::UnsubscribeAll => self.unsubscribe_all_transition(),
        }
    }

    fn transition_to(
        &self,
        state: Option<Self::State>,
        invocations: Option<Vec<Self::Invocation>>,
    ) -> Transition<Self::State, Self::Invocation> {
        let on_enter_invocations = match state.clone() {
            Some(state) => state.enter().unwrap_or_default(),
            None => vec![],
        };

        Transition {
            invocations: self
                .exit()
                .unwrap_or_default()
                .into_iter()
                .chain(invocations.unwrap_or_default())
                .chain(on_enter_invocations)
                .collect(),
            state,
        }
    }
}

#[cfg(test)]
mod should {
    // TODO: EE process tests should be async!
    use futures::FutureExt;
    use test_case::test_case;

    use super::*;
    use crate::{
        core::{event_engine::EventEngine, RequestRetryConfiguration},
        dx::subscribe::{
            event_engine::{
                effects::{
                    EmitMessagesEffectExecutor, EmitStatusEffectExecutor, SubscribeEffectExecutor,
                },
                SubscribeEffect, SubscribeEffectHandler,
            },
            result::SubscribeResult,
        },
        lib::alloc::sync::Arc,
        providers::futures_tokio::RuntimeTokio,
    };

    fn event_engine(
        start_state: SubscribeState,
    ) -> Arc<
        EventEngine<
            SubscribeState,
            SubscribeEffectHandler,
            SubscribeEffect,
            SubscribeEffectInvocation,
        >,
    > {
        let call: Arc<SubscribeEffectExecutor> = Arc::new(|_| {
            async move {
                Ok(SubscribeResult {
                    cursor: Default::default(),
                    messages: vec![],
                })
            }
            .boxed()
        });

        let emit_status: Arc<EmitStatusEffectExecutor> = Arc::new(|_| {});
        let emit_message: Arc<EmitMessagesEffectExecutor> = Arc::new(|_, _| {});

        let (tx, _) = async_channel::bounded(1);

        EventEngine::new(
            SubscribeEffectHandler::new(
                call,
                emit_status,
                emit_message,
                RequestRetryConfiguration::None,
                tx,
            ),
            start_state,
            RuntimeTokio,
        )
    }

    #[test_case(
        SubscribeState::Unsubscribed,
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch1".to_string()]),
            channel_groups: Some(vec!["gr1".to_string()]),
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        };
        "to handshaking on subscription changed"
    )]
    #[test_case(
        SubscribeState::Unsubscribed,
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch1".to_string()]),
            channel_groups: Some(vec!["gr1".to_string()]),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 }
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 1 })
        };
        "to handshaking on subscription restored"
    )]
    #[test_case(
        SubscribeState::Unsubscribed,
        SubscribeEvent::ReceiveFailure {
            reason: PubNubError::Transport { details: "Test".to_string(), response: None }
        },
        SubscribeState::Unsubscribed;
        "to not change on unexpected event"
    )]
    #[tokio::test]
    async fn transition_for_unsubscribed_state(
        init_state: SubscribeState,
        event: SubscribeEvent,
        target_state: SubscribeState,
    ) {
        let engine = event_engine(init_state.clone());
        assert_eq!(engine.current_state(), init_state);

        // Process event.
        engine.process(&event);

        assert_eq!(engine.current_state(), target_state);
    }

    #[test_case(
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        },
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: None,
        };
        "to handshaking on subscription changed"
    )]
    #[test_case(
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        },
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        };
        "to handshaking with custom cursor on subscription changed"
    )]
    #[test_case(
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        },
        SubscribeEvent::HandshakeFailure {
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            attempts:  1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        };
        "to handshake reconnect on handshake failure"
    )]
    #[test_case(
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        },
        SubscribeEvent::HandshakeFailure {
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            attempts:  1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        };
        "to handshake reconnect with custom cursor on handshake failure"
    )]
    #[test_case(
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()]),
            ),
            cursor: None,
        },
        SubscribeEvent::Disconnect,
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        };
        "to handshake stopped on disconnect"
    )]
    #[test_case(
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        },
        SubscribeEvent::Disconnect,
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        };
        "to handshake stopped with custom cursor on disconnect"
    )]
    #[test_case(
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        },
        SubscribeEvent::HandshakeSuccess {
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 }
        },
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 }
        };
        "to receiving on handshake success"
    )]
    #[test_case(
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        },
        SubscribeEvent::HandshakeSuccess {
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 2 }
        },
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "20".into(), region: 2 }
        };
        "to receiving with custom cursor on handshake success"
    )]
    #[test_case(
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        },
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 1 }),
        };
        "to handshaking on subscription restored"
    )]
    #[test_case(
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        },
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 2 }
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 2 }),
        };
        "to handshaking with custom cursor on subscription restored"
    )]
    #[test_case(
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        },
        SubscribeEvent::HandshakeReconnectGiveUp {
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, }
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        };
        "to not change on unexpected event"
    )]
    #[tokio::test]
    async fn transition_handshaking_state(
        init_state: SubscribeState,
        event: SubscribeEvent,
        target_state: SubscribeState,
    ) {
        let engine = event_engine(init_state.clone());
        assert_eq!(engine.current_state(), init_state);

        engine.process(&event);

        assert_eq!(engine.current_state(), target_state);
    }

    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::HandshakeReconnectFailure {
            reason: PubNubError::Transport { details: "Test reason on error".to_string(), response: None, },
        },
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            attempts: 2,
            reason: PubNubError::Transport { details: "Test reason on error".to_string(), response: None, },
        };
        "to handshake reconnecting on reconnect failure"
    )]
    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::HandshakeReconnectFailure {
            reason: PubNubError::Transport { details: "Test reason on error".to_string(), response: None, },
        },
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            attempts: 2,
            reason: PubNubError::Transport { details: "Test reason on error".to_string(), response: None, },
        };
        "to handshake reconnecting with custom cursor on reconnect failure"
    )]
    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: None,
        };
        "to handshaking on subscription change"
    )]
    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        };
        "to handshaking with custom cursor on subscription change"
    )]
    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::Disconnect,
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        };
        "to handshake stopped on disconnect"
    )]
    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::Disconnect,
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        };
        "to handshake stopped with custom cursor on disconnect"
    )]
    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::HandshakeReconnectGiveUp {
            reason: PubNubError::Transport { details: "Test give up reason".to_string(), response: None, }
        },
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            reason: PubNubError::Transport { details: "Test give up reason".to_string(), response: None, }
        };
        "to handshake failed on give up"
    )]
    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::HandshakeReconnectGiveUp {
            reason: PubNubError::Transport { details: "Test give up reason".to_string(), response: None, }
        },
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            reason: PubNubError::Transport { details: "Test give up reason".to_string(), response: None, }
        };
        "to handshake failed with custom cursor on give up"
    )]
    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::HandshakeReconnectSuccess {
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 }
        },
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 }
        };
        "to receiving on reconnect success"
    )]
    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::HandshakeReconnectSuccess {
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 2 }
        },
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "20".into(), region: 2 }
        };
        "to receiving with custom cursor on reconnect success"
    )]
    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 }
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 1 })
        };
        "to handshaking on subscription restored"
    )]
    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 2 }
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 2 }),
        };
        "to handshaking with custom cursor on subscription restored"
    )]
    #[test_case(
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::ReceiveSuccess {
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            messages: vec![]
        },
        SubscribeState::HandshakeReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        };
        "to not change on unexpected event"
    )]
    #[tokio::test]
    async fn transition_handshake_reconnecting_state(
        init_state: SubscribeState,
        event: SubscribeEvent,
        target_state: SubscribeState,
    ) {
        let engine = event_engine(init_state.clone());
        assert_eq!(engine.current_state(), init_state);

        engine.process(&event);

        assert_eq!(engine.current_state(), target_state);
    }

    #[test_case(
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: None,
        };
        "to handshaking on subscription changed"
    )]
    #[test_case(
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        };
        "to handshaking with custom cursor on subscription changed"
    )]
    #[test_case(
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::Reconnect { cursor: None },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        };
        "to handshaking on reconnect"
    )]
    #[test_case(
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::Reconnect {
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 })
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        };
        "to handshaking on reconnect with custom cursor"
    )]
    #[test_case(
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::Reconnect { cursor: None },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        };
        "to handshaking with custom cursor on reconnect"
    )]
    #[test_case(
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::Reconnect {
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 2 })
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 2 }),
        };
        "to handshaking with custom cursor on reconnect with custom cursor"
    )]
    #[test_case(
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 }
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 1 })
        };
        "to handshaking on subscription restored"
    )]
    #[test_case(
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 2 }
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 2 })
        };
        "to handshaking with custom cursor on subscription restored"
    )]
    #[test_case(
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::UnsubscribeAll,
        SubscribeState::Unsubscribed;
        "to unsubscribed on unsubscribe all"
    )]
    #[test_case(
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        },
        SubscribeEvent::ReceiveSuccess {
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            messages: vec![]
        },
        SubscribeState::HandshakeFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, },
        };
        "to not change on unexpected event"
    )]
    #[tokio::test]
    async fn transition_handshake_failed_state(
        init_state: SubscribeState,
        event: SubscribeEvent,
        target_state: SubscribeState,
    ) {
        let engine = event_engine(init_state.clone());
        assert_eq!(engine.current_state(), init_state);

        engine.process(&event);

        assert_eq!(engine.current_state(), target_state);
    }

    #[test_case(
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        },
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()])
        },
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: None
        };
        "to handshaking stopped on subscription changed"
    )]
    #[test_case(
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        },
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()])
        },
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        };
        "to handshaking stopped with custom cursor on subscription changed"
    )]
    #[test_case(
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        },
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 }
        },
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 1 })
        };
        "to handshaking stopped on subscription restored"
    )]
    #[test_case(
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        },
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 2 }
        },
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 2 }),
        };
        "to handshaking stopped with custom cursor on subscription restored"
    )]
    #[test_case(
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        },
        SubscribeEvent::Reconnect { cursor: None },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        };
        "to handshaking on reconnect"
    )]
    #[test_case(
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        },
        SubscribeEvent::Reconnect {
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 })
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        };
        "to handshaking on reconnect with custom cursor"
    )]
    #[test_case(
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        },
        SubscribeEvent::Reconnect { cursor: None },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        };
        "to handshaking with custom cursor on reconnect"
    )]
    #[test_case(
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        },
        SubscribeEvent::Reconnect {
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 2 })
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 2 }),
        };
        "to handshaking with custom cursor on reconnect with custom cursor"
    )]
    #[test_case(
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 1 }),
        },
        SubscribeEvent::UnsubscribeAll,
        SubscribeState::Unsubscribed;
        "to unsubscribed on unsubscribe all"
    )]
    #[test_case(
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        },
        SubscribeEvent::ReceiveSuccess {
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            messages: vec![]
        },
        SubscribeState::HandshakeStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: None,
        };
        "to not change on unexpected event"
    )]
    #[tokio::test]
    async fn transition_handshake_stopped_state(
        init_state: SubscribeState,
        event: SubscribeEvent,
        target_state: SubscribeState,
    ) {
        let engine = event_engine(init_state.clone());
        assert_eq!(engine.current_state(), init_state);

        engine.process(&event);

        assert_eq!(engine.current_state(), target_state);
    }

    #[test_case(
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
        },
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        };
        "to receiving on subscription changed"
    )]
    #[test_case(
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 2 },
        },
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 2 },
        };
        "to receiving on subscription restored"
    )]
    #[test_case(
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::ReceiveSuccess {
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 2 },
            messages: vec![]
        },
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 2 },
        };
        "to receiving on receive success"
    )]
    #[test_case(
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::ReceiveFailure {
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, }
        },
        SubscribeState::ReceiveReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            attempts: 1,
            reason: PubNubError::Transport { details: "Test reason".to_string(), response: None, }
        };
        "to receive reconnecting on receive failure"
    )]
    #[test_case(
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::Disconnect,
        SubscribeState::ReceiveStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        };
        "to receive stopped on disconnect"
    )]
    #[test_case(
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::UnsubscribeAll,
        SubscribeState::Unsubscribed;
        "to unsubscribed on unsubscribe all"
    )]
    #[test_case(
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::HandshakeSuccess {
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 1 },
        },
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        };
        "to not change on unexpected event"
    )]
    #[tokio::test]
    async fn transition_receiving_state(
        init_state: SubscribeState,
        event: SubscribeEvent,
        target_state: SubscribeState,
    ) {
        let engine = event_engine(init_state.clone());
        assert_eq!(engine.current_state(), init_state);

        engine.process(&event);

        assert_eq!(engine.current_state(), target_state);
    }

    #[test_case(
        SubscribeState::ReceiveReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            attempts: 1,
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::ReceiveReconnectFailure {
            reason: PubNubError::Transport { details: "Test reconnect error".to_string(), response: None, }
        },
        SubscribeState::ReceiveReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            attempts: 2,
            reason: PubNubError::Transport { details: "Test reconnect error".to_string(), response: None, }
        };
        "to receive reconnecting on reconnect failure"
    )]
    #[test_case(
        SubscribeState::ReceiveReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            attempts: 1,
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::ReceiveReconnectSuccess {
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 1 },
            messages: vec![]
        },
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 1 },
        };
        "to receiving on reconnect success"
    )]
    #[test_case(
        SubscribeState::ReceiveReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            attempts: 1,
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
        },
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        };
        "to receiving on subscription changed"
    )]
    #[test_case(
        SubscribeState::ReceiveReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            attempts: 1,
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 1 },
        },
        SubscribeState::Receiving {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 1 },
        };
        "to receiving on subscription restored"
    )]
    #[test_case(
        SubscribeState::ReceiveReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            attempts: 1,
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::Disconnect,
        SubscribeState::ReceiveStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        };
        "to receive stopped on disconnect"
    )]
    #[test_case(
        SubscribeState::ReceiveReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            attempts: 1,
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::ReceiveReconnectGiveUp {
            reason: PubNubError::Transport { details: "Test give up error".to_string(), response: None, }
        },
        SubscribeState::ReceiveFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            reason: PubNubError::Transport { details: "Test give up error".to_string(), response: None, }
        };
        "to receive failed on give up"
    )]
    #[test_case(
        SubscribeState::ReceiveReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            attempts: 1,
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::UnsubscribeAll,
        SubscribeState::Unsubscribed;
        "to unsubscribed on unsubscribe all"
    )]
    #[test_case(
        SubscribeState::ReceiveReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            attempts: 1,
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::HandshakeSuccess {
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 1 },
        },
        SubscribeState::ReceiveReconnecting {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            attempts: 1,
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        };
        "to not change on unexpected event"
    )]
    #[tokio::test]
    async fn transition_receiving_reconnecting_state(
        init_state: SubscribeState,
        event: SubscribeEvent,
        target_state: SubscribeState,
    ) {
        let engine = event_engine(init_state.clone());
        assert_eq!(engine.current_state(), init_state);

        engine.process(&event);

        assert_eq!(engine.current_state(), target_state);
    }

    #[test_case(
        SubscribeState::ReceiveFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 1 }),
        };
        "to handshaking on subscription changed"
    )]
    #[test_case(
        SubscribeState::ReceiveFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 1 },
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "100".into(), region: 1 }),
        };
        "to handshaking on subscription restored"
    )]
    #[test_case(
        SubscribeState::ReceiveFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::Reconnect { cursor: None },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 1 }),
        };
        "to handshaking on reconnect"
    )]
    #[test_case(
        SubscribeState::ReceiveFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::Reconnect {
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 3 })
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 3 }),
        };
        "to handshaking on reconnect with custom cursor"
    )]
    #[test_case(
        SubscribeState::ReceiveFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::UnsubscribeAll,
        SubscribeState::Unsubscribed;
        "to unsubscribed on unsubscribe all"
    )]
    #[test_case(
        SubscribeState::ReceiveFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        },
        SubscribeEvent::HandshakeSuccess {
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 1 }
        },
        SubscribeState::ReceiveFailed {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
            reason: PubNubError::Transport { details: "Test error".to_string(), response: None, }
        };
        "to not change on unexpected event"
    )]
    #[tokio::test]
    async fn transition_receive_failed_state(
        init_state: SubscribeState,
        event: SubscribeEvent,
        target_state: SubscribeState,
    ) {
        let engine = event_engine(init_state.clone());
        assert_eq!(engine.current_state(), init_state);

        engine.process(&event);

        assert_eq!(engine.current_state(), target_state);
    }

    #[test_case(
        SubscribeState::ReceiveStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::SubscriptionChanged {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
        },
        SubscribeState::ReceiveStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        };
        "to receive stopped on subscription changed"
    )]
    #[test_case(
        SubscribeState::ReceiveStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::SubscriptionRestored {
            channels: Some(vec!["ch2".to_string()]),
            channel_groups: Some(vec!["gr2".to_string()]),
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 1 },
        },
        SubscribeState::ReceiveStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch2".to_string()]),
                &Some(vec!["gr2".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 1 },
        };
        "to receive stopped on subscription restored"
    )]
    #[test_case(
        SubscribeState::ReceiveStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::Reconnect { cursor: None },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "10".into(), region: 1 }),
        };
        "to handshaking on reconnect"
    )]
    #[test_case(
        SubscribeState::ReceiveStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::Reconnect {
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 3 })
        },
        SubscribeState::Handshaking {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: Some(SubscriptionCursor { timetoken: "20".into(), region: 3 }),
        };
        "to handshaking on reconnect with custom cursor"
    )]
    #[test_case(
        SubscribeState::ReceiveStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::UnsubscribeAll,
        SubscribeState::Unsubscribed;
        "to unsubscribed on unsubscribe all"
    )]
    #[test_case(
        SubscribeState::ReceiveStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        },
        SubscribeEvent::HandshakeSuccess {
            cursor: SubscriptionCursor { timetoken: "100".into(), region: 1 }
        },
        SubscribeState::ReceiveStopped {
            input: SubscriptionInput::new(
                &Some(vec!["ch1".to_string()]),
                &Some(vec!["gr1".to_string()])
            ),
            cursor: SubscriptionCursor { timetoken: "10".into(), region: 1 },
        };
        "to not change on unexpected event"
    )]
    #[tokio::test]
    async fn transition_receive_stopped_state(
        init_state: SubscribeState,
        event: SubscribeEvent,
        target_state: SubscribeState,
    ) {
        let engine = event_engine(init_state.clone());
        assert_eq!(engine.current_state(), init_state);

        engine.process(&event);

        assert_eq!(engine.current_state(), target_state);
    }
}
