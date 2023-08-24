//! Heartbeat effect module.

use crate::{
    core::{
        event_engine::{Effect, EffectInvocation},
        PubNubError, RequestRetryPolicy,
    },
    lib::{
        alloc::{string::String, sync::Arc, vec::Vec},
        core::fmt::{Debug, Formatter},
    },
    presence::{
        event_engine::{
            types::{PresenceInput, PresenceParameters},
            PresenceEffectInvocation,
        },
        HeartbeatResult,
    },
};

use crate::presence::result::LeaveResult;
use async_channel::Sender;
use futures::future::BoxFuture;

mod heartbeat;
mod leave;
mod wait;

pub(in crate::dx::presence) type PresenceEffectExecutor = dyn Fn(PresenceParameters) -> BoxFuture<'static, Result<HeartbeatResult, PubNubError>>
    + Send
    + Sync;

pub(in crate::dx::presence) type WaitEffectExecutor =
    dyn Fn(&str) -> BoxFuture<'static, Result<(), PubNubError>> + Send + Sync;

pub(in crate::dx::presence) type LeaveEffectExecutor = dyn Fn(PresenceParameters) -> BoxFuture<'static, Result<LeaveResult, PubNubError>>
    + Send
    + Sync;

#[allow(dead_code)]
pub(crate) enum PresenceEffect {
    /// Heartbeat effect invocation.
    Heartbeat {
        /// User input with channels and groups.
        ///
        /// Object contains list of channels and groups for which `user_id`
        /// presence should be announced.
        input: PresenceInput,

        /// Executor function.
        ///
        /// Function which will be used to execute heartbeat.
        executor: Arc<PresenceEffectExecutor>,
    },

    /// Delayed heartbeat effect invocation.
    DelayedHeartbeat {
        /// User input with channels and groups.
        ///
        /// Object contains list of channels and groups for which `user_id`
        /// presence should be announced.
        input: PresenceInput,

        /// Current heartbeat retry attempt.
        ///
        /// Used to track overall number of heartbeat retry attempts.
        attempts: u8,

        /// Heartbeat attempt failure reason.
        reason: PubNubError,

        /// Retry policy.
        retry_policy: RequestRetryPolicy,

        /// Executor function.
        ///
        /// Function which will be used to execute heartbeat.
        executor: Arc<PresenceEffectExecutor>,

        /// Cancellation channel.
        ///
        /// Channel which will be used to cancel effect execution.
        cancellation_channel: Sender<String>,
    },

    /// Leave effect invocation.
    Leave {
        /// User input with channels and groups.
        ///
        /// Object contains list of channels and groups for which `user_id`
        /// should leave.
        input: PresenceInput,

        /// Executor function.
        ///
        /// Function which will be used to execute leave.
        executor: Arc<LeaveEffectExecutor>,
    },

    /// Delay effect invocation.
    Wait {
        /// User input with channels and groups.
        ///
        /// Object contains list of channels and groups for which `user_id`
        /// presence should be announced after delay.
        input: PresenceInput,

        /// Cancellation channel.
        ///
        /// Channel which will be used to cancel effect execution.
        cancellation_channel: Sender<String>,

        /// Executor function.
        ///
        /// Function which will be used to execute wait.
        executor: Arc<WaitEffectExecutor>,
    },
}

impl Debug for PresenceEffect {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Heartbeat { input, .. } => write!(
                f,
                "PresenceEffect::Heartbeat {{ channels: {:?}, channel groups: \
                {:?}}}",
                input.channels, input.channel_groups
            ),
            Self::DelayedHeartbeat { input, .. } => write!(
                f,
                "PresenceEffect::DelayedHeartbeat {{ channels: {:?}, channel groups: \
                {:?}}}",
                input.channels, input.channel_groups
            ),
            Self::Leave { input, .. } => write!(
                f,
                "PresenceEffect::Leave {{ channels: {:?}, channel groups: \
                {:?}}}",
                input.channels, input.channel_groups
            ),
            Self::Wait { input, .. } => write!(
                f,
                "PresenceEffect::Wait {{ channels: {:?}, channel groups: \
                {:?}}}",
                input.channels, input.channel_groups
            ),
        }
    }
}

#[async_trait::async_trait]
impl Effect for PresenceEffect {
    type Invocation = PresenceEffectInvocation;

    fn id(&self) -> String {
        match self {
            Self::Heartbeat { .. } => "HEARTBEAT",
            Self::DelayedHeartbeat { .. } => "DELAYED_HEARTBEAT",
            Self::Leave { .. } => "LEAVE",
            Self::Wait { .. } => "WAIT",
        }
        .into()
    }

    async fn run(&self) -> Vec<<Self::Invocation as EffectInvocation>::Event> {
        match self {
            Self::Heartbeat { input, executor } => {
                heartbeat::execute(input, 0, None, &self.id(), &None, executor).await
            }
            Self::DelayedHeartbeat {
                input,
                attempts,
                reason,
                retry_policy,
                executor,
                ..
            } => {
                heartbeat::execute(
                    input,
                    *attempts,
                    Some(reason.clone()),
                    &self.id(),
                    &Some(retry_policy.clone()),
                    executor,
                )
                .await
            }
            Self::Leave { .. } => {
                // TODO: Add leave effect call
                vec![]
            }
            Self::Wait { executor, .. } => wait::execute(&self.id(), executor).await,
        }
    }

    fn cancel(&self) {
        match self {
            PresenceEffect::DelayedHeartbeat {
                cancellation_channel,
                ..
            }
            | PresenceEffect::Wait {
                cancellation_channel,
                ..
            } => {
                cancellation_channel
                    .send_blocking(self.id())
                    .expect("Cancellation pipe is broken!");
            }
            _ => { /* cannot cancel other effects */ }
        }
    }
}
