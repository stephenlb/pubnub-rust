use crate::{
    core::event_engine::EffectHandler,
    dx::subscribe::event_engine::{
        effects::{
            EmitMessagesEffectExecutor, EmitStatusEffectExecutor, HandshakeEffectExecutor,
            ReceiveEffectExecutor,
        },
        SubscribeEffect, SubscribeEffectInvocation,
    },
    lib::core::sync::Arc,
};

/// Subscription effect handler.
///
/// Handler responsible for effects implementation and creation in response on
/// effect invocation.
#[allow(dead_code)]
pub(crate) struct SubscribeEffectHandler {
    /// Handshake function pointer.
    handshake: Arc<Box<HandshakeEffectExecutor>>,

    /// Receive updates function pointer.
    receive: Arc<Box<ReceiveEffectExecutor>>,

    /// Emit status function pointer.
    emit_status: Arc<Box<EmitStatusEffectExecutor>>,

    /// Emit messages function pointer.
    emit_messages: Arc<Box<EmitMessagesEffectExecutor>>,
}

impl<'client> SubscribeEffectHandler {
    /// Create subscribe event handler.
    #[allow(dead_code)]
    pub fn new(
        handshake: Arc<Box<HandshakeEffectExecutor>>,
        receive: Arc<Box<ReceiveEffectExecutor>>,
        emit_status: Arc<Box<EmitStatusEffectExecutor>>,
        emit_messages: Arc<Box<EmitMessagesEffectExecutor>>,
    ) -> Self {
        SubscribeEffectHandler {
            handshake,
            receive,
            emit_status,
            emit_messages,
        }
    }
}

impl EffectHandler<SubscribeEffectInvocation, SubscribeEffect> for SubscribeEffectHandler {
    fn create(&self, invocation: &SubscribeEffectInvocation) -> Option<SubscribeEffect> {
        match invocation {
            SubscribeEffectInvocation::Handshake {
                channels,
                channel_groups,
            } => Some(SubscribeEffect::Handshake {
                channels: channels.clone(),
                channel_groups: channel_groups.clone(),
                executor: self.handshake.clone(),
            }),
            SubscribeEffectInvocation::HandshakeReconnect {
                channels,
                channel_groups,
                attempts,
                reason,
            } => Some(SubscribeEffect::HandshakeReconnect {
                channels: channels.clone(),
                channel_groups: channel_groups.clone(),
                attempts: *attempts,
                reason: reason.clone(),
                executor: self.handshake.clone(),
            }),
            SubscribeEffectInvocation::Receive {
                channels,
                channel_groups,
                cursor,
            } => Some(SubscribeEffect::Receive {
                channels: channels.clone(),
                channel_groups: channel_groups.clone(),
                cursor: cursor.clone(),
                executor: self.receive.clone(),
            }),
            SubscribeEffectInvocation::ReceiveReconnect {
                channels,
                channel_groups,
                cursor,
                attempts,
                reason,
            } => Some(SubscribeEffect::ReceiveReconnect {
                channels: channels.clone(),
                channel_groups: channel_groups.clone(),
                cursor: cursor.clone(),
                attempts: *attempts,
                reason: reason.clone(),
                executor: self.receive.clone(),
            }),
            SubscribeEffectInvocation::EmitStatus(status) => {
                // TODO: Provide emit status effect
                Some(SubscribeEffect::EmitStatus(*status))
            }
            SubscribeEffectInvocation::EmitMessages(messages) => {
                // TODO: Provide emit messages effect
                Some(SubscribeEffect::EmitMessages(messages.clone()))
            }
            _ => None,
        }
    }
}
