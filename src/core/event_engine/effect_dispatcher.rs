use crate::{
    core::{
        event_engine::{Effect, EffectHandler, EffectInvocation},
        runtime::Runtime,
    },
    lib::alloc::{string::String, sync::Arc, vec, vec::Vec},
};
use async_channel::Receiver;
use spin::rwlock::RwLock;

/// State machine effects dispatcher.
#[derive(Debug)]
pub(crate) struct EffectDispatcher<EH, EF, EI>
where
    EI: EffectInvocation<Effect = EF> + Send + Sync,
    EH: EffectHandler<EI, EF>,
    EF: Effect<Invocation = EI>,
{
    /// Effect invocation handler.
    ///
    /// Handler responsible for providing actual implementation of
    handler: EH,

    /// Dispatched effects managed by dispatcher.
    ///
    /// There are effects whose lifetime should be managed by the dispatcher.
    /// State machines may have some effects that are exclusive and can only run
    /// one type of them at once. The dispatcher handles such effects
    /// and cancels them when required.
    managed: Arc<RwLock<Vec<Arc<EF>>>>,

    /// `Effect invocation` handling channel.
    ///
    /// Channel is used to receive submitted `invocations` for new effects
    /// execution.
    invocations_channel: Receiver<EI>,

    /// Whether dispatcher already started or not.
    started: RwLock<bool>,
}

impl<EH, EF, EI> EffectDispatcher<EH, EF, EI>
where
    EI: EffectInvocation<Effect = EF> + Send + Sync + 'static,
    EH: EffectHandler<EI, EF> + Send + Sync + 'static,
    EF: Effect<Invocation = EI> + 'static,
{
    /// Create new an effects dispatcher.
    pub fn new(handler: EH, channel: Receiver<EI>) -> Self {
        EffectDispatcher {
            handler,
            managed: Arc::new(RwLock::new(vec![])),
            invocations_channel: channel,
            started: RwLock::new(false),
        }
    }

    /// Prepare dispatcher for `invocations` processing.
    pub fn start<C, R>(self: &Arc<Self>, completion: C, runtime: R)
    where
        R: Runtime + 'static,
        C: Fn(Vec<<EI as EffectInvocation>::Event>) + Clone + Send + 'static,
    {
        let runtime_clone = runtime.clone();
        let cloned_self = self.clone();

        runtime.spawn(async move {
            log::info!("Event engine has started!");

            loop {
                let invocation = cloned_self.invocations_channel.recv().await;
                match invocation {
                    Ok(invocation) => {
                        if invocation.is_terminating() {
                            log::debug!("Received event engine termination invocation");
                            break;
                        }

                        log::debug!("Received invocation: {}", invocation.id());
                        let effect = cloned_self.dispatch(&invocation);
                        let task_completion = completion.clone();

                        if let Some(effect) = effect {
                            log::debug!("Dispatched effect: {}", effect.name());
                            let cloned_self = cloned_self.clone();

                            runtime_clone.spawn(async move {
                                // There is no need to spawn effect which already has been
                                // cancelled.
                                if effect.is_cancelled() {
                                    return;
                                }
                                let events = effect.run().await;

                                if invocation.is_managed() {
                                    cloned_self.remove_managed_effect(effect.id());
                                }

                                task_completion(events);
                            });
                        } else if invocation.is_cancelling() {
                            log::debug!("Dispatched effect: {}", invocation.id());
                        }
                    }
                    Err(err) => {
                        log::error!("Receive error: {err:?}");
                        break;
                    }
                }
            }
            *cloned_self.started.write() = false;
            log::info!("Event engine has stopped!");
        });

        *self.started.write() = true;
    }

    /// Dispatch effect associated with `invocation`.
    pub fn dispatch(&self, invocation: &EI) -> Option<Arc<EF>> {
        if let Some(effect) = self.handler.create(invocation) {
            let effect = Arc::new(effect);

            if invocation.is_managed() {
                let mut managed = self.managed.write();
                managed.push(effect.clone());
            }

            Some(effect)
        } else {
            if invocation.is_cancelling() {
                self.cancel_effect(invocation);
            }

            None
        }
    }

    /// Handle effect cancellation.
    ///
    /// Effects with managed lifecycle can be cancelled by corresponding effect
    /// invocations.
    fn cancel_effect(&self, invocation: &EI) {
        let mut managed = self.managed.write();
        if let Some(position) = managed.iter().position(|e| invocation.cancelling_effect(e)) {
            managed.remove(position).cancel();
        }
    }

    /// Remove managed effect.
    fn remove_managed_effect(&self, effect_id: String) {
        let mut managed = self.managed.write();
        if let Some(position) = managed.iter().position(|ef| ef.id() == effect_id) {
            managed.remove(position);
        }
    }
}

#[cfg(test)]
mod should {
    use super::*;
    use crate::core::event_engine::Event;

    struct TestEvent;

    impl Event for TestEvent {
        fn id(&self) -> &str {
            "no_id"
        }
    }

    enum TestEffect {
        One,
        Two,
        Three,
    }

    #[async_trait::async_trait]
    impl Effect for TestEffect {
        type Invocation = TestInvocation;

        fn name(&self) -> String {
            match self {
                Self::One => "EFFECT_ONE".into(),
                Self::Two => "EFFECT_TWO".into(),
                Self::Three => "EFFECT_THREE".into(),
            }
        }

        fn id(&self) -> String {
            match self {
                Self::One => "EFFECT_ONE".into(),
                Self::Two => "EFFECT_TWO".into(),
                Self::Three => "EFFECT_THREE".into(),
            }
        }

        async fn run(&self) -> Vec<TestEvent> {
            vec![]
        }

        fn cancel(&self) {
            // Do nothing.
        }

        fn is_cancelled(&self) -> bool {
            false
        }
    }

    enum TestInvocation {
        One,
        Two,
        Three,
        CancelThree,
    }

    impl EffectInvocation for TestInvocation {
        type Effect = TestEffect;
        type Event = TestEvent;

        fn id(&self) -> &str {
            match self {
                Self::One => "EFFECT_ONE_INVOCATION",
                Self::Two => "EFFECT_TWO_INVOCATION",
                Self::Three => "EFFECT_THREE_INVOCATION",
                Self::CancelThree => "EFFECT_THREE_CANCEL_INVOCATION",
            }
        }

        fn is_managed(&self) -> bool {
            matches!(self, Self::Two | Self::Three)
        }

        fn is_cancelling(&self) -> bool {
            matches!(self, Self::CancelThree)
        }

        fn cancelling_effect(&self, effect: &Self::Effect) -> bool {
            match self {
                TestInvocation::CancelThree => matches!(effect, TestEffect::Three),
                _ => false,
            }
        }

        fn is_terminating(&self) -> bool {
            false
        }
    }

    struct TestEffectHandler {}

    impl EffectHandler<TestInvocation, TestEffect> for TestEffectHandler {
        fn create(&self, invocation: &TestInvocation) -> Option<TestEffect> {
            match invocation {
                TestInvocation::One => Some(TestEffect::One),
                TestInvocation::Two => Some(TestEffect::Two),
                TestInvocation::Three => Some(TestEffect::Three),
                _ => None,
            }
        }
    }

    #[test]
    fn create_not_managed_effect() {
        let (_tx, rx) = async_channel::bounded::<TestInvocation>(5);
        let dispatcher = Arc::new(EffectDispatcher::new(TestEffectHandler {}, rx));
        let effect = dispatcher.dispatch(&TestInvocation::One);

        assert_eq!(
            dispatcher.managed.read().len(),
            0,
            "Non managed effects shouldn't be stored"
        );

        assert_eq!(effect.unwrap().id(), "EFFECT_ONE");
    }

    #[tokio::test]
    async fn create_managed_effect() {
        let (_tx, rx) = async_channel::bounded::<TestInvocation>(5);
        let dispatcher = Arc::new(EffectDispatcher::new(TestEffectHandler {}, rx));
        let effect = dispatcher.dispatch(&TestInvocation::Two);

        assert_eq!(
            dispatcher.managed.read().len(),
            1,
            "Managed effect should be removed on completion"
        );

        assert_eq!(effect.unwrap().id(), "EFFECT_TWO");
    }

    #[test]
    fn cancel_managed_effect() {
        let (_tx, rx) = async_channel::bounded::<TestInvocation>(5);
        let dispatcher = Arc::new(EffectDispatcher::new(TestEffectHandler {}, rx));
        dispatcher.dispatch(&TestInvocation::Three);
        let cancellation_effect = dispatcher.dispatch(&TestInvocation::CancelThree);

        assert_eq!(
            dispatcher.managed.read().len(),
            0,
            "Managed effect should be cancelled"
        );

        assert!(cancellation_effect.is_none());
    }
}
