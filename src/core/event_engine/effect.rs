use crate::{
    core::event_engine::EffectInvocation,
    lib::alloc::{string::String, vec::Vec},
};

pub(crate) trait Effect {
    type Invocation: EffectInvocation;

    /// Unique effect identifier.
    fn id(&self) -> String;

    /// Run work associated with effect.
    fn run<F>(&self, f: F)
    where
        F: FnMut(Option<Vec<<Self::Invocation as EffectInvocation>::Event>>);

    /// Cancel any ongoing effect's work.
    fn cancel(&self);
}
