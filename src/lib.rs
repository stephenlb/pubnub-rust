#![warn(missing_docs)]
#![cfg_attr(not(any(feature = "std", test)), no_std)]

//! # PubNub Rust SDK
//!
//! <div align = "center">
//!
//! ![PubNub](https://raw.githubusercontent.com/pubnub/rust/master/logo.svg)
//!
//! ![Tests](https://github.com/pubnub/rust/actions/workflows/run-tests.yml/badge.svg)
//! ![Validations](https://github.com/pubnub/rust/actions/workflows/run-validations.yml/badge.svg)
//! [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/pubnub/rust/LICENSE)
//!
//! **Make your app come alive with real-time experiences!**
//!
//! </div>
//!
//! ## Overview
//!
//! This is the official PubNub Rust SDK repository.
//!
//! [PubNub](https://www.pubnub.com/) takes care of the infrastructure and APIs needed for the realtime
//! communication layer of your application. Work on your app's logic and let
//! PubNub handle sending and receiving data across the world in less than
//! 100ms.
//!
//! ## Getting started
//!
//! Below you can find everything you need to start messaging!
//!
//! ### Get PubNub keys
//!
//! You will need the publish and subscribe keys to authenticate your app. Get your keys from the [Admin Portal](https://dashboard.pubnub.com/login).
//!
//! ### Import using [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html)
//!
//! Add `pubnub` to your Rust project in the `Cargo.toml` file:
//!
//! ```toml
//! # default features
//! [dependencies]
//! pubnub = "0.6.0"
//!
//! # all features
//! [dependencies]
//! pubnub = { version = "0.6.0", features = ["full"] }
//! ```
//!
//! ### Example
//!
//! Try the following sample code to get up and running quickly!
//!
//! ```no_run
//! use pubnub::subscribe::Subscriber;
//! use futures::StreamExt;
//! use tokio::time::sleep;
//! use std::time::Duration;
//! use serde_json;
//! use pubnub::{
//!     dx::subscribe::Update,
//!     subscribe::EventSubscriber,
//!     Keyset, PubNubClientBuilder,
//! };
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     use pubnub::subscribe::{EventEmitter, SubscriptionParams};
//!     let publish_key = "my_publish_key";
//!     let subscribe_key = "my_subscribe_key";
//!     let client = PubNubClientBuilder::with_reqwest_transport()
//!         .with_keyset(Keyset {
//!             subscribe_key,
//!             publish_key: Some(publish_key),
//!             secret_key: None,
//!         })
//!         .with_user_id("user_id")
//!         .build()?;
//!
//!     println!("PubNub instance created");
//!
//!     let subscription = client.subscription(SubscriptionParams {
//!         channels: Some(&["my_channel"]),
//!         channel_groups: None,
//!         options: None
//!     });
//!
//!     let channel_entity = client.channel("my_channel_2");
//!     let channel_entity_subscription = channel_entity.subscription(None);
//!
//!     subscription.subscribe();
//!     channel_entity_subscription.subscribe();
//!
//!     println!("Subscribed to channels");
//!
//!     // Launch a new task to print out each received message
//!     tokio::spawn(client.status_stream().for_each(|status| async move {
//!         println!("\nStatus: {:?}", status)
//!     }));
//!     tokio::spawn(subscription.stream().for_each(|event| async move {
//!         match event {
//!             Update::Message(message) | Update::Signal(message) => {
//!                 // Silently log if UTF-8 conversion fails
//!                 if let Ok(utf8_message) = String::from_utf8(message.data.clone()) {
//!                     if let Ok(cleaned) = serde_json::from_str::<String>(&utf8_message) {
//!                         println!("message: {}", cleaned);
//!                     }
//!                 }
//!             }
//!             Update::Presence(presence) => {
//!                 println!("presence: {:?}", presence)
//!             }
//!             Update::AppContext(object) => {
//!                 println!("object: {:?}", object)
//!             }
//!             Update::MessageAction(action) => {
//!                 println!("message action: {:?}", action)
//!             }
//!             Update::File(file) => {
//!                 println!("file: {:?}", file)
//!             }
//!         }
//!     }));
//!
//!     // Explicitly listen only for real-time `message` updates.
//!     tokio::spawn(
//!         channel_entity_subscription
//!             .messages_stream()
//!             .for_each(|message| async move {
//!                 if let Ok(utf8_message) = String::from_utf8(message.data.clone()) {
//!                     if let Ok(cleaned) = serde_json::from_str::<String>(&utf8_message) {
//!                         println!("message: {}", cleaned);
//!                     }
//!                 }
//!             }),
//!     );
//!
//!    sleep(Duration::from_secs(2)).await;
//!    
//!     // Send a message to the channel
//!     client
//!         .publish_message("hello world!")
//!         .channel("my_channel")
//!         .r#type("text-message")
//!         .execute()
//!         .await?;
//!
//!    // Send a message to another channel
//!     client
//!         .publish_message("hello world on the other channel!")
//!         .channel("my_channel_2")
//!         .r#type("text-message")
//!         .execute()
//!         .await?;
//!
//!     sleep(Duration::from_secs(15)).await;
//!
//!     Ok(())
//! }
//! ```
//!
//! You can find more examples in our [examples](https://github.com/pubnub/rust/tree/master/examples) directory!
//!
//! ## Features
//!
//! The `pubnub` crate is split into multiple features. You can enable or
//! disable them in the `Cargo.toml` file, like so:
//!
//! ```toml
//! # only blocking and access + default features
//! [dependencies]
//! pubnub = { version = "0.6.0", features = ["blocking", "access"] }
//!
//! # only parse_token + default features
//! [dependencies]
//! pubnub = { version = "0.6.0", features = ["parse_token"] }
//! ```
//!
//! ### Available features
//!
//! | Feature name  | Description | Available PubNub APIs |
//! | :------------ | :---------- | :------------- |
//! | `full`        | Enables all non-conflicting features | Configuration, Publish, Subscribe, Access Manager, Parse Token, Presence, Crypto Module |
//! | `default`     | Enables default features: `publish`, `subscribe`, `serde`, `reqwest`, `std` | Configuration, Publish, Subscribe |
//! | `publish`     | Enables Publish API | Configuration, Publish |
//! | `access`      | Enables Access Manager API | Configuration, Access Manager |
//! | `parse_token` | Enables parsing Access Manager tokens | Configuration, Parse Token |
//! | `subscribe`   | Enables Subscribe API | Configuration, Subscribe |
//! | `presence`    | Enables Presence API | Configuration, Presence |
//! | `tokio`       | Enables the [tokio](https://tokio.rs/) asynchronous runtime for Subscribe and Presence APIs | n/a  |
//! | `serde`       | Uses [serde](https://github.com/serde-rs/serde) for serialization | n/a |
//! | `reqwest`     | Uses [reqwest](https://github.com/seanmonstar/reqwest) as a transport layer | n/a |
//! | `blocking`    | Enables blocking executions of APIs | n/a |
//! | `crypto`      | Enables crypto module for data encryption and decryption | n/a |
//! | `std`         | Enables `std` library | n/a |
//!
//! ## Documentation
//!
//! * [API reference for Rust](https://www.pubnub.com/docs/sdks/rust)
//! * [Rust docs](https://www.docs.rs/pubnub/latest/pubnub)
//!
//! ## Wasm support
//!
//! The `pubnub` crate is compatible with WebAssembly. You can use it in your
//! Wasm project.
//!
//! ## `no_std` support
//!
//! The `pubnub` crate is `no_std` compatible. To use it in a `no_std`
//! environment, you have to disable the default features and enable the ones
//! you need, for example:
//!
//! ```toml
//! [dependencies]
//! pubnub = { version = "0.6.0", default-features = false, features = ["serde", "publish",
//! "blocking"] }
//! ```
//!
//! ### Limitations
//!
//! The `no_std` support is limited by the implementation details of the SDK.
//!
//! The SDK uses the `alloc` crate to allocate memory for some operations, which
//! means that certain targets aren't supported. Additionally, as we provide a
//! synchronous API, we use some parts of the `alloc::sync` module, which is
//! also not supported in certain `no_std` environments.
//!
//! Some SDK features aren't supported in a `no_std` environment:
//!
//! * partially `access` module (because of lack of timestamp support)
//! * partially `reqwest` transport (because of the reqwest implementation
//!   details)
//! * partially `subscribe` module (because of the spawning tasks and time
//!   dependence)
//! * partially `presence` module (because of the spawning tasks and time
//!   dependence)
//! * `std` feature (because of the `std` library)
//!
//! We depend on a random number generator to generate data for debugging
//! purposes. If you want to use the SDK in a `no_std` environment, you'll have
//! to provide your own random number generator implementation for certain
//! targets.
//!
//! See more:
//!
//! * [`getrandom` crate](https://docs.rs/getrandom/latest/getrandom/)
//! * [no_std examples](https://github.com/pubnub/rust/tree/master/examples/no_std/)
//!
//! If you're having problems compiling this crate for more exotic targets, you
//! can try to use the `extra_platforms` feature. Be aware that this feature is
//! **not supported** and we do not recommend using it.
//!
//! For more information about this feature. refer to [Cargo.toml](https://github.com/pubnub/rust/blob/master/Cargo.toml) in the `[features]` section.
//!
//! ## Support
//!
//! If you **need help** or have a **general question**, contact
//! support@pubnub.com.
//!
//! ## License
//!
//! This project is licensed under the [PubNub Software Development Kit license](https://github.com/pubnub/rust/blob/master/LICENSE).
//! You can read more about it [here](https://www.pubnub.com/docs/sdks#license-faq).

#[cfg(feature = "access")]
#[doc(inline)]
pub use dx::access;

#[doc(inline)]
#[cfg(feature = "parse_token")]
pub use dx::{parse_token, Token};

#[cfg(feature = "publish")]
#[doc(inline)]
pub use dx::publish;

#[cfg(feature = "subscribe")]
#[doc(inline)]
pub use dx::subscribe;

#[cfg(feature = "presence")]
#[doc(inline)]
pub use dx::presence;

#[doc(inline)]
pub use dx::{Keyset, PubNubClientBuilder, PubNubGenericClient};

#[cfg(feature = "reqwest")]
#[doc(inline)]
pub use dx::PubNubClient;

#[cfg(feature = "std")]
#[doc(inline)]
pub use core::RequestRetryConfiguration;

#[doc(inline)]
pub use core::{Channel, ChannelGroup, ChannelMetadata, UserMetadata};
pub mod core;
pub mod dx;
pub mod providers;
pub mod transport;

/// A facade around all the std types that we use in the library.
/// It is used to make the library `no_std` compatible.
mod lib {
    #[cfg(not(feature = "std"))]
    extern crate alloc as std_alloc;

    pub(crate) mod core {
        #[cfg(not(feature = "std"))]
        pub(crate) use core::*;
        #[cfg(feature = "std")]
        pub(crate) use std::*;
    }

    pub(crate) mod alloc {
        #[cfg(not(feature = "std"))]
        pub(crate) use super::std_alloc::*;

        #[cfg(feature = "std")]
        pub(crate) use std::*;
    }

    pub(crate) mod collections {
        pub(crate) use hash_map::HashMap;

        pub(crate) mod hash_map {
            /// Depending of the `std` feature, this module will re-export
            /// either `std::collections::HashMap` or `hashbrown::HashMap`.
            /// This is needed because there is no `no_std` HashMap available.
            /// We decided to use `hashbrown` because it is fast and has the
            /// same API as `std` HashMap.

            #[cfg(not(feature = "std"))]
            pub(crate) use hashbrown::HashMap;

            #[cfg(feature = "std")]
            pub(crate) use std::collections::HashMap;
        }
    }
}

// Mocking random for checking if `no_std` compiles.
// Don't use that feature in production.
#[cfg(feature = "mock_getrandom")]
mod mock_getrandom {
    use getrandom::{register_custom_getrandom, Error};

    pub fn do_nothing(_buf: &mut [u8]) -> Result<(), Error> {
        Ok(())
    }

    register_custom_getrandom!(do_nothing);
}
