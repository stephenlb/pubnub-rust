extern crate hyper;
extern crate uuid;

//use log::debug; // debug!(...);
use thiserror::Error;
//use uuid::Uuid;
//use std::io::{self, Write};
use tokio::sync::mpsc;
//use std::thread;
//use std::sync::mpsc;
//use hyper::rt::{self, Future, Stream};
use json::JsonValue;
//use std::collections::HashMap;

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
// Error Enumerator
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#[derive(Debug, Error)]
pub enum Error {
    #[error("Publish MPSC Channel write error")]
    PublishChannelWrite(#[source] mpsc::error::TrySendError<PublishMessage>),
    #[error("Publish Socket write error")]
    PublishSocketWrite(#[source] Box<Error>),
    // #[error("Subscribe write error")]
    // SubscribeWrite(#[source] Box<Error>),
}

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
/// # PubNub Message Types
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
pub enum MessageType {
    Publish,     // Response of Publish (Success/Fail)
    Subscribe,   // Response of Subscription ( Usually a Message Payload )
    Presence,    // Presence Event from Channel ( Another Client Joined )
}

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
/// # PubNub Message
///
/// This is the message structure that includes all known information on the
/// message received via `pubnub.next()`.
///
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
pub struct Message {
    pub message_type : MessageType, // Enum Type of Message
    pub channel      : String,      // Origin Channel of Message Receipt
    pub data         : String,      // Payload from Channel
    pub json         : String,      // Decoded JSON Payload from Channel
    pub metadata     : String,      // Metadata of Message
    pub timetoken    : String,      // Message ID Timetoken
    pub success      : bool,        // Useful to see if Publish was Successful
}


// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
/// # PubNub Publish Message
///
/// This is the message structure that includes information needed to publish
/// a message to the PubNub Edge Messaging Network.
///
/// ```
/// use pubnub::{PubNub, Client};
/// 
/// let mut pubnub = PubNub::new().origin("ps.pndsn.com:443").agent("Rust");
/// let mut client = Client::new()
///     .subscribe_key("demo")
///     .publish_key("demo");
///
/// pubnub.add(&client);
///
/// /* TODO
/// let message = pubnub.message().channel("demo").data("Hi!");
/// pubnub.publish(message);*/
/// ```
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#[derive(Debug, Clone)]
pub struct PublishMessage {
    pub channel  : String, // Destination Channel
    pub data     : String, // Message Payload ( JSON )
    pub metadata : String, // Metadata for Message ( JSON )
}

impl PublishMessage {
    pub fn channel(mut self, channel: &str) -> PublishMessage {
        self.channel = channel.to_string();
        self
    }

    pub fn data(mut self, data: &str) -> PublishMessage {
        self.data = data.to_string();
        self
    }

    pub fn json(mut self, data: JsonValue) -> PublishMessage {
        self.data = json::stringify(data);
        self
    }

    pub fn metadata(mut self, metadata: &str) -> PublishMessage {
        self.metadata = metadata.to_string();
        self
    }

    // Add PublishMessage to the publish stream.
    pub fn publish(self, pubnub: &mut PubNub) -> Result<(), Error> {
        match pubnub.submit_publish.try_send(self) {
            Ok(())     => Ok(()),
            Err(error) => Err(Error::PublishChannelWrite(error)),
        }
    }
}

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
/// # PubNub Client
///
/// This is the structure that is used to add and remove client connections
/// for channels and channel groups using additional parameters for filtering.
/// The `userID` is the same as the UUID used in PubNub SDKs.
///
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#[derive(Debug, Clone)]
pub struct Client {
    pub publish_key    : String, // Customer's Publish Key
    pub subscribe_key  : String, // Customer's Subscribe Key
    pub secret_key     : String, // Customer's Secret Key
    pub auth_key       : String, // Client Auth Key for R+W Access
    pub user_id        : String, // Client UserId "UUID" for Presence
    pub channels       : String, // Client Channels Comma Separated
    pub groups         : String, // Client Channel Groups Comma Sepparated
    pub filters        : String, // Metadata Filters on Messages
    pub presence       : bool,   // Enable presence events
    pub json           : bool,   // Enable JSON Decoding
    pub since          : u64,    // Unix Timestamp Fetch History + Subscribe
    pub timetoken      : String, // Current Line-in-Sand for Subscription
}

impl Client {
    pub fn new() -> Client {
        // TODO Start mpsc threads NO - can not have 1 thread per client...
        // Maybe we can have it dedicated to a PubNub pool...
        // AH!! Gets a clone() of the mpsc sender for PubNub for publishing.

        // TODO
        //let default_user_id = Uuid::new_v4().hyphenated();

        Client {
            subscribe_key  : "demo".to_string(),
            publish_key    : "demo".to_string(),
            secret_key     : "".to_string(),
            auth_key       : "".to_string(),
            user_id        : "".to_string(),
            channels       : "demo".to_string(),
            groups         : "".to_string(),
            filters        : "".to_string(),
            presence       : false,
            json           : false,
            since          : 0,
            timetoken      : "0".to_string(),
        }
    }

    pub fn subscribe_key(mut self, subscribe_key: &str) -> Client {
        self.subscribe_key = subscribe_key.to_string();
        self
    }

    pub fn publish_key(mut self, publish_key: &str) -> Client {
        self.publish_key = publish_key.to_string();
        self
    }

    pub fn secret_key(mut self, secret_key: &str) -> Client {
        self.secret_key = secret_key.to_string();
        self
    }

    pub fn auth_key(mut self, auth_key: &str) -> Client {
        self.auth_key = auth_key.to_string();
        self
    }

    pub fn user_id(mut self, user_id: &str) -> Client {
        self.user_id = user_id.to_string();
        self
    }

    pub fn channels(mut self, channels: &str) -> Client {
        self.channels = channels.to_string();
        self
    }

    pub fn groups(mut self, groups: &str) -> Client {
        self.groups = groups.to_string();
        self
    }

    pub fn filters(mut self, filters: &str) -> Client {
        self.filters = filters.to_string();
        self
    }

    pub fn presence(mut self, presence: bool) -> Client {
        self.presence = presence;
        self
    }

    pub fn since(mut self, since: u64) -> Client {
        self.since = since;
        self
    }

    pub fn timetoken(mut self, timetoken: &str) -> Client {
        self.timetoken = timetoken.to_string();
        self
    }

    pub fn message(&self) -> PublishMessage {
        PublishMessage {
            // TODO probabaly need Pubkey/SubKey/ect...
            channel  : "demo".to_string(),
            data     : "test".to_string(),
            metadata : "".to_string(),
        }
    }
}

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
/// # PubNub
///
/// The PubNub lib implements socket pools to relay data requests as a client
/// connection to the PubNub Network.
///
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
pub struct PubNub {
    pub origin          : String,                         // "domain.com:443"
    pub agent           : String,                         // "Rust-Agent"
    pub submit_publish  : mpsc::Sender<PublishMessage>,   // Submit Publish
    pub process_publish : mpsc::Receiver<PublishMessage>, // Process Publish
    pub submit_client   : mpsc::Sender<Client>,           // Add Client
    pub process_client  : mpsc::Receiver<Client>,         // Add Client
    pub submit_result   : mpsc::Sender<Message>,          // Send to App
    pub process_result  : mpsc::Receiver<Message>,        // App Receiver
}

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
/// # PubNub Pool
///
/// This client lib offers publish and subscribe support to PubNub.
/// Additionally creates an upstream pool and maintains connectivity for
/// thousands of clients.  Client count limited to machine resources.
/// Autoscale resources as needed.
///
/// This is the base structure which creates two threads for
/// Publish and Subscribe.
///
/// ```
/// ```
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
impl PubNub {
    pub fn new() -> PubNub {
        let (submit_publish, process_publish) = mpsc::channel(100);
        let (submit_client,  process_client)  = mpsc::channel(100);
        let (submit_result,  process_result)  = mpsc::channel(999);

        PubNub {
            origin : "ps.pndsn.com:443".to_string(),
            agent  : "Rust-Agent".to_string(),
            submit_publish,  // Publish a Message
            process_publish, // Process Message Publish
            submit_client,   // Add a Client
            process_client,  // Process Client Addition
            submit_result,   // Send Result to Application Consumer
            process_result,  // Receiver for Application Consumer
        }
    }

    pub fn origin(mut self, origin: &str) -> PubNub {
        self.origin = origin.to_string();
        self
    }

    pub fn agent(mut self, agent: &str) -> PubNub {
        self.agent = agent.to_string();
        self
    }

    pub fn add(&self, client: &Client) {
        // TODO store client based on subkey/etc...
        self.subscribe(client.clone());
    }

    pub fn remove(&self, client: Client) {}

    pub fn next(&self) {}

    fn subscribe(&self, client: Client) {
        // - Construct URI
        // - add requet to HTTP/2 Pool

	//let uri = "http://httpbin.org/ip".parse().unwrap();

        /*
	self.http
	    .get(uri)
	    .and_then(|res| {
		println!("Response: {}", res.status());
		res
		    .into_body()
		    // Body is a stream, so as each chunk arrives...
		    .for_each(|chunk| {
			io::stdout()
			    .write_all(&chunk)
			    .map_err(|e| {
				panic!("example expects stdout is open, error={}", e)
			    })
		    })
	    })
	    .map_err(|err| {
		println!("Error: {}", err);
	    })
            */
    }
}

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
// Tests for PubNub Pool
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pubnub_time_ok() {
        let host = "0.0.0.0:3000";
        assert!(true);
        assert!(true);
    }

    #[test]
    fn pubnub_subscribe_ok() {
        let publish_key   = "demo";
        let subscribe_key = "demo";
        let channels      = "demo";
        let origin        = "ps.pndsn.com:443";
        let agent         = "Rust-Agent-Test";

        let pubnub = PubNub::new()
            .origin(&origin.to_string())
            .agent(&agent.to_string());

        let client = Client::new()
            .subscribe_key(&subscribe_key)
            .publish_key(&publish_key)
            .channels(&channels);

        pubnub.add(&client);

        // pubnub.next()
    }

    #[test]
    fn pubnub_publish_ok() {
        let publish_key   = "demo";
        let subscribe_key = "demo";
        let channels      = "demo";

        let origin = "ps.pndsn.com:443";
        let agent  = "Rust-Agent-Test";

        let mut pubnub = PubNub::new()
            .origin(&origin.to_string())
            .agent(&agent.to_string());

        assert!(pubnub.origin == origin);
        assert!(pubnub.agent == agent);

        let client = Client::new()
            .subscribe_key(&subscribe_key)
            .publish_key(&publish_key)
            .channels(&channels);

        assert!(client.subscribe_key == subscribe_key);
        assert!(client.publish_key == publish_key);
        assert!(client.channels == channels);

        pubnub.add(&client);

        client.message().channel("demo").data("Hi!").publish(&mut pubnub);

        /*
        while let Some(message) = pubnub.next().await {
            // TODO Match on MessageType match message.message_type {}
            // Print message and channel name.
            println!("{}: {}", message.channel, message.data);
            
            // Remove clients only when you no longer need them
            // When no more clients are in the pool, then `pubnub.next()` will
            // return `None` and the loop will exit.
            pubnub.remove(message.client);
        }
        */
    }
}
