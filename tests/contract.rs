use cucumber::{given, then, when, World};
use pubnub::dx::publish::PublishResult;
use pubnub::dx::pubnub_client::PubNubConfig;
use pubnub::dx::PubNubClient;
use pubnub::transport::middleware::PubNubMiddleware;
use pubnub::transport::TransportReqwest;
use pubnub::PubNubError;
use reqwest::Client;
use std::collections::HashMap;

#[derive(Debug, Default)]
struct Keyset {
    pub subkey: String,
    pub pubkey: String,
}

#[derive(Debug, World)]
pub struct PubNubWorld {
    keyset: Keyset,
    last_result: Result<PublishResult, PubNubError>,
}

impl Default for PubNubWorld {
    fn default() -> Self {
        PubNubWorld {
            keyset: Keyset::default(),
            last_result: Err(PubNubError::TransportError("This is default value".into())),
        }
    }
}

impl PubNubWorld {
    fn get_pub_nub(&self) -> PubNubClient<PubNubMiddleware<TransportReqwest>> {
        PubNubClient {
            transport: PubNubMiddleware {
                transport: TransportReqwest {
                    hostname: "http://localhost:8090/".into(),
                    reqwest_client: Client::default(),
                },
                user_id: "user_id".to_string(),
                instance_id: None,
            },
            next_seqn: 1,
            instance_id: Some("instance".to_owned()),
            config: PubNubConfig {
                publish_key: Some(self.keyset.pubkey.to_owned()),
                subscribe_key: self.keyset.subkey.to_owned(),
                user_id: "some".to_owned(),
                secret_key: Some("some".to_owned()),
            },
        }
    }
}

#[given("the demo keyset")]
fn set_keyset(world: &mut PubNubWorld) {
    world.keyset.pubkey = "demo".to_string();
    world.keyset.subkey = "demo".to_string();
}

async fn init_server(script: String) -> Result<String, Box<dyn std::error::Error>> {
    let url = format!("http://localhost:8090/init?__contract__script__={}", script);
    let client = reqwest::Client::new();
    let body = client.get(url).send().await?.text().await?;
    Ok(body)
}

#[when(expr = "I publish '{word}' string as message to '{word}' channel")]
async fn i_publish_string_as_message_to_channel(
    world: &mut PubNubWorld,
    message: String,
    channel: String,
) {
    world.last_result = world
        .get_pub_nub()
        .publish_message(message)
        .channel(channel)
        .execute()
        .await;
}

#[then("I receive successful response")]
fn i_receive_successful_response(world: &mut PubNubWorld) {
    assert!(world.last_result.is_ok())
}

#[when(expr = "I publish {string} dictionary as message to '{word}' channel with compression")]
fn i_publish_dictionary_as_message_to_channel_with_compression(
    _world: &mut PubNubWorld,
    _dictionary_json: String,
    _channel: String,
) {
}

#[when(regex = r"^I publish '(.*)' dictionary as message to '(.*)' channel as POST body$")]
async fn i_publish_dictionary_as_message_to_channel_as_post_body(
    world: &mut PubNubWorld,
    dictionary_json: String,
    channel: String,
) {
    let message_hash_map: HashMap<String, String> =
        serde_json::from_str(dictionary_json.as_str()).unwrap();
    world.last_result = world
        .get_pub_nub()
        .publish_message(message_hash_map)
        .channel(channel)
        .use_post(true)
        .execute()
        .await;
}

#[when(regex = r"^I publish '(.*)' dictionary as message to '(.*)' channel$")]
async fn i_publish_dictionary_as_message_to_channel(
    world: &mut PubNubWorld,
    dictionary_json: String,
    channel: String,
) {
    let message_hash_map: HashMap<String, String> =
        serde_json::from_str(dictionary_json.as_str()).unwrap();
    world.last_result = world
        .get_pub_nub()
        .publish_message(message_hash_map)
        .channel(channel)
        .execute()
        .await;
}

#[when(regex = r"^I publish '(.*)' array as message to '(.*)' channel$")]
async fn i_publish_array_as_message_to_channel(
    world: &mut PubNubWorld,
    array_str: String,
    channel: String,
) {
    let message_array: [String; 2] = serde_json::from_str(array_str.as_str()).unwrap();
    world.last_result = world
        .get_pub_nub()
        .publish_message(message_array)
        .channel(channel)
        .execute()
        .await;
}

#[when(regex = r"^I publish '(.*)' string as message to '(.*)' channel with '(.*)' set to '(.*)'$")]
async fn i_publish_message_to_channel_with_meta(
    world: &mut PubNubWorld,
    message: String,
    channel: String,
    param: String,
    param_value: String,
) {
    if param.eq("meta") {
        let meta_map: HashMap<String, String> = serde_json::from_str(param_value.as_str()).unwrap();
        world.last_result = world
            .get_pub_nub()
            .publish_message(message)
            .channel(channel)
            .meta(meta_map)
            .execute()
            .await;
    } else if param.eq("store") {
        let store: bool = param_value != "0";
        world.last_result = world
            .get_pub_nub()
            .publish_message(message)
            .channel(channel)
            .store(store)
            .execute()
            .await;
    }
}

#[when(expr = "I publish '{int}' number as message to '{word}' channel")]
async fn i_publish_number_as_message_to_channel(
    world: &mut PubNubWorld,
    number: i32,
    channel: String,
) {
    world.last_result = world
        .get_pub_nub()
        .publish_message(number)
        .channel(channel)
        .execute()
        .await;
}

#[tokio::main]
async fn main() {
    env_logger::builder().try_init().unwrap();
    PubNubWorld::cucumber()
        .before(|_feature, _rule, scenario, _world| {
            futures::FutureExt::boxed(async move {
                if scenario.tags.iter().any(|t| t.starts_with("contract=")) {
                    let tag = scenario
                        .tags
                        .iter()
                        .find(|&t| t.starts_with("contract="))
                        .unwrap();
                    let splitted_values: Vec<&str> = tag.split('=').collect();
                    if !splitted_values[1].is_empty() {
                        let script_name = splitted_values[1];
                        init_server(script_name.to_string()).await.unwrap();
                    }
                }
            })
        })
        .run_and_exit("tests/features/publish")
        .await;
}
