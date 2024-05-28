use std::collections::HashMap;

use pubnub::{Keyset, PubNubClientBuilder};

#[derive(Debug, serde::Serialize)]
struct State {
    is_doing: String,
    flag: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn snafu::Error>> {
    // let publish_key = env::var("SDK_PUB_KEY")?;
    // let subscribe_key = env::var("SDK_SUB_KEY")?;
    let publish_key = "demo";
    let subscribe_key = "demo";

    let client = PubNubClientBuilder::with_reqwest_transport()
        .with_keyset(Keyset {
            subscribe_key,
            publish_key: Some(publish_key),
            secret_key: None,
        })
        .with_user_id("user_id")
        .build()?;

    println!("running!");

    client
        .set_presence_state_with_heartbeat(HashMap::from([
            (
                "my_channel".to_string(),
                State {
                    is_doing: "Something".to_string(),
                    flag: true,
                },
            ),
            (
                "other_channel".to_string(),
                State {
                    is_doing: "Oh no".to_string(),
                    flag: false,
                },
            ),
        ]))
        .channels(["my_channel".into(), "other_channel".into()].to_vec())
        .user_id("user_id")
        .execute()
        .await?;

    client
        .set_presence_state(State {
            is_doing: "Nothing... Just hanging around...".into(),
            flag: false,
        })
        .channels(["my_channel".into(), "other_channel".into()].to_vec())
        .user_id("user_id")
        .execute()
        .await?;

    println!("State set!");
    println!();

    let states = client
        .get_presence_state()
        .channels(["my_channel".into(), "other_channel".into()].to_vec())
        .user_id("user_id")
        .execute()
        .await?;

    println!("All channels state: {:?}", states);

    states.iter().for_each(|channel| {
        println!("Channel: {}", channel.channel);
        println!("State: {:?}", channel.state);
    });

    Ok(())
}
