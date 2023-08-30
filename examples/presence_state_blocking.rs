use pubnub::{Keyset, PubNubClientBuilder};
use std::env;

fn main() -> Result<(), Box<dyn snafu::Error>> {
    let publish_key = env::var("SDK_PUB_KEY")?;
    let subscribe_key = env::var("SDK_SUB_KEY")?;

    let _client = PubNubClientBuilder::with_reqwest_blocking_transport()
        .with_keyset(Keyset {
            subscribe_key,
            publish_key: Some(publish_key),
            secret_key: None,
        })
        .with_user_id("user_id")
        .build()?;

    println!("running!");

    //    client
    //        .set_presence_state()
    //        .channels(["my_channel".into(), "other_channel".into()].to_vec())
    //        .state("{\"What you're doing\": \"Me? Nothing... Just hanging around\"}")
    //        .user_id("user_id")
    //        .execute_blocking()?;
    //
    //    let states = client
    //        .get_presence_state()
    //        .channels(["my_channel".into(), "other_channel".into()].to_vec())
    //        .user_id("user_id")
    //        .execute_blocking()?;
    //
    //    println!("All channels state: {:?}", states);
    //
    //    states.iter().for_each(|channel| {
    //        println!("Channel: {}", channel.channel);
    //        println!("State: {:?}", channel.state);
    //    });
    //
    Ok(())
}
