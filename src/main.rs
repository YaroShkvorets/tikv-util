use std::env;
use tikv_client::IntoOwnedRange;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args: Vec<String> = env::args().collect();

    // Ensure that enough arguments are provided
    if args.len() != 4 {
        eprintln!("Usage: {} <network> <start> <end>, i.e. {} chiado blockmeta_v1_cl blockmeta_v2_cl", args[0], args[0]);
        std::process::exit(1);
    }

    let network = args[1].clone();
    let mut start = args[2].clone();
    let end = args[3].as_str();
    let client = tikv_client::RawClient::new(vec![format!("{}-pd1.mar.eosn.io:2379", network), format!("{}-pd2.mar.eosn.io:2379", network), format!("{}-pd3.mar.eosn.io:2379", network)]).await?;

    let mut deleted = 0;
    loop {
        let keys = match client
            .scan_keys((start.as_str()..end).into_owned(), 100)
            .await {
                Ok(keys) => keys,
                Err(e) => {
                    println!("error scanning keys: {:?}", e);
                    continue;
                }
            };
        if keys.is_empty() {
            break;
        }


        deleted += keys.len();
        start = String::from_utf8_lossy(keys.last().unwrap().into()).into_owned();
        println!("{} {}", deleted, progress(start.to_string()));
    }

    println!("Done! Deleted {} keys", deleted);

    // let key = "blob_v1;xc".as_bytes();
    // match client.get(key.to_owned()).await {
    //     Ok(_) => println!("xc: {:?}", key),
    //     Err(e) => println!("error getting key: {:?}", e),
    // }

    Ok(())
}

fn progress(input: String) -> String {
    if let Some(index) = input.find("0x") {
        if index + 2 + 4 <= input.len() {
            return input[index + 2..index + 2 + 4].into();
        }
    }
    "xxxx".into()
}
