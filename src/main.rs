use std::env;
use tikv_client::IntoOwnedRange;
use indicatif::{ProgressBar, ProgressStyle};

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

    println!("Deleting {} keys on {}", start, network);

    let bar = ProgressBar::new(u64::from_str_radix("ffff", 16).unwrap());
    bar.set_style(ProgressStyle::with_template("[{bar:50.cyan/blue}] {pos:>5}/{len:5} Elapsed: {elapsed_precise} | {msg}")
        .unwrap()
        .progress_chars("##-"));

    let mut deleted = 0;
    let mut errors = 0;
    let mut last_error = "N/A".to_string();
    loop {
        let keys = match client.scan_keys((start.as_str()..end).into_owned(), 100).await {
            Ok(keys) => keys,
            Err(e) => {
                last_error = e.to_string();
                errors += 1;
                continue;
            }
        };
        if keys.is_empty() {
            break;
        }
        if let Err(e) = client.batch_delete(keys.clone()).await {
            last_error = e.to_string();
            errors += 1;
            continue;
        }

        deleted += keys.len();
        start = String::from_utf8_lossy(keys.last().unwrap().into()).into_owned();
        let progress = progress(start.to_string());
        bar.set_position(progress);
        bar.set_message(format!("Deleted: {} | Errors: {}\nLast Key: {}\nLast Error: {}", deleted, errors, start, last_error));
    }

    println!("Done! Deleted {} keys", deleted);

    Ok(())
}

fn progress(input: String) -> u64 {
    if let Some(index) = input.find("0x") {
        if index + 2 + 4 <= input.len() {
            let progress = &input[index + 2..index + 2 + 4];
            if let Ok(prog) = u64::from_str_radix(progress, 16) {
                return prog;
            }
        }
    }
    0
}

