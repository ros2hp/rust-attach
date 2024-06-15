use std::collections::HashMap;
use std::process;

use tokio::time::Instant;

use core::time::Duration;

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::WriteRequest;
use aws_sdk_dynamodb::Client;
//use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
//use aws_smithy_runtime_api::client::result::SdkError;

use uuid::Uuid;

const DYNAMO_BAT_SIZE: usize = 25;
const MAX_TASKS: u8 = 5;

#[::tokio::main]
async fn main() {
    //let mut uuid_store: HashMap<String, Uuid> = HashMap::new();

    // create a dynamodb client
    let config = aws_config::from_env().region("us-east-1").load().await;
    let dynamo_client = Client::new(&config);
    let client = dynamo_client.clone();
    let client2 = &client;

    let table_name = "RustGraph.dev.2";
    let table_name_retry = "RustGraph.dev.1"; //2.retry";

    // copy items from retry table to live table. Split between two tokio tasks:  main -> channel -> load-task

    // paginated scan populates batch, then load batch into live table and repeat until last_evaluated_key is None

    let (task_ch, mut task_rx) = tokio::sync::mpsc::channel::<u8>(MAX_TASKS as usize); //TODO: try 0,2,3,4,5

    let mut lek: Option<HashMap<String, AttributeValue>> = None;

    let mut persist_tasks: u8 = 0;
    let mut tasks_spawned: usize = 0;

    let mut more_items = true;
    let mut first_time = true;
    println!("retry scan_task started...");

    let mut dur_scan = Duration::new(0, 0);
    let mut dur_send = Duration::new(0, 0);
    let start = Instant::now();

    while more_items {
        //start = Instant::now();
        //.select() - default to ALL_ATTRIBUTES
        let result = if first_time {
            first_time = false;
            client2
                .scan()
                .table_name(table_name_retry)
                .limit(1000)
                .send()
                .await
        } else {
            client2
                .scan()
                .table_name(table_name_retry)
                .set_exclusive_start_key(lek)
                .limit(1000)
                .send()
                .await
        };

        if let Err(err) = result {
            panic!("Error in scan of retries table: {}", err); // TODO replace panic
        }
        let scan_output = result.unwrap();

        if scan_output.items != None {
            let client = dynamo_client.clone();

            // equiv to: go persist_task(..)
            // not necessary to .await on spawn as persist_task will send end message on a channel
            // which main will wait to receive. This emulates the await on a task handle.
            tokio::spawn(persist_task(
                tasks_spawned,
                client,
                scan_output.items.unwrap(),
                table_name,
                task_ch.clone(),
            ));
            persist_tasks += 1;
            tasks_spawned += 1;
        }

        // TODO throttle persist_tasks to go from 1..MAX in 30 second increments.
        if persist_tasks > MAX_TASKS {
            let _ = task_rx.recv().await; //TODO many entries in channel
            persist_tasks -= 1;
        }

        if None == scan_output.last_evaluated_key {
            more_items = false;
        } else {
            more_items = true;
        }
        lek = scan_output.last_evaluated_key;
    }

    println!("waiting for tasks to finish... ");
    while persist_tasks > 0 {
        let _ = task_rx.recv();
        persist_tasks -= 1;
        println!("finished ");
    }
    let elapsed = Instant::now().duration_since(start);

    println!(
        "Max Tasks: {} Elapsed Time: {} secs",
        MAX_TASKS,
        elapsed.as_secs()
    );
    // println!("Duraton waiting on channel send: {} ns   {} secs ",dur_send.as_nanos(),dur_send.as_secs_f32());

    println!("exit.");
}

async fn persist_task(
    taskid: usize,
    client: Client,
    items: Vec<HashMap<String, AttributeValue>>,
    table_name: &str,
    ch: tokio::sync::mpsc::Sender<u8>,
) {
    println!("retry load_task started...{}", taskid);

    let mut bat_w_req: Vec<WriteRequest> = vec![];

    for hm in items {
        let mut put = aws_sdk_dynamodb::types::PutRequest::builder();
        for (k, v) in hm {
            if k == "PK" {
                let vv = AttributeValue::B(Blob::new(Uuid::new_v4().as_bytes()));
                put = put.item(k, vv);
            } else {
                put = put.item(k, v);
            }
        }

        // build a WriteRequest
        match put.build() {
            Err(err) => {
                println!("error in write_request builder: {}", err);
            }

            Ok(req) => {
                bat_w_req.push(WriteRequest::builder().put_request(req).build());
            }
        }

        // persist write requests only when dynamodb batch limit reached (25 writerequests).
        if bat_w_req.len() == DYNAMO_BAT_SIZE {
            bat_w_req = persist_batch(client.clone(), bat_w_req, table_name).await;
        }
    }
    //}
    //dur_persist += Instant::now().duration_since(start);

    if bat_w_req.len() > 0 {
        persist_batch(client, bat_w_req, table_name).await;
    }

    // println!("Duration channel recv: {} ms   {} secs",dur_recv.as_nanos(), dur_recv.as_secs_f32());
    // println!("Duraton persist: {} ns   {} secs ",dur_persist.as_millis(), dur_persist.as_secs());

    ch.send(0).await;
}

async fn persist_batch(
    dynamo_client: Client,
    bat_w_req: Vec<WriteRequest>,
    table_name: impl Into<String>,
) -> Vec<WriteRequest> {
    //println!("persist:{} ",bat_w_req.len());
    let bat_w_outp = dynamo_client
        .batch_write_item()
        .request_items(table_name, bat_w_req)
        .send()
        .await;

    match bat_w_outp {
        Err(err) => {
            let e = err.into_source().unwrap();
            println!("{}", e);
            process::exit(-1);
            //panic!("Error in Dynamodb batch write in persist_batch() - {}", err.source().unwrap());
        }
        Ok(resp) => {
            //println!("persist_batch: written to table.");

            if resp.unprocessed_items.as_ref().unwrap().len() > 0 {
                println!(
                    "**** persist_batch: unprocessed items {}",
                    resp.unprocessed_items.unwrap().values().len()
                );
            }
        }
    }
    let new_bat_w_req: Vec<WriteRequest> = vec![];

    new_bat_w_req
}
