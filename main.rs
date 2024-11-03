 //#[deny(unused_imports)]
//#[warn(unused_imports)]
#[allow(unused_imports)]

mod service;
mod types;

use std::collections::HashMap;
use std::env;
use std::string::String;
//use std::sync::Arc;
use std::process::exit;

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::builders::PutRequestBuilder;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::{PutRequest, WriteRequest};
use aws_sdk_dynamodb::Client as DynamoClient;
//use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
//use aws_smithy_runtime_api::client::result::SdkError;

use uuid::Uuid;

use mysql_async::prelude::*;

use tokio::time::{sleep, Duration, Instant};
use tokio::sync::broadcast;
//use tokio::task::spawn;



const CHILD_UID: u8 = 1;
const _UID_DETACHED: u8 = 3; // soft delete. Child detached from parent.
const OV_BLOCK_UID: u8 = 4; // this entry represents an overflow block. Current batch id contained in Id.
const OV_BATCH_MAX_SIZE: u8 = 5; // overflow batch reached max entries - stop using. Will force creating of new overflow block or a new batch.
const _EDGE_FILTERED: u8 = 6; // set to true when edge fails GQL uid-pred  filter
const DYNAMO_BATCH_SIZE: usize = 25;
const MAX_TASKS: usize = 1;

const LS: u8 = 1;
const LN: u8 = 2;
const LB: u8 = 3;
const LBL: u8 = 4;
const _LDT: u8 = 5;

// ==============================================================================
// Overflow block properties - consider making part of a graph type specification
// ==============================================================================

// EMBEDDED_CHILD_NODES - number of cUIDs (and the assoicated propagated scalar data) stored in the paraent uid-pred attribute e.g. A#G#:S.
// All uid-preds can be identified by the following sortk: <partitionIdentifier>#G#:<uid-pred-short-name>
// for a parent with limited amount of scalar data the number of embedded child uids can be relatively large. For a parent
// node with substantial scalar data this parameter should be corresponding small (< 5) to minimise the space consumed
// within the parent block. The more space consumed by the embedded child node data the more RCUs required to read the parent Node data,
// which will be an overhead in circumstances where child data is not required.
const EMBEDDED_CHILD_NODES: usize = 4; //10; // prod value: 20

// MAX_OV_BLOCKS - max number of overflow blocks. Set to the desired number of concurrent reads on overflow blocks ie. the degree of parallelism required. Prod may have upto 100.
// As each block resides in its own UUID (PKey) there shoud be little contention when reading them all in parallel. When max is reached the overflow
// blocks are then reused with new overflow items (Identified by an ID at the end of the sortK e.g. A#G#:S#:N#3, here the id is 3)  being added to each existing block
// There is no limit on the number of overflow items, hence no limit on the number of child nodes attached to a parent node.
const MAX_OV_BLOCKS: usize = 5; // prod value : 100

// OV_MAX_BATCH_SIZE - number of uids to an overflow batch. Always fixed at this value.
// The limit is checked using the database SIZE function during insert of the child data into the overflow block.
// An overflow block has an unlimited number of batches.
const OV_MAX_BATCH_SIZE: usize = 4; //15; // Prod 100 to 500.

// OV_BATCH_THRESHOLD, initial number of batches in an overflow block before creating new Overflow block.
// Once all overflow blocks have been created (MAX_OV_BLOCKS), blocks are randomly chosen and each block
// can have an unlimited number of batches.
const OV_BATCH_THRESHOLD: usize = 4; //100

type SortK = String;
type Cuid = Uuid;
type Puid = Uuid;

// Overflow Block (Uuids) item. Include in each propagate item.
// struct OvB {
//      ovb: Vec<AttributeValue>, //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
//      xf: Vec<AttributeValue>, // used in uid-predicate 3 : ovefflow UID, 4 : overflow block full
// }

struct ReverseEdge {
    pk: AttributeValue,   // cuid
    sk: AttributeValue, // R#sk-of-parent|x    where x is 0 for embedded and non-zero for batch id in ovb
    tuid: AttributeValue, // target-uuid, either parent-uuid for embedded or ovb uuid
}
//
struct OvBatch {
    pk: Uuid, // ovb Uuid
    //
    nd: Vec<AttributeValue>, //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
    xf: Vec<AttributeValue>, // used in uid-predicate 1 : c-UID, 2 : c-UID is soft deleted, 3 : ovefflow UID, 4 : overflow block ful
}

struct ParentEdge {
    //
    nd: Vec<AttributeValue>, //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
    xf: Vec<AttributeValue>, // used in uid-predicate 1 : c-UID, 2 : c-UID is soft deleted, 3 : ovefflow UID, 4 : overflow block full
    id: Vec<u32>,            // most recent batch in overflow
    //
    ty: String,         // node type m|P
    p: String,          // edge predicate (long name) e.g. m|actor.performance - indexed in P_N
    cnt: usize,         // number of edges < 20 (MaxChildEdges)
    rrobin_alloc: bool, // round robin ovb allocation applies (initially false)
    eattr_nm: String,   // edge attribute name (derived from sortk)
    eattr_sn: String,   // edge attribute short name (derived from sortk)
    //
    ovb_idx: usize,          // last ovb populated
    ovbs: Vec<Vec<OvBatch>>, //  each ovb is made up of batches. each ovb simply has a different pk - a batch shares the same pk.
    //
    rvse: Vec<ReverseEdge>,
}

struct PropagateScalar {
    entry: Option<u8>,
    sk: String,
    //
    ls: Vec<AttributeValue>,
    ln: Vec<AttributeValue>, // merely copying values so keep as Number datatype (no conversion to i64,f64)
    lbl: Vec<AttributeValue>,
    lb: Vec<AttributeValue>,
    ldt: Vec<AttributeValue>,
}

enum Operation {
    Attach(ParentEdge),
    Propagate(PropagateScalar),
}

#[::tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
    let _start_1 = Instant::now();
    // ===============================
    // 1. Source environment variables
    // ===============================
    let mysql_host =
        env::var("MYSQL_HOST").expect("env variable `MYSQL_HOST` should be set in profile");
    let mysql_user =
        env::var("MYSQL_USER").expect("env variable `MYSQL_USER` should be set in profile");
    let mysql_pwd =
        env::var("MYSQL_PWD").expect("env variable `MYSQL_PWD` should be set in profile");
    let mysql_dbname =
        env::var("MYSQL_DBNAME").expect("env variable `MYSQL_DBNAME` should be set in profile");
    let graph =
        env::var("GRAPH_NAME").expect("env variable `GRAPH_NAME` should be set in profile");
    let table_name = "RustGraph.dev.4";
    // ===========================
    // 2. Create a Dynamodb Client
    // ===========================
    let config = aws_config::from_env().region("us-east-1").load().await;
    let dynamo_client = DynamoClient::new(&config);
    // =======================================
    // 3. Fetch Graph Data Types from Dynamodb
    // =======================================
    let (node_types, graph_prefix_wdot) = types::fetch_graph_types(&dynamo_client, graph).await?;

    println!("Node Types:");
    for t in node_types.0.iter() {
        println!(
            "Node type {} [{}]    reference {}",
            t.get_long(),
            t.get_short(),
            t.is_reference()
        );
        for attr in t {
            println!("attr.name [{}] dt [{}]  c [{}]", attr.name, attr.dt, attr.c);
        }
    }
    
    // create broadcast channel to shutdown services
    let (shutdown_broadcast_ch, mut shutdown_recv_ch) = broadcast::channel(1); // broadcast::channel::<u8>(1);

    // start Retry service (handles failed putitems)
    println!("start Retry service...");
    let (retry_ch, mut retry_rx) = tokio::sync::mpsc::channel(MAX_TASKS * 2);
    let mut retry_shutdown_ch = shutdown_broadcast_ch.subscribe();
    let retry_service = service::retry::start_service(
        dynamo_client.clone(),
        retry_rx,
        retry_ch.clone(),
        retry_shutdown_ch,
        table_name,
    );
    
    // ================================
    // 4. Setup a MySQL connection pool
    // ================================
    let pool_opts = mysql_async::PoolOpts::new()
        .with_constraints(mysql_async::PoolConstraints::new(5, 30).unwrap())
        .with_inactive_connection_ttl(Duration::from_secs(60));

    let mysql_pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::default()
            //.from_url(url)
            .ip_or_hostname(mysql_host)
            .user(Some(mysql_user))
            .pass(Some(mysql_pwd))
            .db_name(Some(mysql_dbname))
            .pool_opts(pool_opts),
    );
    let pool = mysql_pool.clone();
    let mut conn = pool.get_conn().await.unwrap();

    // ============================
    // 5. MySQL query: parent nodes 
    // ============================
    let mut parent_node: Vec<Uuid> = vec![];

    let parent_edge = "SELECT Uid FROM Edge_test order by cnt desc"
        .with(())
        .map(&mut conn, |puid| parent_node.push(puid))
        .await?;
    // =======================================
    // 6. MySQL query: graph parent node edges 
    // =======================================
    let mut parent_edges: HashMap<Puid, HashMap<SortK, Vec<Cuid>>> = HashMap::new();
    let child_edge = "Select puid,sortk,cuid from test_childedge order by puid,sortk"
        .with(())
        .map(&mut conn, |(puid, sortk, cuid): (Uuid, String, Uuid)| {
            // this version requires no allocation (cloning) of sortk
            match parent_edges.get_mut(&puid) {
                None => {
                    let mut e = HashMap::new();
                    e.insert(sortk, vec![cuid]);
                    parent_edges.insert(puid, e);
                }
                Some(e) => match e.get_mut(&sortk[..]) {
                    None => {
                        let e = match parent_edges.get_mut(&puid) {
                            None => {
                                panic!("logic error in parent_edges get_mut()");
                            }
                            Some(e) => e,
                        };
                        e.insert(sortk, vec![cuid]);
                    }
                    Some(c) => {
                        c.push(cuid);
                    }
                },
            }
        })
        .await?;

    // ===========================================
    // 7. Setup asynchronous tasks infrastructure
    // ===========================================
    let mut tasks: usize = 0;
    let (prod_ch, mut task_rx) = tokio::sync::mpsc::channel::<bool>(MAX_TASKS);
    // ====================================
    // 8. Setup retry failed writes channel
    // ====================================
    let (retry_send_ch, mut retry_rx) =
        tokio::sync::mpsc::channel::<Vec<aws_sdk_dynamodb::types::WriteRequest>>(MAX_TASKS);
    
    // ==============================================================
    // 9. spawn task to attach node edges and propagate scalar values
    // ==============================================================
    for puid in parent_node {
        // ------------------------------------------
        
        // if puid.to_string() != "8d18653f-bf9d-4781-ad64-28385678fb88" { // Peter Sellers in RustGraph.dev.4
        //     continue
        // }
        let mut p_sk_edges = match parent_edges.remove(&puid) {
            None => {
                panic!("logic error. No entry found in parent_edges");
            }
            Some(e) => e,
        };
        // =====================================================
        // 9.1 clone enclosed vars before moving into task block
        // =====================================================
        let task_ch = prod_ch.clone();
        let dyn_client = dynamo_client.clone();
        let retry_ch = retry_send_ch.clone();
        let graph_sn = graph_prefix_wdot.trim_end_matches('.').to_string();
        let node_types = Arc::clone(&node_types);        // Arc instance - single cache in heap storage
       
        tasks += 1;     // concurrent task counter
        // =========================================
        // 9.2 spawn tokio task for each parent node
        // =========================================
        tokio::spawn(async move {
        
            // =====================================================================
            // p_node_ty : find type of puid . use sk "m|T#"  <graph>|<T,partition># //TODO : type short name should be in mysql table - saves fetching here.
            // =====================================================================
            let p_node_ty = fetch_node_type(&dyn_client, &puid, &graph_sn, &node_types, table_name).await;
            // =========================================================================            
            // ty_attr :  attribute identifer prefixed with graph short name e.g. m|name
            // =========================================================================
            let mut ty_attr = graph_sn.clone();
            ty_attr.push('|');
            ty_attr.push_str(&p_node_ty.short_nm());

            
            let mut ovbs_ppg: Vec<AttributeValue> = vec![];             // Container for Overflow Block Uuids, also stores all propagated data.
            let mut items: HashMap<types::SortK, Operation> = HashMap::new();  // split into Attach and Propagate Operations populated with associated enum values.
            
            // ======================================================
            // 9.2.1 for each parent (p) node edge attach child nodes
            // ======================================================
            p_sk_edges.drain().map(|(k, children) |(types::SortK::new(k), children)).for_each(|(p_sk,children)| {

                // =====================================================================================
                // 9.2.1.1 initilise items for current parent edge: (parent-edge-attribute) <- child node
                // =====================================================================================
                let v_edge = match items.get_mut(&p_sk) {
                                   
                    None => {
                        let edge_attr_sn = p_sk.get_attr_sn(); // A#G#:A -> "A" get_attr
                        let edge_attr_nm = p_node_ty.get_attr_nm(edge_attr_sn);

                        // RustGraph Data model, P attribute e.g "m|Film.Director|P" - used as partition key in global indexes P_S, P_N, P_B
                        let mut p_attr = graph_sn.clone();
                        p_attr.push('|');
                        p_attr.push_str(edge_attr_nm);
                        p_attr.push('|');
                        p_attr.push_str(&p_node_ty.short_nm());

                        let pe = ParentEdge {
                            nd: vec![], //AttributeValue::B(Blob::new(cuid))],
                            xf: vec![], //AttributeValue::N(CHILD_UID.to_string())],
                            id: vec![],
                            //
                            p: p_attr, // m|edge-attr-name|P
                            cnt: 0,
                            ty: ty_attr.clone(), // P or m|P
                            rrobin_alloc: false,
                            eattr_nm: edge_attr_nm.to_string(),
                            eattr_sn: edge_attr_sn.to_string(),
                            //
                            ovbs: vec![], // batch within current ovb
                            ovb_idx: 0,   // current ovb index
                            //
                            rvse: vec![],
                        };
                        // let Some(mut x) = items.insert(p_sk, Operation::Attach(pe)) else {panic!("failed insert")};
                        // &mut x
                        items.insert(p_sk.clone(), Operation::Attach(pe));
                        items.get_mut(&p_sk).unwrap()
                    }

                    Some(v) => v,
                };

                // extract ParaentEdge 
                let Operation::Attach(ref mut e) = v_edge else {
                    panic!("expected Operation::Attach")
                };
                let edge_attr_nm = &e.eattr_nm[..];

                // =====================================================
                // 9.2.1.2 attach child nodes
                // =====================================================
                for cuid in children {
                
                    let cuid_p = cuid.clone();
                    let child_ty = node_types.get(p_node_ty.get_edge_child_ty(edge_attr_nm));

                    // ===================================================================
                    // 9.2.1.2.1 attach child node allocating overflow blocks if necessary
                    // ===================================================================
                    e.cnt += 1;

                    if e.cnt <= EMBEDDED_CHILD_NODES {
                        e.nd.push(AttributeValue::B(Blob::new(cuid.clone())));
                        e.xf.push(AttributeValue::N(CHILD_UID.to_string()));
                        e.id.push(0);

                        // reverse edge - only for non-reference nodes  and where no reverse edge exists in child type
                        // e.g. parent has type attribute "director.film" & film type has attribute "film.director" then
                        // no need to add a reverse edge as data model has explicit edge defined and it will be in the data.
                        // However if child type has no "film.director" attribute name then add reverse edge to child
                        // When creating an reverse edge need to decide if is a 1:1 or 1:M.
                        // if !child_ty.is_reference() && child_ty.has_no_explicit_reverse_edge(p_sk) {
                        //     let mut r_sk = "R#".to_string();
                        //     r_sk.push_str(p_sk.from_partition());
                        //     let r = ReverseEdge {
                        //         pk: AttributeValue::B(Blob::new(cuid.clone())),
                        //         sk: AttributeValue::S(r_sk),
                        //         tuid: AttributeValue::B(Blob::new(puid.clone())),
                        //     };
                        //     e.rvse.push(r);
                        // }
                    } else {
                        if !e.rrobin_alloc {
                            if e.ovbs.len() == 0 {
                                // no ovbs exists, create one & first batch
                                let ovb = Uuid::new_v4();
                                // add to node edge
                                ovbs_ppg.push(AttributeValue::B(Blob::new(ovb.clone())));
                                e.nd.push(AttributeValue::B(Blob::new(ovb)));
                                e.xf.push(AttributeValue::N(OV_BLOCK_UID.to_string()));
                                e.id.push(1);
                                e.ovbs.push(vec![OvBatch {
                                    pk: ovb,
                                    nd: vec![AttributeValue::B(Blob::new(cuid.to_owned()))],
                                    xf: vec![AttributeValue::N(CHILD_UID.to_string())],
                                }]);
                                e.ovb_idx = 0;
                                // reverse edge?
                            } else {
                                // add data to current batch until max batch size reached. After max batch size reached create new batch
                                // until max batch threshold reached in which case create new ovb
                                let cur_batch_idx = e.ovbs[e.ovb_idx].len() - 1; // last batch created
                                if e.ovbs[e.ovb_idx].get(cur_batch_idx).unwrap().nd.len()
                                    < OV_MAX_BATCH_SIZE
                                {
                                    // append to batch
                                    let batch = e.ovbs[e.ovb_idx].get_mut(cur_batch_idx).unwrap();
                                    batch.nd.push(AttributeValue::B(Blob::new(cuid.to_owned())));
                                    batch.xf.push(AttributeValue::N(CHILD_UID.to_string()));

                                } else {
                                    // max batch size reaced - creat new batch - check thresholds first though
                                    // as new batch may be added to ovb on rr basis.
                                    if e.ovbs.len() == MAX_OV_BLOCKS
                                        && e.ovbs[e.ovb_idx].len() == OV_BATCH_THRESHOLD
                                    {
                                        e.rrobin_alloc = true; // round-robin allocation now applies for all future attach-node
                                        e.ovb_idx = 0;

                                        // create new batch to first ovb and attach node
                                        let cur_batch_idx = e.ovbs[e.ovb_idx].len() - 1;
                                        let ovbuid =
                                            e.ovbs[e.ovb_idx].get(cur_batch_idx).unwrap().pk;
                                        e.ovbs[e.ovb_idx].push(OvBatch {
                                            pk: ovbuid,
                                            nd: vec![AttributeValue::B(Blob::new(cuid.to_owned()))],
                                            xf: vec![AttributeValue::N(CHILD_UID.to_string())],
                                        });
                                        e.id[e.ovb_idx + EMBEDDED_CHILD_NODES as usize] += 1;
                                        // reverse edge?    
                                    } else {
                                        let cur_batch_idx = e.ovbs[e.ovb_idx].len() - 1;
                                        let ovbuid =
                                            e.ovbs[e.ovb_idx].get(cur_batch_idx).unwrap().pk;

                                        if e.ovbs[e.ovb_idx].len() < OV_BATCH_THRESHOLD {
                                            // create new batch
                                            e.ovbs[e.ovb_idx].push(OvBatch {
                                                pk: ovbuid,
                                                nd: vec![AttributeValue::B(Blob::new(
                                                    cuid.to_owned(),
                                                ))],
                                                xf: vec![AttributeValue::N(CHILD_UID.to_string())],
                                            });
                                            e.id[e.ovb_idx + EMBEDDED_CHILD_NODES as usize] += 1;
                                            // reverse edge? 
                                        } else {
                                            // no more batches allowed in latest ovb. Create new ovb and add batch
                                            let ovbuid = Uuid::new_v4();
                                            // add to ovbs & node edge
                                            ovbs_ppg
                                                .push(AttributeValue::B(Blob::new(ovbuid.clone())));
                                            e.nd.push(AttributeValue::B(Blob::new(ovbuid)));
                                            e.xf.push(AttributeValue::N(OV_BLOCK_UID.to_string()));
                                            e.id.push(1); // AttributeValue::N(0.to_string()));
                                            e.ovbs.push(vec![OvBatch {
                                                pk: ovbuid,
                                                nd: vec![AttributeValue::B(Blob::new(
                                                    cuid.to_owned(),
                                                ))],
                                                xf: vec![AttributeValue::N(CHILD_UID.to_string())],
                                            }]);
                                            // move to next ovb when adding next batch
                                            e.ovb_idx += 1;
                                            // reverse edge?
                                        }
                                    }
                                }
                            }
                        } else {
                            // no more ovbs allowed. keep using current ovb (ovb_idx) until current batch full
                            // add to last batch in this ovb.
                                
                            let mut cur_batch_idx = e.ovbs[e.ovb_idx].len() - 1; 

                            if e.ovbs[e.ovb_idx].get(cur_batch_idx).unwrap().nd.len()
                                < OV_MAX_BATCH_SIZE
                            {
                                let cur_batch = e.ovbs[e.ovb_idx].get_mut(cur_batch_idx).unwrap();
                                cur_batch
                                    .nd
                                    .push(AttributeValue::B(Blob::new(cuid.to_owned())));
                                cur_batch.xf.push(AttributeValue::N(CHILD_UID.to_string()));

                                // reverse edge
                            } else {
                                // current batch in current ovb is full, go to next ovb (using round robin) and add new batch...
                                e.ovb_idx += 1;
                                if e.ovb_idx == MAX_OV_BLOCKS {
                                    e.ovb_idx = 0;
                                }
                                cur_batch_idx = e.ovbs[e.ovb_idx].len() - 1;

                                let ovbuid = e.ovbs[e.ovb_idx].get(cur_batch_idx).unwrap().pk;
                                e.ovbs[e.ovb_idx].push(OvBatch {
                                    pk: ovbuid,
                                    nd: vec![AttributeValue::B(Blob::new(cuid.to_owned()))],
                                    xf: vec![AttributeValue::N(CHILD_UID.to_string())],
                                });
                                e.id[e.ovb_idx + EMBEDDED_CHILD_NODES as usize] += 1;
                                // reverse edge
                            }
                        }
                    }
                }
            });

            // ============================================================
            // 9.2.2 persist attached nodes to each parent edge to database 
            // ============================================================
            persist(
                &dyn_client,
                puid,
                graph_sn.as_str(),
                &retry_ch,
                &task_ch,
                ovbs_ppg,
                items,
                table_name,
            )
            .await;

            // ====================================
            // 9.2.4 send complete message to main
            // ====================================
            if let Err(e) = task_ch.send(true).await {
                panic!("error sending on channel task_ch - {}", e);
            }

        });


        // =============================================================
        // 9.3 Wait for task to complete if max concurrent tasks reached
        // =============================================================
        if tasks == MAX_TASKS {
            // wait for a task to finish...
            task_rx.recv().await;
            tasks -= 1;
        }
        //break;
    }
    
    // =========================================
    // 10.0 Wait for remaining tasks to complete 
    // =========================================
    while tasks > 0 {
        // wait for a task to finish...
        task_rx.recv().await;
        tasks -= 1;
    }   
    
    println!("Waiting for support services to finish...");
    shutdown_broadcast_ch.send(0);
    retry_service.await;
    
    Ok(())
}

async fn persist(
    dyn_client: &aws_sdk_dynamodb::Client,
    puid: Uuid,
    graph_sn: &str,
    retry_ch: &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    task_ch: &tokio::sync::mpsc::Sender<bool>,
    ovbs_ppg: Vec<AttributeValue>,
    items: HashMap<types::SortK, Operation>,
    table_name: &str,
) {
    // persist to database
    let mut bat_w_req: Vec<WriteRequest> = vec![];

    for (sk, v) in items {
        match v {
            Operation::Attach(e) => {
                //println!("Persist Operation::Attach");

                let mut id_av: Vec<AttributeValue> = vec![];
                for i in e.id {
                    id_av.push(AttributeValue::N(i.to_string()));
                }
                //println!("Persist Operation::Attach - id len {}", id_av.len());

                let put = aws_sdk_dynamodb::types::PutRequest::builder();
                let put = put
                    .item(types::PK, AttributeValue::B(Blob::new(puid.clone())))
                    .item(types::SK, AttributeValue::S(sk.string()))
                    .item(types::ND, AttributeValue::L(e.nd))
                    .item(types::XF, AttributeValue::L(e.xf))
                    .item(types::BID, AttributeValue::L(id_av))
                    .item(types::TY, AttributeValue::S(e.ty))
                    .item(types::P, AttributeValue::S(e.p))
                    .item(types::CNT, AttributeValue::N(e.cnt.to_string()));

                bat_w_req = save_item(&dyn_client, bat_w_req, retry_ch, put, table_name).await;

                // reverse edge items - only for non-reference type
                // for r in e.rvse {
                //     let put = aws_sdk_dynamodb::types::PutRequest::builder();
                //     let put = put
                //         .item(types::PK, r.pk)
                //         .item(types::SK, r.sk)
                //         .item(types::TUID, r.tuid);

                //     bat_w_req = save_item(&dyn_client, bat_w_req, retry_ch, put, table_name).await;
                // }

                for ovb in e.ovbs {
                    let mut batch: usize = 1;
                    let pk = ovb[0].pk; // Uuid into<Vec<u8>> ?

                    for ovbat in ovb {
                        if batch == 1 {
                            // OvB header
                            let put = aws_sdk_dynamodb::types::PutRequest::builder();
                            let put = put
                                .item(types::PK, AttributeValue::B(Blob::new(pk.clone())))
                                .item(types::SK, AttributeValue::S("OV".to_string()))
                                .item(types::PARENT, AttributeValue::B(Blob::new(puid.clone())))
                                .item(types::GRAPH, AttributeValue::S(graph_sn.to_owned())); //TODO add graph name

                            bat_w_req = save_item(&dyn_client, bat_w_req, &retry_ch, put, table_name).await;
                        }

                        let mut ovsk = String::from(sk.string());
                        ovsk.push('%');
                        ovsk.push_str(&batch.to_string());

                        let put = aws_sdk_dynamodb::types::PutRequest::builder();
                        let put = put
                            .item(types::PK, AttributeValue::B(Blob::new(pk.clone())))
                            .item(types::SK, AttributeValue::S(ovsk))
                            .item(types::ND, AttributeValue::L(ovbat.nd))
                            .item(types::XF, AttributeValue::L(ovbat.xf));

                        bat_w_req = save_item(&dyn_client, bat_w_req, &retry_ch, put,table_name).await;

                        batch += 1;
                    }
                }
            },
            Operation::Propagate(_) => {},
        } // end match

    } // end for
    
    //println!("About to exit persis()...remaining batch to persist {}",bat_w_req.len());
    if bat_w_req.len() > 0  {
        // =================================================================================
        // persist to Dynamodb
        persist_dynamo_batch(dyn_client, bat_w_req, retry_ch, table_name).await;
        // =================================================================================
    }
}


// returns node type as String, moving ownership from AttributeValue - preventing further allocation.
async fn fetch_node_type<'a, T: Into<String>>(
    dyn_client: &DynamoClient,
    uid: &Uuid,
    graph_sn: T,
    node_types: &'a types::NodeTypes,
    table_name : &str,
) -> &'a types::NodeType {
    let mut sk_for_type: String = graph_sn.into();
    sk_for_type.push_str("|T#");

    let result = dyn_client
        .get_item()
        .table_name(table_name)
        .key(types::PK, AttributeValue::B(Blob::new(uid.clone())))
        .key(types::SK, AttributeValue::S(sk_for_type.clone()))
        .projection_expression("TyIx")
        .send()
        .await;

    if let Err(err) = result {
        panic!(
            "get node type: no item found: expected a type value for node. Error: {}",
            err
        )
    }
    let di: types::DataItem = match result.unwrap().item {
        None => panic!("No type item found in fetch_node_type() for [{}] [{}]", uid, sk_for_type),
        Some(v) => v.into(),
    };
    node_types.get(&di.tyix.unwrap())
}

async fn save_item(
    dyn_client: &DynamoClient,
    mut bat_w_req: Vec<WriteRequest>,
    retry_ch: &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    put: PutRequestBuilder,
    table_name: &str,
) -> Vec<WriteRequest> {
    match put.build() {
        Err(err) => {
            println!("error in write_request builder: {}", err);
        }
        Ok(req) => {
            bat_w_req.push(WriteRequest::builder().put_request(req).build());
        }
    }
    //bat_w_req = print_batch(bat_w_req);

    if bat_w_req.len() == DYNAMO_BATCH_SIZE {
        // =================================================================================
        // persist to Dynamodb
        bat_w_req = persist_dynamo_batch(dyn_client, bat_w_req, retry_ch, table_name).await;
        // =================================================================================
       // bat_w_req = print_batch(bat_w_req);
    }
    bat_w_req
}

async fn persist_dynamo_batch(
    dyn_client: &DynamoClient,
    bat_w_req: Vec<WriteRequest>,
    retry_ch: &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    table_name: &str,
) -> Vec<WriteRequest> {
    let bat_w_outp = dyn_client
        .batch_write_item()
        .request_items(table_name, bat_w_req)
        .send()
        .await;

    match bat_w_outp {
        Err(err) => {
            panic!(
                "Error in Dynamodb batch write in persist_dynamo_batch() - {}",
                err
            );
        }
        Ok(resp) => {
            if resp.unprocessed_items.as_ref().unwrap().values().len() > 0 {
                // send unprocessed writerequests on retry channel
                for (_, v) in resp.unprocessed_items.unwrap() {
                    println!("persist_dynamo_batch, unprocessed items..delay 2secs");
                    sleep(Duration::from_millis(2000)).await;
                    let resp = retry_ch.send(v).await;                // retry_ch auto deref'd to access method send.

                    if let Err(err) = resp {
                        panic!("Error sending on retry channel : {}", err);
                    }
                }

                // TODO: aggregate batchwrite metrics in bat_w_output.
                // pub item_collection_metrics: Option<HashMap<String, Vec<ItemCollectionMetrics>>>,
                // pub consumed_capacity: Option<Vec<ConsumedCapacity>>,
            }
        }
    }
    let new_bat_w_req: Vec<WriteRequest> = vec![];

    new_bat_w_req
}

fn print_batch(bat_w_req: Vec<WriteRequest>) -> Vec<WriteRequest> {
    for r in bat_w_req {
        let WriteRequest {
            put_request: pr, ..
        } = r;
        println!(" ------------------------  ");
        for (attr, attrval) in pr.unwrap().item {
            // HashMap<String, AttributeValue>,
            println!(" putRequest [{}]   {:?}", attr, attrval);
        }
    }

    let new_bat_w_req: Vec<WriteRequest> = vec![];

    new_bat_w_req
}
