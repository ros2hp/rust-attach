use std::collections::HashMap;

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::builders::PutRequestBuilder;
use aws_sdk_dynamodb::types::WriteRequest;
use aws_sdk_dynamodb::Client as DynamoClient;
//use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
//use aws_smithy_runtime_api::client::result::SdkError;

use uuid::Uuid;

use mysql_async::prelude::*;

use tokio::time::{sleep, Duration, Instant};
use tokio::task;

mod types; 


const	CHILD_UID         : u8 = 1;
const	UID_DETACHED      : u8 = 3; // soft delete. Child detached from parent.
const	OV_BLOCK_UID      : u8 = 4; // this entry represents an overflow block. Current batch id contained in Id.
const	OV_BATCH_MAX_SIZE : u8 = 5; // overflow batch reached max entries - stop using. Will force creating of new overflow block or a new batch.
const	EDGE_FILTERED     : u8 = 6; // set to true when edge fails GQL uid-pred  filter
const   DYNAMO_BATCH_SIZE: usize = 25;
const   MAX_TASKS : usize = 12;

 	// EMBEDDED_CHILD_NODES - number of cUIDs (and the assoicated propagated scalar data) stored in the paraent uid-pred attribute e.g. A#G#:S.
	// All uid-preds can be identified by the following sortk: <partitionIdentifier>#G#:<uid-pred-short-name>
	// for a parent with limited amount of scalar data the number of embedded child uids can be relatively large. For a parent
	// node with substantial scalar data this parameter should be corresponding small (< 5) to minimise the space consumed
	// within the parent block. The more space consumed by the embedded child node data the more RCUs required to read the parent Node data,
	// which will be an overhead in circumstances where child data is not required.
const	EMBEDDED_CHILD_NODES : i64 = 10; // prod value: 20
	
	// MAX_OV_BLOCKS - max number of overflow blocks. Set to the desired number of concurrent reads on overflow blocks ie. the degree of parallelism required. Prod may have upto 100.
	// As each block resides in its own UUID (PKey) there shoud be little contention when reading them all in parallel. When max is reached the overflow
	// blocks are then reused with new overflow items (Identified by an ID at the end of the sortK e.g. A#G#:S#:N#3, here the id is 3)  being added to each existing block
	// There is no limit on the number of overflow items, hence no limit on the number of child nodes attached to a parent node.
const	MAX_OV_BLOCKS : usize = 5; // prod value : 100

	// OV_MAX_BATCH_SIZE - number of uids to an overflow batch. Always fixed at this value.
	// The limit is checked using the database SIZE function during insert of the child data into the overflow block.
	// An overflow block has an unlimited number of batches.
const	OV_MAX_BATCH_SIZE : usize = 15; // Prod 100 to 500.

	// OV_BATCH_THRESHOLD, initial number of batches in an overflow block before creating new Overflow block.
	// Once all overflow blocks have been created (MAX_OV_BLOCKS), blocks are randomly chosen and each block
	// can have an unlimited number of batches.
const	OV_BATCH_THRESHOLD : usize = 5; //100

type SortK = String;
type Cuid = Uuid;
type Puid = Uuid;


struct OvBatch {
    pk  : Uuid,
    //
    nd: Vec<AttributeValue>, //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
    xf: Vec<AttributeValue>, // used in uid-predicate 1 : c-UID, 2 : c-UID is soft deleted, 3 : ovefflow UID, 4 : overflow block full
}

struct EdgeAttr {
    //
     nd: Vec<AttributeValue>, //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
     xf: Vec<AttributeValue>, // used in uid-predicate 1 : c-UID, 2 : c-UID is soft deleted, 3 : ovefflow UID, 4 : overflow block full
     id: Vec<u32>,
     id_av: Vec<AttributeValue>, // current maximum overflow batch id. Maps to the overflow item number in Overflow block e.g. A#G:S#:A#3 where Id is 3 meaning its the third item in the overflow block. Each item containing 500 or more UIDs in Lists.
    //
     ty : String,  // node type m|P
     p  : String,  // edge predicate (long name) e.g. m|actor.performance - indexed in P_N
     n  : i64,     // number of edges < 20 (MaxChildEdges)
     //
     ovb_idx: usize,                // last ovb populated
     ovbs : Vec<Vec<OvBatch>>,      //  each ovb is made up of batches. each ovb simply has a different pk i.e a batch shareas same pk.
     //
} 

//  struct pg_scalar_attr {
//      pkey    : Uuid, 
//      sortk   : String,
//     // List types contain propagated scalar data
//      ln  : Vec<String>, 
//      ls  : Vec<String>,
//      lbl : Vec<bool>,
//      lb  : Vec<u8>,
// }



#[::tokio::main]
async fn main()  -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
//   read type data 
//   Scan Edge_test into memory by cnt desc order - scanout
//   Scan test_childedge into memory - edgeout
//   for each item in scan_out {
//       build edge-pred Nd, XF, Id 
//           build overflow block & batch Nd, XF
//       build propagated scalar attributes
//       persist
//   }
    let start_1 = Instant::now();  
    // create a dynamodb client
    let config = aws_config::from_env().region("us-east-1").load().await;
    let dynamo_client = DynamoClient::new(&config);
    let graph = "Movies".to_string();
    
    let graph_short_nm = types::db_graph_prefix(&dynamo_client, graph).await.unwrap();

    println!("graph graph_short_nm : [{}]", graph_short_nm);

    let mut ty_all = types::db_load_types(&dynamo_client, graph_short_nm.as_str()).await.unwrap();

    println!("fetched types #{}", ty_all.0.len());

    for v in ty_all.0.iter() {
        println!("tyall : {} {} {}", v.attr, v.nm, v.ix);
    }

    let type_caches = types::populate_type_cache_1(&dynamo_client, &graph_short_nm[..], &mut ty_all).await.unwrap(); // graph_short_nm consumed...

    let start_2 = Instant::now();
    // create a mysql client
    let pool_opts = mysql_async::PoolOpts::new()
        .with_constraints(mysql_async::PoolConstraints::new(5, 30).unwrap())
        .with_inactive_connection_ttl(Duration::from_secs(60));

    let host = "mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com";
    let mysql_pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::default()
            //.from_url(url)
            .ip_or_hostname(host)
            .user(Some("admin"))
            .pass(Some("gjIe8Hl9SFD1g3ahyu6F"))
            .db_name(Some("GoGraph"))
            .pool_opts(pool_opts),
    );
    let pool = mysql_pool.clone();
    let mut conn = pool.get_conn().await.unwrap();
    
   let mut parent_node : Vec<Uuid> =  vec![];
    
       // Load payments from the database. Type inference will work here.
    let parent_edge = "SELECT Uid FROM Edge_test order by cnt desc"
        .with(())
        .map(&mut conn, |puid| parent_node.push(puid) )
        .await;
    println!("pass ...1"); 
    let mut edge : HashMap<SortK, Vec<Cuid>> = HashMap::new();

    let mut parent_edges : HashMap<Puid, HashMap<SortK, Vec<Cuid>>> = HashMap::new();
     println!("pass ...2"); 
     
    // use SQL below to populate HashMap parent_edges.   
    let child_edge = "Select puid,sortk,cuid from test_childedge"        
        .with(())
        .map(&mut conn, |(puid,sortk ,cuid) : (Uuid,String,Uuid)| { 
            // let sk = sortk.clone();       // clone required as sortk moved into both closures
            // parent_edges
            // .entry(puid)
            // .and_modify( |e| {  // ( move |e| ... note move used in this case because sortk in entry(sortk) requires ownership
            //     e        
            //     .entry(sortk.clone())             // must clone()
            //     .and_modify(|c| c.push(cuid) )
            //     .or_insert( vec![cuid] ) ;
            //     })
            //  .or_insert_with( ||{ // ( move |e| ... note move used in this case because sortk in insert(sortk,..) requires ownership
            //     let mut e = HashMap::new();
            //     e.insert(sk,vec![cuid]);
            //     e
            //     });
            // })
            // .await;
 
            // following does not work. seems to overwrite puid entry giving single edge rahter than multiple
            // this map version requires no clone() of sortk as no closures are used that implement move semantics.
            match parent_edges.get_mut(&puid) {
                None => {
                    let mut e = HashMap::new();
                    e.insert(sortk,vec![cuid]);
                    parent_edges.insert(puid,e);
                    }
                Some(e) => {
                    match e.get_mut(&sortk[..]) {
                        None => {
                            let e = match parent_edges.get_mut(&puid) {
                                None => {panic!("logic error in parent_edges get_mut()");},
                                Some(e) => e
                            };
                            e.insert(sortk,vec![cuid]);
                            }
                        Some(c) => {
                            c.push(cuid);
                            }
                        }
                }
            }
        })
        .await;
        
        

    // println!("len parent_edges {} nodes {}",parent_edges.len(), parent_node.len())   ;
    // let mut i = 0;
    // for p in parent_node {
    //     i+=1;
    //     if i == 9 {
    //       break
    //     }
    //     for (_,v) in &parent_edges[&p] {
    //             println!("len edges = [{}]",v.len());
    //     }
    // }

    // let ovb_hdrs : Vec<ovb_hdr> = vec![];
    // let ovbs : Vec<OvBatch> = vec![];
    let mut tasks : usize = 0;
    let (prod_ch, mut task_rx) = tokio::sync::mpsc::channel::<bool>(MAX_TASKS); 
    let (retry_send_ch, mut retry_rx) = tokio::sync::mpsc::channel(MAX_TASKS);
    
    for puid in parent_node {

        // remove value from HashMap and move it into tokio task
        // if not removed then ref is passed to task which may outlive parent_edges compiler thinks.
        let edges = match parent_edges.remove(&puid) {
            None =>  {panic!("logic error. No entry found in parent_edges");},
            Some(edges) => edges
        };
        //  println!("len edges {}   edges.len {}",puid.to_string(), edges.len());
        //  for (k,v) in &edges {
        //      println!("k,v edges puid {} sk {}  len {}",puid.to_string(),k,v.len());
        //  }
        //let edges = parent_edges.remove(&puid).unwrap();
        let task_ch = prod_ch.clone();
        let dyn_client = dynamo_client.clone();
        let retry_ch = retry_send_ch.clone();
        let graph_snm=(&graph_short_nm[..graph_short_nm.len()-1]).to_string(); //graph_short_nm.clone();
        let ty_c = type_caches.ty_c.clone();
        let attr_ty_s = type_caches.attr_ty_s.clone();
        let ty_short_nm = type_caches.ty_short_nm.clone();
        let ty_long_nm = type_caches.ty_long_nm.clone();
        tasks+=1;
        
        // spawn tokio task - upto MAX_TASKS concurrent
        tokio::spawn( async move {
            // puid, edges moved in
            println!("new task for puid {}",puid.to_string());
            
            // find type of puid . use sk "m|T#"  <graph>|<T,partition># //TODO : type short name should be in mysql table - saves fetching here.
            let mut sk_for_type : String = graph_snm.clone();
            sk_for_type.push_str("|T#");

            let result = dyn_client
                        .get_item()
                        .table_name("RustGraph.dev.2")       
                        .key("PK",AttributeValue::B(Blob::new(puid.clone())))
                        .key("SK",AttributeValue::S(sk_for_type.clone()))
                        .projection_expression("Ty")
                        .send()
                        .await;
            
            // if let Some(items) = results.items {
            //     let ty_names_v: Vec<TyName> = items.iter().map(|v| v.into()).collect();
            //     ty_shortlong_names.0 = ty_names_v;
            // }
    
            // alternative to using From<&HashMap<String, AttributeValue>> Trait - single item only result            
            let node_type_short_nm_av = match result {
                Err(e) => panic!("get node type: no item found: expected a type value for node. Error: {}",e),
                Ok(output) => { match output.item {
                                 None => panic!("get node type: no item found for SK [{}]",sk_for_type),
                                 Some(mut item) => {
                                     match item.remove("Ty") {
                                         None => panic!("get node type: no Ty attribute found"),
                                         Some(v) => v
                                     }
                                   }
                                } 
                            } 
                    };
            
           // println!("node type: {:?}",node_type_short_nm_av);
            let node_type_short_nm = match node_type_short_nm_av {
                AttributeValue::S(s) => s,
                _ => panic!("should be an AttributeValue"),
            };
            let node_type_long_nm = ty_long_nm.get(&node_type_short_nm[..]).unwrap();
            let mut graph_node_type_short_nm = graph_snm.clone();
            graph_node_type_short_nm.push('|');
            graph_node_type_short_nm.push_str(&node_type_short_nm[..]);

            let mut item : HashMap<SortK, EdgeAttr> = HashMap::new();
              
            for (sk,children) in edges {
            
                
                // from sk, assemble edge attribute long and short names
                let mut edge_attr_long_nm : &str = "";
                let edge_attr_short_nm = &sk.split("#").last().unwrap()[1..];   // A#G#:A -> "A"
                // get attr long name from TyC
                let c = ty_c.get(&node_type_long_nm[..]).unwrap();
                for attr in &c.0 {
                    if attr.c == edge_attr_short_nm {
                        edge_attr_long_nm = &attr.name[..]
                    }
                }
                if edge_attr_long_nm == "" {
                    panic!("could not find [{}] in AttyD",edge_attr_short_nm)
                }
                let mut graph_edge_attr_long_nm = graph_snm.clone();
                 graph_edge_attr_long_nm.push('|');
                 graph_edge_attr_long_nm.push_str(edge_attr_long_nm);
                 graph_edge_attr_long_nm.push('|');
                 graph_edge_attr_long_nm.push_str(&node_type_short_nm[..]);
                
                // for each  child node
                for cuid in children {
                
                    item
                    .entry(sk.clone())       // add sk key
                    .and_modify(|e| {        // &mut v  EdgeAttr 
                        
                        e.n+=1;
                        
                        if e.n <= EMBEDDED_CHILD_NODES {
                        
                            e.nd.push(AttributeValue::B(Blob::new(cuid.clone())));
                            e.xf.push(AttributeValue::N(CHILD_UID.to_string()));
                            //e.id.push(AttributeValue::N(0.to_string()));
                            e.id.push(0);
                            
                        } else {
                        
                            if e.ovbs.len() < MAX_OV_BLOCKS {
                            
                                if e.ovbs.len() == 0 {
                                        
                                        // no ovbs exists, create one & first batch
                                        let ovb = Uuid::new_v4();
                                        // add to node edge
                                        e.nd.push(AttributeValue::B(Blob::new(ovb)));
                                        e.xf.push(AttributeValue::N(OV_BLOCK_UID.to_string()));
                                        e.id.push(1);
                                        e.ovbs.push(vec![OvBatch{pk:ovb, nd:vec![AttributeValue::B(Blob::new(cuid.clone()))], xf:vec![AttributeValue::N(CHILD_UID.to_string())]}]);
                                        e.ovb_idx=0; // current ovbs index
                                        //println!("new ovbs and batch  should be 1 = {}",e.ovbs[e.ovb_idx].len());
    
                                } else {
                                
                                    // add data to current batch until max batch sized reached in which case create new batch
                                    // until max batch threshold reached in which case create new ovb
                                    
                                    let cur_batch_idx = e.ovbs[e.ovb_idx].len()-1;
                                    if e.ovbs[e.ovb_idx].get(cur_batch_idx).unwrap().nd.len() < OV_MAX_BATCH_SIZE {
                                     
                                        // append to batch
                                        //let cur_batch = e.ovbs[e.ovb_idx].len()-1;
                                        let batch = e.ovbs[e.ovb_idx].get_mut(cur_batch_idx).unwrap();
                                        batch.nd.push(AttributeValue::B(Blob::new(cuid.clone())));
                                        batch.xf.push(AttributeValue::N(CHILD_UID.to_string()));
                                       
                                    } else {
                                        
                                        // create new batch as max size reaced for current batch.                                    
                                        let cur_batch = e.ovbs[e.ovb_idx].len()-1;
                                        let ovbuid = e.ovbs[e.ovb_idx].get(cur_batch).unwrap().pk;
                                        
                                        if e.ovbs[e.ovb_idx].len() < OV_BATCH_THRESHOLD {
                                                    
                                            // create new batch
                                            //e.ovbs[e.ovb_idx].push(OvBatch{pk:cur_batch.pk, nd:vec![AttributeValue::B(Blob::new(cuid.clone()))], xf:vec![AttributeValue::N(CHILD_UID.to_string())]});
                                            e.ovbs[e.ovb_idx].push(OvBatch{pk:ovbuid, nd:vec![AttributeValue::B(Blob::new(cuid.clone()))], xf:vec![AttributeValue::N(CHILD_UID.to_string())]});
                                            e.id[e.ovb_idx+EMBEDDED_CHILD_NODES as usize]+=1; 
                                            
                                        } else {
                                                    
                                            // no more batches in current ovb. Create new ovb then add batch
                                            let ovb = Uuid::new_v4();
                                            // add to node edge
                                            e.nd.push(AttributeValue::B(Blob::new(ovb)));
                                            e.xf.push(AttributeValue::N(OV_BLOCK_UID.to_string()));
                                            e.id.push(1); // AttributeValue::N(0.to_string()));
                                            e.ovbs.push(vec![OvBatch{pk:ovb, nd:vec![AttributeValue::B(Blob::new(cuid.clone()))], xf:vec![AttributeValue::N(CHILD_UID.to_string())]}]);
                                            e.ovb_idx+=1;
                                        } 
                                    }
                                }
                                
                            } else {
                            
                                // choose an ovb using round robin...
                                e.ovb_idx+=1;
                                if e.ovb_idx >= MAX_OV_BLOCKS-1 {
                                    e.ovb_idx=0;
                                }
                                // add to last batch in this ovb.
                                let idx = e.ovbs[e.ovb_idx].len()-1;
    
                                if e.ovbs[e.ovb_idx].get(idx).unwrap().nd.len() < OV_MAX_BATCH_SIZE {
                                            
                                    let cur_batch = e.ovbs[e.ovb_idx].get_mut(idx).unwrap();
                                    cur_batch.nd.push(AttributeValue::B(Blob::new(cuid.clone())));
                                    cur_batch.xf.push(AttributeValue::N(OV_BLOCK_UID.to_string()));
                                    
                                } else {
                                    // add new batch to ovb
                                    e.id[e.ovb_idx]+=1;  
                                    let ovbuid = e.ovbs[e.ovb_idx].get(idx).unwrap().pk;
                                    e.ovbs[e.ovb_idx].push(OvBatch{pk:ovbuid, nd:vec![AttributeValue::B(Blob::new(cuid.clone()))], xf:vec![AttributeValue::N(CHILD_UID.to_string())]})
                                }
                            }
                        }
                    })
                    .or_insert(
                        EdgeAttr{
                            nd:vec![AttributeValue::B(Blob::new(cuid.clone()))],
                            xf:vec![AttributeValue::N(CHILD_UID.to_string())],
                            id:vec![0],
                            id_av:vec![],
                            p: graph_edge_attr_long_nm.clone(),  // m|name|P 
                            n:1,
                            ty: graph_node_type_short_nm.clone(), // P or m|P
                            //
                            ovbs : vec![],   // batch within current ovb
                            ovb_idx : 0,           // current ovb index
                        }
                    );
                }
            }
            
            // persist to database
            let mut bat_w_req: Vec<WriteRequest> = vec![];
            for (sk,mut e) in item {

                for i in e.id {
                    e.id_av.push(AttributeValue::N(i.to_string()));
                }
                
                let put =  aws_sdk_dynamodb::types::PutRequest::builder();
                let put = put.item("PK", AttributeValue::B(Blob::new(puid.clone())))
                .item("SK", AttributeValue::S(sk.clone()))
                .item("nd", AttributeValue::L(e.nd))
                .item("xf", AttributeValue::L(e.xf))
                .item("id", AttributeValue::L(e.id_av))
                .item("ty", AttributeValue::S(e.ty))
                .item("P", AttributeValue::S(e.p))
                .item("N", AttributeValue::N(e.n.to_string()));
                
                bat_w_req = save_item(&dyn_client, bat_w_req, &retry_ch, put).await;

               for ovb in e.ovbs {
                
                    let mut batch : usize = 1;
                    let pk = ovb[0].pk;  // Uuid into<Vec<u8>> ?
                                            
                    for ovbat in ovb {
                    
                        if batch == 1 {
                        
                            // OvB header
                         	let put =  aws_sdk_dynamodb::types::PutRequest::builder();
                            let put = put.item("PK",AttributeValue::B(Blob::new(pk.clone())))
                            .item("SK", AttributeValue::S("OV".to_string()))
                            .item("parent", AttributeValue::B(Blob::new(puid.clone())))
                            .item("graph", AttributeValue::S(graph_snm.clone())) ;               //TODO add graph name
                            
                            bat_w_req = save_item(&dyn_client, bat_w_req, &retry_ch, put).await;
                         
                        }
                    
                        let mut ovsk = String::from(sk.clone());
                        ovsk.push('%');
                        ovsk.push_str(&batch.to_string());

                    	let put =  aws_sdk_dynamodb::types::PutRequest::builder();
                        let put = put.item("PK", AttributeValue::B(Blob::new(pk.clone())))
                        .item("SK", AttributeValue::S(ovsk))
                        .item("nd", AttributeValue::L(ovbat.nd))
                        .item("xf", AttributeValue::L(ovbat.xf));
                        
                        bat_w_req = save_item(&dyn_client, bat_w_req, &retry_ch, put).await;
                        
                        batch+=1;
                    }
 
                }
                // persist remaining 
                bat_w_req = persist_dynamo_batch(&dyn_client, bat_w_req, &retry_ch).await;
            }
        
            if let Err(e) = task_ch.send(true).await {
                panic!("error sending on channel task_ch - {}",e);
            }
        });
        
        if tasks >= MAX_TASKS {
            // wait for a task to finish...
            task_rx.recv().await;
            tasks-=1;
        }
    }
    println!("*** Duration waiting for services to finish: {:?} secs", Instant::now().duration_since(start_2).as_secs());
    // wait for all tasks to finish
    while tasks != 0 {
        task_rx.recv().await;
        tasks-=1;
    }       
    println!("*** Duration after types loaded: {:?} secs", Instant::now().duration_since(start_2).as_secs());
    println!("*** Duration: {:?} secs", Instant::now().duration_since(start_1).as_secs());
    
    Ok(())
}

async fn save_item(
    dyn_client: &DynamoClient,
    mut bat_w_req: Vec<WriteRequest>,
    retry_ch: &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    put : PutRequestBuilder ) -> Vec<WriteRequest> {
                        
    match put.build() {
        Err(err) => {
                    println!("error in write_request builder: {}",err);
            }
        Ok(req) =>  {
                    bat_w_req.push(WriteRequest::builder().put_request(req).build());
            }
        } 
        if bat_w_req.len() == DYNAMO_BATCH_SIZE {
           bat_w_req = persist_dynamo_batch(dyn_client, bat_w_req, retry_ch).await;
        }
        bat_w_req
}

async fn persist_dynamo_batch(
    dyn_client: &DynamoClient,
    bat_w_req: Vec<WriteRequest>,
    retry_ch: &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
) -> Vec<WriteRequest> {
    
    let bat_w_outp = dyn_client
        .batch_write_item()
        .request_items("RustGraph.dev.2", bat_w_req)
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
                for (k, v) in resp.unprocessed_items.unwrap() {
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

          
    