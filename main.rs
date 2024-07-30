//#[deny(unused_imports)]
//#[warn(unused_imports)]
#[allow(unused_imports)]

use std::collections::HashMap;
use std::string::String;
//use std::sync::Arc;

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::types::{WriteRequest,PutRequest};
use aws_sdk_dynamodb::types::builders::PutRequestBuilder;
//use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
//use aws_smithy_runtime_api::client::result::SdkError;

use uuid::Uuid;

use mysql_async::prelude::*;

use tokio::time::{sleep, Duration, Instant};
//use tokio::task::spawn;

mod types; 


const	CHILD_UID         : u8 = 1;
const	_UID_DETACHED      : u8 = 3; // soft delete. Child detached from parent.
const	OV_BLOCK_UID      : u8 = 4; // this entry represents an overflow block. Current batch id contained in Id.
const	OV_BATCH_MAX_SIZE : u8 = 5; // overflow batch reached max entries - stop using. Will force creating of new overflow block or a new batch.
const	_EDGE_FILTERED     : u8 = 6; // set to true when edge fails GQL uid-pred  filter
const   _DYNAMO_BATCH_SIZE: usize = 25;
const   MAX_TASKS : usize = 1;

const  LS : u8 = 1;
const  LN : u8 = 2;
const  LB : u8 = 3;
const  LBL : u8 = 4;
const  _LDT : u8 = 5;


    // ==============================================================================
    // Overflow block properties - consider making part of a graph type specification
    // ==============================================================================

 	// EMBEDDED_CHILD_NODES - number of cUIDs (and the assoicated propagated scalar data) stored in the paraent uid-pred attribute e.g. A#G#:S.
	// All uid-preds can be identified by the following sortk: <partitionIdentifier>#G#:<uid-pred-short-name>
	// for a parent with limited amount of scalar data the number of embedded child uids can be relatively large. For a parent
	// node with substantial scalar data this parameter should be corresponding small (< 5) to minimise the space consumed
	// within the parent block. The more space consumed by the embedded child node data the more RCUs required to read the parent Node data,
	// which will be an overhead in circumstances where child data is not required.
const	EMBEDDED_CHILD_NODES : usize = 4;//10; // prod value: 20
	
	// MAX_OV_BLOCKS - max number of overflow blocks. Set to the desired number of concurrent reads on overflow blocks ie. the degree of parallelism required. Prod may have upto 100.
	// As each block resides in its own UUID (PKey) there shoud be little contention when reading them all in parallel. When max is reached the overflow
	// blocks are then reused with new overflow items (Identified by an ID at the end of the sortK e.g. A#G#:S#:N#3, here the id is 3)  being added to each existing block
	// There is no limit on the number of overflow items, hence no limit on the number of child nodes attached to a parent node.
const	MAX_OV_BLOCKS : usize = 5; // prod value : 100

	// OV_MAX_BATCH_SIZE - number of uids to an overflow batch. Always fixed at this value.
	// The limit is checked using the database SIZE function during insert of the child data into the overflow block.
	// An overflow block has an unlimited number of batches.
const	OV_MAX_BATCH_SIZE : usize = 4;//15; // Prod 100 to 500.

	// OV_BATCH_THRESHOLD, initial number of batches in an overflow block before creating new Overflow block.
	// Once all overflow blocks have been created (MAX_OV_BLOCKS), blocks are randomly chosen and each block
	// can have an unlimited number of batches.
const	OV_BATCH_THRESHOLD : usize = 4; //100

type SortK = String;
type Cuid = Uuid;
type Puid = Uuid;


// Overflow Block (Uuids) item. Include in each propagate item. 
// struct OvB {
//      ovb: Vec<AttributeValue>, //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
//      xf: Vec<AttributeValue>, // used in uid-predicate 3 : ovefflow UID, 4 : overflow block full
// }


struct ReverseEdge {
    pk : AttributeValue,            // cuid
    sk: AttributeValue ,            // R#sk-of-parent|x    where x is 0 for embedded and non-zero for batch id in ovb
    tuid: AttributeValue,           // target-uuid, either parent-uuid for embedded or ovb uuid
} 
//
struct OvBatch {
    pk  : Uuid,                     // ovb Uuid
    //
    nd: Vec<AttributeValue>,        //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
    xf: Vec<AttributeValue>,        // used in uid-predicate 1 : c-UID, 2 : c-UID is soft deleted, 3 : ovefflow UID, 4 : overflow block ful
}

struct ParentEdge {
    //
     nd: Vec<AttributeValue>,       //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
     xf: Vec<AttributeValue>,       // used in uid-predicate 1 : c-UID, 2 : c-UID is soft deleted, 3 : ovefflow UID, 4 : overflow block full
     id: Vec<u32>,                  // most recent batch in overflow
     //
     ty : String,                   // node type m|P
     p  : String,                   // edge predicate (long name) e.g. m|actor.performance - indexed in P_N
     cnt: usize,                      // number of edges < 20 (MaxChildEdges)
     rrobin_alloc : bool,               // round robin ovb allocation applies (initially false)
     eattr_nm: String,              // edge attribute name (derived from sortk)
     eattr_sn: String,              // edge attribute short name (derived from sortk)  
     //
     ovb_idx: usize,                // last ovb populated
     ovbs : Vec<Vec<OvBatch>>,      //  each ovb is made up of batches. each ovb simply has a different pk - a batch shares the same pk.
     //
     rvse: Vec<ReverseEdge>,
}

struct PropagateScalar {
    entry : Option<u8>,
    sk    : String, 
    //
    ls:  Vec<AttributeValue>,
    ln:  Vec<AttributeValue>,       // merely copying values so keep as Number datatype (no conversion to i64,f64)
    lbl: Vec<AttributeValue>,
    lb:  Vec<AttributeValue>,
    ldt: Vec<AttributeValue>,
}


enum Operation {
    Attach(ParentEdge),
    Propagate(PropagateScalar)
}




#[::tokio::main]
async fn main()  -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {

    let _start_1 = Instant::now();  
    // ==============================================================================
    // Create a Dynamodb Client
    // ==============================================================================
    let config = aws_config::from_env().region("us-east-1").load().await;
    let dynamo_client = DynamoClient::new(&config);
    let graph = "Movies".to_string();
    // ==============================================================================
    // Fetch Graph Types from MySQ based on graph name
    // ==============================================================================
    let (node_types, graph_prefix_wdot) = types::fetch_graph_types(&dynamo_client, graph).await?; 

    println!("Node Types:");
    // let nodetypes = type_caches.node_types.clone();
    //for t in ty_r.0.iter() {
    for t in node_types.0.iter() {
        println!("Node type {} [{}]    reference {}",t.get_long(),t.get_short(),t.is_reference());
        for attr in t {
            println!("attr.name [{}] dt [{}]  c [{}]",attr.name,attr.dt, attr.c);
        }
    }
    // ==============================================================================
    // Setup a MySQL connection pool
    // ==============================================================================
    let pool_opts = mysql_async::PoolOpts::new()
        .with_constraints(mysql_async::PoolConstraints::new(5, 30).unwrap())
        .with_inactive_connection_ttl(Duration::from_secs(60));

    let host = "mysql8.???????.us-east-1.rds.amazonaws.com";
    let mysql_pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::default()
            //.from_url(url)
            .ip_or_hostname(host)
            .user(Some("????"))
            .pass(Some("??????"))
            .db_name(Some("??????"))
            .pool_opts(pool_opts),
    );
    let pool = mysql_pool.clone();
    let mut conn = pool.get_conn().await.unwrap();
    
    // =================================================================================
    // MySQL query: parent nodes order by number of child connections descending
    // =================================================================================
    let mut parent_node : Vec<Uuid> =  vec![];

    let parent_edge = "SELECT Uid FROM Edge_test order by cnt desc"
        .with(())
        .map(&mut conn, |puid| parent_node.push(puid) )
        .await;
    // =================================================================================
    // MySQL query: graph edges by child uuid and sort key value (edge)
    // =================================================================================
    let mut parent_edges : HashMap<Puid, HashMap<SortK, Vec<Cuid>>> = HashMap::new();
    let child_edge = "Select puid,sortk,cuid from test_childedge order by puid,sortk"        
        .with(())
        .map(&mut conn, |(puid,sortk ,cuid) : (Uuid,String,Uuid)| {
            // this version requires no allocation (cloning) of sortk
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
        
    //TODO: check child_edge error??

    // let ovb_hdrs : Vec<ovb_hdr> = vec![];
    // let ovbs : Vec<OvBatch> = vec![];
    let mut tasks : usize = 0;
    let (prod_ch, mut task_rx) = tokio::sync::mpsc::channel::<bool>(MAX_TASKS); 
    let (retry_send_ch, mut retry_rx) = tokio::sync::mpsc::channel::<Vec<aws_sdk_dynamodb::types::WriteRequest>>(MAX_TASKS);
    let mut i = 0;
    
    // ==================================================
    // process nodes in order of most edges first
    // ==================================================
    for puid in parent_node {

            i+=1;
            if i == 250 {
                panic!("exit now...");
            }
        // remove value from HashMap and move it into tokio task
        // if not removed then ref is passed to task which may outlive parent_edges compiler thinks.
        let sk_edges = match parent_edges.remove(&puid) {
                            None =>  {panic!("logic error. No entry found in parent_edges");},
                            Some(e) => e
                        };

        let task_ch = prod_ch.clone();
        let dyn_client = dynamo_client.clone();
        let retry_ch = retry_send_ch.clone();
        let graph_sn = graph_prefix_wdot.trim_end_matches('.').to_string();
        let node_types = node_types.clone();
        tasks+=1;
        
        //spawn tokio task - upto MAX_TASKS concurrent
        tokio::spawn( async move {
        
            // ============================================================================
            // find type of puid . use sk "m|T#"  <graph>|<T,partition># //TODO : type short name should be in mysql table - saves fetching here.
            let p_node_ty = fetch_node_type(&dyn_client, &puid, &graph_sn, &node_types).await;
            // =============================================================================

            //let node_ty_nm = ty_long_nm.get(&node_ty_sn).unwrap();
            // =========================================
            //  m|P
            let mut ty_attr = graph_sn.clone();
                    ty_attr .push('|');
                    ty_attr .push_str(&p_node_ty.short_nm());

            // ==================================================
            // 1. for each parent (p) node edge 
            // =================================================
            // Container for Overflow Block Uuids. Stored with all propagated items in OvB attribute.
            let mut ovbs_ppg : Vec<AttributeValue> = vec![];
            let mut items : HashMap<SortK, Operation > = HashMap::new();
                
            // ==========================
            // attach child nodes
            // ==========================
            for p_sk in sk_edges.keys() { 
            
                // ============================================================================    
                // 1.1 initialisation based on p_sk
                // ============================================================================
                // 1.2 initilisation for attaching edge: (parent-edge-attribute) <- child node

                let v_edge = match items.get_mut(p_sk) {
                
                        None => {
                                let edge_attr_sn = &p_sk[p_sk.rfind(':').unwrap()+1..];  // A#G#:A -> "A"

                                //let edge_attr_nm = ty_c.get_attr_nm(&node_ty_nm[..],edge_attr_sn);
                                let edge_attr_nm = p_node_ty.get_attr_nm(edge_attr_sn);
                
                                // RustGraph Data model, P attribute e.g "m|Film.Director|P" - used as partition key in global indexes P_S, P_N, P_B
                                let mut p_attr = graph_sn.clone();
                                        p_attr.push('|');
                                        p_attr.push_str(edge_attr_nm);
                                        p_attr.push('|');
                                        p_attr.push_str(&p_node_ty.short_nm()); 
                                let pe = ParentEdge {
                                                nd:vec![],  //AttributeValue::B(Blob::new(cuid))],
                                                xf:vec![],  //AttributeValue::N(CHILD_UID.to_string())],
                                                id:vec![],
                                                //
                                                p: p_attr.to_owned(),      // m|edge-attr-name|P 
                                                cnt:0,
                                                ty: ty_attr.to_owned(),    // P or m|P
                                                rrobin_alloc: false,
                                                eattr_nm: edge_attr_nm.to_string(),
                                                eattr_sn: edge_attr_sn.to_string(),
                                                //
                                                ovbs : vec![],                      // batch within current ovb
                                                ovb_idx : 0,                        // current ovb index
                                                //
                                                rvse : vec![],
                                                };
                                items.insert(p_sk.clone(), Operation::Attach(pe));  
                                items.get_mut(p_sk).unwrap()
                                },
                                
                        Some(v) => {v},
                };
                // ==========================
                // ** e ** map - attach to e
                // ==========================
                let Operation::Attach(ref mut e) = v_edge  else {panic!("expected Operation::Attach") };       
                let edge_attr_nm = &e.eattr_nm[..];
                // let edge_attr_sn = &e.eattr_sn[..];

                // =====================================================
                // 1.4 attach child nodes
                // =====================================================
                let Some(children) = sk_edges.get(p_sk) else {panic!("main: data error - no sk  [{}] found in sk_edgs",&p_sk)};
                for cuid in children {
                    
                    let cuid_p = cuid.clone();
                    let child_ty = node_types.get(p_node_ty.get_edge_child_ty(edge_attr_nm));
                        
                    // ===========================================================
                    // 1.4.1 attach child node using overflow blocks if necessary
                    // ===========================================================
                    e.cnt+=1;
                                        
                    if e.cnt <= EMBEDDED_CHILD_NODES {
                                        
                        e.nd.push(AttributeValue::B(Blob::new(cuid.clone())));
                        e.xf.push(AttributeValue::N(CHILD_UID.to_string()));
                        e.id.push(0);
                        
                        // reverse edge - only for non-reference nodes - used to update xf in parent edge when child is deleted
                        // create for attach only. Not necessary for propagation as this reverse entry will suffice to update parents.
                        if !child_ty.is_reference()  {
                            let mut r_sk = "R#".to_string();
                                r_sk.push_str(&p_sk[p_sk.find('#').unwrap()+1..]);
                            let r = ReverseEdge {
                                pk : AttributeValue::B(Blob::new(cuid.clone())),
                                sk : AttributeValue::S(r_sk),
                                tuid: AttributeValue::B(Blob::new(puid.clone())),
                            };
                            e.rvse.push(r);
                        }
                          
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
                                e.ovbs.push(vec![OvBatch{pk:ovb, 
                                                         nd:vec![AttributeValue::B(Blob::new(cuid.to_owned()))], 
                                                         xf:vec![AttributeValue::N(CHILD_UID.to_string())],
                                                        }]
                                            );
                                e.ovb_idx=0; 
                                
                                // reverse edge - used to update xf in parent edge when child is deleted
                                 if !child_ty.is_reference()   {
                                    let mut r_sk = "R#".to_string();
                                        r_sk.push_str(&p_sk[p_sk.find('#').unwrap()+1..]);
                                        r_sk.push_str("%1");
                                    let r = ReverseEdge {
                                        pk : AttributeValue::B(Blob::new(cuid.clone())),
                                        sk : AttributeValue::S(r_sk),
                                        tuid: AttributeValue::B(Blob::new(ovb.clone())),
                                    };
                                    e.rvse.push(r);
                                }
                    
                            } else {
                                                
                                // add data to current batch until max batch size reached. After max batch size reached create new batch
                                // until max batch threshold reached in which case create new ovb
                                let cur_batch_idx = e.ovbs[e.ovb_idx].len()-1; // last batch created
                                if e.ovbs[e.ovb_idx].get(cur_batch_idx).unwrap().nd.len() < OV_MAX_BATCH_SIZE {
                                                     
                                    // append to batch
                                    let batch = e.ovbs[e.ovb_idx].get_mut(cur_batch_idx).unwrap();
                                    batch.nd.push(AttributeValue::B(Blob::new(cuid.to_owned())));
                                    batch.xf.push(AttributeValue::N(CHILD_UID.to_string()));
                                    
                                    // reverse edge
                                    if !child_ty.is_reference()   {
                                        let mut r_sk = "R#".to_string();
                                                r_sk.push_str(&p_sk[p_sk.find('#').unwrap()+1..]);
                                                r_sk.push_str("%");
                                                r_sk.push_str(&(cur_batch_idx+1).to_string());
                                        let r = ReverseEdge {
                                                pk : AttributeValue::B(Blob::new(cuid.clone())),
                                                sk : AttributeValue::S(r_sk),
                                                tuid: ovbs_ppg[e.ovb_idx].clone(),
                                        };
                                        e.rvse.push(r);
                                    }
                                                       
                                } else {
                                                        
                                    // max batch size reaced - creat new batch - check thresholds first though
                                    // as new batch may be added to ovb on rr basis.
                                    if e.ovbs.len() == MAX_OV_BLOCKS  && e.ovbs[e.ovb_idx].len() == OV_BATCH_THRESHOLD {
                                        
                                        e.rrobin_alloc=true;     // round-robin allocation now applies for all future attach-node 
                                        e.ovb_idx=0;
                                        
                                        // create new batch to first ovb and attach node
                                        let cur_batch_idx = e.ovbs[e.ovb_idx].len()-1;
                                        let ovbuid = e.ovbs[e.ovb_idx].get(cur_batch_idx).unwrap().pk;
                                        e.ovbs[e.ovb_idx].push(OvBatch{pk:ovbuid, 
                                                                       nd:vec![AttributeValue::B(Blob::new(cuid.to_owned()))], 
                                                                       xf:vec![AttributeValue::N(CHILD_UID.to_string())],
                                                                       });
                                        e.id[e.ovb_idx+EMBEDDED_CHILD_NODES as usize]+=1; 
                                        // reverse edge
                                        if !child_ty.is_reference()    {
                                            let mut r_sk = "R#".to_string();
                                                r_sk.push_str(&p_sk[p_sk.find('#').unwrap()+1..]);
                                                r_sk.push_str("%");
                                                r_sk.push_str(&(cur_batch_idx+1).to_string());
                                            let r = ReverseEdge {
                                                pk : AttributeValue::B(Blob::new(cuid.clone())),
                                                sk : AttributeValue::S(r_sk),
                                                tuid: ovbs_ppg[e.ovb_idx].clone(),
                                            };
                                            e.rvse.push(r);
                                        }
                                          
                                    } else {
                                    
                                        let cur_batch_idx = e.ovbs[e.ovb_idx].len()-1;
                                        let ovbuid = e.ovbs[e.ovb_idx].get(cur_batch_idx).unwrap().pk;
                                                            
                                        if e.ovbs[e.ovb_idx].len() < OV_BATCH_THRESHOLD {
                                                                        
                                            // create new batch
                                            e.ovbs[e.ovb_idx].push(OvBatch{pk:ovbuid, 
                                                                           nd:vec![AttributeValue::B(Blob::new(cuid.to_owned()))], 
                                                                           xf:vec![AttributeValue::N(CHILD_UID.to_string())],
                                                                           });
                                            e.id[e.ovb_idx+EMBEDDED_CHILD_NODES as usize]+=1; 
                                            
                                            // reverse edge
                                            if !child_ty.is_reference()    {
                                                let mut r_sk = "R#".to_string();
                                                    r_sk.push_str(&p_sk[p_sk.find('#').unwrap()+1..]);
                                                    r_sk.push_str("%");
                                                    r_sk.push_str(&(cur_batch_idx+1).to_string());
                                                let r = ReverseEdge {
                                                    pk : AttributeValue::B(Blob::new(cuid.clone())),
                                                    sk : AttributeValue::S(r_sk),
                                                    tuid: ovbs_ppg[e.ovb_idx].clone(),
                                                };
                                                e.rvse.push(r);
                                            }
                                                                
                                        } else {
                                                 
                                            // no more batches allowed in latest ovb. Create new ovb and add batch
                                            let ovbuid = Uuid::new_v4();
                                            // add to ovbs & node edge
                                            ovbs_ppg.push(AttributeValue::B(Blob::new(ovbuid.clone())));
                                            e.nd.push(AttributeValue::B(Blob::new(ovbuid)));
                                            e.xf.push(AttributeValue::N(OV_BLOCK_UID.to_string()));
                                            e.id.push(1); // AttributeValue::N(0.to_string()));
                                            e.ovbs.push(vec![OvBatch{pk:ovbuid, 
                                                                     nd:vec![AttributeValue::B(Blob::new(cuid.to_owned()))], 
                                                                     xf:vec![AttributeValue::N(CHILD_UID.to_string())],
                                                                     }],
                                                        );
                                            // move to next ovb when adding next batch
                                            e.ovb_idx+=1;
                                            // reverse edge
                                            if !child_ty.is_reference()   {
                                                let mut r_sk = "R#".to_string();
                                                    r_sk.push_str(&p_sk[p_sk.find('#').unwrap()+1..]);
                                                    r_sk.push_str("%1");
                                                let r = ReverseEdge {
                                                    pk : AttributeValue::B(Blob::new(cuid.clone())),
                                                    sk : AttributeValue::S(r_sk),
                                                    tuid: ovbs_ppg[e.ovb_idx].clone(),
                                                };
                                                e.rvse.push(r);
                                            }
                                        } 
                                    }
                                }
                            }
                                                
                        } else {
                                            
                            // no more ovbs allowed. choose from existing ovbs, using round robin...
                            e.ovb_idx+=1;
                            if e.ovb_idx == MAX_OV_BLOCKS {
                                e.ovb_idx = 0;
                            }
                            // add to last batch in this ovb.
                            let cur_batch_idx = e.ovbs[e.ovb_idx].len()-1; //e.ovbs[e.ovb_idx].id
                    
                            if e.ovbs[e.ovb_idx].get(cur_batch_idx).unwrap().nd.len() < OV_MAX_BATCH_SIZE {
                                                            
                                let cur_batch = e.ovbs[e.ovb_idx].get_mut(cur_batch_idx).unwrap();
                                cur_batch.nd.push(AttributeValue::B(Blob::new(cuid.to_owned())));
                                cur_batch.xf.push(AttributeValue::N(CHILD_UID.to_string()));
                                
                                // reverse edge
                                if !child_ty.is_reference()   {
                                    let mut r_sk = "R#".to_string();
                                            r_sk.push_str(&p_sk[p_sk.find('#').unwrap()+1..]);
                                            r_sk.push_str("%");
                                            r_sk.push_str(&(cur_batch_idx+1).to_string());
                                    let r = ReverseEdge {
                                                pk : AttributeValue::B(Blob::new(cuid.clone())),
                                                sk : AttributeValue::S(r_sk),
                                                tuid: ovbs_ppg[e.ovb_idx].clone(),
                                            };
                                    e.rvse.push(r);
                                }
                                                    
                            } else {
                            
                                // all batches in ovb full, add new batch...
                                let ovbuid = e.ovbs[e.ovb_idx].get(cur_batch_idx).unwrap().pk;
                                e.ovbs[e.ovb_idx].push(OvBatch{
                                                    pk:ovbuid, 
                                                    nd:vec![AttributeValue::B(Blob::new(cuid.to_owned()))], 
                                                    xf:vec![AttributeValue::N(CHILD_UID.to_string())],
                                                    });
                                e.id[e.ovb_idx+EMBEDDED_CHILD_NODES as usize]+=1; 
                                
                                // reverse edge
                                if !child_ty.is_reference()    {
                                    let mut r_sk = "R#".to_string();
                                        r_sk.push_str(&p_sk[p_sk.find('#').unwrap()+1..]);
                                        r_sk.push_str("%");
                                        r_sk.push_str(&(e.id[e.ovb_idx+EMBEDDED_CHILD_NODES as usize].to_string()));
                                    let r = ReverseEdge {
                                                pk : AttributeValue::B(Blob::new(cuid.clone())),
                                                sk : AttributeValue::S(r_sk),
                                                tuid: ovbs_ppg[e.ovb_idx].clone(),
                                            };
                                    e.rvse.push(r);
                                }
                            }
                        }
                    }
                }
            }

            println!("== persist ===================================");
            persist(                      
                &dyn_client, 
                puid.clone(), 
                graph_sn.as_str(), 
                &retry_ch,
                &task_ch,
                &ovbs_ppg,
                items,
            ).await;

            let mut items : HashMap<SortK, Operation > = HashMap::new();
            // ==================================================
            // 1. propagate child scalar data to parent
            // =================================================
            for (sk_p, children) in sk_edges { // consume sk_edges now that attach has been done

                let edge_attr_sn = &sk_p[sk_p.rfind(':').unwrap()+1..];  // A#G#:A -> "A"

                let edge_attr_nm = p_node_ty.get_attr_nm(edge_attr_sn);
                
               for cuid in children {

                    let cuid_p = cuid.clone();
                    //let child_ty = ty_c.get_edge_child_ty(&node_ty_nm, edge_attr_nm);
                    //let child_parts = ty_c.get_scalar_partitions(child_ty);
                    let child_ty = node_types.get(p_node_ty.get_edge_child_ty(edge_attr_nm));
                    let child_parts = child_ty.get_scalar_partitions();             
                   // =====================================================================
                   // 1.4.2.1 for each child node's scalar partitions and scalar attributes
                   // =====================================================================                                                       
                    for (partition,attrs)  in child_parts {
                                 
                        // now propagate scalar data
                        // generate sortk's for query
                        let mut sk_query = graph_sn.clone();
                                sk_query.push_str("|A#");
                                sk_query.push_str(partition.as_str());
                                                    
                        if attrs.len() == 1 {
                            sk_query.push_str("#:");
                            sk_query.push_str(attrs[0]);
                        } 

                        // ============================================================
                        // 1.4.2.2 fetch scalar data by sortk partition in child node 
                        // ============================================================ 
                        let result = dyn_client
                                    .query()
                                    .table_name("RustGraph.dev.2") 
                                    .key_condition_expression("#p = :uid and begins_with(#s,:sk_v)")
                                    .expression_attribute_names("#p",types::PK)
                                    .expression_attribute_names("#s",types::SK) 
                                    .expression_attribute_values(":uid",AttributeValue::B(Blob::new(cuid_p.clone())))
                                    .expression_attribute_values(":sk_v",AttributeValue::S(sk_query)) 
                                    .send()
                                    .await;
                                                     
                        if let Err(err) = result {
                                panic!("error in query() {}",err);
                        }
                        
                        // ============================================================
                        // 1.4.2.3 populate node cach (nc) from query result
                        // ============================================================
                        let mut nc : Vec<types::DataItem> = vec![];
                        let mut nc_attr_map : types::NodeMap = types::NodeMap(HashMap::new()); // HashMap<types::AttrShortNm, types::DataItem> = HashMap::new();
                        
                        if let Some(items) = result.unwrap().items {
                            nc = items.into_iter().map(|v| v.into()).collect();
                        } 

                        for c in nc {
                            nc_attr_map.0.insert(c.sk.attribute_sn().to_owned(), c);

                        }
                        // ===============================================================================
                        // 1.4.2.4 add scalar data for each attribute queried above to edge in items
                        // ===============================================================================
                        for attr_sn in attrs {
                            
                            // associated parent node sort key to attach child's scalar data 
                            // generate sk for propagated (ppg) data
                            let mut ppg_sk = sk_p.clone();
                                    ppg_sk.push('#');
                                    ppg_sk.push_str(partition.as_str());
                                    ppg_sk.push_str("#:");
                                    ppg_sk.push_str(attr_sn);
                                
                            //let dt = ty_c.get_attr_dt(child_ty,attr_sn);
                            let dt = child_ty.get_attr_dt(attr_sn);    
                            // check if ppg_sk in HashMap items
                            let op_ppg = match items.get_mut(&ppg_sk[..]) {  
                                    None => {  
                                            let op = Operation::Propagate(
                                                    PropagateScalar { 
                                                            entry: None,
                                                            sk : ppg_sk.clone(),
                                                            ls:  vec![],
                                                            ln:  vec![],
                                                            lbl: vec![],
                                                            lb:  vec![],
                                                            ldt: vec![],
                                                    });
                                            items.insert(ppg_sk.clone(), op);
                                            items.get_mut(&ppg_sk[..]).unwrap()
                                            },
                                    Some(es) =>  { es },
                            };
                                
                            let e_p = match op_ppg {
                                    Operation::Propagate(ref mut e_) =>  {e_},
                                    _ => { panic!("Expected Operation::Propagate")},
                            };
                                
                            let Some(di) = nc_attr_map.0.remove(attr_sn) else {panic!("not found in nc_attr_map [{}]",ppg_sk)};
                              
                            match dt {
                            
                                "S" => { match di.s {
                                                    None => {
                                                            if !child_ty.is_atttr_nullable(attr_sn) { 
                                                                panic!("Data Error: Attribute {} in type {} is not null but null returned from db",attr_sn,child_ty.long_nm())
                                                            }
                                                            e_p.ls.push(AttributeValue::Null(true)); 
                                                            },
                                                    Some(v) => {
                                                            e_p.ls.push(AttributeValue::S(v))                                                        
                                                            },
                                                    }
                                                 e_p.entry=Some(LS);
                                        },
                                            
                                "I"|"F" => { match di.n {       // no conversion into int or float. Keep as String for propagation purposes.
                                                    None => {
                                                            if !child_ty.is_atttr_nullable(attr_sn) { 
                                                                panic!("Data Error: Attribute {} in type {} is not null but null returned from db",attr_sn,child_ty.long_nm())
                                                            }
                                                            e_p.ln.push(AttributeValue::Null(true)); 
                                                            },
                                                    Some(v) => {
                                                            e_p.ln.push(AttributeValue::N(v))                                                        
                                                            },
                                                    }
                                                    e_p.entry=Some(LN);
                                            },

                                "B" => { match di.b {
                                                    None => {
                                                            if !child_ty.is_atttr_nullable(attr_sn) { 
                                                                panic!("Data Error: Attribute {} in type {} is not null but null returned from db",attr_sn,child_ty.long_nm())
                                                            }
                                                            e_p.lb.push(AttributeValue::Null(true)); 
                                                            },
                                                    Some(v) => {
                                                            e_p.lb.push(AttributeValue::B(Blob::new(v)))                                                        
                                                            },
                                                    }
                                                    e_p.entry=Some(LB);
                                        },
                                        //"DT" => e_p.ldt.push(AttributeValue::S(di.dt)), 
                                        
                                "Bl" => { match di.bl {
                                                    None => {
                                                            if !child_ty.is_atttr_nullable(attr_sn) { 
                                                                panic!("Data Error: Attribute {} in type {} is not null but null returned from db",attr_sn,child_ty.long_nm())
                                                            }
                                                            e_p.lbl.push(AttributeValue::Null(true)); 
                                                            },
                                                    Some(v) => {
                                                            e_p.lbl.push(AttributeValue::Bool(v))                                                        
                                                            },
                                                    }
                                                    e_p.entry=Some(LBL);
                                        },
                                             
                                _ => { panic!("expected Scalar Type, got [{}]",dt) },
                            }                                          
                        }
                    }
                }
            }
            persist(                      
                        &dyn_client, 
                        puid, 
                        graph_sn.as_str(), 
                        &retry_ch,
                        &task_ch,
                        &ovbs_ppg,
                        items
                ).await;
          
            if let Err(e) = task_ch.send(true).await {
                panic!("error sending on channel task_ch - {}",e);
            }
        });
        
        if tasks == MAX_TASKS {
            // wait for a task to finish...
            task_rx.recv().await;
            tasks-=1;
        }
    }
    Ok(())
}


async fn persist( 
            dyn_client : &aws_sdk_dynamodb::Client,
            puid : Uuid,
            graph_sn : &str,
            retry_ch : &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
            task_ch :  &tokio::sync::mpsc::Sender<bool>,
            ovbs_ppg : &Vec<AttributeValue>, 
            items : HashMap<SortK, Operation>,
)  {
            
            // persist to database
            let mut bat_w_req: Vec<WriteRequest> = vec![];
            
            for (sk, v) in items {
         
                match v {
                
                Operation::Attach(e) => {
                
                    println!("Persist Operation::Attach");
                    
                    let mut id_av : Vec<AttributeValue> = vec![];
                    for i in e.id {
                        id_av.push(AttributeValue::N(i.to_string()));
                    }
                    println!("Persist Operation::Attach - id len {}",id_av.len());
                    
                    let put =  aws_sdk_dynamodb::types::PutRequest::builder();
                    let put = put.item(types::PK, AttributeValue::B(Blob::new(puid.clone())))
                                 .item(types::SK, AttributeValue::S(sk.clone()))
                                 .item(types::ND, AttributeValue::L(e.nd))
                                 .item(types::XF, AttributeValue::L(e.xf))
                                 .item(types::BID, AttributeValue::L(id_av))
                                 .item(types::TY, AttributeValue::S(e.ty))
                                 .item(types::P, AttributeValue::S(e.p))
                                 .item(types::CNT, AttributeValue::N(e.cnt.to_string()));   
    
                    bat_w_req = save_item(&dyn_client, bat_w_req, retry_ch, put).await;
                    
                    // reverse edge items - only for non-reference type
                    for r in e.rvse {
                        let put = aws_sdk_dynamodb::types::PutRequest::builder();
                        let put = put.item(types::PK, r.pk)
                                     .item(types::SK, r.sk)
                                     .item(types::TUID, r.tuid);
                                     
                        bat_w_req = save_item(&dyn_client, bat_w_req, retry_ch, put).await;    
                    }
                                    
    
                    for ovb in e.ovbs {
                    
                        let mut batch : usize = 1;
                        let pk = ovb[0].pk;  // Uuid into<Vec<u8>> ?
                                                
                        for ovbat in ovb {
                        
                            if batch == 1 {
                            
                                // OvB header
                             	let put =  aws_sdk_dynamodb::types::PutRequest::builder();
                                let put = put.item(types::PK,AttributeValue::B(Blob::new(pk.clone())))
                                .item(types::SK, AttributeValue::S("OV".to_string()))
                                .item(types::PARENT, AttributeValue::B(Blob::new(puid.clone())))
                                .item(types::GRAPH, AttributeValue::S(graph_sn.to_owned())) ;               //TODO add graph name
                                
                                bat_w_req = save_item(&dyn_client, bat_w_req, &retry_ch, put).await;
                             
                            }
                        
                            let mut ovsk = String::from(sk.clone());
                            ovsk.push('%');
                            ovsk.push_str(&batch.to_string());
    
                        	let put =  aws_sdk_dynamodb::types::PutRequest::builder();
                            let put = put.item(types::PK, AttributeValue::B(Blob::new(pk.clone())))
                                         .item(types::SK, AttributeValue::S(ovsk))
                                         .item(types::ND, AttributeValue::L(ovbat.nd))
                                         .item(types::XF, AttributeValue::L(ovbat.xf));
  
                            bat_w_req = save_item(&dyn_client, bat_w_req, &retry_ch, put).await;
                            
                            batch+=1;
                        }
                    }
                },
                
                
                
                Operation::Propagate(mut e) => {

                        println!("Persist Operation::Propagate");
                    
                        let mut finished = false;
                        
                        let put =  aws_sdk_dynamodb::types::PutRequest::builder();
                        let mut put = put.item(types::PK, AttributeValue::B(Blob::new(puid.clone())))
                                     .item(types::SK, AttributeValue::S(sk.clone()));
                        
                        match e.entry.unwrap() {
                        
                        LS =>  {
                                if e.ls.len() <= EMBEDDED_CHILD_NODES {
                                    let embedded : Vec<_> = e.ls.drain(..e.ls.len()).collect();
                                    put = put.item(types::LS, AttributeValue::L(embedded));
                                    finished=true;
                                } else {
                                    let embedded : Vec<_> = e.ls.drain(..EMBEDDED_CHILD_NODES).collect();
                                    put = put.item(types::LS, AttributeValue::L(embedded));
                                }
                            },
                        LN => {
                                if e.ln.len() <= EMBEDDED_CHILD_NODES {
                                    let embedded : Vec<_> = e.ln.drain(..e.ln.len()).collect();
                                    put = put.item(types::LS, AttributeValue::L(embedded));
                                    finished=true;
                                } else {
                                    let embedded : Vec<_> = e.ln.drain(..EMBEDDED_CHILD_NODES).collect();
                                    put = put.item(types::LN, AttributeValue::L(embedded));
                                }
                            },
                        LBL => {
                               if e.lbl.len() <= EMBEDDED_CHILD_NODES {
                                    let embedded : Vec<_> = e.lbl.drain(..e.lbl.len()).collect();
                                    put = put.item(types::LS, AttributeValue::L(embedded));
                                    finished=true;
                                } else {
                                    let embedded : Vec<_> = e.lbl.drain(..EMBEDDED_CHILD_NODES).collect();
                                    put = put.item(types::LBL, AttributeValue::L(embedded));
                                }
                            },
                        LB => {
                               if e.lb.len() <= EMBEDDED_CHILD_NODES {
                                    let embedded : Vec<_> = e.lb.drain(..e.lb.len()).collect();
                                    put = put.item(types::LS, AttributeValue::L(embedded));
                                    finished=true;
                                } else {
                                    let embedded : Vec<_> = e.lb.drain(..EMBEDDED_CHILD_NODES).collect();
                                    put = put.item(types::LB, AttributeValue::L(embedded));
                                }
                            },
                        _ => { panic!("unexpected entry match in Operation::Propagate") },
                        
                        };
                        
                        bat_w_req = save_item(&dyn_client, bat_w_req, retry_ch, put).await;
                            
                        if finished {
                            continue
                        }
                           
                        let mut bid = 0;
                        
                        // =========================================
                        // distribute data across ovbs
                        // =========================================
                        for ovb in ovbs_ppg {
                            
                            for _bat in 0..OV_BATCH_THRESHOLD {
                            
                                bid += 1;
                                
                                let mut sk_w_bid = sk.clone();
                                    sk_w_bid.push('%');
                                    sk_w_bid.push_str(&bid.to_string());
                                    
                                let put =  aws_sdk_dynamodb::types::PutRequest::builder();
                                let mut put = put.item(types::PK, ovb.clone())
                                             .item(types::SK, AttributeValue::S(sk_w_bid));  //TODO: add batch id
    
                                match e.entry.unwrap() {
                                    
                                        LS =>  {
                                            if e.ls.len() <= OV_MAX_BATCH_SIZE {
                                                let batch : Vec<_> = e.ls.drain(..e.ls.len()).collect();
                                                put = put.item(types::LS, AttributeValue::L(batch));
                                                finished=true;
                                            } else {
                                                let batch : Vec<_> = e.ls.drain(..OV_MAX_BATCH_SIZE).collect();
                                                put = put.item(types::LS, AttributeValue::L(batch));
                                            }
                                          },
                                          
                                        LN => {
                                            if e.ln.len() <= OV_MAX_BATCH_SIZE {
                                                let batch : Vec<_> = e.ln.drain(..e.ln.len() ).collect();
                                                put = put.item(types::LN, AttributeValue::L(batch));
                                                finished=true;
                                            } else {
                                                let batch : Vec<_> = e.ln.drain(..OV_MAX_BATCH_SIZE).collect();
                                                put = put.item(types::LN, AttributeValue::L(batch));
                                            }
                                          },
                                          
                                        LBL => {
                                            if e.lbl.len() <= OV_MAX_BATCH_SIZE {
                                                let batch : Vec<_> = e.lbl.drain(..e.lbl.len()).collect();
                                                put = put.item(types::LBL, AttributeValue::L(batch));
                                                finished=true;
                                            } else {
                                                let batch : Vec<_> = e.lbl.drain(..OV_MAX_BATCH_SIZE).collect();
                                                put = put.item(types::LBL, AttributeValue::L(batch));
                                            }
                                          },
                                          
                                        LB => {
                                            if e.lb.len() <= OV_MAX_BATCH_SIZE {
                                                let batch : Vec<_> = e.lb.drain(..e.lb.len()).collect();
                                                put = put.item(types::LB, AttributeValue::L(batch));
                                                finished=true;
                                            } else {
                                                let batch : Vec<_> = e.lb.drain(..OV_MAX_BATCH_SIZE).collect();
                                                put = put.item(types::LB, AttributeValue::L(batch));
                                            }
                                          },
                                        _ => { panic!("unexpected entry match in Operation::Propagate") },
                                }
                                 
                                bat_w_req = save_item(&dyn_client, bat_w_req, retry_ch, put).await;
                                  
                                if finished {
                                    break
                                }
                            }
                             
                            if finished {
                                break
                            }
                        }
                        
                        if finished {
                            continue
                        }
                        
                        while !finished {
                        
                            for ovb in ovbs_ppg {
                            
                                bid += 1;
                                
                                let mut sk_w_bid = sk.clone();
                                    sk_w_bid.push('%');
                                    sk_w_bid.push_str(&bid.to_string());
                   
                                let put =  aws_sdk_dynamodb::types::PutRequest::builder();
                                let mut put = put.item(types::PK, ovb.clone())
                                                 .item(types::SK, AttributeValue::S(sk_w_bid));  
    
                                match e.entry.unwrap() {
                                    
                                        LS =>  {
                                            if e.ls.len() <= OV_MAX_BATCH_SIZE {
                                                let batch : Vec<_> = e.ls.drain(..e.ls.len() ).collect();
                                                put = put.item(types::LS, AttributeValue::L(batch));
                                                finished=true;
                                            } else {
                                                let batch : Vec<_> = e.ls.drain(..OV_MAX_BATCH_SIZE).collect();
                                                put = put.item(types::LS, AttributeValue::L(batch));
                                            }
                                          },
                                          
                                        LN => {
                                            if e.ln.len() <= OV_MAX_BATCH_SIZE {
                                                let batch : Vec<_> = e.ln.drain(..e.ln.len()).collect();
                                                put = put.item(types::LN, AttributeValue::L(batch));
                                                finished=true;
                                            } else {
                                                let batch : Vec<_> = e.ln.drain(..OV_MAX_BATCH_SIZE).collect();
                                                put = put.item(types::LN, AttributeValue::L(batch));
                                            }
                                          },
                                          
                                        LBL => {
                                            if e.lbl.len() <= OV_MAX_BATCH_SIZE {
                                                let batch : Vec<_> = e.ln.drain(..e.lbl.len()).collect();
                                                put = put.item(types::LBL, AttributeValue::L(batch));
                                                finished=true;
                                            } else {
                                                let batch : Vec<_> = e.ln.drain(..OV_MAX_BATCH_SIZE).collect();
                                                put = put.item(types::LBL, AttributeValue::L(batch));
                                            }
                                          },
                                          
                                        LB => {
                                            if e.lb.len() <= OV_MAX_BATCH_SIZE {
                                                let batch : Vec<_> = e.lb.drain(..e.lb.len()).collect();
                                                put = put.item(types::LB, AttributeValue::L(batch));
                                                finished=true;
                                            } else {
                                                let batch : Vec<_> = e.lb.drain(..OV_MAX_BATCH_SIZE).collect();
                                                put = put.item(types::LB, AttributeValue::L(batch));
                                            }
                                          },
                                        _ => { panic!("unexpected entry match in Operation::Propagate") },
                                }
                                
                                bat_w_req = save_item(&dyn_client, bat_w_req, retry_ch, put).await;
     
     
                                if finished {
                                    break
                                }
                            }
                        }
                },
                } // end match
      
                if let Err(e) = task_ch.send(true).await {
                    panic!("error sending on channel task_ch - {}",e);
                }
            } // end for
}


//struct NodeMap(HashMap<SortK, Operation >);

// snm : short name

// struct NodeType(String);

// impl From<std::collections::HashMap<String, AttributeValue>> for NodeType {
     
//      fn from(mut value: HashMap<String, AttributeValue>) -> Self {
       
//         let (k,v) = value.drain().next().unwrap();
//         return match k.as_str() {
//                 "Ty" => NodeType(types::as_string2(v).unwrap()),
//                 _ => panic!("expected Ty attribute"),
//             }
//     }
// }
// returns node type as String, moving ownership from AttributeValue - preventing further allocation.
async fn fetch_node_type<'a, T: Into<String>>(
                            dyn_client : &DynamoClient,
                            uid : &Uuid,
                            graph_sn: T,
                            node_types: &'a types::NodeTypes
) -> &'a types::NodeType {
    
    let mut sk_for_type : String = graph_sn.into();
    sk_for_type.push_str("|T#"); 
    
    let result = dyn_client
                .get_item()
                .table_name("RustGraph.dev.2")       
                .key(types::PK,AttributeValue::B(Blob::new(uid.clone())))
                .key(types::SK,AttributeValue::S(sk_for_type))
                .projection_expression("Ty")
                .send()
                .await;
                
    if let Err(err) = result {
        panic!("get node type: no item found: expected a type value for node. Error: {}",err)
    }          
    let nd : NodeType = match result.unwrap().item {
            None => panic!("No type item found in fetch_node_type() for [{}]",uid),
            Some(v) => v.into(),
    };
    node_types.get(&nd.0)
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
        bat_w_req = print_batch(bat_w_req);
                  
        // if bat_w_req.len() == DYNAMO_BATCH_SIZE {
        //     // =================================================================================
        //     // persist to Dynamodb
        //        bat_w_req = persist_dynamo_batch(dyn_client, bat_w_req, retry_ch);.await; 
        //     // =================================================================================
        //     bat_w_req = print_batch(bat_w_req);
        // }
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
                for (_, v) in resp.unprocessed_items.unwrap() {
                    println!("persist_dynamo_batch, unprocessed items..delay 2secs");
                    sleep(Duration::from_millis(2000)).await;
                    //let resp = retry_ch.send(v).await;                // retry_ch auto deref'd to access method send.                                 

                    // if let Err(err) = resp {
                    //     panic!("Error sending on retry channel : {}", err);
                    // }
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



fn print_batch(
    bat_w_req: Vec<WriteRequest>,
) -> Vec<WriteRequest> {
    
    for r in bat_w_req {
    
        let WriteRequest{put_request: pr, ..} = r;
        println!(" ------------------------  ");
        for (attr, attrval) in pr.unwrap().item { // HashMap<String, AttributeValue>,
            println!(" putRequest [{}]   {:?}", attr,attrval);
        }
    }
    
    let new_bat_w_req: Vec<WriteRequest> = vec![];

    new_bat_w_req
}
                
                    
                    
         