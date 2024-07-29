// mod types { // do not include in mod definition file
//#![allow(dead_code)]
pub mod block;

// export block types
//pub use block::{ DataItem, NodeBlock};
pub use block::{PK,SK,PARENT,SK_,GRAPH,ISNODE,OP,IX,N,S,BL,B,TY,DT,LS,LN,LB,LBL,SB,SN,SS,ND,BID,XF,P,CNT,TUID,DataItem,NodeMap};

#[macro_use]

use std::collections::{HashMap, HashSet};
//use std::borrow::Cow;
use std::sync::Arc;
use std::str::FromStr;
use std::convert::TryFrom;
use std::iter::IntoIterator;
use std::slice::Iter;

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::primitives::Blob;
//use aws_sdk_dynamodb::primitives::Blob;

//&use aws_sdk_dynamodb as aws_sdk_dynamodb;
//use aws_config::aws_config;

//use aws_types::region::Region;
//use aws_sdk_dynamodb::config::{Builder, Config};
use aws_sdk_dynamodb::Client;

use uuid::{self, Builder, Uuid}; //, as_vec_string};

//use crate::lazy_static;

//use lazy_static::lazy_static;

//const LOGID: &str = "types";


// type FacetIdent : String; // type:attr:facet
//
//pub struct TyCache(HashMap<Ty, block::AttrBlock>);
// lazy_static! {
//     static ref TYIBLOCK: block::AttrItemBlock = async {
//         let mut m = db_load_types("movies".into()).await;
//         m.expect("no graph found for types")
//     };
// }

// lazy_static! {
//     static ref TYPREFIX: String = {
//         let mut m = get_type_prefix().expect("type prefix does not exist for requested graph");
//         m
//     };
// }

// lazy_static! {
//     static  ref CLIENT:  aws_sdk_dynamodb::Client = {
//         let mut m = make_dynamodb_client().expect("dynamodb client");
//         m
//     };
// }

// aws_sdk_dynamodb type conversion from AttributeValue to Rust type

pub fn as_string(val: Option<&AttributeValue>, default: &String) -> String {
    if let Some(v) = val {
        if let Ok(s) = v.as_s() {
            return s.to_owned(); // clone from aws_sdk_dynamodb source
        }
    }
    default.to_owned()
}

pub fn as_string2(val: AttributeValue) -> Option<String> {
    let AttributeValue::S(s) = val else {panic!("as_string2(): Expected AttributeValue::S")};
    Some(s)
}

pub fn as_dt2(val: AttributeValue) -> Option<String> {
    let AttributeValue::S(s) = val else {panic!("as_dt2(): Expected AttributeValue::S")};
    Some(s)
}


pub fn as_float2(val: AttributeValue) -> Option<f64> {
    let AttributeValue::N(v) = val else {panic!("as_float2(): Expected AttributeValue::N")};
    //let Ok(f) = f64::from_str(s.as_str()) else {panic!("as_float2() : failed to convert String [{}] to f64",v)};
    let Ok(f) = v.as_str().parse::<f64>() else {panic!("as_float2() : failed to convert String [{:?}] to f64",v)};
    Some(f)
}

pub fn as_lf2(val: AttributeValue) -> Option<Vec<f64>> {

    let mut vs : Vec<f64> = vec![];
    let AttributeValue::L(inner) = val else {panic!("as_lf2(): Expected AttributeValue::L")};
    for s in inner {
        let AttributeValue::N(v) = s else {panic!("as_lf2: Expected AttributeValue::Bool")}; 
        let Ok(f) = f64::from_str(v.as_str())  else {panic!("as_lf2() : failed to convert String [{:?}] to f64",v)};
        vs.push(f);
    }
    Some(vs)
}

pub fn as_vi32(val: AttributeValue) -> Option<Vec<i32>> {

    let mut vs : Vec<i32> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
               AttributeValue::N(s) => { 
                    let Ok(i) = i32::from_str(s.as_str()) else {panic!("parse to i8 error in as_vi32 [{:?}]",s)};
                    vs.push(i);
                    },
                _ => { panic!("as_vi32: Expected AttributeValue::N") },
            }
        }
    } else {
        panic!("as_vi32: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_vi8(val: AttributeValue) -> Option<Vec<i8>> {
    let mut vs : Vec<i8> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
               AttributeValue::N(s) => { 
                    let Ok(i) = i8::from_str(s.as_str()) else {panic!("parse to i8 error in as_vi8 [{:?}]",s)};
                    vs.push(i);
                    },
                _ => { panic!("as_vi8: Expected AttributeValue::N") },
            }
        }
    } else {
        panic!("as_vi8: Expected AttributeValue::L");
    }
    Some(vs)
}


pub fn as_xf(val: AttributeValue) -> Option<Vec<i8>> {

    let mut vs : Vec<i8> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
               AttributeValue::N(s) => { 
                    let Ok(i) = i8::from_str(s.as_str()) else {panic!("parse to i8 error in as_xf [{:?}]",s)};
                    vs.push(i);
                    },
                _ => { panic!("as_xf: Expected AttributeValue::N") },
            }
        }
    } else {
        panic!("as_xf: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_i8_2(val: AttributeValue) -> Option<i8> {

    if let AttributeValue::N(v) = val {
        let Ok(i) = i8::from_str(&v)  else {panic!("as_i16_2() : failed to convert String [{:?}] to i64",v)};
        return Some(i);
    }
    None
}


pub fn as_i16_2(val: AttributeValue) -> Option<i16> {
    if let AttributeValue::N(v) = val {
        let Ok(i) = i16::from_str(v.as_str())  else {panic!("as_i16_2() : failed to convert String [{:?}] to i64",v)};
        return Some(i);
    }
    None
}

pub fn as_i32_2(val: AttributeValue) -> Option<i32> {
    if let AttributeValue::N(v) = val {
        let Ok(i) = i32::from_str(v.as_str())  else {panic!("as_i32_2() : failed to convert String [{:?}] to i64",v)};
        return Some(i);
    }
    None
}

pub fn as_int2(val: AttributeValue) -> Option<i64> {

    if let AttributeValue::N(v) = val {
        let Ok(i) = i64::from_str(&v)  else {panic!("as_int2() : failed to convert String [{}] to i64",v)};
        return Some(i);
    }
    // must be AttributeValue::Null
    None
}

pub fn as_li2(val: AttributeValue) -> Option<Vec<i64>> {

    let mut vs : Vec<i64> = vec![];
    let AttributeValue::L(inner) = val else {panic!("as_li2(): Expected AttributeValue::L")};
    for s in inner {
        let AttributeValue::N(v) = s else {panic!("as_li2: Expected AttributeValue::Bool")}; 
        let Ok(f) = i64::from_str(v.as_str())  else {panic!("as_li2() : failed to convert String [{}] to i64",v)};
        vs.push(f);
    }
    Some(vs)   
}

pub fn as_bool2(val: AttributeValue) -> Option<bool> {
    let AttributeValue::Bool(bl) = val else {panic!("as_bool2(): Expected AttributeValue::Bool")};
    Some(bl)
}

pub fn as_bool(val: Option<&AttributeValue>, default: bool) -> bool {
    if let Some(v) = val {
        if let Ok(s) = v.as_bool() {
            return s.clone();
        }
    }
    default
}

pub fn as_blob2(val: AttributeValue) -> Option<Vec<u8>> {
    if let AttributeValue::B(blob) = val {
        return Some(blob.into_inner());
    }
    panic!("as_blob2: Expected AttributeValue::B");
}

pub fn as_lblob2(val: AttributeValue) -> Option<Vec<Vec<u8>>> {

    let mut vs : Vec<Vec<u8>> = vec![];
    if let AttributeValue::L(vb) = val {
        for v in vb {
            if let AttributeValue::B(blob) = v {
                vs.push(blob.into_inner());
            } else {
                panic!("as_lblob2: Expected AttributeValue::B"); 
            }
        }
    } else {
        panic!("as_lblob2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_luuid(val: AttributeValue) -> Option<Vec<Uuid>> {

    let mut vs : Vec<Uuid> = vec![];
    if let AttributeValue::L(vb) = val {
        for v in vb {
            if let AttributeValue::B(blob) = v {
                match <[u8;16] as TryFrom<Vec<u8>>>::try_from(blob.into_inner()) {
                    Err(e) => { panic!("TryFrom error in as_luuid") },
                    Ok(ary) => { vs.push(Builder::from_bytes(ary).into_uuid()) },
                }
            } else {
                panic!("as_lblob2: Expected AttributeValue::B"); 
            }
        }
    } else {
        panic!("as_lblob2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_uuid(val: AttributeValue) -> Option<Uuid> {
                    
    if let AttributeValue::B(blob) = val {
        let uuid = match <[u8;16] as TryFrom<Vec<u8>>>::try_from(blob.into_inner()) {
            Err(_) => { panic!("TryFrom error in as_uuid ") },
            Ok(ary) => { Builder::from_bytes(ary).into_uuid() },
        };
        return Some(uuid);
    } else {
        panic!("as_uuid: Expected AttributeValue::B");
    }
}




pub fn as_i64(val: Option<&AttributeValue>, default: i64) -> i64 {
    if let Some(v) = val {
        if let Ok(n) = v.as_n() {
            if let Ok(n) = n.parse::<i64>() {
                return n;
            }
        }
    }
    default
}

pub fn as_i16(val: Option<&AttributeValue>, default: i16) -> i16 {
    if let Some(v) = val {
        if let Ok(n) = v.as_n() {
            if let Ok(n) = n.parse::<i16>() {
                return n;
            }
        }
    }
    default
}


pub fn as_lbool2(val: AttributeValue) -> Option<Vec<Option<bool>>> {

    let mut vs : Vec<Option<bool>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::Bool(bl) => { vs.push(Some(bl)) },
                AttributeValue::Null(_)  => { vs.push(None) },
                _ => panic!("as_lbool2: Expected AttributeValue::N"),
            }
        }
    } else {
        panic!("as_lbool2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_ln2(val: AttributeValue) -> Option<Vec<Option<String>>> {

    let mut vs : Vec<Option<String>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::N(s)     => { vs.push(Some(s)) },
                AttributeValue::Null(_)  => { vs.push(None) },
                _ => panic!("as_ln2: Expected AttributeValue::N"),
            }
        }
    } else {
        panic!("as_ln2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_li8_2(val: AttributeValue) -> Option<Vec<Option<i8>>> {

    let mut vs : Vec<Option<i8>> = vec![];
    if let AttributeValue::L(inner) = val {
        
        for v in inner {
            match v {
                AttributeValue::N(s) => { 
                        let Ok(i) = i8::from_str(s.as_str()) else {panic!("parse error in as_li8_2")};
                        vs.push(Some(i));
                        },
                AttributeValue::Null(_)  => { vs.push(None) },
             _ => panic!("as_li8_2: Expected AttributeValue::N"), 
            }
        }
        
    } else {
        panic!("as_li8_2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_li16_2(val: AttributeValue) -> Option<Vec<Option<i16>>> {

    let mut vs : Vec<Option<i16>> = vec![];
    if let AttributeValue::L(inner) = val {
        
        for v in inner {
            match v {
                AttributeValue::N(s) => { 
                        let Ok(i) = i16::from_str(s.as_str()) else {panic!("parse error in as_li16_2")};
                        vs.push(Some(i));
                        },
                AttributeValue::Null(_)  => { vs.push(None) },
                _ => panic!("as_li16_2: Expected AttributeValue::N"), 
            }
        }
        
    } else {
        panic!("as_li16_2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_li32_2(val: AttributeValue) -> Option<Vec<Option<i32>>> {
                    
    let mut vs : Vec<Option<i32>> = vec![];
    if let AttributeValue::L(inner) = val {
        
        for v in inner {
            match v {
                AttributeValue::N(s) => { 
                        let Ok(i) = i32::from_str(s.as_str()) else {panic!("parse error in as_li32_2")};
                        vs.push(Some(i));
                        },
                AttributeValue::Null(_)  => { vs.push(None) },
                _ => panic!("as_li32_2: Expected AttributeValue::N"), 
            }
        }
        
    } else {
        panic!("as_li32_2: Expected AttributeValue::L");
    }
    Some(vs)
}


pub fn as_li64_2(val: AttributeValue) -> Option<Vec<Option<i64>>> {

    let mut vs : Vec<Option<i64>> = vec![];
    if let AttributeValue::L(inner) = val {
        
        for v in inner {
            match v {
                AttributeValue::N(s) => { 
                        let Ok(i) = i64::from_str(s.as_str()) else {panic!("parse error in as_li64_2")};
                        vs.push(Some(i));
                        },
                AttributeValue::Null(_)  => { vs.push(None) },
                _ => panic!("as_li64_2: Expected AttributeValue::N"), 
            }
        }
        
    } else {
        panic!("as_li64_2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_lint2(val: AttributeValue) -> Option<Vec<Option<i64>>> {

    let mut vs : Vec<Option<i64>> = vec![];
    if let AttributeValue::L(inner) = val {
        
        for v in inner {
            match v {
                AttributeValue::N(s) => { 
                        let Ok(i) = i64::from_str(s.as_str()) else {panic!("parse error in as_lint2")};
                        vs.push(Some(i));
                        },
                AttributeValue::Null(_)  => { vs.push(None) },
                _ => panic!("as_lint2: Expected AttributeValue::N"), 
            }
        }
        
    } else {
        panic!("as_lint2: Expected AttributeValue::L");
    }
    Some(vs)
}


pub fn as_lfloat2(val: AttributeValue) -> Option<Vec<Option<f64>>> {

    let mut vs : Vec<Option<f64>> =vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::N(s) => {
                        let Ok(i) = f64::from_str(&s) else {panic!("parse error in as_lf64_2")};
                        vs.push(Some(i));
                        },
                AttributeValue::Null(_)  => { vs.push(None) },
                _ => panic!("as_li16_2: Expected AttributeValue::N"), 
            }
        }
    } else {
        panic!("as_lfloat2: Expected AttributeValue::L");
    }
    Some(vs)
}


pub fn as_lb2(val: AttributeValue) -> Option<Vec<Option<Vec<u8>>>> {

    let mut vs : Vec<Option<Vec<u8>>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::B(blob) => {vs.push(Some(blob.as_ref().into()))},
                AttributeValue::Null(_)  => { vs.push(None) },
                _ => panic!("as_lb2: Expected AttributeValue::N"), 
            }
        }
    } else {
        panic!("as_lb2: Expected AttributeValue::L");
    }
    Some(vs)
}


pub fn as_ls2(val: AttributeValue) -> Option<Vec<Option<String>>> {

    let mut vs : Vec<Option<String>> = vec![];
    if let AttributeValue::L(inner) = val {
                
        for v in inner {
            match v {
                AttributeValue::S(s)     => { vs.push(Some(s)) },
                AttributeValue::Null(_)  => { vs.push(None) },
                _ => panic!("as_ls2: Expected AttributeValue::S"), 
            }
        }
        
    } else {
        panic!("as_ls2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_ldt2(val: AttributeValue) -> Option<Vec<Option<String>>> {

    let mut vs : Vec<Option<String>> = vec![];
    if let AttributeValue::L(inner) = val {
                
        for v in inner {
            match v {
                AttributeValue::S(s)     => { vs.push(Some(s)) },
                AttributeValue::Null(_)  => { vs.push(None) },
                _ => panic!("as_ldt2: Expected AttributeValue::S"),
            }
        }
        
    } else {
        panic!("as_ldt2: Expected AttributeValue::L");
    }
    Some(vs)
}


struct Prefix(String);

impl Prefix {
    fn new() -> Self {
        Prefix(String::new())
    }
}

impl From<HashMap<String, AttributeValue>> for Prefix {
    fn from(mut value: HashMap<String, AttributeValue>) -> Self {
        let mut prefix: Prefix = Prefix::new();
        let Some(p) = value.remove("SortK") else {panic!("no SortK value for Prefix")};
        let Some(s) = as_string2(p) else {panic!("expected Some for as_string2() got None")};
        prefix.0=s;
        prefix
    }
}


#[derive(Debug)]
pub struct NodeType { // previous name TyName
    short: String,
    long: String,
    reference : bool,
    attrs : Option<block::AttrBlock>,
}

impl<'a> IntoIterator for &'a NodeType {

    type Item = &'a Arc<block::AttrD>;
    type IntoIter = Iter<'a, Arc<block::AttrD>>; 
    
    fn into_iter(self) -> Iter<'a, Arc<block::AttrD>> {
        if let None = self.attrs  {
            println!("IntoIterator error: type {} [{}] has no attrs",self.long, self.short)
        }
        (self.attrs).as_ref().unwrap().0.iter()
    }
}

impl NodeType {

    fn new() -> Self {
        NodeType {
            short: String::new(),   // long type name
            long : String::new(),   // sort type name
            reference: false,       // is type a container for reference data (i.e. static data)
            attrs: None,            // type attributes
        }
    }
    
    pub fn long_nm(&self) -> &str {
        return &self.long;
    }
    pub fn short_nm(&self) -> &str {
        return &self.short;
    }
    
    pub fn is_reference(&self) -> bool {
        return self.reference;
    }
    
    pub fn get_long(&self) -> &str {
        return &self.long;
    }
    
    pub fn get_short(&self) -> &str {
        return &self.short;
    }
           
    pub fn is_atttr_nullable(&self, attr_sn : &str) -> bool {
        for attr in self  {
        //for attr in self {
            if attr.c == attr_sn {
                return attr.nullable
            }
        }
        panic!("is_atttr_nullable() not cached for [{}] [{}]",attr_sn, self.long);
    }
    
    pub fn get_attr_nm(&self, attr_sn : &str)  -> &str { // TODO: Result(&str,Error)

        for attr in self {
                if attr.c == attr_sn {
                    return & attr.name
                }
        }
        panic!("get_attr_nm() - attribute [{}] not in node type [{}]",attr_sn, self.long);   
    }
    
    pub fn get_attr_sn(&self, attr_nm : &str)  -> &str {

        for attr in self  {
                if attr.name == attr_nm {
                    return &attr.c
                }
        }
        panic!("get_edge_attr() - attribute [{}] not found in node type [{}]",attr_nm, self.long);   
    }
    
    pub fn get_scalar_partitions(&self) -> HashMap<String,Vec<&str>> {
    
        let mut scalar_partitions : HashMap<String,Vec<&str>> = HashMap::new();
        
        for v in self {
            
            if v.dt.as_str() != "Nd" && v.pg {
            
                scalar_partitions
                .entry(v.p.clone())
                .and_modify(|r| {
                    r.push(&v.c[..]);
                  }
                )
                .or_insert(
                    vec![&v.c[..]]
                );
                }
        }
        scalar_partitions
    }
    
        
    pub fn get_edge_child_ty(&self, edge : &str) -> &str {

        //for attr in (&self.attrs).as_ref().unwrap().0.iter()  {
        for attr in self {
            if attr.name == edge && attr.dt == "Nd" {
                return &attr.ty
            }
        }
        panic!("get_edge_child_ty() - [{}] is not an edge attribute in node type [{}]",edge, self.long);
    }
    
    pub fn get_attr_dt(&self, attr_sn : &str) -> &str {

        for attr in self  {
            if attr.c == attr_sn {
                return &attr.dt
            }
        }
        panic!("get_attr_dt() not cached for [{}] [{}]",attr_sn, self.long);
    }
}



impl From<HashMap<String, AttributeValue>> for NodeType {
    
    fn from(mut value: HashMap<String, AttributeValue>) -> Self {
        // ownerships transferred from AttributeValue into NodeType
        let mut ty = NodeType::new();
        
        for (k,v) in value.drain() {
            match k.as_str() {
                "PKey"  => {},
                "SortK" => ty.short = as_string2(v).unwrap(),
                "Name"  => ty.long = as_string2(v).unwrap(),
                "Reference" => ty.reference = as_bool2(v).unwrap(),
                _       => panic!("NodeType from impl: expected Sortk | Name, got [{}]",k),
            }
        }
        ty
    }
}

pub struct NodeTypes(pub Vec<NodeType>);

impl<'a>  NodeTypes {
    
    pub fn get(&self, ty_nm : &str) -> &NodeType {
    
        for ty in self.0.iter() {
            if ty.long == ty_nm {
                return ty
            }
        }
        for ty in self.0.iter() {
            if ty.short == ty_nm {
                return ty
            }
        }
        panic!("get error: Node type [{}] not found",ty_nm);
    }
    
    pub fn set_attrs(&'a mut self, ty_nm : String, attrs : block::AttrBlock)  {//-> Result<(),Error> {
    
            for ty in self.0.iter_mut() {
                if ty.long == ty_nm {
                    ty.attrs=Some(attrs);
                    return
                }
            }
            for ty in self.0.iter_mut() {
                if ty.short == ty_nm {
                    ty.attrs=Some(attrs);
                    return
                }
            }
        //Error::Err("get error: Node type [{}] not found",ty_nm);
    }
}

pub async fn fetch_graph_types(
    client: &aws_sdk_dynamodb::Client,
    graph: String,
    //mut tyall: block::AttrItemBlock,
) -> Result<(Arc<NodeTypes>, String), aws_sdk_dynamodb::Error> {

    let mut ty_c_: HashMap<String, block::AttrBlock> = HashMap::new();

    //let mut ty_cache = NodeTypes(vec![]);    // ty_cache previous name ty_shortlong_names


    let table_name = "GoGraphSS";
    
    // ================================================================    
    // Fetch Graph Short Name (used as prefix in some PK values)
    // ================================================================
    let results = client
        .query()
        .table_name(table_name)
        .projection_expression("SortK")
        .key_condition_expression("PKey =  :pkey")
        .expression_attribute_values(":pkey", AttributeValue::S("#Graph".to_string()))
        .filter_expression("#nm = :graph")
        .expression_attribute_names("#nm", "Name")
        .expression_attribute_values(":graph", AttributeValue::S(graph.clone()))
        .send()
        .await?;

    // TODO: handle no-data-found
    let graph_prefix_wdot : String = if let Some(items) = results.items {
                                        if items.len() != 1 {
                                            panic!("graph prefix query expected 1 item got [{}]",items.len());
                                        }
                                        let mut ty_prefix: Prefix = items.into_iter().next().unwrap().into(); 
                                        ty_prefix.0.push('.');
                                        ty_prefix.0
                                     } else {
                                        panic!("graph prefix query returned Option::None");
                                     };
                            
    // ==============================    
    // Fetch Node Types  
    // ============================== 
    let mut tys = String::new();
    tys.push('#');
    tys.push_str(&graph_prefix_wdot);
    tys.push('T');
    
    let results = client
        .scan()
        .table_name(table_name)
        .filter_expression("#nm = :prefix")
        .expression_attribute_names("#nm", "PKey")
        .expression_attribute_values(":prefix", AttributeValue::S(tys))
        .send()
        .await?;
    
    let nt : Vec<NodeType> = if let Some(items) = results.items {
                                let ty_names_v: Vec<NodeType> = items.into_iter().map(|v| v.into()).collect(); 
                                ty_names_v
                            } else {
                                vec![]
                            };
    if nt.len() == 0 {
        panic!("fetch node type data failed. ")
    }
    let mut ty_cache = NodeTypes(nt);
    
    // also package as HashSet   
    let mut set = HashSet::<String>::new();
    for k in &ty_cache.0 {
        set.insert(k.long.clone());
    }
  
    // ===================================    
    // Fetch Attributes for all Node Types
    // ===================================  
    let results = client
        .scan()
        .table_name(table_name)
        .filter_expression("begins_with(#nm, :prefix)")
        .expression_attribute_names("#nm", "PKey")
        .expression_attribute_values(":prefix", AttributeValue::S(graph_prefix_wdot.clone()))
        .send()
        .await?;

    // TODO Handle error
    let ty_all = if let Some(items) = results.items {
                        let ty_block_v: Vec<block::AttrItem> = items.into_iter().map(|v| v.into()).collect(); // dot takes mut v
                        block::AttrItemBlock(ty_block_v)
                 } else {
                        block::AttrItemBlock(vec![])
                 };
    if ty_all.0.len() == 0 {
        panic!("fetch attribute data failed.")
    }

    // =================================
    // package into NodeTypes cache
    // =================================
    for v in ty_all.0 {
    
        let mut a = block::AttrD::new();

        let ty_ = v.ty.clone().unwrap();
            
        if ty_[0..1].contains('[') {
            // equiv: *v.ty.index(0..1).   Use of Index trait which has generic that itself is a SliceIndex trait which uses Ranges...
            a = block::AttrD {
               name: v.attr.unwrap(), // TODO: v.clone_attr(),v.get_attr()
               dt: "Nd".to_string(),
               c: v.c.unwrap(),
               ty: ty_[1..ty_.len() - 1].to_string(),//nm.clone(), //"XX".to_string(),// v.ty.unwrap()[1..v.ty.unwrap().len() - 1].to_string(),
               p: v.p.unwrap(),
               pg: v.pg.unwrap_or(false),
               nullable: v.n.unwrap_or(false),
               //incP: v.incp,
               ix: v.ix.unwrap_or(String::new()),
               card: "1:N".to_string(),
            }
        } else {
            // check if Ty is a tnown Type
            if set.contains(&ty_) {
               a = block::AttrD {
                   name: v.attr.unwrap(),
                   dt: "Nd".to_string(),
                   c: v.c.unwrap(),
                   ty: ty_,
                   p: v.p.unwrap(),
                   pg: v.pg.unwrap_or(false),
                   nullable: v.n.unwrap_or(false),
                   //incp: v.incp,
                   ix: v.ix.unwrap_or(String::new()),
                   card: "1:1".to_string(),
               }
            } else {
               // scalar
               a = block::AttrD {
                   name: v.attr.unwrap(),
                   dt: v.ty.unwrap(),
                   c: v.c.unwrap(),
                   p: v.p.unwrap(),
                   nullable: v.n.unwrap_or(false),
                   pg: v.pg.unwrap_or(false),
                   //incp: v.incp,
                   ix: v.ix.unwrap_or(String::new()),
                   card: "".to_string(),
                   ty: "".to_string(),
               }
            }
        //}
        }
        
        let mut nm = v.nm.unwrap();
        nm.drain(0..nm.find('.').unwrap()+1);
        
        // group AttrD by v.nm (PK in query)
        if let Some(c) = ty_c_.get_mut(&nm[..]) {
           c.0.push(Arc::new(a));
        } else {
           ty_c_.insert(nm, block::AttrBlock(vec![Arc::new(a)]));
        }
    }
    // repackage (& consume) ty_c_ into NodeTypes cache
    for (k, v) in ty_c_ {
        ty_cache.set_attrs(k,v);
    }
       
    Ok((Arc::new(ty_cache), graph_prefix_wdot))
}

