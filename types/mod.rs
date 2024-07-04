// mod types { // do not include in mod definition file
//#![allow(dead_code)]
pub mod block;

// export block types
pub use block::{TyAttrBlock, TyAttrD};

#[macro_use]
use std::collections::{HashMap, HashSet};
use aws_sdk_dynamodb::types::AttributeValue;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::cell::RefCell;
use std::sync::Arc;
//use block::{TyAttrBlock,TyAttrD};
//&use aws_sdk_dynamodb as aws_sdk_dynamodb;
//use aws_config::aws_config;

use aws_types::region::Region;
//use aws_sdk_dynamodb::config::{Builder, Config};
use aws_sdk_dynamodb::Client;

//use crate::lazy_static;

//use lazy_static::lazy_static;

const LOGID: &str = "types";

// type FacetIdent : String; // type:attr:facet
//
//pub struct TyCache(HashMap<Ty, block::TyAttrBlock>);
// lazy_static! {
//     static ref TYIBLOCK: block::TyIBlock = async {
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

fn as_string(val: Option<&AttributeValue>, default: &String) -> String {
    if let Some(v) = val {
        if let Ok(s) = v.as_s() {
            return s.to_owned(); // clone from aws_sdk_dynamodb source
        }
    }
    default.to_owned()
}

// fn as_vec_string(val: Option<&AttributeValue>, default: Vec<String>) -> Vec<String> {
//     if let Some(v) = val {
//         if let Ok(vs) = v.as_l() {
//             let v : Vec<String> = vs.iter().as_s(default).collect();
//             v
//         }
//     }
//     default
// }

fn as_bool(val: Option<&AttributeValue>, default: bool) -> bool {
    if let Some(v) = val {
        if let Ok(s) = v.as_bool() {
            return s.clone();
        }
    }
    default
}

fn as_i32(val: Option<&AttributeValue>, default: i32) -> i32 {
    if let Some(v) = val {
        if let Ok(n) = v.as_n() {
            if let Ok(n) = n.parse::<i32>() {
                return n;
            }
        }
    }
    default
}

fn as_i16(val: Option<&AttributeValue>, default: i16) -> i16 {
    if let Some(v) = val {
        if let Ok(n) = v.as_n() {
            if let Ok(n) = n.parse::<i16>() {
                return n;
            }
        }
    }
    default
}

struct Prefix(String);

impl Prefix {
    fn new() -> Self {
        Prefix(String::new())
    }
}

impl From<&HashMap<String, AttributeValue>> for Prefix {
    fn from(value: &HashMap<String, AttributeValue>) -> Self {
        let mut prefix: Prefix = Prefix::new();
        // for (k,v) in value {
        //      println!("From HashMap into Prefix:{} {}",k,as_string(Some(v), &"".to_string()));
        // }
        prefix.0 = as_string(value.get("SortK"), &"".to_string());

        prefix
    }
}

// async fn db_graph_prefix<G>(graph : G ) -> Result<String, aws_sdk_dynamodb::Error>
// where G : Into<Cow<'static, str>> {
// async fn db_graph_prefix<G: AsRef(str)>>(graph : G )   -> Result<String, aws_sdk_dynamodb::Error>

//async fn db_graph_prefix<'a>(client : &aws_sdk_dynamodb::Client, graph : impl Into<Cow<'a, str>> ) -> Result<String, aws_sdk_dynamodb::Error> {
pub async fn db_graph_prefix<'a>(
    client: &aws_sdk_dynamodb::Client,
    graph: String,
) -> Result<String, aws_sdk_dynamodb::Error> {
    let table_name = "GoGraphSS";

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

    if let Some(items) = results.items {
        if items.len() == 0 {
            panic!("No rows returned from db_graph_prefix query for name [{}]",graph);
        }
        let mut ty_prefixs: Vec<Prefix> = items.iter().map(|v| v.into()).collect(); // TODO: change to single row query?
        let mut ty_prefix = ty_prefixs.remove(0); // single row only expected
        ty_prefix.0.push('.');
        Ok(ty_prefix.0)
    } else {
        Ok(Prefix::new().0)
    }
}

#[derive(Debug)]
struct TyName {
    short: String,
    long: String,
}

struct TySLnames(Vec<TyName>);

impl From<&HashMap<String, AttributeValue>> for TyName {
    fn from(value: &HashMap<String, AttributeValue>) -> Self {
        TyName {
            short: as_string(value.get("SortK"), &"".to_string()),
            long: as_string(value.get("Name"), &"".to_string()),
        }
    }
}

//pub async fn db_load_types<'a>(client : &aws_sdk_dynamodb::Client, prefix : impl Into<Cow<'a, str>>) -> Result<block::TyIBlock, aws_sdk_dynamodb::Error> {
pub async fn db_load_types<'a>(
    client: &aws_sdk_dynamodb::Client,
    prefix: impl ToString,
) -> Result<block::TyIBlock, aws_sdk_dynamodb::Error> {
    //let prefix  = db_graph_prefix(client, graph).await?;

    //println!("graph prefix : [{}]",prefix);

    let table_name = "GoGraphSS";

    // load graph types, long and short names
    // "PKey", "#"+gId+"T")

    // load graph type meta-data
    let results = client
        .scan()
        .table_name(table_name)
        .filter_expression("begins_with(#nm, :prefix)")
        .expression_attribute_names("#nm", "PKey")
        .expression_attribute_values(":prefix", AttributeValue::S(prefix.to_string()))
        .send()
        .await?;

    if let Some(items) = results.items {
        let ty_block_v: Vec<block::TyItem> = items.iter().map(|v| v.into()).collect();
        Ok(block::TyIBlock(ty_block_v))
    } else {
        Ok(block::TyIBlock(vec![]))
    }
}

pub async fn make_dynamodb_client(
    region: impl Into<Cow<'static, str>>,
) -> aws_sdk_dynamodb::Client {
    let config = aws_config::from_env().region("us-east-1").load().await;
    let client = Client::new(&config);
    client
}

// This failed to connect...
// pub fn make_dynamodb_client(region : impl Into<Cow<'static, str>>) -> aws_sdk_dynamodb::Client {

//     let config = aws_sdk_dynamodb::Config::builder()
//     //.region(Region::new("us-east-1"))//region) //Region::new(region))
//     .build();

//     let client = aws_sdk_dynamodb::Client::from_conf(config);
//     client

// }

type Ty = String; // type
type TyAttr = String; // type:attr
pub type AttrTyS = String; // attr#ty

pub struct TypeCache {
    pub ty_attr_d: HashMap<TyAttr, block::TyAttrD>, // TyAttrC
    pub ty_c: Arc<HashMap<Ty, block::TyAttrBlock>>, // TyC
    pub attr_ty_s: Arc<HashMap<AttrTyS, String>>,   // AttrTyS
    pub set: HashSet<String>,
    pub ty_short_nm: Arc<HashMap<String, String>>,
    pub ty_long_nm: Arc<HashMap<String, String>>,
}

pub async fn populate_type_cache_1<'a>(
    client: &aws_sdk_dynamodb::Client,
    prefix: impl AsRef<str>,
    tyall: &'a mut block::TyIBlock,
) -> Result<TypeCache, aws_sdk_dynamodb::Error> {
    let mut ty_c_: HashMap<Ty, block::TyAttrBlock> = HashMap::new();
    let mut attr_ty_: HashMap<String, String> = HashMap::new();

    //let mut tyShortnm: HashMap<AttrTyS, String> = HashMap::new();

    let mut set = HashSet::<String>::new();

    let mut ty_short_nm: HashMap<String, String> = HashMap::new();
    let mut ty_long_nm: HashMap<String, String> = HashMap::new();

    let mut ty_shortlong_names = TySLnames(vec![]);

    let table_name = "GoGraphSS";

    let mut tys = String::new();
    tys.push('#');
    tys.push_str(prefix.as_ref());
    tys.push('T');

    // fetch graph types : name, short name
    let results = client
        .scan()
        .table_name(table_name)
        .filter_expression("#nm = :prefix")
        .expression_attribute_names("#nm", "PKey")
        .expression_attribute_values(":prefix", AttributeValue::S(tys))
        .send()
        .await?;

    if let Some(items) = results.items {
        let ty_names_v: Vec<TyName> = items.iter().map(|v| v.into()).collect();
        ty_shortlong_names.0 = ty_names_v;
    }

    for k in &ty_shortlong_names.0 {
        println!("TyNames {:?}", k);
    }

    for v in &ty_shortlong_names.0 {
        ty_short_nm.insert(v.long.clone(), v.short.clone());
    }

    for (k, v) in &ty_short_nm {
        println!("ty_short_nm {:?} {:?}", k, v);
    }

    for v in ty_shortlong_names.0 {
        ty_long_nm.insert(v.short, v.long);
    }

    for (k, v) in &ty_long_nm {
        println!("ty_long_nm {:?} {:?}", k, v);
    }

    let get_ty_short_name = |n: &str| -> &str { ty_short_nm.get(n).unwrap() };
    //     ////////////////////////////////////////////////
    for ty in &mut tyall.0 {
        ty.nm = ty.nm.split_off(ty.nm.find('.').unwrap() + 1);

        // get_or_insert is a nightly build.
        // rustup toolchain list
        // rustup show
        // rustup toolchain install nightly
        // rustup override set nightly | rustup override set stable
        // cargo +stable  run -- -g Movies -f movie-2.rdf

        // cargo +nightly  run -- -g Movies -f movie-2.rdf
        if !set.contains(ty.nm.as_str()) {
            set.insert(ty.nm.clone());
        }

        // stable equivalent of get_or_inser

        // HashSet generates hash from argument, compares against values in Set using hash value to expedite search.
        // if !set.contains(&ty.nm) {
        // // if !set.contains(ty.nm.as_str()) {
        //     // note: to void cloning nm can use HashSet<&String> ??? THis shouldn't work...
        //     set.insert(ty.nm.clone());
        // }
    }
    println!("set: {:?}", set);
    // set

    for ty in &set {
        let mut tc = block::TyAttrBlock(vec![]);

        for v in &tyall.0 {
            // if not current ty then
            println!("v.nm != ty {:?} {:?}", v.nm.as_str(), ty);
            if v.nm.as_str() != ty {
                continue;
            }

            let mut key = String::new();
            key.push_str(&v.attr);
            key.push('#');
            key.push_str(&v.nm);
            //attrTy.entry(key).or_insert(v.c.clone());
            //println!("key shortname {:?}  {:?}  ty {:?}", key, v.c, v.ty);
            attr_ty_.insert(key, v.c.clone());

            let a: block::TyAttrD;

            if v.ty[0..1].contains('[') {
                // equiv: *v.ty.index(0..1).   Use of Index trait which has generic that itself is a SliceIndex trait which uses Ranges...
                a = block::TyAttrD {
                    name: v.attr.clone(),
                    dt: "Nd".to_string(),
                    c: v.c.to_string(),
                    ty: v.ty[1..v.ty.len() - 1].to_string(),
                    p: v.p.clone(),
                    pg: v.pg,
                    n: v.n,
                    //incP: v.incp,
                    ix: v.ix.clone(),
                    card: "1:N".to_string(),
                }
            } else {
                // check if Ty is a tnown Type
                if set.contains(&v.ty) {
                    a = block::TyAttrD {
                        name: v.attr.clone(),
                        dt: "Nd".to_string(),
                        c: v.c.clone(),
                        ty: v.ty.clone(),
                        p: v.p.clone(),
                        pg: v.pg.clone(),
                        n: v.n,
                        //incp: v.incp,
                        ix: v.ix.clone(),
                        card: "1:1".to_string(),
                    }
                } else {
                    // scalar
                    a = block::TyAttrD {
                        name: v.attr.clone(),
                        dt: v.ty.to_string(),
                        c: v.c.clone(),
                        p: v.p.clone(),
                        n: v.n,
                        pg: v.pg,
                        //incp: v.incp,
                        ix: v.ix.clone(),
                        card: "".to_string(),
                        ty: "".to_string(),
                    }
                }
            }
            println!("tc {:?}", a);
            tc.0.push(a);
        }

        ty_c_.insert(ty.clone(), tc);
    }

    let mut ty_attr_d_: HashMap<String, block::TyAttrD> = HashMap::new();

    //   pub ty_attr_d: HashMap<TyAttr, &'a block::TyAttrD>, // TyAttrC

    //  ty_c: Arc<HashMap<Ty, block::TyAttrBlock>>,

    for (k, v) in ty_c_.iter() {
        // k &String, v &block::TyAttrBlock
        for vv in &v.0 {
            let a = block::TyAttrD {
                name: vv.name.clone(),
                dt: vv.dt.to_string(),
                c: vv.c.clone(),
                p: vv.p.clone(),
                n: vv.n.clone(),
                pg: vv.pg.clone(),
                //incp: v.incp,
                ix: vv.ix.clone(),
                card: vv.card.clone(),
                ty: vv.ty.clone(),
            };
            ty_attr_d_.insert(gen_type_attr(k.as_str(), &vv.name), a);
        }
    }

    let mut tyc = TypeCache {
        ty_attr_d: ty_attr_d_, //HashMap::new(),
        ty_c: Arc::new(ty_c_),
        attr_ty_s: Arc::new(attr_ty_),
        set: set,
        ty_short_nm: Arc::new(ty_short_nm),
        ty_long_nm: Arc::new(ty_long_nm),
    };

    //   	let mut ty_attr_d_  : HashMap<String, &'a block::TyAttrD> = HashMap::new();

    //     for (k,v) in &tyc.ty_c {
    //         for i in &v.0 {
    //           ty_attr_d_.insert(gen_type_attr(k, i.name), i);
    //          //ty_attr_d_.insert(gen_type_attr(get_ty_short_name(k), i.name), i);
    //           //println!(" ** {:?} {:?} {:?}",k,gen_type_attr(get_ty_short_name(k), i.name), i);
    //         }
    //      }
    //      ty.ty_attr_d=ty_attr_d_;

    Ok(tyc) // tyc is moved to return value. Any references to tyc are now invalid if they are required to live longer than this func, as does ty_attr_d.
}

pub fn populate_type_cache_2(
    ty_c: Arc<HashMap<Ty, block::TyAttrBlock>>,
) -> HashMap<String, block::TyAttrD> {
    let mut ty_attr_d_: HashMap<String, block::TyAttrD> = HashMap::new();

    //   pub ty_attr_d: HashMap<TyAttr, &'a block::TyAttrD>, // TyAttrC

    //  ty_c: Arc<HashMap<Ty, block::TyAttrBlock>>,

    for (k, v) in ty_c.iter() {
        // k &String, v &block::TyAttrBlock
        for vv in &v.0 {
            let a = block::TyAttrD {
                name: vv.name.clone(),
                dt: vv.dt.to_string(),
                c: vv.c.clone(),
                p: vv.p.clone(),
                n: vv.n.clone(),
                pg: vv.pg.clone(),
                //incp: v.incp,
                ix: vv.ix.clone(),
                card: vv.card.clone(),
                ty: vv.ty.clone(),
            };
            ty_attr_d_.insert(gen_type_attr(k.as_str(), &vv.name), a);
        }
    }

    ty_attr_d_
}

//static ty_names: HashMap<String,String> =  HashMap::new();

pub fn gen_type_attr(ty: &str, attr: &str) -> String {
    // use String deref to pass to &str args
    let mut s = String::new();
    // generte key for ty_attr_c:  <typeName>:<attrName> e.g. Person:Age
    s.push_str(ty);
    s.push(':');
    s.push_str(attr);
    s
}
