#![warn(unused_variables)]
#![warn(dead_code)]
use super::{as_bool, as_i16, as_i32, as_string};
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;
#[warn(non_camel_case_types)]
//#[allow(non_camel_case_types)]
//#[warn(dead_code)]

//mod block {. // do not include in mod definition file
use std::time;
use uuid::{self, Uuid}; //, as_vec_string};

// Propagation UID flags. XF flag
const CHILD_UID: i8 = 0;
const CUID_INUSE: i8 = 1; // deprecated
const UID_DETACHED: i8 = 2; // soft delete. Child detached from parent.
const OVFL_BLOCK_UID: i8 = 3; // this entry represents an overflow block. Current batch id contained in Id.
const OUID_INUSE: i8 = 4; // deprecated
const OBACH_SIZE_LIMIT: i8 = 5; // overflow batch reached max entries - stop using. Will force creating of new overflow block or a new batch.
const EDGE_FILTERED: i8 = 6; // set to true when edge fails GQL uid-pred  filter

// DataItem  maps database attribute (and its associated type) to golang type.
// Used during the database fetch of node block data into the memory cache.
// ToStruct(<dataItem>) for Spanner and dynamodbattribute.UnmarshalListOfMaps(result.Items, &data) for dynamodb.
// Data in then used to populate the NV.value attribute in the UnmarshalCache() ie. from []DataItem -> NV.value
// via the DataItem Get methods
#[allow(dead_code)]
pub struct DataItem {
    pub pkey: Vec<u8>, //  `dynamodbav:"PKey"`, // uuid.UID
    pub sortk: String, //   `dynamodbav:"SortK"`,
    //
    pub graph: String,
    pub is_node: String,
    pub ix: String, // used during double-propagation load "X": not processed, "Y": processed
    //
    pub op: Vec<u8>, // parent UID in overflow blocks
    //attrName : String
    //
    // scalar types
    //
    // F : f64 // spanner float
    // I int64   // spanner int
    pub n: String, //edge count (dynamodb, spanner), integer, float DTs (dynamodb)
    //N []big.Rat      Spanner: if Numeric is chosen to store all F and I types
    pub s: String,
    pub bl: bool,
    pub b: Vec<u8>,
    pub dt: time::Instant, // DateTime
    //
    // node type - listed in GSI so value can be associated with type for "has" operator
    pub ty: String, // type of node
    // child node counters
    pub asz: i64, // attribute of  overflow batch UID-PRED.
    //
    // List (ordered set of any type but constrainted to a single type in DynaGraph)
    //
    pub ls: Vec<String>,
    //pub lf : Vec<f64>,
    pub ln: Vec<Vec<String>>, // LN []big.Rat       if Numeric is chosen to store all F and I types
    //pub li  : Vec<i64>,
    pub lb: Vec<Vec<u8>>,
    pub lbl: Vec<bool>,
    pub ldt: Vec<time::Instant>,
    //
    pub pbs: Vec<u8>,
    pub bs: Vec<u8>,
    //
    pub nd: Vec<Vec<u8>>, //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
    //
    // Set (unordered set of a single type)
    //
    // NS : f64
    // SS : String
    // BS : Vec<u8>
    // base64.StdEncoding.Decode requires : Vec<u8> argument hence XB : Vec<u8> (len 1) rather thab : Vec<u8>
    // also Dynamo ListAppend only works with slices so : Vec<u8> to : Vec<u8>
    // note: I use XB rather than X as X causes a Unmarshalling error. See description of field in doco.
    pub xbl: Vec<bool>, // used for propagated child scalars (List data). True means associated child value is NULL (ie. is not defined)
    pub xf: Vec<i8>, // used in uid-predicate 1 : c-UID, 2 : c-UID is soft deleted, 3 : ovefflow UID, 4 : overflow block full
    pub id: Vec<i16>, // current maximum overflow batch id. Maps to the overflow item number in Overflow block e.g. A#G:S#:A#3 where Id is 3 meaning its the third item in the overflow block. Each item containing 500 or more UIDs in Lists.
                      //
}

// NodeBlock represents the graph data in storage format in chunksk of Dynamodb 4k block or an Oracle 4,8,16,32k block?
pub struct NodeBlock<'a>(Vec<&'a DataItem>);

//type NodeBlock = Vec<&DataItem>;

// pub struct obj_type<T>  {
// 	ty  :  String,   // type def from Type
// 	value : T       // rdf object value : was interface{}
// }

// pub struct obj_type  {
// 	ty  :  String,   // type def from Type
// 	value : Box<dyn ?>       // rdf object value : was interface{}
// }

// channel payload used in client.AttachNode()
pub struct ChPayload<'a> {
    pub tuid: Uuid, // target UID for scalar propagation
    pub cuid: Uuid,
    pub nd_index: i32, // index into nd,xf,id
    pub batch_id: i32, // overflow block batch id
    // nodedata for uid-predicate
    pub di: &'a DataItem,
    //
    pub osortk: String, // overflow sortk
    // parent node type
    pub pty: TyAttrBlock,
    //
    pub random: bool,
}

// keys

impl DataItem {
    pub fn get_pkey(&self) -> &Vec<u8> {
        &self.pkey
    }

    pub fn get_sortk(&self) -> &str {
        &self.sortk
    }

    // Scalars - scalar data has no associated XBl null inidcator. Absense of item/predicate means it is null.
    pub fn get_s(&self) -> &str {
        &self.s
    }

    pub fn get_ty(&self) -> &str {
        if let Some(l) = self.ty.find('|') {
            &self.ty[l + 1..]
        } else {
            &self.ty[..]
        }
    }

    // Dynamodb TODO: use compiler directives to use slightly differnt GetI()
    // pub fn  GetI() int64 {
    // 	//	dgv.I // TODO: how to handle. SQL uses I & F no-SQL uses N. Maybe both SQL & non-SQL use I & F which are both Number type in no-SQL ???
    // 	int64(dgv.N)

    // }

    pub fn get_n(&self) -> &str {
        &self.n
    }

    pub fn get_dt(&self) -> &time::Instant {
        &self.dt
    }

    pub fn get_b(&self) -> &Vec<u8> {
        &self.b
    }

    pub fn get_bl(&self) -> bool {
        self.bl
    }

    // Lists - embedded in item
    fn get_ls(&self) -> &Vec<String> {
        &self.ls
    }

    // TODO - should this be []int??
    // pub fn get_li(&self) -> &Vec<i64>  {
    // 	&self.li
    // }

    // pub fn  get_lf(&self) -> &Vec<f64> {
    // 	&self.lf
    // }

    pub fn get_lb(&self) -> &Vec<Vec<u8>> {
        &self.lb
    }
    pub fn get_lbl(&self) -> &Vec<bool> {
        &self.lbl
    }

    // Lists - only used for containing propagated values
    pub fn get_uls(&self) -> (&Vec<String>, &Vec<bool>) {
        (&self.ls, &self.xbl)
    }

    // TODO - should this be []int??
    pub fn get_uln(&self) -> (&Vec<Vec<String>>, &Vec<bool>) {
        // is := make(Vec<i64> , len(dgv.LN), len(self.LN))
        // for i, _ := range self.LN {
        // 	is[i] = int64(self.LN[i])
        // }
        //self.LN = nil // free
        (&self.ln, &self.xbl)
    }

    // pub fn  get_ulf(&self) -> (&Vec<f64>, &Vec<bool>) {
    // 	(&self.lf, &self.xbl)
    // }
    pub fn get_ulb(&self) -> (&Vec<Vec<u8>>, &Vec<bool>) {
        (&self.lb, &self.xbl)
    }
    pub fn get_ulbl(&self) -> (&Vec<bool>, &Vec<bool>) {
        (&self.lbl, &self.xbl)
    }

    pub fn get_id(&self) -> &Vec<i16> {
        &self.id
    }

    pub fn get_xf(&self) -> &Vec<i8> {
        &self.xf
    }

    pub fn get_ln(&self) -> &Vec<Vec<String>> {
        &self.ln
    }

    // Propagated Scalars - all List based (UID-pred stuff)
    //
    //	pub fn  GetULS() ([]string, []bool) {
    //		self.LS, self.XBl
    //	}
    //
    //	pub fn  GetULI() (Vec<i64> , []bool) {
    //		is := make(Vec<i64> , len(dgv.LN), len(dgv.LN))
    //		for i, _ := range dgv.LN {
    //			is[i] = int64(dgv.LN[i])
    //		}
    //		//dgv.LN = nil // free
    //		is
    //	}
    //
    //	pub fn  GetULF() ([]float64, []bool) {
    //		dgv.LN
    //	}
    //
    //	pub fn  GetULB() ([][]byte, []bool) {
    //		dgv.LB
    //	}
    //
    //	pub fn  GetULBl() ([]bool, []bool) {
    //		dgv.LBl
    //	}
    // this needs a refactor - too much cloning.
    pub fn get_nd(&self) -> (Vec<Vec<u8>>, Vec<i8>, Vec<Vec<u8>>) {
        // let nd : Vec<Vec<u8>>;
        // let xf : Vec<i64>;
        let mut ovfl: Vec<Vec<u8>> = vec![];
        let mut nd: Vec<Vec<u8>> = vec![];
        let mut xf: Vec<i8> = vec![];

        for (i, v) in self.nd.iter().enumerate() {
            // v = &Vec<u8>

            let x = self.xf[i];
            if x < UID_DETACHED {
                //	ChildUID ,	CuidInuse  ,UIDdetached
                nd.push(v.clone());
                xf.push(x);
            } else if x == OVFL_BLOCK_UID {
                // 	OVFL_BLOCK_UID  ,	OuidInuse      ,OBatchSizeLimit
                ovfl.push(v.clone());
            }
        }
        (nd, xf, ovfl)
    }

    // GetOfNd() takes a copy of the data cache result and returns the copy
    // The data cache should be protected by a read lock when the copy is taken
    // After the cache is read it should be unlocked. The copy can be accessed after
    // the lock is released as its a different memory object.
    pub fn get_ofnd(&self) -> (&Vec<Vec<u8>>, &Vec<i8>) {
        (&self.nd, &self.xf)
    }
}

pub struct OverflowItem {
    pub pkey: Vec<u8>,
    pub sortk: String, // "A#G#:N@1","A#G#:N@2"
    //
    // List (ordered set of any type but constrainted to a single type in DynaGraph)
    //
    pub nd: Vec<Vec<u8>>, // list of child node UIDs
    pub b: Vec<u8>,       // parent UID
    // scalar data
    pub ls: Vec<String>,
    pub li: Vec<i64>,
    pub lf: Vec<f64>,
    //LN  []float64
    pub lb: Vec<Vec<u8>>,
    pub lbl: Vec<bool>,
    pub ldt: Vec<String>,
    // flags
    pub xbl: Vec<bool>,
}

pub struct OverflowBlock<'a>(Vec<Vec<&'a OverflowItem>>);

pub struct Index {
    pub pkey: Vec<u8>,
    pub sortk: String,
    //
    pub ouid: Vec<Vec<u8>>, // overflow block UIDs - use type  UIDS = Vec<u8>; type IndexBlock = Vec<UIDS>
    //
    pub xb: Vec<Vec<u8>>,
    pub xf: Vec<Vec<i8>>,
}

pub struct IndexBlock<'a>(Vec<&'a Index>);

//=============== TyItem  ========================================================

// type dictionary
#[derive(Debug)]
pub struct TyItem {
    pub nm: String,     //`dynamodbav:"PKey"`  // type name
    pub attr: String,   //`dynamodbav:"SortK"` // attribute name
    pub ty: String,     // DataType
    pub f: Vec<String>, // facets name#DataType#CompressedIdentifer
    pub c: String,      // short name for attribute
    pub p: String,      // data partition containig attribute data - TODO: is this obselete???
    pub pg: bool,       // true: propagate scalar data to parent
    pub n: bool, // NULLABLE. False : not null (attribute will always exist ie. be populated), True: nullable (attribute may not exist)
    pub cd: i16, // cardinality - NOT USED
    pub sz: i16, // average size of attribute data - NOT USED
    pub ix: String, // supported indexes: FT=Full Text (S type only), "x" combined with Ty will index in GSI Ty_Ix
                    //	pub incp: Vec<String>, // (optional). List of attributes to be propagated. If empty all scalars will be propagated.
                    //	cardinality string   // 1:N , 1:1
}

impl From<&HashMap<String, AttributeValue>> for TyItem {
    fn from(value: &HashMap<String, AttributeValue>) -> Self {
        TyItem {
            nm: as_string(value.get("PKey"), &"".to_string()), // TODO: change to ),"") for all..
            attr: as_string(value.get("SortK"), &"".to_string()),
            ty: as_string(value.get("Ty"), &"".to_string()),
            f: vec![],
            c: as_string(value.get("C"), &"".to_string()),
            p: as_string(value.get("P"), &"".to_string()),
            pg: as_bool(value.get("Pg"), false),
            n: as_bool(value.get("N"), false),
            cd: as_i16(value.get("Cd"), 0),
            sz: as_i16(value.get("Sz"), 0),
            ix: as_string(value.get("Ix"), &"".to_string()),
            //incp : as_vec_string(value.get("incp"), vec![]),
        }
    }
}

pub struct TyIBlock(pub Vec<TyItem>);

//=============== TyAttrD (D for derived-type) ========================================================

// type attribute-block-derived from TyItem
#[derive(Debug, Clone)]
pub struct TyAttrD {
    pub name: String, // Attribute Identfier
    pub dt: String, // Derived value. Attribute Data Type - Nd (for uid-pred attribute only), (then scalars) DT,I,F,S,LI,SS etc
    pub c: String,  // Attribute short identifier
    pub ty: String, // For uid-pred only, the type it respresents e.g "Person"
    pub p: String,  // data partition (aka shard) containing attribute
    pub n: bool,    // true: nullable (attribute may not exist) false: not nullable
    pub pg: bool,   // true: propagate scalar data to parent
    //	pub incp : Vec<String>,
    pub ix: String, // index type
    pub card: String,
}

pub struct TyAttrBlock(pub Vec<TyAttrD>);

impl TyAttrBlock {
    pub fn get_uidpred(&self) -> Vec<&str> {
        let mut predc: Vec<&str> = vec![];
        for v in &self.0 {
            if v.dt == "Nd" {
                predc.push(&v.c);
            }
        }
        predc
    }
}

//
// type TyCache map[Ty]blk.TyAttrBlock
// var TyC TyCache
// type TyAttrCache map[Ty_Attr]blk.TyAttrD // map[Ty_Attr]blk.TyItem
// var TyAttrC TyAttrCache

// ***************** rdf ***********************

// struct objt<T> {
// 	ty : String,  // type def from Type
// 	value: T, // interface{} // rdf object value
// }

// // RDF SPO
// type RDF<T> struct {
// 	s : String, 	//[]byte // subject
// 	p : String, 	// predicate
// 	o : objt<T>,	// object
// }

//}
