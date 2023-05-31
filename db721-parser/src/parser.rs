use std::char::from_u32_unchecked;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::str::from_utf8;
use lazy_static::lazy_static;
use std::sync::Mutex;
/* 
CREATE FOREIGN TABLE IF NOT EXISTS db721_farm
(
    farm_name       varchar,
--     sexes           varchar[],
    min_age_weeks   real,
    max_age_weeks   real
) SERVER db721_server OPTIONS
(
    filename '/home/kapi/git/postgres/data-farms.db721',
    tablename 'Farm'
);
);
*/
use std::{fs::File, collections::HashMap};
use std::io::Read;
use nom::{IResult, bytes::streaming::take};
use serde_json::Value;
use serde::{Serialize, Deserialize};

lazy_static!(
        // column name as key
      pub static ref COLUMN_META: Mutex<BTreeMap<usize, ColumnMeta>> = {
        let mut m = Mutex::new(BTreeMap::new());
        m
    };

);

pub struct Parser<'par> {
    filename: String,
    tablename: String,
    data: Vec<u8>,
    // also start index as key
    column_raw: HashMap<usize, &'par[u8]>,
}

/* 
metadata["Table"]: the table name (string)

metadata["Max Values Per Block"]: the maximum number of values in each block (int)

metadata["Columns"]: the table's columns (JSON dict)
    Keys: column names (string)
    Values: column data, see below (JSON dict)

metadata["Columns"]["Column Name"]: column data (JSON dict)
    Keys:
    "type": the column type (str), possible values are:
        "float" | "int" | "str"

    "start_offset": the offset in the file for the first block of this column (int)

    "num_blocks": the number of blocks for this column (int)

    "block_stats": statistics for the 0-indexed fixed-size blocks (JSON dict)
        Keys: block index (string)
        Values: statistics for the corresponding block (JSON dict)
            Keys:
            "num": the number of values in this block (int)
            "min": the minimum value in this block (same type as column)
            "max": the maximum value in this block (same type as column)
            "min_len": only exists for str column; the min length of a string in this block (int)
            "max_len": only exists for str column; the max length of a string in this block (int)
*/

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockStats {
    num: u32,
    min: Value,
    max: Value,
    min_len: Option<u32>,
    max_len: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    #[serde(rename = "type")]
    column_type: String,
    start_offset: usize,
    num_blocks: u32,
    block_stats: HashMap<String, BlockStats>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Metadata {
    table: String,
    #[serde( deserialize_with = "column_maker")]
    columns: HashMap<String, Column>,
    #[serde(rename = "Max Values Per Block")]
    max_values_per_block: u32,
}

// The most frequently used column data
#[derive(Debug)]
pub struct ColumnMeta {
    column_name: String,
    column_type: String,
    elem_size: u32, // size of the element in bytes
}

fn column_maker<'de, D>(deserializer: D) -> Result<HashMap<String, Column>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mut map: HashMap<String, Column> = HashMap::new();
    let value: Value = serde::Deserialize::deserialize(deserializer)?;
    if let Value::Object(obj) = value {
        for (key, value) in obj {
            let column: Column = serde_json::from_value(value).unwrap();
            map.insert(key.clone(), column.clone());

            COLUMN_META.lock().unwrap().insert(column.start_offset, ColumnMeta {
                column_name: key,
                column_type: column.column_type.clone(),
                // FIXME: change this into dynamically calculated value
                elem_size: match column.column_type.as_str() {
                    "float" => 4,
                    "int" => 4,
                    "str" => 32,
                    _ => panic!("Invalid column type"),
                },
            });
        }
    }
    Ok(map)
}


impl<'par> Parser<'par> {
    pub fn new(filename: String, tablename: String) -> Parser<'par> {
        Parser {
            filename: filename,
            tablename: tablename,
            data: Vec::new(),
            column_raw: HashMap::new(),
        }
    }

    fn read_file_contents(filename: &str) -> Result<Vec<u8>, std::io::Error> {
        let mut file = File::open(filename)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    // pushdowns: qual and sort. 
    // qual stores an index, 
    // sort needs an array of index, so we save indexes to b+ trees.
    fn parse_raw (rawdata: &'par [u8], tempraw: &'par mut HashMap<usize, &'par[u8]>) {
        let rawb = &*(COLUMN_META.lock().unwrap()); 
        let mut offsets = vec![];
        rawb.keys().for_each(|k| {
            offsets.push(k);
        });
        let len = rawdata.len();
        offsets.push(&len);
        println!("{:#?}", offsets);
        let mut curdata = rawdata;
        offsets.windows(2).for_each(|w| {
            let (i, o) = take::<usize, &[u8], nom::error::Error<&[u8]>> (
                w[1] - w[0])(curdata).unwrap();
            curdata = i;
            tempraw.insert(*w[0], o);
        }); 
        println!("{:?}", tempraw);

    }

    pub fn parse(&'par mut self) {
        // three sections: raw meta and size
        self.data = Parser::read_file_contents(&self.filename).unwrap();
        let body_bytes = &self.data[..];
        let (i, o) = take::<usize, &[u8], nom::error::Error<&[u8]>> (
            body_bytes.len() - 4)(body_bytes).unwrap();
        println!("i: {:?}", i);
        println!("o: {:?}", o);
        let size: u32 = u32::from_le_bytes(i.try_into().unwrap());
        println!("{}", size);
        let (meta, raw) = take::<usize, &[u8], nom::error::Error<&[u8]>> (
            o.len() - size as usize)(o).unwrap();
        let json_str = std::str::from_utf8(meta).expect("DB721|Metadata: Invalid MetaData format");
        println!("json_str: {:?}", json_str);
        let json_struct: Metadata = serde_json::from_str(json_str).expect("DB721|Metadata: Invalid json format");
        println!("meta_struct: {:#?}", json_struct);
        println!("{:?}",COLUMN_META.lock().unwrap());
        println!("raw size: {}", raw.len());

        let tempraw = &mut self.column_raw;
        Parser::parse_raw(raw, tempraw);
        //let (f, s) = take::<usize, &[u8], nom::error::Error<&[u8]>> (
            //24)(raw).unwrap();
        //let (f, s) = take::<usize, &[u8], nom::error::Error<&[u8]>> (
            //192)(s).unwrap();
        //let name_str = std::str::from_utf8(s).expect("DB721|Metadata: Invalid MetaData format");
        //println!("name_str: {:?}", name_str);
        // float(4) str(32) float(4)

        

    }
}