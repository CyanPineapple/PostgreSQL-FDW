use std::hash::Hash;
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

pub struct Parser {
    filename: String,
    tablename: String,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockStats {
    num: u32,
    min: Value,
    max: Value,
    min_len: Option<u32>,
    max_len: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    #[serde(rename = "type")]
    column_type: String,
    start_offset: u32,
    num_blocks: u32,
    block_stats: HashMap<String, BlockStats>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Metadata {
    table: String,
    columns: HashMap<String, Column>,
    #[serde(rename = "Max Values Per Block")]
    max_values_per_block: u32,
}

impl Parser {
    pub fn new(filename: String, tablename: String) -> Parser {
        Parser {
            filename,
            tablename
        }
    }

    fn read_file_contents(filename: &str) -> Result<Vec<u8>, std::io::Error> {
        let mut file = File::open(filename)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    pub fn parse(&self) {
        // three sections: raw meta and size
        let file_contents = Parser::read_file_contents(&self.filename).unwrap();
        let body_bytes = &file_contents[..];
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
        println!("meta_struct: {:?}", json_struct);

        

    }
}