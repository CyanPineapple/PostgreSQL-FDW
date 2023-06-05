use pgx::pg_sys;
use pgx::prelude::PgSqlErrorCode;
use pgx::Spi;
use std::collections::HashMap;

use supabase_wrappers::prelude::*;

use crate::fdw::db721_fdw::{Parser, ParserBuilder};

/* We support the following pushdowns:

wrappers=# explain select id from hello where id = 1 order by col limit 1;
                                                              QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=1.01..1.01 rows=1 width=40)
   ->  Sort  (cost=1.01..1.01 rows=1 width=40)
         Sort Key: col
         ->  Foreign Scan on hello  (cost=0.00..1.00 rows=1 width=0)
               Filter: (id = 1)
               Wrappers: quals = [Qual { field: "id", operator: "=", value: Cell(I32(1)), use_or: false, param: None }]
               Wrappers: tgts = [Column { name: "id", num: 1, type_oid: Oid(20) }, Column { name: "col", num: 2, type_oid: Oid(25) }]
               Wrappers: sorts = [Sort { field: "col", field_no: 2, reversed: false, nulls_first: false, collate: None }]
               Wrappers: limit = Some(Limit { count: 1, offset: 0 })
*/

#[wrappers_fdw(
    version = "0.1.0",
    author = "CyanPineapple",
    website = "www.github.com"
)]
pub(crate) struct PoloFdw {
    cur_row: i32,
    tgt_cols: Vec<Column>,
    parser: Parser,
}

impl PoloFdw {
    // foreign table and data source
}

impl ForeignDataWrapper for PoloFdw {
    fn new(options: &HashMap<String, String>) -> Self {
        let parser = ParserBuilder::new(
            // TODO: get filename from options
            String::from("/home/polo/Polo/PostgreSQL-FDW/db721-gen/data-farms.db721"),
            String::new(),
        )
        .build()
        .unwrap();

        //let owner = parser.data.borrow_owner();
        //let dep = parser.data.borrow_dependent();
        //println!("{:#?}",owner);
        //println!("{:#?}",dep.0.get(&0));

        Self {
            cur_row: 0,
            tgt_cols: Vec::new(),
            parser: parser,
        }
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual], // TODO: Propagate filters
        columns: &[Column],
        _sorts: &[Sort],        // TODO: Propagate sort
        _limit: &Option<Limit>, // TODO: maxRecords
        options: &HashMap<String, String>,
    ) {
        let tablename = options.get("tablename").unwrap();
        self.parser.tablename = tablename.to_string();
        self.tgt_cols = columns.to_vec();
    }

    fn iter_scan(&mut self, row: &mut Row) -> Option<()> {
        // TODO: add check col name
        let dep = self.parser.data.borrow_dependent();
        for tgt_col in &self.tgt_cols {
            if tgt_col.name == "farm_name" {
                row.push(&tgt_col.name, Some(Cell::String("mercy".to_string())));
                continue;
            }
            let arr = dep.0.get(&tgt_col.name).unwrap();
            row.push(&tgt_col.name, Some(Cell::I8(arr[0] as i8)));
        }
        if self.cur_row == 0 {
            self.cur_row += 1;
            return Some(());
        }
        None
    }

    fn end_scan(&mut self) {}

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) {}
}
