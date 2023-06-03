mod parser;
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses() {
        let parser = parser::ParserBuilder::new(
            String::from("/home/polo/Polo/PostgreSQL-FDW/db721-gen/data-farms.db721"),
            String::from("Farm")
        )
        //.parse()
        //.build_index()
        .build()
        .unwrap();
        //println!("{:?}", parser.column_raw.clone());

        let owner = parser.data.borrow_owner();
        let dep = parser.data.borrow_dependent();
        println!("{:#?}",owner);
        println!("{:#?}",dep.0.get(&0));
    }
}
