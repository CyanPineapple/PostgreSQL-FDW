mod parser;
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses() {
        let mut parser = parser::Parser::new(
            String::from("/home/polo/Polo/PostgreSQL-FDW/db721-gen/data-farms.db721"),
            String::from("Farm")
        );
        parser.parse();
        //println!("{:?}", parser.column_raw.clone());
    }
}
