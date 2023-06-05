#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]

mod tests {
    use pgx::pg_test;
    use pgx::prelude::*;

    #[pg_test]
    fn polo_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"
                DROP foreign data wrapper IF EXISTS polo_wrapper CASCADE;
                "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                CREATE foreign data wrapper polo_wrapper
                    handler polo_fdw_handler
                    validator polo_fdw_validator;
                    "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                CREATE SERVER IF NOT EXISTS polo_server
                foreign data wrapper polo_wrapper; 
             "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                CREATE FOREIGN TABLE IF NOT EXISTS db721_farm
                (                                   
                    farm_name       varchar,
                    min_age_weeks   real,
                    max_age_weeks   real
                ) SERVER polo_server OPTIONS
                (
                    tablename 'Farm'
                );
             "#,
                None,
                None,
            )
            .unwrap();

            /*
             The tables below come from the code in docker-compose.yml that looks like this:

             ```
             volumes:
                   - ${PWD}/dockerfiles/bigquery/data.yaml:/app/data.yaml
             ```
            */

            let results = c
                .select("SELECT * FROM db721_farm", None, None)
                .unwrap();

            println!("{:#?}",results);
                
                
                //.filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                //.collect::<Vec<_>>();

            //assert_eq!(results, vec!["foo", "bar"]);

        });
    }
}
