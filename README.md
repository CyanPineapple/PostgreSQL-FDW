# PostgreSQL-FDW
Rust impl of CMU 15-721 23' Proj 1

Based on [Supabase Wrappers](https://github.com/supabase/wrappers) 

The core functions are under `db721-parser`<br>
The building scripts(including sql files to deploy fdw) are under `db721-gen`<br>
If you want to use it directly. Clone [Supabase Wrappers](https://github.com/supabase/wrappers) repo. Then:
```bash
cp -r db721_fdw $PATH_TO_SUPABASE_WRAPPERS/wrappers/src/fdw
```
You also need to modify Corresponding `mod.rs` and `Cargo.toml` <br>
For example, in my configuration. I add:
```toml
[features]
db721_fdw = ["serde", "serde_json", "nom", "self_cell", "lazy_static"]
[dependencies]
nom = { version = "7.1.3", optional = true }
lazy_static = { version = "1.4.0", optional = true }
self_cell = { version = "1.0.0", optional = true }
...
```
in `$PATH_TO_SUPABASE_WRAPPERS/wrappers/Cargo.toml`  and
```rust
cfg_if! {
    if #[cfg(feature = "db721_fdw")] {
        mod db721_fdw;
    }
}
```
in `$PATH_TO_SUPABASE_WRAPPERS/wrappers/src/fdw/mod.rs`

Then you can test your FDW using
```bash
cargo pgx test --features db721_fdw
```
or mannually run in psql using 
```bash
cargo pgx run --features db721_fdw

# load operation.sql. You can find it under db721-gen
wrappers=# \i operation.sql
# see FDW working
wrappers=# select * from db721_farm;
```
For any problems of `pgx` check [pgrx github page](https://github.com/tcdi/pgrx), [pgx crate.io](https://crates.io/crates/pgx), and [Supabase Wrappers](https://github.com/supabase/wrappers) 

