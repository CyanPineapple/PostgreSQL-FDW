use std::process::Command;
const GENPY: &str = "/home/polo/Polo/PostgreSQL-FDW/db721-gen/chicken_farm_gen.py";

fn main() {
    Command::new("python3")
        .arg(GENPY)
        .spawn()
        .expect("failed to execute process");
}
