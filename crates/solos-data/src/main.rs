//! `solos-data <collector|decoder|augment|dev> <command> [flags]`.

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    std::process::exit(solos_data::cli::main(&args));
}
