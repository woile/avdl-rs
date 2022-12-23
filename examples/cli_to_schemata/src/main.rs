#[macro_use]
extern crate serde_json;

use apache_avro::Schema;
use avdl_rs::parser::{parse_protocol};
use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("must provide filepath and output folder");
        return;
    }
    let file_path = &args[1];
    let output_dir = &args[2];
    println!("{file_path} {output_dir}");
    let input = fs::read_to_string(file_path)
        .expect("Should have been able to read the file");
    let (_tail, schemas) = parse_protocol(&input).expect("failed to parse");
    fs::create_dir_all(output_dir).expect("failed to create outdir");

    for schema in schemas {
        match &schema {

            Schema::Record { name, aliases, doc, fields, lookup } => {
                let filename = format!("{name}.avsc");
                let outpath = Path::new(output_dir).join(filename);
                // let contents = schema.canonical_form();
                let json = serde_json::to_string_pretty(&schema).unwrap();
                fs::write(outpath, json).expect("Failed to write to file");

            },
            _ => panic!("Invalid")
        }

    }

}
