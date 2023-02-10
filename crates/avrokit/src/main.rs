use apache_avro::Schema;
use clap::{Parser, Subcommand};
use std::path::{PathBuf, Path};
use avdl_parser::parser::parse_protocol;
use std::fs;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Convert from AVDL to JSON AVSC schemas
    #[command(arg_required_else_help = true)]
    Idl2schemata {
        /// Path to AVDL file
        #[arg(required = true)]
        idl: PathBuf,

        /// Target folder to place the avsc schemas
        #[arg(required = false, value_parser, default_value = ".")]
        out: PathBuf
    },
}

fn main() {
    let args = Cli::parse();
    match args.command {
        Commands::Idl2schemata { idl, out } => {
            let input = fs::read_to_string(idl)
            .expect("Should have been able to read the file");
        let (_tail, schemas) = parse_protocol(&input).expect("failed to parse");
        fs::create_dir_all(&out).expect("failed to create outdir");
        for schema in schemas {
            if let Schema::Record { name, aliases, doc, fields, lookup, attributes } = &schema {
                let filename = format!("{name}.avsc");
                let outpath = Path::new(&out).join(filename);
                // let contents = schema.canonical_form();
                let json = serde_json::to_string_pretty(&schema).unwrap();
                fs::write(outpath, json).expect("Failed to write to file");

            }
            // match &schema {

            //     Schema::Record { name, aliases, doc, fields, lookup, attributes } => {
            //         let filename = format!("{name}.avsc");
            //         let outpath = Path::new(&out).join(filename);
            //         // let contents = schema.canonical_form();
            //         let json = serde_json::to_string_pretty(&schema).unwrap();
            //         fs::write(outpath, json).expect("Failed to write to file");

            //     },
            //     _ => panic!("Invalid")
            // }

        }
        },
    }
}
