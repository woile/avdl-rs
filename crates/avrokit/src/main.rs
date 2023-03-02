use apache_avro::Schema;
use clap::{Parser, Subcommand, ValueEnum};
use std::path::{PathBuf, Path};
use avdl_parser::parse;
use std::fs;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, PartialEq, ValueEnum)]
enum ConvertTarget {
    // Idl,
    // Protocol,
    Schema,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Convert from AVDL to JSON AVSC schemas
    #[command(arg_required_else_help = true)]
    Convert {
        /// Type of conversion
        #[arg(required = true)]
        target: ConvertTarget,

        /// Path to AVDL file
        #[arg(required = true)]
        idl_file: PathBuf,

        /// Target folder to place the avsc schemas
        #[arg(required = false, value_parser, default_value = ".")]
        out: PathBuf
    },
}

fn main() {
    let args = Cli::parse();
    match args.command {
        Commands::Convert { target, idl_file: idl, out } => {
            let input = fs::read_to_string(idl)
            .expect("Should have been able to read the file");
        let (_tail, schemas) = parse(&input).expect("failed to parse");
        fs::create_dir_all(&out).expect("failed to create outdir");
        for schema in schemas {
            if let Schema::Record { name, aliases, doc, fields, lookup, attributes } = &schema {
                let filename = &name.name;
                let filename = format!("{filename}.avsc");
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
