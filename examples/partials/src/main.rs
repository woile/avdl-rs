use apache_avro::Schema;
use avdl_rs::parser::{parse_enum_symbols, parse_enum};

fn main() {

    let o = parse_enum_symbols("{ HOLI, QUEHACE}");
    println!("{o:?}");

    let input = "enum Shapes {
        SQUARE, TRIANGLE, CIRCLE, OVAL
    }";
    let (tail, schema) = parse_enum(input).unwrap();
    let can = schema.canonical_form();
    let schema_str = r#"{"name":"Shapes","type":"enum","symbols":["SQUARE","TRIANGLE","CIRCLE","OVAL"], "default": "SQUARE"}"#;
    let r = Schema::parse_str(schema_str).unwrap();
    let can = r.canonical_form();
    println!("{r:?}");
    println!("{can}");
    println!("{o:?}");

}
