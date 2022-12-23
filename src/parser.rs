use std::collections::BTreeMap;

use apache_avro::schema::{Alias, Aliases, Name, Namespace, RecordField, RecordFieldOrder, Schema};
use nom::{
    branch::{alt, permutation},
    bytes::complete::{escaped, is_a, tag, take_until, take_while, take_while1},
    character::{
        complete::{alphanumeric0, alphanumeric1, anychar, char, digit1, line_ending, multispace0},
        is_alphanumeric,
        streaming::one_of,
    },
    combinator::{cut, map, map_res, opt, recognize, value},
    error::context,
    multi::{many0, many1, separated_list1, self},
    sequence::{delimited, preceded, terminated, tuple},
    AsChar, IResult, InputIter, InputTake, InputTakeAtPosition, Parser,
};
use serde_json::{Number, Value};

fn parse_enum_item(input: &str) -> IResult<&str, &str> {
    delimited(multispace0, alphanumeric1, multispace0)(input)
}

pub fn parse_enum_symbols(input: &str) -> IResult<&str, Vec<&str>> {
    delimited(
        multispace0,
        delimited(
            tag("{"),
            separated_list1(tag(","), parse_enum_item),
            tag("}"),
        ),
        multispace0,
    )(input)
}

// TODO: Review this
fn parse_enum_name(input: &str) -> IResult<&str, &str> {
    space_delimited(preceded(tag("enum "), alphanumeric1))(input)
}

fn space_delimited<Input, Output, Error>(
    parser: impl Parser<Input, Output, Error>,
) -> impl FnMut(Input) -> IResult<Input, Output, Error>
where
    Error: nom::error::ParseError<Input>,
    Input: InputTake + InputTakeAtPosition,
    <Input as InputTakeAtPosition>::Item: AsChar,
    <Input as InputTakeAtPosition>::Item: Clone,
{
    delimited(multispace0, parser, multispace0)
}

// Example:
// ```
// @aliases(["org.foo.KindOf"])
// ```
// TODO: Take into account spaces
fn parse_aliases(i: &str) -> IResult<&str, Vec<Alias>> {
    preceded(
        tag("@aliases"),
        delimited(
            tag("(["),
            separated_list1(
                space_delimited(tag(",")),
                // delimited(multispace0, tag(","), multispace0),
                map_res(parse_namespace_value, |namespace| Alias::new(&namespace)),
            ),
            tag("])"),
        ),
    )(i)
}

// TODO: First and last letter should be alpha only
fn parse_namespace_value(input: &str) -> IResult<&str, String> {
    let ns = take_while(|c| char::is_alphanumeric(c) || c == '.' || c == '_');
    map(delimited(char('"'), ns, char('"')), |s: &str| {
        String::from(s)
    })(input)
}

// Example:
// ```
// @namespace("org.foo.KindOf")
// ```
fn parse_namespace(input: &str) -> IResult<&str, String> {
    preceded(
        tag("@namespace"),
        delimited(
            space_delimited(tag("(")),
            parse_namespace_value,
            preceded(multispace0, tag(")")),
        ),
    )(input)
}

fn parse_enum_default(input: &str) -> IResult<&str, &str> {
    terminated(
        preceded(space_delimited(tag("=")), parse_enum_item),
        tag(";"),
    )(input)
}

pub fn parse_enum(input: &str) -> IResult<&str, Schema> {
    let (tail, (aliases, name, body, _default)) = tuple((
        opt(parse_aliases),
        parse_enum_name,
        parse_enum_symbols,
        opt(parse_enum_default),
    ))(input)?;
    let n = Name::new(name).unwrap();

    // TODO: Check if we need to validate enum's default against one of the options
    if _default.is_some() {
        println!("Warning: default is being ignored as of now.")
    }

    Ok((
        tail,
        Schema::Enum {
            name: n,
            aliases: aliases,
            doc: None,
            symbols: body.into_iter().map(String::from).collect::<Vec<String>>(),
        },
    ))
}

fn parse_str<'a, E: nom::error::ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    escaped(alphanumeric1, '\\', one_of("\"n\\"))(i)
}

pub fn parse_string_default(input: &str) -> IResult<&str, &str> {
    context(
        "string default",
        preceded(
            space_delimited(tag("=")),
            preceded(char('"'), cut(terminated(parse_str, char('"')))),
        ),
    )(input)
}

pub fn parse_string(input: &str) -> IResult<&str, (&str, Option<&str>)> {
    preceded(
        tag("string"),
        cut(terminated(
            space_delimited(tuple((alphanumeric1, opt(parse_string_default)))),
            char(';'),
        )),
    )(input)
}

pub fn parse_boolean_default(input: &str) -> IResult<&str, bool> {
    let parse_true = value(true, tag("true"));
    let parse_false = value(false, tag("false"));
    let bool_parser = alt((parse_true, parse_false));
    context(
        "boolean default",
        preceded(space_delimited(tag("=")), bool_parser),
    )(input)
}

pub fn parse_boolean(input: &str) -> IResult<&str, (&str, Option<bool>)> {
    preceded(
        tag("boolean"),
        cut(terminated(
            space_delimited(tuple((alphanumeric1, opt(parse_boolean_default)))),
            char(';'),
        )),
    )(input)
}

pub fn parse_int_default(input: &str) -> IResult<&str, i32> {
    let parse_int = map_res(digit1, |v: &str| v.parse::<i32>());
    context(
        "int default",
        preceded(space_delimited(tag("=")), parse_int),
    )(input)
}

pub fn parse_int(input: &str) -> IResult<&str, (&str, Option<i32>)> {
    preceded(
        tag("int"),
        cut(terminated(
            space_delimited(tuple((alphanumeric1, opt(parse_int_default)))),
            char(';'),
        )),
    )(input)
}

pub fn parse_long_default(input: &str) -> IResult<&str, i64> {
    let parse_long = map_res(digit1, |v: &str| v.parse::<i64>());
    context(
        "long default",
        preceded(space_delimited(tag("=")), parse_long),
    )(input)
}

pub fn parse_long(input: &str) -> IResult<&str, (&str, Option<i64>)> {
    preceded(
        tag("long"),
        cut(terminated(
            space_delimited(tuple((alphanumeric1, opt(parse_long_default)))),
            char(';'),
        )),
    )(input)
}

pub fn parse_float_default(input: &str) -> IResult<&str, f32> {
    let parse_float = map_res(
        take_while1(|c| char::is_digit(c, 10) || c == '.' || c == 'e'),
        |v: &str| v.parse::<f32>(),
    );
    context(
        "float default",
        preceded(space_delimited(tag("=")), parse_float),
    )(input)
}

pub fn parse_float(input: &str) -> IResult<&str, (&str, Option<f32>)> {
    preceded(
        tag("float"),
        cut(terminated(
            space_delimited(tuple((alphanumeric1, opt(parse_float_default)))),
            char(';'),
        )),
    )(input)
}

pub fn parse_double_default(input: &str) -> IResult<&str, f64> {
    let parse_double = map_res(
        take_while1(|c| char::is_digit(c, 10) || c == '.' || c == 'e'),
        |v: &str| v.parse::<f64>(),
    );
    context(
        "double default",
        preceded(space_delimited(tag("=")), parse_double),
    )(input)
}

pub fn parse_double(input: &str) -> IResult<&str, (&str, Option<f64>)> {
    preceded(
        tag("double"),
        cut(terminated(
            space_delimited(tuple((alphanumeric1, opt(parse_double_default)))),
            char(';'),
        )),
    )(input)
}

pub fn parse_array(input: &str) -> IResult<&str, (&str, Option<f64>)> {
    todo!("I think we are gonna have to change to an enum. It should return one of the enum")
}

pub fn parse_doc(input: &str) -> IResult<&str, &str> {
    delimited(tag("/**"), take_until("*/"), tag("*/"))(input)
}

fn parse_record_name(input: &str) -> IResult<&str, &str> {
    preceded(tag("record"), space_delimited(alphanumeric1))(input)
}

fn parse_field(input: &str) -> IResult<&str, RecordField> {
    // let (tail, (rstring, rbool, rint, rlong, rfloat, rdouble)) = alt((
    preceded(
        multispace0,
        alt((
            map(parse_string, |(name, default)| RecordField {
                name: name.to_string(),
                doc: None,
                default: default.map(|v| Value::String(v.to_string())),
                schema: Schema::String,
                order: RecordFieldOrder::Ascending,
                position: 0,
            }),
            map(parse_boolean, |(name, default)| RecordField {
                name: name.to_string(),
                doc: None,
                default: default.map(|v| Value::Bool(v)),
                schema: Schema::Boolean,
                order: RecordFieldOrder::Ascending,
                position: 0,
            }),
            map(parse_int, |(name, default)| RecordField {
                name: name.to_string(),
                doc: None,
                default: default.map(|v| Value::Number(v.into())),
                schema: Schema::Int,
                order: RecordFieldOrder::Ascending,
                position: 0,
            }),
            map(parse_long, |(name, default)| RecordField {
                name: name.to_string(),
                doc: None,
                default: default.map(|v| Value::Number(v.into())),
                schema: Schema::Long,
                order: RecordFieldOrder::Ascending,
                position: 0,
            }),
            map(parse_float, |(name, default)| RecordField {
                name: name.to_string(),
                doc: None,
                default: default.map(|v| {
                    Value::Number(Number::from_f64(v.into()).expect("Could not handle f32"))
                }),
                schema: Schema::Float,
                order: RecordFieldOrder::Ascending,
                position: 0,
            }),
            map(parse_double, |(name, default)| RecordField {
                name: name.to_string(),
                doc: None,
                default: default
                    .map(|v| Value::Number(Number::from_f64(v).expect("Could not handle f64"))),
                schema: Schema::Double,
                order: RecordFieldOrder::Ascending,
                position: 0,
            }),
        )),
    )(input)
}

// Sample of record
// ```
// record Employee {
//     string name;
//     boolean active = true;
//     long salary;
// }
// ```
pub fn parse_record(input: &str) -> IResult<&str, Schema> {
    let (tail, ((namespace, aliases), name, fields)) = tuple((
        // TODO: Review this permutation, it's only working one of the two permutations
        // Follow https://github.com/Geal/nom/issues/1153
        permutation((
            opt(terminated(
                preceded(multispace0, parse_namespace),
                tuple((line_ending, multispace0)),
            )),
            opt(terminated(
                preceded(multispace0, parse_aliases),
                tuple((line_ending, multispace0)),
            )),
        )),
        preceded(multispace0, parse_record_name),
        preceded(
            multispace0,
            delimited(
                tag("{"),
                many1(parse_field),
                preceded(multispace0, tag("}")),
            ),
        ),
    ))(input)?;
    let mut name = Name::new(name).unwrap();

    name.namespace = namespace;

    Ok((
        tail,
        Schema::Record {
            name: name,
            aliases: aliases,
            doc: None,
            fields: fields,
            lookup: BTreeMap::new(),
        },
    ))
}

pub fn parse_protocol(input: &str) -> IResult<&str, Vec<Schema>> {
    let (tail, (_name, schema)) = tuple((
        preceded(
            multispace0,
            preceded(tag("protocol"),
            space_delimited(alphanumeric1),
        )
        ),
        delimited(
            space_delimited(tag("{")),
            many1(space_delimited(parse_record)),
            preceded(multispace0, tag("}")),
        )
    ))(input)?;
    Ok((tail, schema))
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::{
        parse_aliases, parse_boolean, parse_doc, parse_double, parse_enum, parse_enum_default,
        parse_enum_item, parse_enum_symbols, parse_field, parse_float, parse_int, parse_long,
        parse_namespace, parse_namespace_value, parse_record, parse_record_name, parse_string,
        parse_protocol
    };
    use apache_avro::schema::{Alias, Name, RecordField, RecordFieldOrder, Schema};
    use rstest::rstest;
    use serde_json::{Number, Value};

    #[rstest]
    #[case("string message;", ("message", None))]
    #[case("string  message;", ("message", None))]
    #[case("string message ;", ("message", None))]
    #[case(r#"string message = "holis" ;"#, ("message", Some("holis")))]
    #[case(r#"string message = "holis";"#, ("message", Some("holis")))]
    fn test_parse_string_ok(#[case] input: &str, #[case] expected: (&str, Option<&str>)) {
        assert_eq!(parse_string(input), Ok(("", expected)));
    }

    #[test]
    fn test_parse_string_fail() {
        let invalid_strings = [
            "string message",              // missing semi-colon
            r#"string message = "holis"#,  // unclosed quote
            r#"string message = "holis""#, // no semi-colon
        ];
        for input in invalid_strings {
            assert!(parse_string(input).is_err());
        }
    }

    #[rstest]
    #[case("boolean active;", ("active", None))]
    #[case("boolean active = true;", ("active", Some(true)))]
    #[case("boolean active = false;", ("active", Some(false)))]
    #[case("boolean   active   =   false ;", ("active", Some(false)))]
    fn test_parse_boolean_ok(#[case] input: &str, #[case] expected: (&str, Option<bool>)) {
        assert_eq!(parse_boolean(input), Ok(("", expected)));
    }

    #[test]
    fn test_parse_boolean_fail() {
        let invalid_booleans = [
            "boolean message",              // missing semi-colon
            r#"boolean message = "false""#, // wrong type
            r#"boolean message = true"#,    // missing semi-colon with default
        ];
        for input in invalid_booleans {
            assert!(parse_boolean(input).is_err());
        }
    }

    #[rstest]
    #[case("int age;", ("age", None))]
    #[case("int age = 12;", ("age", Some(12)))]
    #[case("int age = 0;", ("age", Some(0)))]
    #[case("int   age   =   123 ;", ("age", Some(123)))]
    fn test_parse_int_ok(#[case] input: &str, #[case] expected: (&str, Option<i32>)) {
        assert_eq!(parse_int(input), Ok(("", expected)));
    }

    #[test]
    fn test_parse_int_fail() {
        let invalid_ints = [
            "int age",                        // missing semi-colon
            r#"int age = "false""#,           // wrong type
            r#"int age = 123"#,               // missing semi-colon with default
            "int age = 9223372036854775807;", // longer than i32
        ];
        for input in invalid_ints {
            assert!(parse_int(input).is_err());
        }
    }

    #[rstest]
    #[case("long stock;", ("stock", None))]
    #[case("long stock = 12;", ("stock", Some(12)))]
    #[case("long stock = 9223372036854775807;", ("stock", Some(9223372036854775807)))]
    #[case("long stock = 0;", ("stock", Some(0)))]
    #[case("long   stock   =   123 ;", ("stock", Some(123)))]
    fn test_parse_long_ok(#[case] input: &str, #[case] expected: (&str, Option<i64>)) {
        assert_eq!(parse_long(input), Ok(("", expected)));
    }

    #[rstest]
    #[case("float age;", ("age", None))]
    #[case("float age = 12;", ("age", Some(12.0)))]
    #[case("float age = 12.0;", ("age", Some(12.0)))]
    #[case("float age = 0.0;", ("age", Some(0.0)))]
    #[case("float age = .0;", ("age", Some(0.0)))]
    #[case("float age = 0.1123;", ("age", Some(0.1123)))]
    #[case("float age = 3.40282347e38;", ("age", Some(f32::MAX)))]
    #[case("float age = 0;", ("age", Some(0.0)))]
    #[case("float   age   =   123 ;", ("age", Some(123.0)))]
    fn test_parse_float_ok(#[case] input: &str, #[case] expected: (&str, Option<f32>)) {
        assert_eq!(parse_float(input), Ok(("", expected)));
    }

    #[test]
    fn test_parse_float_fail() {
        let invalid_floats = [
            "float age",                  // missing semi-colon
            r#"float age = "false""#,     // wrong type
            r#"float age = 123"#,         // missing semi-colon with default
            "float age = 3.50282347e39;", // longer than i32
        ];

        for input in invalid_floats {
            println!("input: {input}");
            assert!(parse_float(input).is_err());
        }
    }

    #[rstest]
    #[case("double stock;", ("stock", None))]
    #[case("double stock = 12;", ("stock", Some(12.0)))]
    #[case("double stock = 9223372036854775807;", ("stock", Some(9223372036854775807.0)))]
    #[case("double stock = 123.456;", ("stock", Some(123.456)))]
    #[case("double stock = 1.7976931348623157e308;", ("stock", Some(f64::MAX)))]
    #[case("double stock = 0.0;", ("stock", Some(0.0)))]
    #[case("double stock = .0;", ("stock", Some(0.0)))]
    #[case("double stock = 0;", ("stock", Some(0.0)))]
    #[case("double   stock   =   123.3 ;", ("stock", Some(123.3)))]
    fn test_parse_double_ok(#[case] input: &str, #[case] expected: (&str, Option<f64>)) {
        assert_eq!(parse_double(input), Ok(("", expected)));
    }

    #[test]
    fn test_parse_double_fail() {
        let invalid_doubles = [
            "double stock",              // missing semi-colon
            r#"double stock = "false""#, // wrong type
            r#"double stock = 123"#,     // missing semi-colon with default
        ];
        for input in invalid_doubles {
            assert!(parse_double(input).is_err());
        }
    }

    #[test]
    fn test_parse_item() {
        let items = ["   CIRCLE  ", "\nCIRCLE\n\n"];
        for item in items {
            let out = parse_enum_item(item);
            assert_eq!(out, Ok(("", "CIRCLE")))
        }
    }

    #[test]
    fn test_enum_body() {
        let bodies = [
            "{ SQUARE, TRIANGLE, CIRCLE, OVAL }",
            "{SQUARE,TRIANGLE, CIRCLE,OVAL }",
            "{ SQUARE,TRIANGLE,CIRCLE,OVAL}",
            "{SQUARE,TRIANGLE,CIRCLE,OVAL}",
        ];
        let expected = vec!["SQUARE", "TRIANGLE", "CIRCLE", "OVAL"];
        for body in bodies {
            let out = parse_enum_symbols(body);
            assert_eq!(out, Ok(("", expected.clone())))
        }
    }

    #[test]
    fn test_parse_enum() {
        let input = "enum Shapes {
            SQUARE, TRIANGLE, CIRCLE, OVAL
        }";
        let o = parse_enum(input);
        let expected = Schema::Enum {
            name: Name::new("Shapes").unwrap(),
            aliases: None,
            doc: None,
            symbols: vec![
                String::from("SQUARE"),
                String::from("TRIANGLE"),
                String::from("CIRCLE"),
                String::from("OVAL"),
            ],
        };
        assert_eq!(o, Ok(("", expected)));
    }

    #[test]
    fn test_parse_enum_with_alias() {
        let input = r#"@aliases(["org.old.OldRecord", "org.ancient.AncientRecord"])
        enum Shapes {
            SQUARE, TRIANGLE, CIRCLE, OVAL
        }"#;
        let o = parse_enum(input);
        let expected = Schema::Enum {
            name: Name::new("Shapes").unwrap(),
            aliases: Some(vec![
                Alias::new("org.old.OldRecord").unwrap(),
                Alias::new("org.ancient.AncientRecord").unwrap(),
            ]),
            doc: None,
            symbols: vec![
                String::from("SQUARE"),
                String::from("TRIANGLE"),
                String::from("CIRCLE"),
                String::from("OVAL"),
            ],
        };
        assert_eq!(o, Ok(("", expected)));
    }

    #[test]
    fn test_parse_enum_with_alias_and_default() {
        let input = r#"@aliases(["org.old.OldRecord", "org.ancient.AncientRecord"])
        enum Shapes {
            SQUARE, TRIANGLE, CIRCLE, OVAL
        } = SQUARE;"#;
        let o = parse_enum(input);
        let expected = Schema::Enum {
            name: Name::new("Shapes").unwrap(),
            aliases: Some(vec![
                Alias::new("org.old.OldRecord").unwrap(),
                Alias::new("org.ancient.AncientRecord").unwrap(),
            ]),
            doc: None,
            symbols: vec![
                String::from("SQUARE"),
                String::from("TRIANGLE"),
                String::from("CIRCLE"),
                String::from("OVAL"),
            ],
        };
        assert_eq!(o, Ok(("", expected)));
    }

    #[rstest]
    #[case(r#"@aliases(["oldField", "ancientField"])"#, vec![Alias::new("oldField").unwrap(), Alias::new("ancientField").unwrap()])]
    #[case(r#"@aliases(["oldField","ancientField"])"#, vec![Alias::new("oldField").unwrap(), Alias::new("ancientField").unwrap()])]
    #[case(r#"@aliases(["org.old.OldRecord","org.ancient.AncientRecord"])"#, vec![Alias::new("org.old.OldRecord").unwrap(), Alias::new("org.ancient.AncientRecord").unwrap()])]
    fn test_alias(#[case] input: &str, #[case] expected: Vec<Alias>) {
        assert_eq!(parse_aliases(input), Ok(("", expected)));
    }

    #[rstest]
    #[case(
        r#"@namespace("org.apache.avro.test")"#,
        String::from("org.apache.avro.test")
    )]
    #[case(
        r#"@namespace  ( "org.apache.avro.test" )"#,
        String::from("org.apache.avro.test")
    )]
    #[case(
        r#"@namespace  ( "org.apache.avro.test" )"#,
        String::from("org.apache.avro.test")
    )]
    #[case(
        r#"@namespace  (
        "org.apache.avro.test"
    )"#,
        String::from("org.apache.avro.test")
    )]
    fn test_parse_namespace(#[case] input: &str, #[case] expected: String) {
        assert_eq!(parse_namespace(input), Ok(("", expected)));
    }

    #[rstest]
    #[case(r#""org.ancient.AncientRecord""#, "org.ancient.AncientRecord".to_string())]
    #[case(r#""ancientField""#, "ancientField".to_string())]
    fn test_namespace_parser(#[case] input: &str, #[case] expected: String) {
        assert_eq!(parse_namespace_value(input), Ok(("", expected)))
    }

    #[rstest]
    #[case(r#"= holis;"#, "holis")]
    #[case(r#"= holis ;"#, "holis")]
    #[case(r#"= CIRCLE;"#, "CIRCLE")]
    fn test_parse_enum_default(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(parse_enum_default(input), Ok(("", expected)))
    }

    #[rstest]
    #[case(
        "/** Documentation for the enum type Kind */",
        " Documentation for the enum type Kind "
    )]
    fn test_parse_doc(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(parse_doc(input), Ok(("", expected)))
    }

    #[rstest]
    #[case("record Hello", "Hello")]
    #[case("record   OneTwo  ", "OneTwo")]
    fn test_parse_record_name(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(parse_record_name(input), Ok(("", expected)))
    }

    #[rstest]
    #[case("string Hello;", RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::String, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case(r#"string nickname = "Woile";"#, RecordField{ name: String::from("nickname"), doc: None, default: Some(Value::String("Woile".to_string())), schema: Schema::String, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case("boolean Hello;", RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::Boolean, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case("boolean Hello = true;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Bool(true)), schema: Schema::Boolean, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case("int Hello;", RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::Int, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case("int Hello = 1;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Number(1.into())), schema: Schema::Int, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case("long Hello;", RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::Long, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case("long Hello = 123;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Number(123.into())), schema: Schema::Long, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case("float Hello;", RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::Float, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case("float Hello = 123;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Number(Number::from_f64(123.0).unwrap())), schema: Schema::Float, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case("float Hello = 123.0;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Number(Number::from_f64(123.0).unwrap())), schema: Schema::Float, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case("double Hello;", RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::Double, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case("double Hello = 123;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Number(Number::from_f64(123.0).unwrap())), schema: Schema::Double, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    #[case("double Hello = 123.0;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Number(Number::from_f64(123.0).unwrap())), schema: Schema::Double, order: apache_avro::schema::RecordFieldOrder::Ascending, position: 0 })]
    fn test_parse_field(#[case] input: &str, #[case] expected: RecordField) {
        assert_eq!(parse_field(input), Ok(("", expected)))
    }

    #[test]
    fn test_parse_record() {
        let sample = r#"record Employee {
            string name;
            boolean active = true;
            long salary;
        }"#;
        let (_tail, schema) = parse_record(sample).unwrap();
        let canonical_form = schema.canonical_form();
        let expected = r#"{"name":"Employee","type":"record","fields":[{"name":"name","type":"string"},{"name":"active","type":"boolean"},{"name":"salary","type":"long"}]}"#;
        assert_eq!(canonical_form, expected)
    }

    #[test]
    fn test_parse_record_alias() {
        let sample = r#"@aliases(["org.old.OldRecord", "org.ancient.AncientRecord"])
        record Employee {
            string name;
        }"#;
        let (_tail, schema) = parse_record(sample).unwrap();
        let expected = Schema::Record {
            name: Name {
                name: "Employee".into(),
                namespace: None,
            },
            aliases: Some(vec![
                Alias::new("org.old.OldRecord".into()).unwrap(),
                Alias::new("org.ancient.AncientRecord".into()).unwrap(),
            ]),
            doc: None,
            fields: vec![RecordField {
                name: "name".into(),
                doc: None,
                default: None,
                schema: Schema::String,
                order: RecordFieldOrder::Ascending,
                position: 0,
            }],
            lookup: BTreeMap::new(),
        };
        println!("{schema:#?}");
        assert_eq!(schema, expected);
    }

    #[rstest]
    #[case(
        r#"@namespace("org.apache.avro.someOtherNamespace")
    @aliases(["org.old.OldRecord", "org.ancient.AncientRecord"])
    record Employee {
        string name;
    }"#
    )]
    #[case(
        r#"
        @aliases(["org.old.OldRecord", "org.ancient.AncientRecord"])
        @namespace("org.apache.avro.someOtherNamespace")
    record Employee {
        string name;
    }"#
    )]
    fn test_parse_record_alias_and_namespace(#[case] input: &str) {
        let (_tail, schema) = parse_record(input).unwrap();

        let expected = Schema::Record {
            name: Name {
                name: "Employee".into(),
                namespace: Some("org.apache.avro.someOtherNamespace".into()),
            },
            aliases: Some(vec![
                Alias::new("org.old.OldRecord".into()).unwrap(),
                Alias::new("org.ancient.AncientRecord".into()).unwrap(),
            ]),
            doc: None,
            fields: vec![RecordField {
                name: "name".into(),
                doc: None,
                default: None,
                schema: Schema::String,
                order: RecordFieldOrder::Ascending,
                position: 0,
            }],
            lookup: BTreeMap::new(),
        };
        assert_eq!(schema, expected);
    }
    #[rstest]
    #[case(r#"protocol MyProtocol {
        record Hello {
            string name;
        }
    }"#)]
    fn test_parse_protocol(#[case]input: &str) {
        let r = parse_protocol(input).unwrap();
        println!("{r:#?}");
    }
}