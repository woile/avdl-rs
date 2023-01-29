use std::collections::BTreeMap;

use apache_avro::schema::{
    Alias, Name, RecordFieldOrder,
};


use nom::combinator::{map_opt, verify};

use nom::{
    branch::{alt, permutation},
    bytes::complete::{escaped, tag, take_until, take_while, take_while1},
    character::{
        complete::{alphanumeric1, char, digit1, line_ending, multispace0},
        streaming::one_of,
    },
    combinator::{cut, map, map_res, opt, value},
    error::context,
    multi::{many1, separated_list1},
    sequence::{delimited, preceded, terminated, tuple},
    AsChar, IResult, InputTake, InputTakeAtPosition, Parser,
};
use serde_json::{Number, Value};

use crate::schema::{Schema, SchemaKind, RecordField, UnionSchema};

// Alias to give more clarity on what is being returned
type VarName<'a> = &'a str;
type EnumSymbol<'a> = &'a str;

// Samples:
// ```
// COIN
// NUMBER
// ```
fn parse_enum_item(input: &str) -> IResult<&str, VarName> {
    delimited(multispace0, parse_var_name, multispace0)(input)
}

// Sample:
// ```
// { COIN, NUMBER }
// ```
pub fn parse_enum_symbols(input: &str) -> IResult<&str, Vec<EnumSymbol>> {
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
// ```
// enum Items
// ```
fn parse_enum_name(input: &str) -> IResult<&str, VarName> {
    space_delimited(preceded(space_delimited(tag("enum")), parse_var_name))(input)
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

// The name portion of the fullname of named types, record field names, and enum symbols must:
//
// - start with [A-Za-z_]
// - subsequently contain only [A-Za-z0-9_]
// https://avro.apache.org/docs/1.11.1/specification/#names
fn parse_var_name(input: &str) -> IResult<&str, &str> {
    verify(
        take_while(|c| char::is_alphanumeric(c) || c == '_'),
        |s: &str| s.chars().take(1).any(|c| char::is_alpha(c) || c == '_'),
    )(input)
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

// Example:
// ```
// @order("ascending")  // default
// @order("descending")
// @order("ignore")
// ```
pub fn parse_order(input: &str) -> IResult<&str, RecordFieldOrder> {
    let ascending = value(RecordFieldOrder::Ascending, tag(r#""ascending""#));
    let descending = value(RecordFieldOrder::Descending, tag(r#""descending""#));
    let ignore = value(RecordFieldOrder::Ignore, tag(r#""ignore""#));
    let order_parser = alt((ascending, descending, ignore));
    preceded(
        tag("@order"),
        delimited(
            space_delimited(tag("(")),
            order_parser,
            preceded(multispace0, tag(")")),
        ),
    )(input)
}
// Sample:
// ```
// = COIN;
// ```
fn parse_enum_default(input: &str) -> IResult<&str, &str> {
    terminated(
        preceded(space_delimited(tag("=")), parse_enum_item),
        tag(";"),
    )(input)
}

// Sample:
// ```
// enum Items { COIN, NUMBER } = COIN;
// ```
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
            attributes: BTreeMap::new(),
        },
    ))
}

fn parse_str<'a, E: nom::error::ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    escaped(alphanumeric1, '\\', one_of("\"n\\"))(i)
}

// Sample
// ```
// "pepe"
// ```
pub fn map_string(input: &str) -> IResult<&str, &str> {
    preceded(char('"'), cut(terminated(parse_str, char('"'))))(input)
}

// Sample
// ```
// = "pepe"
// ```
pub fn parse_string_default(input: &str) -> IResult<&str, &str> {
    context(
        "string default",
        preceded(space_delimited(tag("=")), map_string),
    )(input)
}

// Sample:
// ```
// string name = "jon";
// ```
pub fn parse_string(
    input: &str,
) -> IResult<
    &str,
    (
        Option<RecordFieldOrder>,
        Option<Vec<Alias>>,
        VarName,
        Option<&str>,
    ),
> {
    preceded(
        tag("string"),
        cut(terminated(
            space_delimited(tuple((
                opt(space_delimited(parse_order)),
                opt(space_delimited(parse_aliases)),
                parse_var_name,
                opt(parse_string_default),
            ))),
            char(';'),
        )),
    )(input)
}

// Sample:
// ```
// bytes name = "jon";
// ```
// For the default the only reference I found is:
//      https://docs.oracle.com/cd/E26161_02/html/GettingStartedGuide/avroschemas.html
// It reads: Default values for bytes and fixed fields are JSON strings.
pub fn parse_bytes(
    input: &str,
) -> IResult<
    &str,
    (
        Option<RecordFieldOrder>,
        Option<Vec<Alias>>,
        VarName,
        Option<&str>,
    ),
> {
    preceded(
        tag("bytes"),
        cut(terminated(
            space_delimited(tuple((
                opt(space_delimited(parse_order)),
                opt(space_delimited(parse_aliases)),
                parse_var_name,
                opt(parse_string_default),
            ))),
            char(';'),
        )),
    )(input)
}

// Sample:
// ```
// true
// ```
pub fn map_bool(input: &str) -> IResult<&str, bool> {
    let parse_true = value(true, tag("true"));
    let parse_false = value(false, tag("false"));
    alt((parse_true, parse_false))(input)
}

// Sample:
// ```
// = true
// ```
pub fn parse_boolean_default(input: &str) -> IResult<&str, bool> {
    context(
        "boolean default",
        preceded(space_delimited(tag("=")), map_bool),
    )(input)
}

// Sample:
// ```
// boolean active = true;
// ```
pub fn parse_boolean(
    input: &str,
) -> IResult<&str, (Option<RecordFieldOrder>, VarName, Option<bool>)> {
    preceded(
        tag("boolean"),
        cut(terminated(
            space_delimited(tuple((
                opt(space_delimited(parse_order)),
                parse_var_name,
                opt(parse_boolean_default),
            ))),
            char(';'),
        )),
    )(input)
}

// Sample:
// ```
// 20
// ```
pub fn map_int(input: &str) -> IResult<&str, i32> {
    map_res(digit1, |v: &str| v.parse::<i32>())(input)
}

// Sample:
// ```
// = 20
// ```
pub fn parse_int_default(input: &str) -> IResult<&str, i32> {
    context("int default", preceded(space_delimited(tag("=")), map_int))(input)
}

// Sample:
// ```
// int age = 20;
// ```
pub fn parse_int(input: &str) -> IResult<&str, (Option<RecordFieldOrder>, VarName, Option<i32>)> {
    preceded(
        tag("int"),
        cut(terminated(
            space_delimited(tuple((
                opt(space_delimited(parse_order)),
                parse_var_name,
                opt(parse_int_default),
            ))),
            char(';'),
        )),
    )(input)
}

// Sample:
// ```
// 20
// ```
pub fn map_long(input: &str) -> IResult<&str, i64> {
    map_res(digit1, |v: &str| v.parse::<i64>())(input)
}

// Sample:
// ```
// = 20
// ```
pub fn parse_long_default(input: &str) -> IResult<&str, i64> {
    context(
        "long default",
        preceded(space_delimited(tag("=")), map_long),
    )(input)
}

// Sample:
// ```
// long age = 20;
// ```
pub fn parse_long(input: &str) -> IResult<&str, (Option<RecordFieldOrder>, VarName, Option<i64>)> {
    preceded(
        tag("long"),
        cut(terminated(
            space_delimited(tuple((
                opt(space_delimited(parse_order)),
                parse_var_name,
                opt(parse_long_default),
            ))),
            char(';'),
        )),
    )(input)
}

// Sample:
// ```
// 20.0
// ```
pub fn map_float(input: &str) -> IResult<&str, f32> {
    map_res(
        take_while1(|c| char::is_digit(c, 10) || c == '.' || c == 'e'),
        |v: &str| v.parse::<f32>(),
    )(input)
}

// Sample:
// ```
// = 20.0
// ```
pub fn parse_float_default(input: &str) -> IResult<&str, f32> {
    context(
        "float default",
        preceded(space_delimited(tag("=")), map_float),
    )(input)
}

// Sample:
// ```
// float age = 20;
// ```
pub fn parse_float(input: &str) -> IResult<&str, (Option<RecordFieldOrder>, VarName, Option<f32>)> {
    preceded(
        tag("float"),
        cut(terminated(
            space_delimited(tuple((
                opt(space_delimited(parse_order)),
                parse_var_name,
                opt(parse_float_default),
            ))),
            char(';'),
        )),
    )(input)
}

// Sample:
// ```
// 20.0
// ```
fn map_double(input: &str) -> IResult<&str, f64> {
    map_res(
        take_while1(|c| char::is_digit(c, 10) || c == '.' || c == 'e'),
        |v: &str| v.parse::<f64>(),
    )(input)
}

// Sample:
// ```
// = 20.0
// ```
pub fn parse_double_default(input: &str) -> IResult<&str, f64> {
    context(
        "double default",
        preceded(space_delimited(tag("=")), map_double),
    )(input)
}

// Sample:
// ```
// double age = 20.0;
// ```
pub fn parse_double(
    input: &str,
) -> IResult<&str, (Option<RecordFieldOrder>, VarName, Option<f64>)> {
    preceded(
        tag("double"),
        cut(terminated(
            space_delimited(tuple((
                opt(space_delimited(parse_order)),
                parse_var_name,
                opt(parse_double_default),
            ))),
            char(';'),
        )),
    )(input)
}

// Sample
// ```
// array<long> arrayOfLongs;
// ```
pub fn parse_array(input: &str) -> IResult<&str, (&str, Option<f64>)> {
    todo!("I think we are gonna have to change to an enum. It should return one of the enum")
}

pub fn parse_union_default(input: &str) -> IResult<&str, &str> {
    // This should be take_until ";"
    preceded(space_delimited(tag("=")), take_until(";"))(input)
}

// Sample
// ```
// union { null, string } item_id = null;
// ```
// TODO: Handle @order + @alias properly, they can happen in any order, between
// the list of types and the variable name
pub fn parse_union(
    input: &str,
) -> IResult<
    &str,
    (
        (
            Vec<Schema>,
            Option<RecordFieldOrder>,
            Option<Vec<Alias>>,
            VarName,
        ),
        Option<Value>,
    ),
> {
    let parse_union_types = preceded(
        tag("union"),
        delimited(
            space_delimited(tag("{")),
            separated_list1(
                space_delimited(tag(",")),
                map(alphanumeric1, |value_type| match value_type {
                    "null" => Schema::Null,
                    "boolean" => Schema::Boolean,
                    "string" => Schema::String,
                    "int" => Schema::Int,
                    "double" => Schema::Double,
                    "float" => Schema::Float,
                    "long" => Schema::Long,
                    "bytes" => Schema::Bytes,
                    // TOOD: return nom Error instead of panic
                    _ => panic!("Something went wrong {value_type}"),
                }),
            ),
            space_delimited(tag("}")),
        ),
    );
    let (tail, x) = tuple((
        parse_union_types,
        opt(parse_order),
        opt(parse_aliases),
        parse_var_name,
    ))(input)?;
    let first_schema =
        x.0.first()
            .expect("there should be at least one schema in the union");
    let first_schema_kind: SchemaKind = first_schema.into();
    let (tail, y) = terminated(
        opt(map_opt(parse_union_default, |v| {
            Some(map_schema_to_value(first_schema_kind.clone(), v))
        })),
        char(';'),
    )(tail)?;
    Ok((tail, (x, y)))
}

// Sample
// ```
// /** This is a doc */
// ```
pub fn parse_doc(input: &str) -> IResult<&str, &str> {
    delimited(tag("/**"), take_until("*/"), tag("*/"))(input)
}

// Sample
// ```
// record TestRecord
// ```
fn parse_record_name(input: &str) -> IResult<&str, &str> {
    preceded(tag("record"), space_delimited(alphanumeric1))(input)
}

fn map_schema_to_value(schema: SchemaKind, value: &str) -> Value {
    match schema {
        SchemaKind::Null => Value::Null,
        SchemaKind::Boolean => {
            let (_, v) = map_bool(value).unwrap();
            Value::Bool(v)
        }
        SchemaKind::Int => {
            let (_, v) = map_int(value).unwrap();
            Value::Number(v.into())
        }
        SchemaKind::Long => {
            let (_, v) = map_long(value).unwrap();
            Value::Number(v.into())
        }
        SchemaKind::Float => {
            let (_, v) = map_float(value).unwrap();
            Value::Number(Number::from_f64(v.into()).expect("Could not handle f32"))
        }
        SchemaKind::Double => {
            let (_, v) = map_double(value).unwrap();
            Value::Number(Number::from_f64(v).expect("Could not handle f64"))
        }
        SchemaKind::Bytes => {
            let (_, pstr) = map_string(value).expect("invalid string");
            let v: Vec<u8> = Vec::from(pstr);

            Value::Array(v.into_iter().map(|b| b.into()).collect())
        }
        SchemaKind::String => {
            let (_, pstr) = map_string(value).expect("invalid string");
            Value::String(pstr.to_string())
        }
        _ => unimplemented!("Not implemented yet"),
    }
}

// Sample
// ```
// string @order("ignore") name = "jon";
// ```
fn parse_field(input: &str) -> IResult<&str, RecordField> {
    preceded(
        multispace0,
        alt((
            map(
                tuple((opt(space_delimited(parse_doc)), parse_string)),
                |(doc, (order, aliases, name, default))| RecordField {
                    name: name.to_string(),
                    doc: doc.map(String::from),
                    default: default.map(|v| Value::String(v.to_string())),
                    schema: Schema::String,
                    order: order.unwrap_or(RecordFieldOrder::Ascending),
                    aliases: aliases,
                    position: 0,
                    custom_attributes: BTreeMap::new(),
                },
            ),
            map(parse_boolean, |(order, name, default)| RecordField {
                name: name.to_string(),
                doc: None,
                default: default.map(|v| Value::Bool(v)),
                schema: Schema::Boolean,
                order: order.unwrap_or(RecordFieldOrder::Ascending),
                aliases: None,
                position: 0,
                custom_attributes: BTreeMap::new(),
            }),
            map(parse_int, |(order, name, default)| RecordField {
                name: name.to_string(),
                doc: None,
                default: default.map(|v| Value::Number(v.into())),
                schema: Schema::Int,
                order: order.unwrap_or(RecordFieldOrder::Ascending),
                aliases: None,
                position: 0,
                custom_attributes: BTreeMap::new(),
            }),
            map(parse_long, |(order, name, default)| RecordField {
                name: name.to_string(),
                doc: None,
                default: default.map(|v| Value::Number(v.into())),
                schema: Schema::Long,
                order: order.unwrap_or(RecordFieldOrder::Ascending),
                aliases: None,
                position: 0,
                custom_attributes: BTreeMap::new(),
            }),
            map(parse_float, |(order, name, default)| RecordField {
                name: name.to_string(),
                doc: None,
                default: default.map(|v| {
                    Value::Number(Number::from_f64(v.into()).expect("Could not handle f32"))
                }),
                schema: Schema::Float,
                order: order.unwrap_or(RecordFieldOrder::Ascending),
                aliases: None,
                position: 0,
                custom_attributes: BTreeMap::new(),
            }),
            map(parse_double, |(order, name, default)| RecordField {
                name: name.to_string(),
                doc: None,
                default: default
                    .map(|v| Value::Number(Number::from_f64(v).expect("Could not handle f64"))),
                schema: Schema::Double,
                order: order.unwrap_or(RecordFieldOrder::Ascending),
                aliases: None,
                position: 0,
                custom_attributes: BTreeMap::new(),
            }),
            map(parse_union, |((schemas, order, aliases, name), default)| {
                RecordField {
                    name: name.to_string(),
                    doc: None,
                    default: default,
                    schema: Schema::Union(
                        UnionSchema::new(schemas).expect("Failed to create union schema"),
                    ),
                    order: order.unwrap_or(RecordFieldOrder::Ascending),
                    aliases: aliases,
                    position: 0,
                    custom_attributes: BTreeMap::new(),
                }
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
            attributes: BTreeMap::new(),
        },
    ))
}

// Sample:
// ```
// protocol Simple {
//    record Simple {
//      string name;
//      int age;
//    }
// }
// ```
pub fn parse_protocol(input: &str) -> IResult<&str, Vec<Schema>> {
    let (tail, (_name, schema)) = tuple((
        preceded(
            multispace0,
            preceded(tag("protocol"), space_delimited(alphanumeric1)),
        ),
        delimited(
            space_delimited(tag("{")),
            many1(space_delimited(parse_record)),
            preceded(multispace0, tag("}")),
        ),
    ))(input)?;
    Ok((tail, schema))
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::{
        parse_aliases, parse_boolean, parse_bytes, parse_doc, parse_double, parse_enum,
        parse_enum_default, parse_enum_item, parse_enum_symbols, parse_field, parse_float,
        parse_int, parse_long, parse_namespace, parse_namespace_value, parse_order, parse_protocol,
        parse_record, parse_record_name, parse_string, parse_union, parse_var_name, VarName,
    };
    use crate::schema::{RecordField, Schema};
    use apache_avro::schema::{Alias, Name, RecordFieldOrder, Schema as SourceSchema};
    use rstest::rstest;
    use serde_json::{Number, Value};

    #[rstest]
    #[case("string message;", (None, None, "message",None))]
    #[case("string  message;", (None, None, "message",None))]
    #[case("string message ;", (None, None, "message",None))]
    #[case(r#"string message = "holis" ;"#, (None, None, "message",Some("holis")))]
    #[case(r#"string message = "holis";"#, (None, None, "message",Some("holis")))]
    #[case(r#"string @order("ignore") message = "holis";"#, (Some(RecordFieldOrder::Ignore), None, "message",Some("holis")))]
    fn test_parse_string_ok(
        #[case] input: &str,
        #[case] expected: (
            Option<RecordFieldOrder>,
            Option<Vec<Alias>>,
            &str,
            Option<&str>,
        ),
    ) {
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
    #[case("my_name", "my_name", "")]
    #[case("myname", "myname", "")]
    #[case("numbers3", "numbers3", "")]
    #[case("numbers3_", "numbers3_", "")]
    #[case("n20umbers3", "n20umbers3", "")]
    #[case("_n20umbers3", "_n20umbers3", "")]
    #[case("_n20umbers3_", "_n20umbers3_", "")]
    fn test_varname(#[case] input: &str, #[case] expected: &str, #[case] tail: &str) {
        assert_eq!(parse_var_name(input), Ok((tail, expected)))
    }

    #[test]
    fn test_parse_var_name_fail() {
        let invalid_var_name = [
            "1var_name",
            "-1var_name",
            "$0_1var_name",
            "1_n20umbers3",
            "1_n20umbers3_",
        ];
        for input in invalid_var_name {
            assert!(parse_var_name(input).is_err());
        }
    }

    #[rstest]
    #[case("bytes message;", (None, None, "message",None))]
    #[case("bytes  message;", (None, None, "message",None))]
    #[case("bytes message ;", (None, None, "message",None))]
    #[case(r#"bytes message = "holis" ;"#, (None, None, "message",Some("holis")))]
    #[case(r#"bytes message = "holis";"#, (None, None, "message",Some("holis")))]
    #[case(r#"bytes @order("ignore") message = "holis";"#, (Some(RecordFieldOrder::Ignore), None, "message",Some("holis")))]
    fn test_parse_bytes_ok(
        #[case] input: &str,
        #[case] expected: (
            Option<RecordFieldOrder>,
            Option<Vec<Alias>>,
            &str,
            Option<&str>,
        ),
    ) {
        assert_eq!(parse_bytes(input), Ok(("", expected)));
    }

    #[rstest]
    #[case("boolean active;", (None, "active", None))]
    #[case(r#"boolean @order("ignore") active;"#, (Some(RecordFieldOrder::Ignore), "active", None))]
    #[case("boolean active = true;", (None, "active", Some(true)))]
    #[case("boolean active = false;", (None, "active", Some(false)))]
    #[case("boolean   active   =   false ;", (None, "active", Some(false)))]
    fn test_parse_boolean_ok(
        #[case] input: &str,
        #[case] expected: (Option<RecordFieldOrder>, &str, Option<bool>),
    ) {
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
    #[case("int age;", (None, "age", None))]
    #[case("int age = 12;", (None, "age", Some(12)))]
    #[case("int age = 0;", (None, "age", Some(0)))]
    #[case("int   age   =   123 ;", (None, "age", Some(123)))]
    fn test_parse_int_ok(
        #[case] input: &str,
        #[case] expected: (Option<RecordFieldOrder>, &str, Option<i32>),
    ) {
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
    #[case("long stock;", (None, "stock", None))]
    #[case("long stock = 12;", (None, "stock", Some(12)))]
    #[case("long stock = 9223372036854775807;", (None, "stock", Some(9223372036854775807)))]
    #[case("long stock = 0;", (None, "stock", Some(0)))]
    #[case("long   stock   =   123 ;", (None, "stock", Some(123)))]
    fn test_parse_long_ok(
        #[case] input: &str,
        #[case] expected: (Option<RecordFieldOrder>, &str, Option<i64>),
    ) {
        assert_eq!(parse_long(input), Ok(("", expected)));
    }

    #[rstest]
    #[case("float age;", (None, "age", None))]
    #[case("float age = 12;", (None, "age", Some(12.0)))]
    #[case("float age = 12.0;", (None, "age", Some(12.0)))]
    #[case("float age = 0.0;", (None, "age", Some(0.0)))]
    #[case("float age = .0;", (None, "age", Some(0.0)))]
    #[case("float age = 0.1123;", (None, "age", Some(0.1123)))]
    #[case("float age = 3.40282347e38;", (None, "age", Some(f32::MAX)))]
    #[case("float age = 0;", (None, "age", Some(0.0)))]
    #[case("float   age   =   123 ;", (None, "age", Some(123.0)))]
    fn test_parse_float_ok(
        #[case] input: &str,
        #[case] expected: (Option<RecordFieldOrder>, &str, Option<f32>),
    ) {
        assert_eq!(parse_float(input), Ok(("", expected)));
    }

    #[test]
    fn test_parse_float_fail() {
        let invalid_floats = [
            "float age",                  // missing semi-colon
            r#"float age = "false""#,     // wrong type
            r#"float age = 123"#,         // missing semi-colon with default
            "float age = 3.50282347e40;", // longer than f32
        ];

        for input in invalid_floats {
            println!("input: {input}");
            assert!(parse_float(input).is_err());
        }
    }

    #[rstest]
    #[case("double stock;", (None, "stock", None))]
    #[case("double stock = 12;", (None, "stock", Some(12.0)))]
    #[case("double stock = 9223372036854775807;", (None, "stock", Some(9223372036854775807.0)))]
    #[case("double stock = 123.456;", (None, "stock", Some(123.456)))]
    #[case("double stock = 1.7976931348623157e308;", (None, "stock", Some(f64::MAX)))]
    #[case("double stock = 0.0;", (None, "stock", Some(0.0)))]
    #[case("double stock = .0;", (None, "stock", Some(0.0)))]
    #[case("double stock = 0;", (None, "stock", Some(0.0)))]
    #[case(r#"double @order("descending") stock = 0;"#, (Some(RecordFieldOrder::Descending), "stock", Some(0.0)))]
    #[case("double   stock   =   123.3 ;", (None, "stock", Some(123.3)))]
    fn test_parse_double_ok(
        #[case] input: &str,
        #[case] expected: (Option<RecordFieldOrder>, &str, Option<f64>),
    ) {
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
            attributes: BTreeMap::new(),
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
            attributes: BTreeMap::new(),
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
            attributes: BTreeMap::new(),
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
    #[case(
        r#"union { null, string } item_id = null;"#, ((vec![Schema::Null, Schema::String], None, None,"item_id"), Some(Value::Null))
    )]
    #[case(
        r#"union { null, string } item = null;"#, ((vec![Schema::Null, Schema::String], None, None,"item"), Some(Value::Null))
    )]
    #[case(
        r#"union { int, string } item = 1;"#, ((vec![Schema::Int, Schema::String], None, None,"item"), Some(Value::Number(1.into())))
    )]
    #[case(
        r#"union { string, int } item = "1";"#, ((vec![Schema::String, Schema::Int], None, None,"item"), Some(Value::String("1".to_string())))
    )]
    fn test_union(
        #[case] input: &str,
        #[case] expected: (
            (
                Vec<Schema>,
                Option<RecordFieldOrder>,
                Option<Vec<Alias>>,
                VarName,
            ),
            Option<Value>,
        ),
    ) {
        assert_eq!(parse_union(input), Ok(("", expected)));
    }

    #[rstest]
    #[case(r#"@order("ascending")"#, RecordFieldOrder::Ascending)]
    #[case(
        r#"@order(
        "ascending"
    )"#,
        RecordFieldOrder::Ascending
    )]
    #[case(r#"@order("descending")"#, RecordFieldOrder::Descending)]
    #[case(r#"@order("ignore")"#, RecordFieldOrder::Ignore)]
    fn test_parse_order(#[case] input: &str, #[case] expected: RecordFieldOrder) {
        assert_eq!(parse_order(input), Ok(("", expected)));
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
    #[case("string Hello;", RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::String, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case(r#"string nickname = "Woile";"#, RecordField{ name: String::from("nickname"), doc: None, default: Some(Value::String("Woile".to_string())), schema: Schema::String, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case("boolean Hello;", RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::Boolean, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case("boolean Hello = true;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Bool(true)), schema: Schema::Boolean, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case("int Hello;", RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::Int, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case("int Hello = 1;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Number(1.into())), schema: Schema::Int, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case("long Hello;", RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::Long, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case("long Hello = 123;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Number(123.into())), schema: Schema::Long, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case("float Hello;", RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::Float, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case("float Hello = 123;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Number(Number::from_f64(123.0).unwrap())), schema: Schema::Float, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case("float Hello = 123.0;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Number(Number::from_f64(123.0).unwrap())), schema: Schema::Float, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case("double Hello;", RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::Double, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case(r#"double @order("ignore") Hello;"#, RecordField{ name: String::from("Hello"), doc: None, default: None, schema: Schema::Double, order: apache_avro::schema::RecordFieldOrder::Ignore, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case("double Hello = 123;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Number(Number::from_f64(123.0).unwrap())), schema: Schema::Double, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
    #[case("double Hello = 123.0;", RecordField{ name: String::from("Hello"), doc: None, default: Some(Value::Number(Number::from_f64(123.0).unwrap())), schema: Schema::Double, order: apache_avro::schema::RecordFieldOrder::Ascending, aliases: None, position: 0, custom_attributes: BTreeMap::new() })]
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
        let schema: SourceSchema = schema.into();
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
                aliases: None,
                position: 0,
                custom_attributes: BTreeMap::new(),
            }],
            lookup: BTreeMap::new(),
            attributes: BTreeMap::new(),
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
                aliases: None,
                position: 0,
                custom_attributes: BTreeMap::new(),
            }],
            lookup: BTreeMap::new(),
            attributes: BTreeMap::new(),
        };
        assert_eq!(schema, expected);
    }
    #[rstest]
    #[case(
        r#"protocol MyProtocol {
        record Hello {
            string name;
        }
    }"#
    )]
    fn test_parse_protocol(#[case] input: &str) {
        let r = parse_protocol(input).unwrap();
        println!("{r:#?}");
    }

    #[test]
    fn test_parse_big_record() {
        let input_schema = r#"@namespace("org.apache.avro.someOtherNamespace")
        @aliases(["org.old.OldRecord", "org.ancient.AncientRecord"])
        record Employee {
            /** person fullname */
            string name;
            string @aliases(["item"]) item_id = "ABC123";
            int age;
        }"#;
        let (_tail, schema) = parse_record(input_schema).unwrap();
        let out = serde_json::to_string_pretty(&schema).unwrap();
        println!("{out}");
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
            fields: vec![
                RecordField {
                    name: "name".into(),
                    doc: Some(String::from("person fullname")),
                    default: None,
                    schema: Schema::String,
                    order: RecordFieldOrder::Ascending,
                    aliases: None,
                    position: 0,
                    custom_attributes: BTreeMap::new(),
                },
                RecordField {
                    name: "item_id".into(),
                    doc: None,
                    default: Some(Value::String(String::from("ABC123"))),
                    schema: Schema::String,
                    order: RecordFieldOrder::Ascending,
                    aliases: None,
                    position: 0,
                    custom_attributes: BTreeMap::new(),
                },
                RecordField {
                    name: "age".into(),
                    doc: None,
                    default: None,
                    schema: Schema::Int,
                    order: RecordFieldOrder::Ascending,
                    aliases: None,
                    position: 0,
                    custom_attributes: BTreeMap::new(),
                },
            ],
            lookup: BTreeMap::new(),
            attributes: BTreeMap::new(),
        };
        assert_eq!(schema, expected);
    }
}
