use std::collections::{BTreeMap, HashMap};

use crate::string_parser::parse_string as parse_string_uni;
use apache_avro::schema::{Alias, Name, RecordFieldOrder};
use apache_avro::schema::{RecordField, Schema, SchemaKind, UnionSchema};
use apache_avro::types::Value as AvroValue;
use nom::bytes::complete::take_till;
use nom::character::complete::space0;

use nom::combinator::{map_opt, verify};

use nom::multi::separated_list0;
use nom::sequence::pair;
use nom::{
    branch::alt,
    bytes::complete::{escaped, tag, take_until, take_while, take_while1},
    character::{
        complete::{alphanumeric0, alphanumeric1, char, digit1, multispace0},
        streaming::one_of,
    },
    combinator::{cut, map, map_res, opt, value},
    error::context,
    multi::{many1, separated_list1},
    sequence::{delimited, preceded, terminated, tuple},
    AsChar, IResult, InputTake, InputTakeAtPosition, Parser,
};
use nom_permutation::permutation_opt;
use serde_json::{Map, Number, Value};
use std::str::FromStr;
use uuid::Uuid;

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
fn parse_enum_symbols(input: &str) -> IResult<&str, Vec<EnumSymbol>> {
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

fn comment_delimited<'a, Input: 'a, Output: 'a, Error: 'a>(
    parser: impl Parser<Input, Output, Error> + 'a,
) -> impl FnMut(Input) -> IResult<Input, Output, Error> + 'a
where
    Error: nom::error::ParseError<Input>,
    Input: InputTake
        + InputTakeAtPosition
        + std::clone::Clone
        + nom::Compare<&'a str>
        // + nom::InputIter
        + nom::InputIter
        + nom::InputLength
        + nom::FindSubstring<&'a str>,
    <Input as InputTakeAtPosition>::Item: AsChar,
    <Input as InputTakeAtPosition>::Item: Clone,
    <Input as InputTakeAtPosition>::Item: PartialEq<char>,
{
    delimited(
        space_delimited(opt(parse_comment)),
        parser,
        space_delimited(opt(parse_comment)),
    )
}

// Example:
// ```
// @aliases(["org.foo.KindOf"])
// ```
// TODO: Take into account spaces
fn parse_aliases(i: &str) -> IResult<&str, Vec<String>> {
    preceded(
        tag("@aliases"),
        delimited(
            tag("(["),
            separated_list1(
                space_delimited(tag(",")),
                // delimited(multispace0, tag(","), multispace0),
                // map_res(parse_namespace_value, |namespace| Alias::new(&namespace)),
                parse_namespace_value,
            ),
            tag("])"),
        ),
    )(i)
}

fn map_parse_aliases(i: &str) -> IResult<&str, Vec<Alias>> {
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

// Example:
// ```
// @logicalType("timestamp-micros")
// ```
fn parse_logical_type(i: &str) -> IResult<&str, Schema> {
    preceded(
        tag("@logicalType"),
        delimited(
            tag("("),
            map(parse_string_uni, |s| match s.as_str() {
                "timestamp-micros" => {
                    println!("MATHCES");
                    return Schema::TimestampMicros;
                }
                "time-micros" => Schema::TimeMicros,
                "duration" => Schema::Duration,
                _ => todo!(),
            }),
            comment_delimited(tag(")")),
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
fn parse_enum_default(input: &str) -> IResult<&str, String> {
    terminated(
        preceded(
            space_delimited(tag("=")),
            map(parse_enum_item, |value| value.to_string()),
        ),
        tag(";"),
    )(input)
}

// Sample:
// ```
// enum Items { COIN, NUMBER } = COIN;
// ```
fn parse_enum(input: &str) -> IResult<&str, Schema> {
    let (tail, (aliases, name, body, default)) = tuple((
        opt(map_parse_aliases),
        parse_enum_name,
        parse_enum_symbols,
        opt(parse_enum_default),
    ))(input)?;
    let n = Name::new(name).unwrap();

    // TODO: Check if we need to validate enum's default against one of the options
    if default.is_some() {
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

/** ***************************** */
/** Map Native and logical types  */
/** ***************************** */

// Sample
// ```
// "pepe"
// ```
fn map_string(input: &str) -> IResult<&str, AvroValue> {
    map(parse_string_uni, |v| AvroValue::String(v))(input)
}

fn map_uuid(input: &str) -> IResult<&str, AvroValue> {
    map_res(parse_string_uni, |v| -> Result<AvroValue, String> {
        let uuid_val = Uuid::from_str(&v).map_err(|_e| "not a valid uuid".to_string())?;
        Ok(AvroValue::Uuid(uuid_val))
    })(input)
}

fn map_bytes(input: &str) -> IResult<&str, AvroValue> {
    map(parse_string_uni, |v| {
        let v: Vec<u8> = Vec::from(v);
        AvroValue::Bytes(v)
    })(input)
}

// Sample
// ```
// null
// ```
fn map_null(input: &str) -> IResult<&str, AvroValue> {
    value(AvroValue::Null, tag("null"))(input)
}

// Sample:
// ```
// true
// ```
fn map_bool(input: &str) -> IResult<&str, AvroValue> {
    let parse_true = value(true, tag("true"));
    let parse_false = value(false, tag("false"));
    map(alt((parse_true, parse_false)), |v| AvroValue::Boolean(v))(input)
}

// Sample:
// ```
// 20
// ```
fn map_int(input: &str) -> IResult<&str, AvroValue> {
    map(map_res(digit1, |v: &str| v.parse::<i32>()), |v| {
        AvroValue::Int(v)
        // Value::Number(v.into())
    })(input)
}

// Sample:
// ```
// 20
// ```
fn map_long(input: &str) -> IResult<&str, AvroValue> {
    map(map_res(digit1, |v: &str| v.parse::<i64>()), |v| {
        AvroValue::Long(v)
        // Value::Number(v.into())
    })(input)
}

// Sample:
// ```
// 20.0
// ```
fn map_float(input: &str) -> IResult<&str, AvroValue> {
    map(
        map_res(
            take_while1(|c| char::is_digit(c, 10) || c == '.' || c == 'e'),
            |v: &str| v.parse::<f32>(),
        ),
        |v| AvroValue::Float(v),
    )(input)
}

// Sample:
// ```
// 20.0
// ```
fn map_double(input: &str) -> IResult<&str, AvroValue> {
    map(
        map_res(
            take_while1(|c| char::is_digit(c, 10) || c == '.' || c == 'e'),
            |v: &str| v.parse::<f64>(),
        ),
        |v| AvroValue::Double(v),
        // Some(Value::Number(Number::from_f64(v)?)),
    )(input)
}

// Used to parse decimal information
fn map_usize(input: &str) -> IResult<&str, usize> {
    map_res(digit1, |v: &str| v.parse::<usize>())(input)
}

// Identify correct Schema
fn map_type_to_schema(input: &str) -> IResult<&str, Schema> {
    alt((
        preceded(
            tag("array"),
            delimited(
                tag("<"),
                map(map_type_to_schema, |s| Schema::Array(Box::new(s))),
                tag(">"),
            ),
        ),
        map(
            preceded(
                tag("union"),
                delimited(
                    space_delimited(tag("{")),
                    separated_list1(space_delimited(tag(",")), map_type_to_schema),
                    space_delimited(tag("}")),
                ),
            ),
            |union_schemas| {
                Schema::Union(
                    UnionSchema::new(union_schemas).expect("Failed to create union schema"),
                )
            },
        ),
        value(Schema::Null, tag("null")),
        value(Schema::Boolean, tag("boolean")),
        value(Schema::String, tag("string")),
        value(Schema::Int, tag("int")),
        value(Schema::Double, tag("double")),
        value(Schema::Float, tag("float")),
        value(Schema::Long, tag("long")),
        value(Schema::Bytes, tag("bytes")),
        value(Schema::TimeMillis, tag("time_ms")),
        value(Schema::TimestampMillis, tag("timestamp_ms")),
        value(Schema::Date, tag("date")),
        value(Schema::Uuid, tag("uuid")),
        map(
            preceded(
                tag("decimal"),
                delimited(tag("("), pair(map_usize, map_usize), tag(")")),
            ),
            |(precision, scale)| {
                // TODO: Review If inner should be float or calculated differently
                Schema::Decimal {
                    precision: precision,
                    scale: scale,
                    inner: Box::new(Schema::Bytes),
                }
            },
        ),
    ))(input)
}

// Identify default parser based on the given Schema
fn parse_based_on_schema<'r>(
    schema: Box<Schema>,
) -> Box<dyn FnMut(&'r str) -> IResult<&'r str, AvroValue>> {
    match *schema {
        Schema::Null => Box::new(map_null),
        Schema::Boolean => Box::new(map_bool),
        Schema::Int => Box::new(map_int),
        Schema::Long => Box::new(map_long),
        Schema::Float => Box::new(map_float),
        Schema::Double => Box::new(map_double),
        Schema::Bytes => Box::new(map_bytes),
        Schema::String => Box::new(map_string),
        Schema::Array(schema) => {
            // let schema = *schema;
            // let array_parser = parse_based_on_schema(&schema);
            Box::new(move |input: &'r str| {
                delimited(
                    tag("["),
                    map(
                        separated_list0(tag(","), parse_based_on_schema(schema.clone())),
                        |s| AvroValue::Array(s),
                    ),
                    tag("]"),
                )(input)
            }) as Box<dyn FnMut(&'r str) -> IResult<&'r str, AvroValue> + '_>
        }
        Schema::Union(union_schema) => {
            let schema = union_schema
                .variants()
                .first()
                .expect("There should be at least 2 schemas in the union");

            parse_based_on_schema(Box::new(schema.clone()))
        }

        // Logical Types
        Schema::Date => Box::new(map_int),
        Schema::TimeMillis => Box::new(map_int),
        Schema::TimestampMillis => Box::new(map_long),
        Schema::Uuid => Box::new(map_uuid),
        Schema::Decimal {
            precision: _,
            scale: _,
            inner: _,
        } => Box::new(map_bytes),
        Schema::TimestampMicros => Box::new(map_long),
        Schema::TimeMicros => Box::new(map_long),
        Schema::Duration => todo!("This should be fixed"),

        _ => unimplemented!("Not implemented yet"),
    }
}

// Sample:
// ```
// string name = "jon";
// bytes name = "jon";
// float age = 20;
// double age = 20.0;
// ```
fn parse_field(
    input: &str,
) -> IResult<
    &str,
    (
        Schema,
        Option<RecordFieldOrder>,
        Option<Vec<String>>,
        VarName,
        Option<Value>,
    ),
> {
    let (tail, logical_schema) = opt(comment_delimited(parse_logical_type))(input)?;
    // opt(terminated(parse_logical_type, space_delimited(line_ending)))(input)?;
    let (tail, schema) = map_type_to_schema(tail)?;

    let schema = match logical_schema {
        Some(s) => s,
        None => schema,
    };

    let boxed_schema = Box::new(schema.clone());
    let default_parser = parse_based_on_schema(boxed_schema);
    let (tail, ((order, aliases), varname, defaults)) = terminated(
        tuple((
            permutation_opt((
                comment_delimited(parse_order),
                comment_delimited(parse_aliases),
            )),
            comment_delimited(parse_var_name),
            // default
            opt(preceded(
                comment_delimited(tag("=")),
                map_res(default_parser, |value| value.try_into()),
            )),
        )),
        preceded(space0, comment_delimited(tag(";"))),
    )(tail)?;

    Ok((tail, (schema, order, aliases, varname, defaults)))
}

/** ***************  */
/**  Complex Types  */
/** *************** */

// Samples
// ```
// array<long> arrayOfLongs;
// array<long> @aliases(["vecOfLongs"]) arrayOfLongs;
// ```
fn parse_array(
    input: &str,
) -> IResult<
    &str,
    (
        Schema,
        Option<RecordFieldOrder>,
        Option<Vec<String>>,
        VarName,
        Option<Value>,
    ),
> {
    let (tail, schema_array_type) = preceded(
        tag("array"),
        delimited(tag("<"), map_type_to_schema, tag(">")),
    )(input)?;
    let schema = Box::new(schema_array_type.clone());
    let array_default_parser = parse_based_on_schema(schema);
    let (tail, (order, aliases, varname, defaults)) = terminated(
        tuple((
            opt(space_delimited(parse_order)),
            opt(space_delimited(parse_aliases)),
            space_delimited(parse_var_name),
            // default
            opt(preceded(
                space_delimited(tag("=")),
                delimited(
                    tag("["),
                    map_res(
                        separated_list0(tag(","), array_default_parser),
                        |value| AvroValue::Array(value).try_into(),
                        // Value::Array,
                    ),
                    tag("]"),
                ),
            )),
        )),
        tag(";"),
    )(tail)?;

    Ok((
        tail,
        (
            Schema::Array(Box::new(schema_array_type)),
            order,
            aliases,
            varname,
            defaults,
        ),
    ))
}

// Sample:
// ```
// map<int> foo2 = {};
// ```
fn parse_map(
    input: &str,
) -> IResult<
    &str,
    (
        Schema,
        Option<RecordFieldOrder>,
        Option<Vec<String>>,
        VarName,
        Option<Value>,
    ),
> {
    let (tail, schema) = preceded(
        tag("map"),
        delimited(tag("<"), map_type_to_schema, tag(">")),
    )(input)?;
    let schema_for_parser = Box::new(schema.clone());
    let map_default_parser = parse_based_on_schema(schema_for_parser);
    let (tail, (order, aliases, varname, defaults)) = terminated(
        tuple((
            opt(space_delimited(parse_order)),
            opt(space_delimited(parse_aliases)),
            space_delimited(parse_var_name),
            // default
            opt(preceded(
                space_delimited(tag("=")),
                delimited(
                    tag("{"),
                    map_res(
                        separated_list0(
                            space_delimited(tag(",")),
                            pair(
                                parse_string_uni,
                                preceded(space_delimited(tag(":")), map_default_parser),
                            ),
                        ),
                        |v| AvroValue::Map(HashMap::from_iter(v)).try_into(),
                    ),
                    tag("}"),
                ),
            )),
        )),
        tag(";"),
    )(tail)?;

    Ok((
        tail,
        (
            Schema::Map(Box::new(schema)),
            order,
            aliases,
            varname,
            defaults,
        ),
    ))
}

fn parse_union(
    input: &str,
) -> IResult<
    &str,
    (
        Schema,
        Option<RecordFieldOrder>,
        Option<Vec<String>>,
        VarName,
        Option<Value>,
    ),
> {
    let (tail, schema) = map_type_to_schema(input)?;

    let boxed_schema = Box::new(schema.clone());
    let default_parser = parse_based_on_schema(boxed_schema);
    let (tail, ((order, aliases), varname, defaults)) = terminated(
        tuple((
            permutation_opt((
                comment_delimited(parse_order),
                comment_delimited(parse_aliases),
            )),
            comment_delimited(parse_var_name),
            // default
            opt(preceded(
                comment_delimited(tag("=")),
                map_res(default_parser, |value| value.try_into()),
            )),
        )),
        preceded(space0, comment_delimited(tag(";"))),
    )(tail)?;

    Ok((tail, (schema, order, aliases, varname, defaults)))
}

/** ***************************************** */
/**  Custom Types: Fixed, Records, etc        */
/**  These types can be declared used fields  */
/** ***************************************** */

// Samples
// ```
// fixed MD5(16);
// fixed @aliases(["md1"]) MD5(16);
// ```
// TODO: This should be parsed OUTSIDE of the recordfield
fn parse_fixed(input: &str) -> IResult<&str, Schema> {
    let (tail, (doc, (order, aliases, name, size))) = tuple((
        space_delimited(opt(parse_doc)),
        preceded(
            tag("fixed"),
            cut(terminated(
                space_delimited(tuple((
                    opt(space_delimited(parse_order)),
                    opt(space_delimited(map_parse_aliases)),
                    parse_var_name,
                    delimited(tag("("), map_usize, tag(")")),
                ))),
                char(';'),
            )),
        ),
    ))(input)?;

    Ok((
        tail,
        Schema::Fixed {
            name: name.into(),
            aliases: aliases.clone(),
            doc: doc,
            size: size,
            attributes: BTreeMap::new(),
        },
    ))
}

// Sample
// ```
// /** This is a doc */
// ```
fn parse_doc(input: &str) -> IResult<&str, String> {
    delimited(tag("/**"), map(take_until("*/"), String::from), tag("*/"))(input)
}

// Sample
// ```
// record TestRecord
// ```
fn parse_record_name(input: &str) -> IResult<&str, &str> {
    preceded(tag("record"), space_delimited(parse_var_name))(input)
}

// Sample
// This returns a whole schema::RecordField
// ```
// string @order("ignore") name = "jon";
// ```
fn parse_record_field(input: &str) -> IResult<&str, RecordField> {
    preceded(
        multispace0,
        comment_delimited(alt((
            map(parse_union, |(schema, order, aliases, name, default)| {
                RecordField {
                    name: name.to_string(),
                    doc: None,
                    default: default,
                    schema: schema,
                    order: order.unwrap_or(RecordFieldOrder::Ascending),
                    aliases: aliases,
                    position: 0,
                    custom_attributes: BTreeMap::new(),
                }
            }),
            map(parse_map, |(schemas, order, aliases, name, default)| {
                RecordField {
                    name: name.to_string(),
                    doc: None,
                    default: default,
                    schema: schemas,
                    order: order.unwrap_or(RecordFieldOrder::Ascending),
                    aliases: aliases,
                    position: 0,
                    custom_attributes: BTreeMap::new(),
                }
            }),
            map(parse_array, |(schemas, order, aliases, name, default)| {
                RecordField {
                    name: name.to_string(),
                    doc: None,
                    default: default,
                    schema: schemas,
                    order: order.unwrap_or(RecordFieldOrder::Ascending),
                    aliases: aliases,
                    position: 0,
                    custom_attributes: BTreeMap::new(),
                }
            }),
            map(parse_field, |(schemas, order, aliases, name, default)| {
                RecordField {
                    name: name.to_string(),
                    doc: None,
                    default: default,
                    schema: schemas,
                    order: order.unwrap_or(RecordFieldOrder::Ascending),
                    aliases: aliases,
                    position: 0,
                    custom_attributes: BTreeMap::new(),
                }
            }),
        ))),
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
    let (tail, ((aliases, namespace), name, fields)) = tuple((
        permutation_opt((
            comment_delimited(map_parse_aliases),
            comment_delimited(parse_namespace),
        )),
        preceded(multispace0, parse_record_name),
        preceded(
            multispace0,
            delimited(
                tag("{"),
                many1(parse_record_field),
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
// `/* Hello */`
// `// Hello\n`
fn parse_comment<'a, T, E>(input: T) -> IResult<T, T, E>
where
    E: nom::error::ParseError<T>,
    T: InputTake
        + InputTakeAtPosition
        + std::clone::Clone
        + nom::Compare<&'a str>
        + nom::InputIter
        + nom::InputLength
        + nom::FindSubstring<&'a str>,
    <T as InputTakeAtPosition>::Item: AsChar,
    <T as InputTakeAtPosition>::Item: Clone,
    <T as InputTakeAtPosition>::Item: PartialEq<char>,
{
    alt((
        delimited(tag("/*"), take_until("*/"), tag("*/")),
        delimited(tag("//"), take_till(|c| c == '\n'), tag("\n")),
    ))(input)
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
            many1(comment_delimited(alt((
                parse_record,
                parse_enum,
                parse_fixed,
            )))),
            preceded(multispace0, tag("}")),
        ),
    ))(input)?;
    Ok((tail, schema))
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::*;
    // use crate::schema::{RecordField, Schema};
    use apache_avro::schema::{Alias, Name, RecordField, RecordFieldOrder, Schema};
    use rstest::rstest;
    use serde_json::{Map, Number, Value};

    #[rstest]
    #[case("string message;", (Schema::String, None, None, "message",None))]
    #[case("string  message;", (Schema::String, None, None, "message",None))]
    #[case("string message ;", (Schema::String, None, None, "message",None))]
    #[case(r#"string message = "holis" ;"#, (Schema::String, None, None, "message",Some(Value::String("holis".into()))))]
    #[case(r#"string message = "holis";"#, (Schema::String, None, None, "message",Some(Value::String("holis".into()))))]
    #[case(r#"string @order("ignore") message = "holis";"#, (Schema::String, Some(RecordFieldOrder::Ignore), None, "message",Some(Value::String("holis".into()))))]
    #[case(r#"string @order("ignore") message = "holis how are you";"#, (Schema::String, Some(RecordFieldOrder::Ignore), None, "message",Some(Value::String("holis how are you".into()))))]
    fn test_parse_string_ok(
        #[case] input: &str,
        #[case] expected: (
            Schema,
            Option<RecordFieldOrder>,
            Option<Vec<String>>,
            VarName,
            Option<Value>,
        ),
    ) {
        assert_eq!(parse_field(input), Ok(("", expected)));
    }

    #[rstest]
    #[case("string message")] // no semi-colon
    #[case(r#"string message = "holis"#)] // unclosed quote
    #[case(r#"string message = "holis""#)] // default no semi-colon
    fn test_parse_string_fail(#[case] input: &str) {
        assert!(parse_field(input).is_err());
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

    #[rstest]
    #[case("1var_name")]
    #[case("-1var_name")]
    #[case("$0_1var_name")]
    #[case("1_n20umbers3")]
    #[case("1_n20umbers3_")]
    fn test_parse_var_name_fail(#[case] input: &str) {
        assert!(parse_var_name(input).is_err());
    }

    #[rstest]
    #[case("bytes message;", (Schema::Bytes, None, None, "message",None))]
    #[case("bytes  message;", (Schema::Bytes, None, None, "message",None))]
    #[case("bytes message ;", (Schema::Bytes, None, None, "message",None))]
    #[case(r#"bytes message = "holis" ;"#, (Schema::Bytes, None, None, "message",Some(Value::Array(Vec::from([Value::Number(104.into()), Value::Number(111.into()), Value::Number(108.into()), Value::Number(105.into()), Value::Number(115.into())])))))]
    #[case(r#"bytes message = "holis";"#, (Schema::Bytes, None, None, "message",Some(Value::Array(Vec::from([Value::Number(104.into()), Value::Number(111.into()), Value::Number(108.into()), Value::Number(105.into()), Value::Number(115.into())])))))]
    #[case(r#"bytes @order("ignore") message = "holis";"#, (Schema::Bytes, Some(RecordFieldOrder::Ignore), None, "message",Some(Value::Array(Vec::from([Value::Number(104.into()), Value::Number(111.into()), Value::Number(108.into()), Value::Number(105.into()), Value::Number(115.into())])))))]
    fn test_parse_bytes_ok(
        #[case] input: &str,
        #[case] expected: (
            Schema,
            Option<RecordFieldOrder>,
            Option<Vec<String>>,
            VarName,
            Option<Value>,
        ),
    ) {
        assert_eq!(parse_field(input), Ok(("", expected)));
    }

    #[rstest]
    #[case("boolean active;", (Schema::Boolean, None, None, "active", None))]
    #[case(r#"boolean @order("ignore") active;"#, (Schema::Boolean, Some(RecordFieldOrder::Ignore), None, "active", None))]
    #[case("boolean active = true;", (Schema::Boolean, None, None, "active", Some(Value::Bool(true))))]
    #[case("boolean active = false;", (Schema::Boolean, None, None, "active", Some(Value::Bool(false))))]
    #[case("boolean   active   =   false ;", (Schema::Boolean, None, None, "active", Some(Value::Bool(false))))]
    fn test_parse_boolean_ok(
        #[case] input: &str,
        #[case] expected: (
            Schema,
            Option<RecordFieldOrder>,
            Option<Vec<String>>,
            VarName,
            Option<Value>,
        ),
    ) {
        assert_eq!(parse_field(input), Ok(("", expected)));
    }

    #[rstest]
    #[case("boolean message")] // no semi-colon
    #[case(r#"boolean message = "false""#)] // wrong type
    #[case(r#"boolean message = true"#)] // no semi-colon with default
    fn test_parse_boolean_fail(#[case] input: &str) {
        assert!(parse_field(input).is_err());
    }

    #[rstest]
    #[case("int age;", (Schema::Int, None, None, "age", None))]
    #[case("int age = 12;", (Schema::Int, None, None, "age", Some(Value::Number(12.into()))))]
    #[case("int age = 0;", (Schema::Int, None, None, "age", Some(Value::Number(0.into()))))]
    #[case("int   age   =   123 ;", (Schema::Int, None, None, "age", Some(Value::Number(123.into()))))]
    fn test_parse_int_ok(
        #[case] input: &str,
        #[case] expected: (
            Schema,
            Option<RecordFieldOrder>,
            Option<Vec<String>>,
            VarName,
            Option<Value>,
        ),
    ) {
        assert_eq!(parse_field(input), Ok(("", expected)));
    }

    #[rstest]
    #[case("int age")] // missing semi-colon
    #[case(r#"int age = "false""#)] // wrong type
    #[case(r#"int age = 123"#)] // missing semi-colon with default
    #[case("int age = 9223372036854775807;")] // longer than i32
    fn test_parse_int_fail(#[case] input: &str) {
        assert!(parse_field(input).is_err());
    }

    #[rstest]
    #[case("int age;", (Schema::Int, None, None, "age", None))]
    #[case("int age = 12;", (Schema::Int, None, None, "age", Some(Value::Number(12.into()))))]
    #[case("int age = 0;", (Schema::Int, None, None, "age", Some(Value::Number(0.into()))))]
    #[case("int   age   =   123 ;", (Schema::Int, None, None, "age", Some(Value::Number(123.into()))))]
    #[case("time_ms age;", (Schema::TimeMillis, None, None, "age", None))]
    #[case("time_ms age = 12;", (Schema::TimeMillis, None, None, "age", Some(Value::Number(12.into()))))]
    #[case("time_ms age = 0;", (Schema::TimeMillis, None, None, "age", Some(Value::Number(0.into()))))]
    #[case("time_ms   age   =   123 ;", (Schema::TimeMillis, None, None, "age", Some(Value::Number(123.into()))))]
    #[case("timestamp_ms age;", (Schema::TimestampMillis, None, None, "age", None))]
    #[case("timestamp_ms age = 12;", (Schema::TimestampMillis, None, None, "age", Some(Value::Number(12.into()))))]
    #[case("@logicalType(\"timestamp-micros\")\nlong ts = 12;", (Schema::TimestampMicros, None, None, "ts", Some(Value::Number(12.into()))))]
    #[case("date age;", (Schema::Date, None, None, "age", None))]
    #[case("date age = 12;", (Schema::Date, None, None, "age", Some(Value::Number(12.into()))))]
    #[case(r#"uuid pk = "a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8";"#, (Schema::Uuid, None, None, "pk", Some(Value::String("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8".into()))))]
    fn test_parse_logical_field_ok(
        #[case] input: &str,
        #[case] expected: (
            Schema,
            Option<RecordFieldOrder>,
            Option<Vec<String>>,
            VarName,
            Option<Value>,
        ),
    ) {
        assert_eq!(parse_field(input), Ok(("", expected)));
    }

    #[rstest]
    #[case("int age")] // missing semi-colon
    #[case(r#"int age = "false""#)] // wrong type
    #[case(r#"int age = 123"#)] // missing semi-colon with default
    #[case("int age = 9223372036854775807;")] // longer than i32
    #[case("time_ms age")] // missing semi-colon
    #[case(r#"time_ms age = "false""#)] // wrong type
    #[case(r#"time_ms age = 123"#)] // missing semi-colon with default
    #[case("time_ms age = 9223372036854775807;")] // longer than i32
    #[case(r#"uuid pk = "asd";"#)] // longer than i32
    fn test_parse_logical_field_fail(#[case] input: &str) {
        assert!(parse_field(input).is_err());
    }

    #[rstest]
    #[case("long stock;", (Schema::Long, None, None, "stock", None))]
    #[case("long stock = 12;", (Schema::Long, None, None, "stock", Some(Value::Number(12.into()))))]
    #[case("long stock = 9223372036854775807;", (Schema::Long, None, None, "stock", Some(Value::Number(Number::from(9223372036854775807 as i64)))))]
    #[case("long stock = 0;", (Schema::Long, None, None, "stock", Some(Value::Number(0.into()))))]
    #[case("long   stock   =   123 ;", (Schema::Long, None, None, "stock", Some(Value::Number(123.into()))))]
    fn test_parse_long_ok(
        #[case] input: &str,
        #[case] expected: (
            Schema,
            Option<RecordFieldOrder>,
            Option<Vec<String>>,
            VarName,
            Option<Value>,
        ),
    ) {
        assert_eq!(parse_field(input), Ok(("", expected)));
    }
    //
    #[rstest]
    #[case("float age;", (Schema::Float, None, None, "age", None))]
    #[case("float age = 12;", (Schema::Float, None, None, "age", Some(Value::Number(Number::from_f64(12.0).unwrap()))))]
    #[case("float age = 12.0;", (Schema::Float, None, None, "age", Some(Value::Number(Number::from_f64(12.0).unwrap()))))]
    #[case("float age = 0.0;", (Schema::Float, None, None, "age", Some(Value::Number(Number::from_f64(0.0).unwrap()))))]
    #[case("float age = .0;", (Schema::Float, None, None, "age", Some(Value::Number(Number::from_f64(0.0).unwrap()))))]
    #[case("float age = 0.1123;", (Schema::Float, None, None, "age", Some(Value::Number(Number::from_f64(0.1123).unwrap()))))]
    #[case("float age = 3.40282347e38;", (Schema::Float, None, None, "age", Some(Value::Number(Number::from_f64(f32::MAX.into()).unwrap()))))]
    #[case("float age = 0;", (Schema::Float, None, None, "age", Some(Value::Number(Number::from_f64(0.0).unwrap()))))]
    #[case("float   age   =   123 ;", (Schema::Float, None, None, "age", Some(Value::Number(Number::from_f64(123.0).unwrap()))))]
    fn test_parse_float_ok(
        #[case] input: &str,
        #[case] expected: (
            Schema,
            Option<RecordFieldOrder>,
            Option<Vec<String>>,
            VarName,
            Option<Value>,
        ),
    ) {
        assert_eq!(parse_field(input), Ok(("", expected)));
    }

    #[rstest]
    #[case("float age")] // missing semi-colon
    #[case(r#"float age = "false""#)] // wrong type
    #[case(r#"float age = 123"#)] // missing semi-colon with default
    #[case("float age = 3.50282347e40;")] // longer than f32
    fn test_parse_float_fail(#[case] input: &str) {
        assert!(parse_field(input).is_err());
    }

    #[rstest]
    #[case("double stock;", (Schema::Double, None, None, "stock", None))]
    #[case("double stock = 12;", (Schema::Double, None, None, "stock", Some(Value::Number(Number::from_f64(12.0).unwrap()))))]
    #[case("double stock = 9223372036854775807;", (Schema::Double, None, None, "stock", Some(Value::Number(Number::from_f64(9223372036854775807.0).unwrap()))))]
    #[case("double stock = 123.456;", (Schema::Double, None, None, "stock", Some(Value::Number(Number::from_f64(123.456).unwrap()))))]
    #[case("double stock = 1.7976931348623157e308;", (Schema::Double, None, None, "stock", Some(Value::Number(Number::from_f64(f64::MAX).unwrap()))))]
    #[case("double stock = 0.0;", (Schema::Double, None, None, "stock", Some(Value::Number(Number::from_f64(0.0).unwrap()))))]
    #[case("double stock = .0;", (Schema::Double, None, None, "stock", Some(Value::Number(Number::from_f64(0.0).unwrap()))))]
    #[case("double stock = 0;", (Schema::Double, None, None, "stock", Some(Value::Number(Number::from_f64(0.0).unwrap()))))]
    #[case(r#"double @order("descending") stock = 0;"#, (Schema::Double, Some(RecordFieldOrder::Descending), None, "stock", Some(Value::Number(Number::from_f64(0.0).unwrap()))))]
    #[case("double   stock   =   123.3 ;", (Schema::Double, None, None, "stock", Some(Value::Number(Number::from_f64(123.3).unwrap()))))]
    fn test_parse_double_ok(
        #[case] input: &str,
        #[case] expected: (
            Schema,
            Option<RecordFieldOrder>,
            Option<Vec<String>>,
            VarName,
            Option<Value>,
        ),
    ) {
        assert_eq!(parse_field(input), Ok(("", expected)));
    }

    #[rstest]
    #[case("double stock")] // missing semi-colon
    #[case(r#"double stock = "false""#)] // wrong type
    #[case(r#"double stock = 123"#)] // missing semi-colon with default
    fn test_parse_double_fail(#[case] input: &str) {
        assert!(parse_field(input).is_err());
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
        assert_eq!(map_parse_aliases(input), Ok(("", expected)));
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
    #[case(r#"array<array<string>> stock = [["cacao"]];"#, (Schema::Array(Box::new(Schema::Array(Box::new(Schema::String)))), None, None, "stock", Some(Value::Array(Vec::from([Value::Array(Vec::from([Value::String(String::from("cacao"))]))])))))]
    #[case(r#"array<string> stock = ["cacao"];"#, (Schema::Array(Box::new(Schema::String)), None, None, "stock", Some(Value::Array(Vec::from([Value::String(String::from("cacao"))])))))]
    #[case("array<string> stock;", (Schema::Array(Box::new(Schema::String)), None, None, "stock", None))]
    #[case("array<string> stock = [];", (Schema::Array(Box::new(Schema::String)), None, None, "stock", Some(Value::Array(Vec::new()))))]
    #[case(r#"array<string> stock = [""];"#, (Schema::Array(Box::new(Schema::String)), None, None, "stock", Some(Value::Array(Vec::from([Value::String(String::from(""))])))))]
    #[case(r#"array<string> stock = ["cacao nibs"];"#, (Schema::Array(Box::new(Schema::String)), None, None, "stock", Some(Value::Array(Vec::from([Value::String(String::from("cacao nibs"))])))))]
    #[case(r#"array<string> @aliases(["item"]) stock;"#, (Schema::Array(Box::new(Schema::String)), None, Some(vec![String::from("item")]), "stock", None))]
    #[case(r#"array<string> @order("ascending") stock;"#, (Schema::Array(Box::new(Schema::String)), Some(RecordFieldOrder::Ascending), None, "stock", None))]
    fn test_parse_array_ok(
        #[case] input: &str,
        #[case] expected: (
            Schema,
            Option<RecordFieldOrder>,
            Option<Vec<String>>,
            VarName,
            Option<Value>,
        ),
    ) {
        assert_eq!(parse_array(input), Ok(("", expected)));
    }

    #[rstest]
    #[case(r#"map<string> stock;"#, (Schema::Map(Box::new(Schema::String)), None, None, "stock", None))]
    #[case(r#"map<string> @order("ascending") stock;"#, (Schema::Map(Box::new(Schema::String)), Some(RecordFieldOrder::Ascending), None, "stock", None))]
    #[case(r#"map<string> stock = {"hey": "hello"};"#, (Schema::Map(Box::new(Schema::String)), None, None, "stock", Some(Value::Object(Map::from_iter([(String::from("hey"), Value::String(String::from("hello")))])))))]
    fn test_parse_map_ok(
        #[case] input: &str,
        #[case] expected: (
            Schema,
            Option<RecordFieldOrder>,
            Option<Vec<String>>,
            VarName,
            Option<Value>,
        ),
    ) {
        assert_eq!(parse_map(input), Ok(("", expected)));
    }

    #[rstest]
    #[case(r#"fixed MD5(16);"#, (Schema::Fixed { name: "MD5".into(), aliases: None, doc: None, size: 16, attributes: BTreeMap::new()}))]
    #[case("/** my hash */ \nfixed MD5(16);", (Schema::Fixed { name: "MD5".into(), aliases: None, doc: Some("my hash".to_string()), size: 16, attributes: BTreeMap::new()}))]
    #[case(r#"fixed @aliases(["md1"]) MD5(16);"#, (Schema::Fixed { name: "MD5".into(), aliases: None, doc: None, size: 16, attributes: BTreeMap::new()}))]
    fn test_parse_fixed_ok(#[case] input: &str, #[case] expected: Schema) {
        assert_eq!(parse_fixed(input), Ok(("", expected)));
    }

    #[rstest]
    #[case(
        r#"union { null, string } item_id = null;"#, (Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::String]).unwrap()), None, None, "item_id", Some(Value::Null))
    )]
    #[case(
        r#"union { null, string } item = null;"#, (Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::String]).unwrap()), None, None, "item", Some(Value::Null))
    )]
    #[case(
        r#"union { int, string } item = 1;"#, (Schema::Union(UnionSchema::new(vec![Schema::Int, Schema::String]).unwrap()), None, None, "item", Some(Value::Number(1.into())))
    )]
    #[case(
        r#"union { string, int } item = "1";"#, (Schema::Union(UnionSchema::new(vec![Schema::String, Schema::Int]).unwrap()), None, None, "item", Some(Value::String("1".to_string())))
    )]
    fn test_union(
        #[case] input: &str,
        #[case] expected: (
            Schema,
            Option<RecordFieldOrder>,
            Option<Vec<String>>,
            VarName,
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
        assert_eq!(parse_enum_default(input), Ok(("", expected.to_string())))
    }

    #[rstest]
    #[case(
        "/** Documentation for the enum type Kind */",
        " Documentation for the enum type Kind "
    )]
    fn test_parse_doc(#[case] input: &str, #[case] expected: String) {
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
        assert_eq!(parse_record_field(input), Ok(("", expected)))
    }

    #[test]
    fn test_parse_record() {
        let sample = r#"record Employee {
            string name;
            boolean active = true;
            long salary;
        }"#;
        let (_tail, schema) = parse_record(sample).unwrap();
        // let schema: SourceSchema = schema.into();
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

    #[rstest]
    #[case("// holis\n", " holis")]
    #[case(
        "// TODO: Move to another place, etc.\n",
        " TODO: Move to another place, etc."
    )]
    #[case("/*Som343f */", "Som343f ")]
    #[case("//Som343f\n", "Som343f")]
    #[case("/* holis */", " holis ")]
    #[case(
        "/* TODO: Move to another place, etc. */",
        " TODO: Move to another place, etc. "
    )]
    fn test_parse_comment_ok<'a>(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(parse_comment::<'a, &str, ()>(input), Ok(("", expected)));
    }
}
