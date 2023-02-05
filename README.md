# AVDL-RS

> Parse Avro AVDL files

## Notes

- `apache_avro::Schema` cannot be used to generate the avsc because `RecordField` is missing things like `aliases`. We have a reimplementation internally with some extras. I try to keep it as close as possible
to the original, in case we can use it in the future. [AVRO-3709](https://issues.apache.org/jira/browse/AVRO-3709)

## Parsers

- [x] [Enums](https://avro.apache.org/docs/1.11.1/idl-language/#defining-an-enumeration)
- [x] [Alias](https://avro.apache.org/docs/1.11.1/idl-language/#annotations-for-ordering-and-namespaces)
- [x] [Namespace](https://avro.apache.org/docs/1.11.1/idl-language/#annotations-for-ordering-and-namespaces)
- [x] [Order](https://avro.apache.org/docs/1.11.1/idl-language/#annotations-for-ordering-and-namespaces)
- [ ] [Fixed length](https://avro.apache.org/docs/1.11.1/idl-language/#defining-a-fixed-length-field)
- [ ] [Records and errors](https://avro.apache.org/docs/1.11.1/idl-language/#defining-records-and-errors)
    - [x] `Record`
    - [x] `RecordField`
    - [ ] Error
- [ ] [Protocol](https://avro.apache.org/docs/1.11.1/idl-language/#defining-a-protocol-in-avro-idl)
- [Primitive types](https://avro.apache.org/docs/1.11.1/idl-language/#primitive-types)
    - [x] `string` = &str
        - [x] properly parse unicode strings
    - [x] `boolean` = bool
    - [x] `int` = i32
    - [x] `long` = i64
    - [x] `float` = f32
    - [x] `double` = f64
    - [x] `null` = ?
    - [x] `bytes` = [u8]
- [Logical types](https://avro.apache.org/docs/1.11.1/idl-language/#logical-types)
    - [x] `uuid` -> valid uuid `string`
    - [ ] `decimal` (logical type decimal)
    - [x] `date` (logical type date) -> `int`
    - [x] `time_ms` (logical type `time-millis`) -> `int`
    - [ ] logical type `time-micros` -> `long`
    - [x] `timestamp_ms` (logical type `timestamp-millis`) -> `long`
    - [ ] logical type `timestamp-micros` -> `long`
    - [x] `duration` -> `fixed` type of size 12
        - TODO: Validations
- [Complex types](https://avro.apache.org/docs/1.11.1/idl-language/#complex-types)
    - [x] Arrays
        - [x] basic support
        - [x] defaults?
        - [x] array of array
    - [x] Maps
    - [x] Unions
- [Default values](https://avro.apache.org/docs/1.11.1/idl-language/#default-values)
    - [x] `Enum`
- [Comments](https://avro.apache.org/docs/1.11.1/idl-language/#comments)
    - [x] doc (`/** foo */`)
    - [ ] comments


## Resources

- [simple.avdl](https://github.com/apache/avro/blob/b918209e42f18174bc90b1d8bd68402d96d93353/lang/java/compiler/src/test/idl/input/simple.avdl#L37)
- [field validations](https://github.com/apache/avro/blob/master/lang/java/avro/src/main/java/org/apache/avro/Schema.java#L959)

## Thanks

All the people that helped in nom's matrix server!
