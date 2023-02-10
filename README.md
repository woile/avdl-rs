# AVDL-RS

> Parse Avro AVDL files

## CLI

To run avro-kit you can use

```sh
cargo run idl2schema
```

or build it

```sh
cargo build
```

### TODO

- [ ] Dockerfile
- [ ] Fake content based on schema
- [ ] nix package
- [ ] cli tests with insta
- [ ] benchmarks

## Parsers

- [x] [Enums](https://avro.apache.org/docs/1.11.1/idl-language/#defining-an-enumeration)
- [x] [Alias](https://avro.apache.org/docs/1.11.1/idl-language/#annotations-for-ordering-and-namespaces)
- [x] [Namespace](https://avro.apache.org/docs/1.11.1/idl-language/#annotations-for-ordering-and-namespaces)
- [x] [Order](https://avro.apache.org/docs/1.11.1/idl-language/#annotations-for-ordering-and-namespaces)
- [x] [Fixed length](https://avro.apache.org/docs/1.11.1/idl-language/#defining-a-fixed-length-field)
    - TODO: default on record?
    - [ ] Why is it not possible to set an `aliases` on a fixed?
- [ ] [Records and errors](https://avro.apache.org/docs/1.11.1/idl-language/#defining-records-and-errors)
    - [x] `Record`
    - [x] `RecordField`
    - [ ] Error
- [ ] RecordField
    - Named schema's `aliases` are for the schema's `name` which might be namespaced. Record field's aliases are for the field's `name` which is not namespaced. The field's `type` might be a (namespaced) reference to Schema.[src](https://github.com/apache/avro/pull/2087#discussion_r1101061294)
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
    - [x] `decimal` (logical type decimal)
    - [x] `date` (logical type date) -> `int`
    - [x] `time_ms` (logical type `time-millis`) -> `int`
    - [x] `time-micros` by `@logicalType` -> `long`
    - [x] `timestamp_ms` (logical type `timestamp-millis`) -> `long`
    - [x] `timestamp-micros` by `@logicalType` -> `long`
    - [x] `duration` -> `fixed` type of size 12
        - TODO: Validations
        - TODO: Improve parsing of default
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
    - [x] comments
        - [ ] TODO: Move everything to use the field_parser
        - [ ] TODO: Write more tests for comments


## Resources

- [simple.avdl](https://github.com/apache/avro/blob/b918209e42f18174bc90b1d8bd68402d96d93353/lang/java/compiler/src/test/idl/input/simple.avdl#L37)
- [field validations](https://github.com/apache/avro/blob/master/lang/java/avro/src/main/java/org/apache/avro/Schema.java#L959)

## Thanks

All the people that helped in nom's matrix server!
