## v0.2.0 (2023-03-03)

### Feat

- add initial support for namespaces
- add schema_solver
- add parse_doc
- parse permutations optionally
- add comment parser
- add avrokit and make use of workspaces
- add parse_comment
- add fixed field
- add decimal
- add uuid logicaltype
- add logical field
- add avro map
- add proper array handler
- add array support
- render aliases properly
- add union support
- add order parser
- add protocol + clitoschema sample
- add support for many items

### Fix

- float field parser
- var_names are correctly parsed now
- add boolean to union

### Refactor

- small cleanup
- enums, aliases and some clean up
- use fixed correctly
- better parse_union
- use types::Value instead of serde::Value
- use parse_field to parse most field and clean up unused
- string parser and tests
- improve schema parsers
