use std::collections::HashMap;

use crate::schema::{Documentation, Namespace, Schema};

enum Types {
    Record(Schema),
    Enum(Schema),
    Fixed(Schema),
    Error(Schema),
}

struct Message {
    name: String,
    doc: Documentation,
    request: Vec<HashMap<String, String>>,
    response: String,
    errors: Vec<String>,
}

struct Protocol {
    name: String,
    namespace: Namespace,
    doc: Documentation,
    types: Vec<Types>,
    messages: Vec<HashMap<String, Message>>
}
