use std::collections::BTreeMap;
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Serialize, Serializer,
};
use apache_avro::{
    schema::{
        Aliases, Documentation, Name, RecordField as SourceRecordField, RecordFieldOrder,
        Schema as SourceSchema, SchemaKind as SourceSchemaKind, UnionSchema as SourceUnionSchema,
    },
    AvroResult, Error,
};
use serde_json::Value;
use strum_macros::EnumDiscriminants;

type DecimalMetadata = usize;

/// Represents a `field` in a `record` Avro schema.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordField {
    /// Name of the field.
    pub name: String,
    /// Documentation of the field.
    pub doc: Documentation,
    /// Default value of the field.
    /// This value will be used when reading Avro datum if schema resolution
    /// is enabled.
    pub default: Option<Value>,
    /// Schema of the field.
    pub schema: Schema,
    /// Order of the field.
    ///
    /// **NOTE** This currently has no effect.
    pub order: RecordFieldOrder,

    // AVDL-RS specific
    pub aliases: Aliases,

    /// Position of the field in the list of `field` of its parent `Schema`
    pub position: usize,
    /// A collection of all unknown fields in the record field.
    pub custom_attributes: BTreeMap<String, Value>,
}

impl Into<SourceRecordField> for RecordField {
    fn into(self) -> SourceRecordField {
        SourceRecordField {
            name: self.name,
            doc: self.doc,
            default: self.default,
            schema: self.schema.into(),
            order: self.order,
            position: self.position,
            custom_attributes: self.custom_attributes,
        }
    }
}

#[derive(Clone, Debug, EnumDiscriminants)]
#[strum_discriminants(name(SchemaKind), derive(Hash, Ord, PartialOrd))]
pub enum Schema {
    /// A `null` Avro schema.
    Null,
    /// A `boolean` Avro schema.
    Boolean,
    /// An `int` Avro schema.
    Int,
    /// A `long` Avro schema.
    Long,
    /// A `float` Avro schema.
    Float,
    /// A `double` Avro schema.
    Double,
    /// A `bytes` Avro schema.
    /// `Bytes` represents a sequence of 8-bit unsigned bytes.
    Bytes,
    /// A `string` Avro schema.
    /// `String` represents a unicode character sequence.
    String,
    /// A `array` Avro schema. Avro arrays are required to have the same type for each element.
    /// This variant holds the `Schema` for the array element type.
    Array(Box<Schema>),
    /// A `map` Avro schema.
    /// `Map` holds a pointer to the `Schema` of its values, which must all be the same schema.
    /// `Map` keys are assumed to be `string`.
    Map(Box<Schema>),
    /// A `union` Avro schema.
    Union(UnionSchema),
    /// A `record` Avro schema.
    ///
    /// The `lookup` table maps field names to their position in the `Vec`
    /// of `fields`.
    Record {
        name: Name,
        aliases: Aliases,
        doc: Documentation,
        fields: Vec<RecordField>,
        lookup: BTreeMap<String, usize>,
        attributes: BTreeMap<String, Value>,
    },
    /// An `enum` Avro schema.
    Enum {
        name: Name,
        aliases: Aliases,
        doc: Documentation,
        symbols: Vec<String>,
        attributes: BTreeMap<String, Value>,
    },
    /// A `fixed` Avro schema.
    Fixed {
        name: Name,
        aliases: Aliases,
        doc: Documentation,
        size: usize,
        attributes: BTreeMap<String, Value>,
    },
    /// Logical type which represents `Decimal` values. The underlying type is serialized and
    /// deserialized as `Schema::Bytes` or `Schema::Fixed`.
    ///
    /// `scale` defaults to 0 and is an integer greater than or equal to 0 and `precision` is an
    /// integer greater than 0.
    Decimal {
        precision: DecimalMetadata,
        scale: DecimalMetadata,
        inner: Box<Schema>,
    },
    /// A universally unique identifier, annotating a string.
    Uuid,
    /// Logical type which represents the number of days since the unix epoch.
    /// Serialization format is `Schema::Int`.
    Date,
    /// The time of day in number of milliseconds after midnight with no reference any calendar,
    /// time zone or date in particular.
    TimeMillis,
    /// The time of day in number of microseconds after midnight with no reference any calendar,
    /// time zone or date in particular.
    TimeMicros,
    /// An instant in time represented as the number of milliseconds after the UNIX epoch.
    TimestampMillis,
    /// An instant in time represented as the number of microseconds after the UNIX epoch.
    TimestampMicros,
    /// An amount of time defined by a number of months, days and milliseconds.
    Duration,
    // A reference to another schema.
    Ref {
        name: Name,
    },
}

impl Into<SourceSchema> for Schema {
    fn into(self) -> SourceSchema {
        match self {
            Schema::Null => SourceSchema::Null,
            Schema::Boolean => SourceSchema::Boolean,
            Schema::Int => SourceSchema::Int,
            Schema::Long => SourceSchema::Long,
            Schema::Float => SourceSchema::Float,
            Schema::Double => SourceSchema::Double,
            Schema::Bytes => SourceSchema::Bytes,
            Schema::String => SourceSchema::String,
            Schema::Array(v) => SourceSchema::Array(Box::new((*v).into())),
            Schema::Map(v) => SourceSchema::Map(Box::new((*v).into())),
            Schema::Union(u) => SourceSchema::Union(u.into()),
            Schema::Record {
                name,
                aliases,
                doc,
                fields,
                lookup,
                attributes,
            } => SourceSchema::Record {
                name,
                aliases,
                doc,
                fields: fields.iter().cloned().map(|field| field.into()).collect(),
                lookup,
                attributes,
            },
            Schema::Enum {
                name,
                aliases,
                doc,
                symbols,
                attributes,
            } => SourceSchema::Enum {
                name,
                aliases,
                doc,
                symbols,
                attributes,
            },
            Schema::Fixed {
                name,
                aliases,
                doc,
                size,
                attributes,
            } => SourceSchema::Fixed {
                name,
                aliases,
                doc,
                size,
                attributes,
            },
            Schema::Decimal {
                precision,
                scale,
                inner,
            } => SourceSchema::Decimal {
                precision,
                scale,
                inner: Box::new((*inner).into()),
            },
            Schema::Uuid => SourceSchema::Uuid,
            Schema::Date => SourceSchema::Date,
            Schema::TimeMillis => SourceSchema::TimeMillis,
            Schema::TimeMicros => SourceSchema::TimeMicros,
            Schema::TimestampMillis => SourceSchema::TimestampMillis,
            Schema::TimestampMicros => SourceSchema::TimestampMicros,
            Schema::Duration => SourceSchema::Duration,
            Schema::Ref { name } => SourceSchema::Ref { name },
        }
    }
}

impl PartialEq for Schema {
    /// Assess equality of two `Schema` based on [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    fn eq(&self, other: &Self) -> bool {
        let src_schema: SourceSchema = self.clone().into();
        let other_schema: SourceSchema = other.clone().into();
        src_schema.canonical_form() == other_schema.canonical_form()
    }
}

impl Into<SourceSchemaKind> for SchemaKind {
    fn into(self) -> SourceSchemaKind {
        match self {
            SchemaKind::Null => SourceSchemaKind::Null,
            SchemaKind::Boolean => SourceSchemaKind::Boolean,
            SchemaKind::Int => SourceSchemaKind::Int,
            SchemaKind::Long => SourceSchemaKind::Long,
            SchemaKind::Float => SourceSchemaKind::Float,
            SchemaKind::Double => SourceSchemaKind::Double,
            SchemaKind::Bytes => SourceSchemaKind::Bytes,
            SchemaKind::String => SourceSchemaKind::String,
            SchemaKind::Array => SourceSchemaKind::Array,
            SchemaKind::Map => SourceSchemaKind::Map,
            SchemaKind::Union => SourceSchemaKind::Union,
            SchemaKind::Record => SourceSchemaKind::Record,
            SchemaKind::Enum => SourceSchemaKind::Enum,
            SchemaKind::Fixed => SourceSchemaKind::Fixed,
            SchemaKind::Decimal => SourceSchemaKind::Decimal,
            SchemaKind::Uuid => SourceSchemaKind::Uuid,
            SchemaKind::Date => SourceSchemaKind::Date,
            SchemaKind::TimeMillis => SourceSchemaKind::TimeMillis,
            SchemaKind::TimeMicros => SourceSchemaKind::TimeMicros,
            SchemaKind::TimestampMillis => SourceSchemaKind::TimestampMillis,
            SchemaKind::TimestampMicros => SourceSchemaKind::TimestampMicros,
            SchemaKind::Duration => SourceSchemaKind::Duration,
            SchemaKind::Ref => SourceSchemaKind::Ref,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UnionSchema {
    pub(crate) schemas: Vec<Schema>,
    // Used to ensure uniqueness of schema inputs, and provide constant time finding of the
    // schema index given a value.
    // **NOTE** that this approach does not work for named types, and will have to be modified
    // to support that. A simple solution is to also keep a mapping of the names used.
    variant_index: BTreeMap<SchemaKind, usize>,
}

impl UnionSchema {
    /// Creates a new UnionSchema from a vector of schemas.
    pub fn new(schemas: Vec<Schema>) -> AvroResult<Self> {
        let mut vindex = BTreeMap::new();
        for (i, schema) in schemas.iter().enumerate() {
            if let Schema::Union(_) = schema {
                return Err(Error::GetNestedUnion);
            }
            let kind = SchemaKind::from(schema);
            let kind_src: SourceSchemaKind = kind.clone().into();
            if !kind_src.is_named() && vindex.insert(kind, i).is_some() {
                return Err(Error::GetUnionDuplicate);
            }
        }
        Ok(UnionSchema {
            schemas,
            variant_index: vindex,
        })
    }

    /// Returns a slice to all variants of this schema.
    pub fn variants(&self) -> &[Schema] {
        &self.schemas
    }
}

impl Into<SourceUnionSchema> for UnionSchema {
    fn into(self) -> SourceUnionSchema {
        let schemas: Vec<SourceSchema> = self.schemas.iter().cloned().map(|s| s.into()).collect();
        SourceUnionSchema::new(schemas).unwrap()
    }
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Schema::Ref { ref name } => serializer.serialize_str(&name.fullname(None)),
            Schema::Null => serializer.serialize_str("null"),
            Schema::Boolean => serializer.serialize_str("boolean"),
            Schema::Int => serializer.serialize_str("int"),
            Schema::Long => serializer.serialize_str("long"),
            Schema::Float => serializer.serialize_str("float"),
            Schema::Double => serializer.serialize_str("double"),
            Schema::Bytes => serializer.serialize_str("bytes"),
            Schema::String => serializer.serialize_str("string"),
            Schema::Array(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "array")?;
                map.serialize_entry("items", &*inner.clone())?;
                map.end()
            }
            Schema::Map(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "map")?;
                map.serialize_entry("values", &*inner.clone())?;
                map.end()
            }
            Schema::Union(ref inner) => {
                let variants = inner.variants();
                let mut seq = serializer.serialize_seq(Some(variants.len()))?;
                for v in variants {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            Schema::Record {
                ref name,
                ref aliases,
                ref doc,
                ref fields,
                ..
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "record")?;
                if let Some(ref n) = name.namespace {
                    map.serialize_entry("namespace", n)?;
                }
                map.serialize_entry("name", &name.name)?;
                if let Some(ref docstr) = doc {
                    map.serialize_entry("doc", docstr)?;
                }
                if let Some(ref aliases) = aliases {
                    map.serialize_entry("aliases", aliases)?;
                }
                map.serialize_entry("fields", fields)?;
                map.end()
            }
            Schema::Enum {
                ref name,
                ref symbols,
                ref aliases,
                ..
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "enum")?;
                if let Some(ref n) = name.namespace {
                    map.serialize_entry("namespace", n)?;
                }
                map.serialize_entry("name", &name.name)?;
                map.serialize_entry("symbols", symbols)?;

                if let Some(ref aliases) = aliases {
                    map.serialize_entry("aliases", aliases)?;
                }
                map.end()
            }
            Schema::Fixed {
                ref name,
                ref doc,
                ref size,
                ref aliases,
                ..
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "fixed")?;
                if let Some(ref n) = name.namespace {
                    map.serialize_entry("namespace", n)?;
                }
                map.serialize_entry("name", &name.name)?;
                if let Some(ref docstr) = doc {
                    map.serialize_entry("doc", docstr)?;
                }
                map.serialize_entry("size", size)?;

                if let Some(ref aliases) = aliases {
                    map.serialize_entry("aliases", aliases)?;
                }
                map.end()
            }
            Schema::Decimal {
                ref scale,
                ref precision,
                ref inner,
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", &*inner.clone())?;
                map.serialize_entry("logicalType", "decimal")?;
                map.serialize_entry("scale", scale)?;
                map.serialize_entry("precision", precision)?;
                map.end()
            }
            Schema::Uuid => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "string")?;
                map.serialize_entry("logicalType", "uuid")?;
                map.end()
            }
            Schema::Date => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "int")?;
                map.serialize_entry("logicalType", "date")?;
                map.end()
            }
            Schema::TimeMillis => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "int")?;
                map.serialize_entry("logicalType", "time-millis")?;
                map.end()
            }
            Schema::TimeMicros => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "time-micros")?;
                map.end()
            }
            Schema::TimestampMillis => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "timestamp-millis")?;
                map.end()
            }
            Schema::TimestampMicros => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "timestamp-micros")?;
                map.end()
            }
            Schema::Duration => {
                let mut map = serializer.serialize_map(None)?;

                // the Avro doesn't indicate what the name of the underlying fixed type of a
                // duration should be or typically is.
                let inner = Schema::Fixed {
                    name: Name::new("duration").unwrap(),
                    aliases: None,
                    doc: None,
                    size: 12,
                    attributes: Default::default(),
                };
                map.serialize_entry("type", &inner)?;
                map.serialize_entry("logicalType", "duration")?;
                map.end()
            }
        }
    }
}

impl Serialize for RecordField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("type", &self.schema)?;

        if let Some(ref default) = self.default {
            map.serialize_entry("default", default)?;
        }

        if let Some(ref aliases) = self.aliases {
            map.serialize_entry("aliases", aliases)?;
        }

        map.end()
    }
}