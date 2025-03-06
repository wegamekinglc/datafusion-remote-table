use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum RemoteDataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    List(Box<RemoteField>),
}

impl RemoteDataType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            RemoteDataType::Boolean => DataType::Boolean,
            RemoteDataType::Int8 => DataType::Int8,
            RemoteDataType::Int16 => DataType::Int16,
            RemoteDataType::Int32 => DataType::Int32,
            RemoteDataType::Int64 => DataType::Int64,
            RemoteDataType::UInt8 => DataType::UInt8,
            RemoteDataType::UInt16 => DataType::UInt16,
            RemoteDataType::UInt32 => DataType::UInt32,
            RemoteDataType::UInt64 => DataType::UInt64,
            RemoteDataType::Float16 => DataType::Float16,
            RemoteDataType::Float32 => DataType::Float32,
            RemoteDataType::Float64 => DataType::Float64,
            RemoteDataType::List(field) => DataType::List(Arc::new(field.to_arrow_field())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteField {
    pub name: String,
    pub data_type: RemoteDataType,
    pub nullable: bool,
}

impl RemoteField {
    pub fn new(name: impl Into<String>, data_type: RemoteDataType, nullable: bool) -> Self {
        RemoteField {
            name: name.into(),
            data_type,
            nullable,
        }
    }

    pub fn to_arrow_field(&self) -> Field {
        Field::new(
            self.name.clone(),
            self.data_type.to_arrow_type(),
            self.nullable,
        )
    }
}

#[derive(Debug, Clone)]
pub struct RemoteSchema {
    pub fields: Vec<RemoteField>,
}

impl RemoteSchema {
    pub fn empty() -> Self {
        RemoteSchema { fields: vec![] }
    }
    pub fn new(fields: Vec<RemoteField>) -> Self {
        RemoteSchema { fields }
    }

    pub fn to_arrow_schema(&self) -> Schema {
        let mut fields = vec![];
        for remote_field in self.fields.iter() {
            fields.push(remote_field.to_arrow_field());
        }
        Schema::new(fields)
    }
}
