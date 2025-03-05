use datafusion::arrow::datatypes::{DataType, Field, Schema};

#[derive(Debug, Clone)]
pub enum RemoteDataType {
    Boolean,
}

impl RemoteDataType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            RemoteDataType::Boolean => DataType::Boolean,
        }
    }
}

pub struct RemoteField {
    pub name: String,
    pub data_type: RemoteDataType,
}

impl RemoteField {
    pub fn new(name: String, data_type: RemoteDataType) -> Self {
        RemoteField { name, data_type }
    }
}

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
}

fn remote_schema_to_arrow_schema(remote_schema: RemoteSchema) -> Schema {
    let mut fields = vec![];
    for remote_field in remote_schema.fields.iter() {
        fields.push(Field::new(
            remote_field.name.clone(),
            remote_field.data_type.to_arrow_type(),
            true,
        ));
    }
    Schema::new(fields)
}
