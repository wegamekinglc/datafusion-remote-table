use datafusion::arrow::datatypes::{DataType, Field, Schema};

#[derive(Debug, Clone)]
pub enum RemoteDataType {
    Boolean,
    Int16,
    Int32,
}

impl RemoteDataType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            RemoteDataType::Boolean => DataType::Boolean,
            RemoteDataType::Int16 => DataType::Int16,
            RemoteDataType::Int32 => DataType::Int32,
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
    pub fn new(name: String, data_type: RemoteDataType, nullable: bool) -> Self {
        RemoteField {
            name,
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
