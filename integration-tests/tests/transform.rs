use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Float64Array, Int64Array, NullArray, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_remote_table::{
    ConnectionOptions, RemotePhysicalCodec, RemoteTable, Transform, TransformArgs, TransformCodec,
};
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::test]
async fn transform() {
    let options = ConnectionOptions::Sqlite(PathBuf::from(format!(
        "{}/testdata/sqlite3.db",
        env!("CARGO_MANIFEST_DIR")
    )));

    let table = RemoteTable::try_new_with_transform(
        options,
        "SELECT * from supported_data_types",
        Arc::new(MyTransform {}),
    )
    .await
    .unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let result = ctx
        .sql("select * from remote_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        r#"+-----------------------------------------+-------------------------------------------------+----------------------------------------------------+-------------------------------------------------+--------------------------------------------------------+
| transformed_null                        | transformed_int64                               | transformed_float64                                | transformed_utf8                                | transformed_binary                                     |
+-----------------------------------------+-------------------------------------------------+----------------------------------------------------+-------------------------------------------------+--------------------------------------------------------+
| transform_null-0-Null-Sqlite(Null)-NULL | transform_int64-1-Int64-Sqlite(Integer)-Some(1) | transform_float64-2-Float64-Sqlite(Real)-Some(1.1) | transform_utf8-3-Utf8-Sqlite(Text)-Some("text") | transform_binary-4-Binary-Sqlite(Blob)-Some([1, 2, 3]) |
| transform_null-0-Null-Sqlite(Null)-NULL | transform_int64-1-Int64-Sqlite(Integer)-None    | transform_float64-2-Float64-Sqlite(Real)-None      | transform_utf8-3-Utf8-Sqlite(Text)-None         | transform_binary-4-Binary-Sqlite(Blob)-None            |
+-----------------------------------------+-------------------------------------------------+----------------------------------------------------+-------------------------------------------------+--------------------------------------------------------+"#
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn transform_serialization() {
    let options = ConnectionOptions::Sqlite(PathBuf::from(format!(
        "{}/testdata/sqlite3.db",
        env!("CARGO_MANIFEST_DIR")
    )));

    let table = RemoteTable::try_new_with_transform(
        options,
        "SELECT * from supported_data_types",
        Arc::new(MyTransform {}),
    )
    .await
    .unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();
    let plan = ctx.sql("SELECT * FROM remote_table").await.unwrap();
    let exec_plan = plan.create_physical_plan().await.unwrap();
    let result = collect(exec_plan.clone(), ctx.task_ctx()).await.unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    let codec = RemotePhysicalCodec::new(Some(Arc::new(MyTransformCodec {})));
    let mut plan_buf: Vec<u8> = vec![];
    let plan_proto = PhysicalPlanNode::try_from_physical_plan(exec_plan, &codec).unwrap();
    plan_proto.try_encode(&mut plan_buf).unwrap();

    let new_plan: Arc<dyn ExecutionPlan> = PhysicalPlanNode::try_decode(&plan_buf)
        .and_then(|proto| proto.try_into_physical_plan(&ctx, &ctx.runtime_env(), &codec))
        .unwrap();

    let serde_result = collect(new_plan, ctx.task_ctx()).await.unwrap();
    println!("{}", pretty_format_batches(&serde_result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        pretty_format_batches(&serde_result).unwrap().to_string()
    );
}

#[derive(Debug)]
pub struct MyTransformCodec {}

impl TransformCodec for MyTransformCodec {
    fn try_encode(&self, value: &dyn Transform) -> Result<Vec<u8>, DataFusionError> {
        if value.as_any().downcast_ref::<MyTransform>().is_some() {
            Ok("MyTransform".as_bytes().to_vec())
        } else {
            Err(DataFusionError::Internal(
                "Unexpected transform type".to_string(),
            ))
        }
    }

    fn try_decode(&self, value: &[u8]) -> Result<Arc<dyn Transform>, DataFusionError> {
        if value == "MyTransform".as_bytes() {
            Ok(Arc::new(MyTransform {}))
        } else {
            Err(DataFusionError::Internal(
                "Unexpected transform type".to_string(),
            ))
        }
    }
}

#[derive(Debug)]
pub struct MyTransform {}

impl Transform for MyTransform {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn transform_null(
        &self,
        array: &NullArray,
        args: TransformArgs,
    ) -> Result<(ArrayRef, Field), DataFusionError> {
        let mut data = Vec::with_capacity(array.len());
        for _ in 0..array.len() {
            data.push(format!(
                "transform_null-{}-{}-{:?}-NULL",
                args.col_index,
                args.field.data_type(),
                args.remote_field.as_ref().unwrap().remote_type
            ))
        }
        Ok((
            Arc::new(StringArray::from(data)),
            Field::new("transformed_null", DataType::Utf8, false),
        ))
    }

    fn transform_int64(
        &self,
        array: &Int64Array,
        args: TransformArgs,
    ) -> Result<(ArrayRef, Field), DataFusionError> {
        let mut data = Vec::with_capacity(array.len());
        for row in array.iter() {
            data.push(format!(
                "transform_int64-{}-{}-{:?}-{row:?}",
                args.col_index,
                args.field.data_type(),
                args.remote_field.as_ref().unwrap().remote_type
            ))
        }
        Ok((
            Arc::new(StringArray::from(data)),
            Field::new("transformed_int64", DataType::Utf8, false),
        ))
    }

    fn transform_float64(
        &self,
        array: &Float64Array,
        args: TransformArgs,
    ) -> Result<(ArrayRef, Field), DataFusionError> {
        let mut data = Vec::with_capacity(array.len());
        for row in array.iter() {
            data.push(format!(
                "transform_float64-{}-{}-{:?}-{row:?}",
                args.col_index,
                args.field.data_type(),
                args.remote_field.as_ref().unwrap().remote_type
            ))
        }
        Ok((
            Arc::new(StringArray::from(data)),
            Field::new("transformed_float64", DataType::Utf8, false),
        ))
    }

    fn transform_utf8(
        &self,
        array: &StringArray,
        args: TransformArgs,
    ) -> Result<(ArrayRef, Field), DataFusionError> {
        let mut data = Vec::with_capacity(array.len());
        for row in array.iter() {
            data.push(format!(
                "transform_utf8-{}-{}-{:?}-{row:?}",
                args.col_index,
                args.field.data_type(),
                args.remote_field.as_ref().unwrap().remote_type
            ))
        }
        Ok((
            Arc::new(StringArray::from(data)),
            Field::new("transformed_utf8", DataType::Utf8, false),
        ))
    }

    fn transform_binary(
        &self,
        array: &BinaryArray,
        args: TransformArgs,
    ) -> Result<(ArrayRef, Field), DataFusionError> {
        let mut data = Vec::with_capacity(array.len());
        for row in array.iter() {
            data.push(format!(
                "transform_binary-{}-{}-{:?}-{row:?}",
                args.col_index,
                args.field.data_type(),
                args.remote_field.as_ref().unwrap().remote_type
            ))
        }
        Ok((
            Arc::new(StringArray::from(data)),
            Field::new("transformed_binary", DataType::Utf8, false),
        ))
    }
}
