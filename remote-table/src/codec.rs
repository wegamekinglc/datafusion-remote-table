use crate::generated::prost as protobuf;
use crate::{
    connect, ConnectionOptions, DFResult, MysqlConnectionOptions, OracleConnectionOptions,
    PostgresConnectionOptions, RemoteTableExec, Transform,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::convert_required;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

pub trait TransformCodec: Debug + Send + Sync {
    fn try_encode(&self, value: &dyn Transform) -> DFResult<Vec<u8>>;
    fn try_decode(&self, value: &[u8]) -> DFResult<Arc<dyn Transform>>;
}

#[derive(Debug)]
pub struct RemotePhysicalCodec {
    transform_codec: Option<Arc<dyn TransformCodec>>,
}

impl RemotePhysicalCodec {
    pub fn new(transform_codec: Option<Arc<dyn TransformCodec>>) -> Self {
        Self { transform_codec }
    }
}

impl PhysicalExtensionCodec for RemotePhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let proto = protobuf::RemoteTableExec::decode(buf).map_err(|e| {
            DataFusionError::Internal(format!(
                "Failed to decode remote table execution plan: {e:?}"
            ))
        })?;

        let transform = if let Some(bytes) = proto.transform {
            let Some(transform_codec) = self.transform_codec.as_ref() else {
                return Err(DataFusionError::Execution(
                    "No transform codec found".to_string(),
                ));
            };
            Some(transform_codec.try_decode(&bytes)?)
        } else {
            None
        };

        let projected_schema: SchemaRef = Arc::new(convert_required!(&proto.projected_schema)?);

        let projection: Option<Vec<usize>> = proto
            .projection
            .map(|p| p.projection.iter().map(|n| *n as usize).collect());

        let conn_options = parse_connection_options(proto.conn_options.unwrap());
        let conn = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let pool = connect(&conn_options).await?;
                let conn = pool.get().await?;
                Ok::<_, DataFusionError>(conn)
            })
        })?;

        Ok(Arc::new(RemoteTableExec::new(
            conn_options,
            projected_schema,
            proto.sql,
            projection,
            transform,
            conn,
        )))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        if let Some(exec) = node.as_any().downcast_ref::<RemoteTableExec>() {
            let serialized_transform = if let Some(transform) = exec.transform.as_ref() {
                let Some(transform_codec) = self.transform_codec.as_ref() else {
                    return Err(DataFusionError::Execution(
                        "No transform codec found".to_string(),
                    ));
                };
                let bytes = transform_codec.try_encode(transform.as_ref())?;
                Some(bytes)
            } else {
                None
            };

            let serialized_connection_options = serialize_connection_options(&exec.conn_options);

            let proto = protobuf::RemoteTableExec {
                conn_options: Some(serialized_connection_options),
                sql: exec.sql.clone(),
                projected_schema: Some(exec.schema().as_ref().try_into()?),
                projection: exec
                    .projection
                    .as_ref()
                    .map(|p| serialize_projection(p.as_slice())),
                transform: serialized_transform,
            };

            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Failed to encode remote table execution plan: {e:?}"
                ))
            })?;
            Ok(())
        } else {
            Err(DataFusionError::Execution(format!(
                "Failed to encode {}",
                RemoteTableExec::static_name()
            )))
        }
    }
}

fn serialize_connection_options(options: &ConnectionOptions) -> protobuf::ConnectionOptions {
    match options {
        ConnectionOptions::Postgres(options) => protobuf::ConnectionOptions {
            connection_options: Some(protobuf::connection_options::ConnectionOptions::Postgres(
                protobuf::PostgresConnectionOptions {
                    host: options.host.clone(),
                    port: options.port as u32,
                    username: options.username.clone(),
                    password: options.password.clone(),
                    database: options.database.clone(),
                },
            )),
        },
        ConnectionOptions::Mysql(options) => protobuf::ConnectionOptions {
            connection_options: Some(protobuf::connection_options::ConnectionOptions::Mysql(
                protobuf::MysqlConnectionOptions {
                    host: options.host.clone(),
                    port: options.port as u32,
                    username: options.username.clone(),
                    password: options.password.clone(),
                    database: options.database.clone(),
                },
            )),
        },
        ConnectionOptions::Oracle(options) => protobuf::ConnectionOptions {
            connection_options: Some(protobuf::connection_options::ConnectionOptions::Oracle(
                protobuf::OracleConnectionOptions {
                    host: options.host.clone(),
                    port: options.port as u32,
                    username: options.username.clone(),
                    password: options.password.clone(),
                    service_name: options.service_name.clone(),
                },
            )),
        },
        ConnectionOptions::Sqlite(path) => protobuf::ConnectionOptions {
            connection_options: Some(protobuf::connection_options::ConnectionOptions::Sqlite(
                protobuf::SqliteConnectionOptions {
                    path: path.to_str().unwrap().to_string(),
                },
            )),
        },
    }
}

fn parse_connection_options(options: protobuf::ConnectionOptions) -> ConnectionOptions {
    match options.connection_options {
        Some(protobuf::connection_options::ConnectionOptions::Postgres(options)) => {
            ConnectionOptions::Postgres(PostgresConnectionOptions {
                host: options.host,
                port: options.port as u16,
                username: options.username,
                password: options.password,
                database: options.database,
            })
        }
        Some(protobuf::connection_options::ConnectionOptions::Mysql(options)) => {
            ConnectionOptions::Mysql(MysqlConnectionOptions {
                host: options.host,
                port: options.port as u16,
                username: options.username,
                password: options.password,
                database: options.database,
            })
        }
        Some(protobuf::connection_options::ConnectionOptions::Oracle(options)) => {
            ConnectionOptions::Oracle(OracleConnectionOptions {
                host: options.host,
                port: options.port as u16,
                username: options.username,
                password: options.password,
                service_name: options.service_name,
            })
        }
        Some(protobuf::connection_options::ConnectionOptions::Sqlite(options)) => {
            ConnectionOptions::Sqlite(Path::new(&options.path).to_path_buf())
        }
        _ => panic!("Failed to parse connection options: {options:?}"),
    }
}

fn serialize_projection(projection: &[usize]) -> protobuf::Projection {
    protobuf::Projection {
        projection: projection.iter().map(|n| *n as u32).collect(),
    }
}
