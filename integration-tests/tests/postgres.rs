use integration_tests::shared_containers::setup_shared_containers;
use integration_tests::utils::{assert_result, assert_sqls};

#[tokio::test]
pub async fn supported_postgres_types() {
    setup_shared_containers();
    assert_result(
        "postgres",
        "SELECT * from supported_data_types",
        "SELECT * FROM remote_table",
        r#"+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+
| smallint_col | integer_col | bigint_col | real_col | double_col | numeric_col | char_col   | varchar_col | bpchar_col | text_col | bytea_col | date_col   | time_col | timestamp_col       | timestamptz_col      | interval_col   | boolean_col | json_col          | jsonb_col         | geometry_col                                       | smallint_array_col | integer_array_col | bigint_array_col | real_array_col | double_array_col | char_array_col           | varchar_array_col    | bpchar_array_col   | text_array_col | bool_array_col |
+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+
| 1            | 2           | 3          | 1.1      | 2.2        | 3.30        | char       | varchar     | bpchar     | text     | deadbeef  | 2023-10-01 | 12:34:56 | 2023-10-01T12:34:56 | 2023-10-01T12:34:56Z | 3 mons 14 days | true        | {"key1":"value1"} | {"key2":"value2"} | 010100002038010000000000000000f03f000000000000f03f | [1, 2]             | [3, 4]            | [5, 6]           | [1.1, 2.2]     | [3.3, 4.4]       | [char0     , char1     ] | [varchar0, varchar1] | [bpchar0, bpchar1] | [text0, text1] | [true, false]  |
|              |             |            |          |            |             |            |             |            |          |           |            |          |                     |                      |                |             |                   |                   |                                                    |                    |                   |                  |                |                  |                          |                      |                    |                |                |
+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+"#,
    ).await;

    assert_result(
        "postgres",
        "SELECT * from supported_data_types",
        "SELECT integer_col, varchar_col FROM remote_table",
        r#"+-------------+-------------+
| integer_col | varchar_col |
+-------------+-------------+
| 2           | varchar     |
|             |             |
+-------------+-------------+"#,
    )
    .await;
}

#[tokio::test]
pub async fn table_columns() {
    setup_shared_containers();
    let sql = format!(
        r#"
        SELECT
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
    t.typname AS udt_name
FROM
    pg_catalog.pg_attribute a
JOIN
    pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN
    pg_catalog.pg_type t ON a.atttypid = t.oid
WHERE
    c.relname = '{}'
    AND c.relkind IN ('r', 'v', 'm')
    AND a.attnum > 0
    AND NOT a.attisdropped
ORDER BY
    a.attnum
        "#,
        "simple_table"
    );
    assert_result(
        "postgres",
        &sql,
        "SELECT * FROM remote_table",
        r#"+-------------+------------------------+-------------+----------+
| column_name | data_type              | is_nullable | udt_name |
+-------------+------------------------+-------------+----------+
| id          | integer                | NO          | int4     |
| name        | character varying(255) | NO          | varchar  |
+-------------+------------------------+-------------+----------+"#,
    )
    .await;
}

#[tokio::test]
pub async fn various_sqls() {
    setup_shared_containers();

    assert_sqls(
        "postgres",
        vec!["select * from pg_catalog.pg_stat_all_tables"],
    )
    .await;
}
