CREATE DATABASE test;
USE test;

CREATE TABLE supported_data_types
(
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    integer_col INT,
    mediumint_col MEDIUMINT,
    bigint_col BIGINT,
    float_col FLOAT,
    double_col DOUBLE,
    decimal_col DECIMAL(60, 10),
    date_col DATE,
    datetime_col DATETIME,
    time_col TIME,
    timestamp_col TIMESTAMP,
    year_col YEAR,
    char_col CHAR(10),
    varchar_col VARCHAR(255),
    binary_col BINARY(10),
    varbinary_col VARBINARY(100),
    tinytext_col TINYTEXT,
    text_col TEXT,
    mediumtext_col MEDIUMTEXT,
    longtext_col LONGTEXT,
    tinyblob_col TINYBLOB,
    blob_col BLOB,
    mediumblob_col MEDIUMBLOB,
    longblob_col LONGBLOB,
    json_col JSON,
    geometry_col GEOMETRY
);

INSERT INTO supported_data_types values
(1, 2, 3, 4, 5, 1.1, 2.2, 3.33, '2025-03-14', '2025-03-14 17:36:25', '11:11:11', '2025-03-14 11:11:11', '1999', 'char', 'varchar', X'01', X'02', 'tinytext', 'text', 'mediumtext', 'longtext', X'01', X'02', X'03', X'04', '{"key": "value"}', Point(15, 20)),
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);