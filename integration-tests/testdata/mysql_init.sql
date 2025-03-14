CREATE DATABASE test;
USE test;

CREATE TABLE supported_data_types
(
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    integer_col INT,
    bigint_col BIGINT,
    float_col FLOAT,
    double_col DOUBLE,
    char_col CHAR(10),
    varchar_col VARCHAR(255),
    tinytext_col TINYTEXT,
    text_col TEXT,
    tinyblob_col TINYBLOB,
    blob_col BLOB
);

INSERT INTO supported_data_types values
(1, 2, 3, 4, 1.1, 2.2, 'char', 'varchar', 'tinytext', 'text', X'01', X'02'),
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);