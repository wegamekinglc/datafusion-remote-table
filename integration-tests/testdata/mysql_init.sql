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
    mediumtext_col MEDIUMTEXT,
    longtext_col LONGTEXT,
    tinyblob_col TINYBLOB,
    blob_col BLOB,
    mediumblob_col MEDIUMBLOB,
    longblob_col LONGBLOB
);

INSERT INTO supported_data_types values
(1, 2, 3, 4, 1.1, 2.2, 'char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext', X'01', X'02', X'03', X'04'),
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);