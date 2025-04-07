CREATE TABLE supported_data_types (
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    int_col INTEGER,
    bigint_col BIGINT,
    float_col FLOAT,
    double_col DOUBLE,
    real_col REAL,
    char_col CHAR,
    varchar_col VARCHAR,
    text_col TEXT,
    binary_col BINARY,
    varbinary_col VARBINARY,
    blob_col BLOB
);

INSERT INTO supported_data_types VALUES
(1, 2, 3, 4, 1.1, 2.2, 3.3, 'char', 'varchar','text', X'01', X'02', X'03'),
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE simple_table (
    id INTEGER,
    name TEXT
);

INSERT INTO simple_table VALUES (1, 'Tom'), (2, 'Jerry'), (3, 'Spike');