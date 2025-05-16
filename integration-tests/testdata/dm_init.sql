CREATE TABLE supported_data_types (
    bit_col BIT,
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    integer_col INTEGER,
    bigint_col BIGINT,
    real_col REAL,
    float_col FLOAT,
    double_col DOUBLE,
    numeric_col NUMERIC(10, 2),
    decimal_col DECIMAL(10, 2),
    char_col CHAR(10),
    varchar_col VARCHAR(255),
    binary_col BINARY(1),
    varbinary_col VARBINARY(10),
    timestamp_col TIMESTAMP,
    date_col DATE
);

INSERT INTO supported_data_types VALUES
(1, 1, 2, 3, 4, 1.1, 2.2, 3.3, 4.4, 5.5, 'char', 'varchar', X'01', X'02', TIMESTAMP '2002-12-12 09:10:21', '2023-10-01'),
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
COMMIT;





CREATE TABLE supported_data_types (
    bit_col BIT,
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    integer_col INTEGER,
    bigint_col BIGINT,
    real_col REAL,
    float_col FLOAT,
    double_col DOUBLE,
    numeric_col NUMERIC(10, 2),
    decimal_col DECIMAL(10, 2)
);

INSERT INTO supported_data_types VALUES (1, 1, 2, 3, 4, 1.1, 2.2, 3.3, 4.4, 5.5), (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
COMMIT;