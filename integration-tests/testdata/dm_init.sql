CREATE TABLE supported_data_types (
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    integer_col INTEGER,
    bigint_col BIGINT,
    real_col REAL,
    double_col DOUBLE
);

INSERT INTO supported_data_types VALUES
(1, 2, 3, 4, 1.1, 2.2),
(NULL, NULL, NULL, NULL, NULL, NULL);