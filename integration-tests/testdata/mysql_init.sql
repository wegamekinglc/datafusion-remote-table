CREATE DATABASE test;
USE test;

CREATE TABLE supported_data_types
(
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    integer_col INT,
    bigint_col BIGINT,
    float_col FLOAT,
    double_col DOUBLE
);

INSERT INTO supported_data_types values (1, 2, 3, 4, 1.1, 2.2);