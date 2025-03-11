CREATE DATABASE test;
USE test;

CREATE TABLE supported_data_types
(
    tinyint_col TINYINT,
    smallint_col SMALLINT
);

INSERT INTO supported_data_types values (1, 2);