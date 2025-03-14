CREATE TABLE supported_data_types (
    smallint_col SMALLINT,
    integer_col INTEGER,
    bigint_col BIGINT,
    serial_col SERIAL,
    bigserial_col BIGSERIAL,

    char_col CHAR(10),
    varchar_col VARCHAR(255),
    text_col TEXT,

    bytea_col BYTEA,

    date_col DATE,
    time_col TIME,
    timestamp_col TIMESTAMP,
    timestamptz_col TIMESTAMPTZ,

    boolean_col BOOLEAN,

    integer_array_col INTEGER[],
    text_array_col TEXT[]
);

INSERT INTO supported_data_types VALUES
(1, 2, 3, 4, 5, 'char', 'varchar', 'text', E'\\xDEADBEEF', '2023-10-01', '12:34:56', '2023-10-01 12:34:56', '2023-10-01 12:34:56+00', TRUE, ARRAY[1, 2], ARRAY['text0', 'text1']),
(NULL, NULL, NULL, 4, 5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE simple_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

INSERT INTO simple_table VALUES (1, 'Tom');