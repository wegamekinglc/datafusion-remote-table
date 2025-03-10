CREATE TABLE supported_data_types (
    smallint_column SMALLINT,
    integer_column INTEGER,
    bigint_column BIGINT,
    serial_column SERIAL,
    bigserial_column BIGSERIAL,

    char_column CHAR(10),
    varchar_column VARCHAR(255),
    text_column TEXT,

    bytea_column BYTEA,

    date_column DATE,
    time_column TIME,
    timestamp_column TIMESTAMP,
    timestamptz_column TIMESTAMPTZ,

    boolean_column BOOLEAN,

    integer_array_column INTEGER[],
    text_array_column TEXT[]
);

INSERT INTO supported_data_types values (1, 2, 3, 4, 5, 'char', 'varchar', 'text', E'\\xDEADBEEF', '2023-10-01', '12:34:56', '2023-10-01 12:34:56', '2023-10-01 12:34:56+00', TRUE, ARRAY[1, 2], ARRAY['text0', 'text1']);