CREATE TABLE supported_data_types (
    null_col NULL,
    int_col INTEGER,
    real_col REAL,
    text_col TEXT,
    blob_col BLOB
);

INSERT INTO supported_data_types VALUES
(NULL, 1, 1.1, 'text', X'010203'),
(NULL, NULL, NULL, NULL, NULL);

CREATE TABLE simple_table (
    id INTEGER,
    name TEXT
);

INSERT INTO simple_table VALUES (1, 'Tom'), (2, 'Jerry'), (3, 'Spike');