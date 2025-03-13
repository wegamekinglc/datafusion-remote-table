CREATE TABLE supported_data_types (
    null_col NULL,
    int_col INTEGER,
    real_col REAL,
    text_col TEXT,
    blob_col BLOB
);

INSERT INTO supported_data_types VALUES (NULL, 1, 1.1, 'text', X'010203');