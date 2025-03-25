CREATE TABLE supported_data_types
(
    boolean_col BOOLEAN,
    smallint_col SMALLINT,
    integer_col INTEGER,
    binary_float_col BINARY_FLOAT,
    binary_double_col BINARY_DOUBLE,
    number_col NUMBER(10, 2),
    varchar2_col VARCHAR2(255),
    char_col CHAR(10),
    blob_col BLOB,
    date_col DATE,
    timestamp_col TIMESTAMP
);

INSERT INTO supported_data_types values (true, 1, 2, 1.1, 2.2, 3.3, 'varchar2', 'char', UTL_RAW.CAST_TO_RAW('blob'), TO_DATE('2003/05/03 21:02:44', 'yyyy/mm/dd hh24:mi:ss'), TO_TIMESTAMP('2023-10-01 14:30:45.123456', 'YYYY-MM-DD HH24:MI:SS.FF'));
INSERT INTO supported_data_types values (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
