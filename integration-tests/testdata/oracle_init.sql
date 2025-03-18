CREATE TABLE supported_data_types
(
    varchar2_col VARCHAR2(255),
    char_col CHAR(10),
    number_col NUMBER(10, 2),
    date_col DATE,
    timestamp_col TIMESTAMP
);

INSERT INTO supported_data_types values ('varchar2', 'char', 1.1, TO_DATE('2003/05/03 21:02:44', 'yyyy/mm/dd hh24:mi:ss'), TO_TIMESTAMP('2023-10-01 14:30:45.123456', 'YYYY-MM-DD HH24:MI:SS.FF'));
INSERT INTO supported_data_types values (NULL, NULL, NULL, NULL, NULL);
