CREATE TABLE supported_data_types
(
    varchar2_col VARCHAR2(255),
    char_col CHAR(10),
    number_col NUMBER(10, 2),
    date_col DATE
);

INSERT INTO supported_data_types values ('varchar2', 'char', 1.1, TO_DATE('2003/05/03 21:02:44', 'yyyy/mm/dd hh24:mi:ss'));
INSERT INTO supported_data_types values (NULL, NULL, NULL, NULL);
