CREATE TABLE supported_data_types
(
    varchar2_col VARCHAR2(255),
    char_col CHAR(10),
    number_col NUMBER(10, 2)
);

INSERT INTO supported_data_types values ('varchar2', 'char', 1.1);
INSERT INTO supported_data_types values (NULL, NULL, NULL);
