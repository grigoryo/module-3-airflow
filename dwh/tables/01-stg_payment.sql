CREATE EXTERNAL TABLE gosipenkov.stg_payment (
    user_id INT,
    pay_doc_type TEXT,
    pay_doc_num INT,
    account TEXT,
    phone TEXT,
    billing_period TEXT,
    pay_date TEXT,
    sum FLOAT
)
LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/payment/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

/*
user_id   pay_doc_type   pay_doc_num   account         phone          billing_period   pay_date     sum
I         S              I             S               S              S                S            F
--------------------------------------------------------------------------------------------------------
10550     MASTER         1441059183    FL-1871212747   +79961744719   2013-04          2013-05-20   4912
10560     MIR            1144766230    FL-492228274    +79625313382   2013-03          2013-04-24   2690
10050     MASTER         1912650142    FL-1492227448   +79211824168   2013-02          2013-03-05   9315
*/
