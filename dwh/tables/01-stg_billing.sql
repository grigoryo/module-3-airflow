CREATE EXTERNAL TABLE gosipenkov.stg_billing (
    user_id INT,
    billing_period INT,
    service TEXT,
    tariff TEXT,
    amount DECIMAL,
    created_at DATE
)
LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-gosipenkov/data_lake/ods/billing/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

/*
10450   201502  lbuhvqdgcZneaq  Maxi    5       2019-05-11      2019
10560   201901  vgdznghxDzevzn  Maxi    4       2019-02-14      2019
10510   201704  xwjxkamgSkwmYq  Gigabyte        1       2019-04-14      2019
10620   201801  yeJtuKxwzqSCvn  Gigabyte        3       2019-11-05      2019
10670   201407  xpmivFVvSiDtnk  Mini    5       2019-02-23      2019
10360   201412  wfafuxkfmasbjo  Megabyte        5       2019-10-30      2019
10160   201807  bpewdwtflxyulb  Megabyte        2       2019-01-02      2019
10240   201511  ljfRNfbyVbgLmc  Gigabyte        2       2019-04-21      2019
10940   201503  dvnhhhevdblKpe  Gigabyte        1       2019-12-26      2019
10230   202002  epkbhbCblgdvgZ  Mini    3       2019-12-16      2019
10290   201603  htcwjlpwhygmee  Maxi    3       2019-01-13      2019
10880   201507  aAlLekhkhquget  Maxi    1       2019-04-03      2019
10670   201312  zevzixffejbokb  Mini    2       2019-08-29      2019
11090   201412  raxhNcmnynvghy  Maxi    4       2019-03-08      2019
10120   201802  zoDxrntrkFoqkf  Megabyte        1       2019-12-09      2019
10740   201908  cbjlmlzrfjkfgb  Gigabyte        5       2019-05-28      2019
10560   202102  dvhydOrrnytqkk  Gigabyte        2       2019-10-21      2019
10530   201708  olctqtsktugezi  Mini    3       2019-02-06      2019
10100   202103  kabbkoCiyxfthm  Gigabyte        4       2019-10-14      2019
10830   201504  aliojtdjxxuynw  Gigabyte        1       2019-02-15      2019
10690   202003  rykafswzysnsme  Mini    5       2019-08-21      2019
10530   201505  Uzvcbroyruqkek  Maxi    3       2019-09-16      2019
10860   201605  nadrdohwwSLzrz  Mini    1       2019-04-25      2019
10370   201610  idsCiymrnVwtFI  Megabyte        2       2019-11-29      2019
10600   202103  gZcjhqosdjaqfe  Mini    5       2019-07-28      2019
*/
