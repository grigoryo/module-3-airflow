# SQL SCRIPTS

<https://github.com/mukunku/ParquetViewer>

## STG

```sql

-- billing

CREATE EXTERNAL TABLE gosipenkov.stg_billing (
    user_id INT,
    billing_period STRING,
    service STRING,
    tariff STRING,
    sum STRING,
    created_at STRING
)
STORED AS PARQUET
    LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-gosipenkov/data_lake/stg/billing';

SELECT * FROM gosipenkov.stg_billing LIMIT 25;

-- issue

CREATE EXTERNAL TABLE gosipenkov.stg_issue (
    user_id STRING,
    start_time STRING,
    end_time STRING,
    title STRING,
    description STRING,
    service STRING
)
STORED AS PARQUET
    LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-gosipenkov/data_lake/stg/issue';

SELECT * FROM gosipenkov.stg_issue LIMIT 25;

-- payment

CREATE EXTERNAL TABLE gosipenkov.stg_payment (
    user_id INT,
    pay_doc_type STRING,
    pay_doc_num INT,
    account STRING,
    phone STRING,
    billing_period STRING,
    pay_date STRING,
    `sum` DOUBLE
)
STORED AS PARQUET
    LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-gosipenkov/data_lake/stg/payment';

SELECT * FROM gosipenkov.stg_payment LIMIT 25;

-- traffic

CREATE EXTERNAL TABLE gosipenkov.stg_traffic (
    user_id INT,
    `timestamp` STRING,
    device_id STRING,
    device_ip_addr STRING,
    bytes_sent INT,
    bytes_received INT
)
STORED AS PARQUET
    LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-gosipenkov/data_lake/stg/traffic';

SELECT * FROM gosipenkov.stg_traffic LIMIT 25;

```

## STG DATA

```sql

SELECT * FROM gosipenkov.stg_billing LIMIT 25;

/*
10750   2016-10 yiybdhyryvtitv  Maxi    5       2020-10-13
10410   2018-11 swjocgspUcvehb  Gigabyte        2       2014-08-19
11150   2015-09 pxfdgghpntflui  Gigabyte        1       2015-06-29
10750   2015-12 JrxazapuHhrnJv  Mini    5       2013-09-04
10190   2019-02 giixefohvHktle  Mini    5       2017-08-14
10450   2015-02 lbuhvqdgcZneaq  Maxi    5       2019-05-11
10450   2017-04 nqSotqlqvpvahm  Gigabyte        4       2015-10-14
10890   2017-01 hfmrkfYHutHqUo  Gigabyte        4       2015-01-10
10210   2013-12 ffbzymlxlawwfX  Gigabyte        5       2016-01-01
11040   2013-10 lctcrcgyzxvuev  Mini    2       2016-01-26
11210   2015-09 bscfBuwgbtoxwu  Gigabyte        5       2014-12-06
10690   2014-04 WcvpuofKmikqdM  Megabyte        5       2016-12-27
10030   2021-03 jbqoRkkacFsnoU  Gigabyte        4       2020-06-04
10370   2014-08 ieLUmftlZucobA  Gigabyte        5       2016-04-04
10560   2019-01 vgdznghxDzevzn  Maxi    4       2019-02-14
10930   2019-10 vfyclpplzmyeux  Megabyte        1       2013-01-10
11210   2013-10 wlzlQxnlsovwoF  Gigabyte        5       2020-06-20
10270   2019-05 byyIippyvimrxa  Gigabyte        1       2021-02-09
10510   2017-04 xwjxkamgSkwmYq  Gigabyte        1       2019-04-14
11140   2015-06 rhruqauprdgqfd  Megabyte        4       2017-01-31
10520   2019-12 tyrhkMykqgkMdc  Mini    4       2015-08-28
10170   2014-06 QcecthjLjiudfr  Megabyte        2       2017-11-26
10490   2016-10 iuwwxgcwOkhega  Maxi    5       2020-01-27
11120   2014-02 srvxRcqlgqexxj  Megabyte        3       2013-08-25
11040   2018-08 fllajqznwrpkMu  Megabyte        4       2015-03-25
*/

SELECT * FROM gosipenkov.stg_issue LIMIT 25;

/*
10170   2017-04-02 22:22:31     2017-07-20 22:22:31     eqzfouyUfljcea  bpunujbttvoory  Setup Environment
11040   2017-04-17 22:22:31     2015-06-18 22:22:31     zyzkxjeElpgFyd  zetplbxCzcdlxW  Setup Environment
10820   2014-07-28 22:22:31     2013-11-07 22:22:31     nWxnrxwQxsosmy  vsozrzecakznDt  Connect
10650   2020-05-26 22:22:31     2015-08-14 22:22:31     tpzAmkdbpyxwjd  pbMlAWaajMmort  Setup Environment
10320   2020-10-28 22:22:31     2017-03-03 22:22:31     mioyeNlrdxxujh  hgsAfgnwbfdaok  Disconnect
11070   2018-10-03 22:22:31     2013-06-27 22:22:31     uskmAgpIsufvZf  ievinjquzkrltk  Setup Environment
10260   2014-09-03 22:22:31     2017-10-22 22:22:31     xpKruAiPapdGlM  enmNvwnelbatve  Disconnect
11120   2017-06-02 22:22:31     2013-04-04 22:22:31     fixldjCAbfukUn  hfeteInIoydkph  Setup Environment
10080   2020-07-04 22:22:31     2018-11-21 22:22:31     awKwavebtcggvj  lqrxzgmAfwdyex  Disconnect
10250   2013-05-22 22:22:31     2015-01-04 22:22:31     txBhMmbrtoqber  oizcaKGrfkGesm  Disconnect
10300   2017-04-09 22:22:31     2019-02-11 22:22:31     cyjkcIbafpqryn  yVhohbkvmgdyjs  Disconnect
10100   2016-09-22 22:22:31     2017-08-19 22:22:31     gsmvdusyfdmzpo  fxtgFunRwhtozn  Disconnect
10660   2015-04-26 22:22:31     2014-04-13 22:22:31     rbmohhaRQvgscz  kgmsKmRqgwgnxv  Disconnect
10110   2013-03-30 22:22:31     2021-02-24 22:22:31     wmlmggqbwDQqjv  jIhmqjcClzIder  Setup Environment
10650   2019-01-06 22:22:31     2013-12-28 22:22:31     avxFlcukgnzYiu  DaxryppmCxGsky  Disconnect
10680   2018-01-30 22:22:31     2013-06-27 22:22:31     wtcyxBnePlyifj  ibktAiYipwggue  Connect
10560   2015-09-22 22:22:31     2016-08-16 22:22:31     NPuwhymvlkxxxz  zqlCrlsEguxrdl  Connect
10800   2013-04-11 22:22:31     2014-05-04 22:22:31     QiYgkrJfakRhWl  AiMuvwnwtoufnj  Setup Environment
11190   2019-08-10 22:22:31     2020-04-30 22:22:31     humtovjlwrkBqe  bRdqhrnivqdgvf  Connect
11070   2015-04-22 22:22:31     2013-10-15 22:22:31     ixfhklfpydrzql  kdhZinIqlvqvuq  Disconnect
10830   2013-02-10 22:22:31     2018-10-01 22:22:31     ibslqmzwhrrqff  fyDtcSvjNmuAcf  Connect
10070   2021-02-08 22:22:31     2016-02-29 22:22:31     keqcfzyizxbdqo  pgdzigiypRfggs  Disconnect
11140   2018-03-27 22:22:31     2013-04-27 22:22:31     bwkzlIvnvzfwcs  whyEzMnqxiktyw  Setup Environment
10670   2021-02-07 22:22:31     2016-09-07 22:22:31     lKhkqgvdfsdxht  kytadeewreszgq  Setup Environment
10250   2018-08-30 22:22:31     2013-05-13 22:22:31     utbcnbrpftqsoZ  Iiafynwhbhsltg  Disconnect
*/

SELECT * FROM gosipenkov.stg_payment LIMIT 25;

/*
10250   MIR     3974    FL-49385        +79011276137    2020-02 2015-05-03      37831.0
11200   MASTER  39830   FL-2705 +79015636467    2013-07 2019-09-20      46921.0
10540   MASTER  35904   FL-833  +79017548916    2020-04 2016-04-26      33739.0
10220   MIR     19450   FL-29144        +79010980561    2013-02 2013-02-22      26343.0
10650   VISA    5337    FL-46012        +79012101819    2020-04 2013-11-03      32982.0
10570   MIR     35809   FL-47369        +79019505673    2021-01 2014-07-21      41751.0
11130   MIR     49336   FL-28951        +79016344371    2018-09 2020-01-22      37880.0
11110   MIR     14024   FL-14714        +79013438875    2015-06 2013-07-17      40925.0
10340   MASTER  31861   FL-37080        +79017790658    2020-07 2019-11-03      16909.0
10560   VISA    1259    FL-49373        +79016011971    2018-02 2017-07-09      48769.0
11150   MIR     37944   FL-40252        +79015436192    2018-06 2015-09-19      36718.0
10140   MIR     6696    FL-295  +79011736295    2014-12 2013-11-13      19677.0
10700   VISA    26200   FL-18063        +79014477264    2016-06 2017-10-24      1518.0
10950   VISA    21701   FL-5244 +79017779812    2016-01 2017-05-21      9989.0
10250   MIR     41974   FL-40970        +79016619575    2015-09 2014-06-22      43497.0
10150   VISA    3219    FL-27434        +79010366245    2015-12 2020-09-28      27767.0
10200   VISA    30083   FL-6185 +79019122258    2013-04 2014-09-26      26638.0
10230   MASTER  32859   FL-45208        +79015457025    2020-04 2013-01-16      24876.0
11100   MASTER  40910   FL-15199        +79010421789    2019-10 2017-03-31      39909.0
10890   VISA    4270    FL-35246        +79010646303    2019-04 2021-01-20      37551.0
10310   MIR     48849   FL-5494 +79019098275    2015-10 2014-06-24      4737.0
10760   VISA    21664   FL-35220        +79015128044    2014-04 2013-07-18      21002.0
10280   MASTER  23119   FL-19562        +79016757129    2016-07 2014-05-19      46336.0
10750   VISA    39191   FL-26945        +79014015493    2017-06 2018-01-19      22080.0
10030   MASTER  18011   FL-44750        +79016944523    2016-03 2013-10-15      1648.0
*/

SELECT * FROM gosipenkov.stg_traffic LIMIT 25;

/*
11110   1475868151268   d002    161.113.152.164 44243   14168
10390   1511205751268   d007    58.175.121.85   16038   49543
10710   1606764151268   d009    101.23.121.157  13507   35786
10500   1456600951268   d006    2.51.72.106     13462   13892
10730   1407435751268   d006    46.57.186.42    35966   31449
11140   1546111351268   d008    180.102.122.108 2636    46443
11130   1459884151268   d001    184.22.103.179  43353   4368
10170   1527276151268   d006    40.147.72.25    44790   16622
10590   1416252151268   d002    166.28.166.21   10101   26234
10670   1600802551268   d008    175.59.175.11   21027   38184
10980   1438975351268   d006    187.112.154.17  39732   6455
11190   1560626551268   d007    45.21.88.79     45546   13866
10620   1420053751268   d003    150.19.37.72    19155   41656
11130   1518204151268   d006    61.125.70.132   6124    46915
10370   1417548151268   d006    140.113.73.93   48      9218
10110   1372616551268   d006    190.163.88.42   29794   41550
11180   1491592951268   d007    97.114.86.44    3768    3014
10050   1402597351268   d006    38.140.124.58   957     12351
10750   1499887351268   d009    27.170.146.47   27399   35403
10230   1455823351268   d007    98.129.42.177   14193   27106
10160   1543519351268   d007    142.29.126.50   26412   30058
10870   1436728951268   d002    12.42.66.78     43102   33235
10020   1600024951268   d003    147.28.120.62   30475   29623
10630   1468005751268   d003    116.138.133.104 8077    35635
10400   1595445751268   d005    16.82.141.165   600     18651
*/

```

## ODS

```sql

-- billing

CREATE EXTERNAL TABLE gosipenkov.ods_billing (
    user_id INT,
    billing_period INT,
    service STRING,
    tariff STRING,
    amount DECIMAL,
    created_at DATE
)
PARTITIONED BY (year INT)
STORED AS PARQUET
    LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-gosipenkov/data_lake/ods/billing';

INSERT OVERWRITE TABLE gosipenkov.ods_billing
PARTITION (year = 2019)
SELECT
    user_id,
    CAST(SUBSTR(billing_period, 1, 4) || SUBSTR(billing_period, 6, 2) AS INT) AS billing_period,
    service,
    tariff,
    CAST(`sum` AS DECIMAL) AS amount,
    CAST(created_at AS DATE) AS created_at
FROM gosipenkov.stg_billing
WHERE YEAR(created_at) = 2019;

SELECT * FROM gosipenkov.ods_billing LIMIT 25;

-- issue

CREATE EXTERNAL TABLE gosipenkov.ods_issue (
    user_id INT,
    start_date DATE,
    end_date DATE,
    title STRING,
    description STRING,
    service STRING
)
PARTITIONED BY (year INT)
STORED AS PARQUET
    LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-gosipenkov/data_lake/ods/issue';

INSERT OVERWRITE TABLE gosipenkov.ods_issue
PARTITION (year = 2019)
SELECT
    CAST(user_id AS INT) AS user_id,
    CAST(CAST(start_time AS TIMESTAMP) AS DATE) AS start_date,
    CAST(CAST(end_time AS TIMESTAMP) AS DATE) AS end_date,
    title,
    description,
    service
FROM gosipenkov.stg_issue
WHERE YEAR(end_time) = 2019;

SELECT * FROM gosipenkov.ods_issue LIMIT 25;

-- payment

CREATE EXTERNAL TABLE gosipenkov.ods_payment (
    user_id INT,
    doc_type STRING,
    doc_num INT,
    account STRING,
    phone STRING,
    billing_period INT,
    pay_date DATE,
    amount DECIMAL
)
PARTITIONED BY (year INT)
STORED AS PARQUET
    LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-gosipenkov/data_lake/ods/payment';

INSERT OVERWRITE TABLE gosipenkov.ods_payment
PARTITION (year = 2019)
SELECT
    user_id,
    pay_doc_type AS doc_type,
    pay_doc_num AS doc_num,
    account,
    phone,
    CAST(SUBSTR(billing_period, 1, 4) || SUBSTR(billing_period, 6, 2) AS INT) AS billing_period,
    CAST(pay_date AS DATE) AS pay_date,
    CAST(`sum` AS DECIMAL) AS amount
FROM gosipenkov.stg_payment
WHERE YEAR(pay_date) = 2019;

SELECT * FROM gosipenkov.ods_payment LIMIT 25;

-- traffic

CREATE EXTERNAL TABLE gosipenkov.ods_traffic (
    user_id INT,
    unixtime INT,
    device_id STRING,
    device_ip_addr STRING,
    bytes_sent INT,
    bytes_received INT
)
PARTITIONED BY (year INT)
STORED AS PARQUET
    LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-gosipenkov/data_lake/ods/traffic';

INSERT OVERWRITE TABLE gosipenkov.ods_traffic
PARTITION (year = 2019)
SELECT
    user_id,
    CAST((CAST(`timestamp` AS BIGINT) DIV 1000) AS INT) AS unixtime,
    device_id,
    device_ip_addr,
    bytes_sent,
    bytes_received
FROM gosipenkov.stg_traffic
WHERE YEAR(FROM_UNIXTIME(CAST(`timestamp` AS BIGINT) DIV 1000)) = 2019;

SELECT * FROM gosipenkov.ods_traffic LIMIT 25;

```

## ODS DATA

```sql

SELECT * FROM gosipenkov.ods_billing LIMIT 25;

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

SELECT * FROM gosipenkov.ods_issue LIMIT 25;

/*
10300   2017-04-09      2019-02-11      cyjkcIbafpqryn  yVhohbkvmgdyjs  Disconnect      2019
10330   2014-03-31      2019-01-28      oWktasuacntkyE  rrubiOMeDydjPt  Setup Environment       2019
10600   2020-05-01      2019-02-12      pyhhatuXwxkzqs  uDcuvynxooflzq  Setup Environment       2019
11050   2014-05-25      2019-12-17      ogusenvdntjicp  yxfyzgjugebpmh  Connect 2019
10560   2014-03-04      2019-12-19      onbLKEihnbnExs  hmaegazTCfljsL  Disconnect      2019
10590   2015-05-17      2019-03-04      kpjkTbrftemhwF  vDspbthblyAkgg  Disconnect      2019
10450   2019-03-15      2019-08-09      sradxwvcoyhcig  wdmgSqkdnjvyka  Setup Environment       2019
11100   2016-08-09      2019-11-27      gbywqnbmyPzmsz  gpiekLmdSQvnxu  Connect 2019
10480   2019-07-28      2019-08-03      ztvgHeyJagryzi  LmmoqLnyeINilN  Connect 2019
10110   2016-08-08      2019-03-10      ddxIhucvajbwmz  bmdydiinfguiRx  Setup Environment       2019
10640   2015-02-26      2019-04-19      ffilsobkalRfsx  tkoscuopsgdkdr  Setup Environment       2019
10170   2019-09-29      2019-10-21      xWQfyOsikdwsuv  exKmpadwdbsckh  Connect 2019
10210   2020-06-04      2019-11-26      gwlhkxmipoemoo  evthzecbkatcxM  Connect 2019
10990   2021-01-13      2019-02-20      lopbhsahnuznfg  sdtawthshonkJv  Connect 2019
10550   2016-12-23      2019-12-28      zcpgxhklcphrhi  ejovqwqqswbycl  Connect 2019
10030   2016-01-17      2019-03-04      vqsdkpaaolpHps  jqhayfegecjatv  Setup Environment       2019
11100   2020-07-29      2019-08-07      Nwzbjootugpnwu  noHdrlfcccMjnx  Setup Environment       2019
10850   2014-05-11      2019-03-01      mgxboYcbiahcli  ligwgoeKxcgnnb  Setup Environment       2019
10740   2017-12-14      2019-10-15      plgfyDrfqhqylp  xkectqTxxzsAHh  Setup Environment       2019
11030   2013-10-05      2019-01-17      iSkciztlhufgzq  rmdrbmznthtdmr  Setup Environment       2019
11050   2021-01-31      2019-11-15      nwatdopesunaEM  PagnofgdudzlGk  Disconnect      2019
11030   2016-09-09      2019-09-22      yZckCxwjctmLog  uecqdsnwkjTaeg  Disconnect      2019
10240   2020-10-25      2019-02-17      axjjzyrzffyorj  chSzFzwftroazv  Connect 2019
10830   2016-05-24      2019-12-27      pmwgmqoxgtrkvK  qsnxKwduhAVexx  Setup Environment       2019
10340   2014-06-24      2019-03-06      eMtgcomszosukg  PaketRzeggteig  Setup Environment       2019
*/

SELECT * FROM gosipenkov.ods_payment LIMIT 25;

/*
11200   MASTER  39830   FL-2705 +79015636467    201307  2019-09-20      46921   2019
10340   MASTER  31861   FL-37080        +79017790658    202007  2019-11-03      16909   2019
10810   MIR     9222    FL-19919        +79015092452    201808  2019-11-10      11599   2019
10010   VISA    10746   FL-30695        +79015122336    201308  2019-08-28      13674   2019
11040   MIR     33010   FL-46914        +79019397141    201311  2019-09-22      23072   2019
10910   VISA    45279   FL-41803        +79014428640    201311  2019-01-31      37756   2019
10960   MASTER  40679   FL-21140        +79015278407    201610  2019-07-27      9247    2019
10250   VISA    11051   FL-21035        +79018790636    201408  2019-04-23      16911   2019
10730   MIR     29021   FL-713  +79016224351    201406  2019-08-03      23048   2019
11010   VISA    25111   FL-20826        +79015795020    201812  2019-04-28      4133    2019
10400   VISA    48370   FL-32596        +79019520296    202011  2019-06-14      49788   2019
10740   MASTER  8525    FL-45419        +79010882545    201303  2019-07-13      28241   2019
10310   VISA    49167   FL-43201        +79019986095    201303  2019-08-22      41469   2019
10830   MASTER  12128   FL-41111        +79012019595    201709  2019-02-20      43548   2019
10530   VISA    24301   FL-37821        +79012902136    201805  2019-02-05      46609   2019
10370   VISA    49045   FL-19644        +79019607834    201712  2019-05-02      35995   2019
10740   MASTER  45617   FL-36087        +79016111463    201708  2019-09-21      22638   2019
10540   MASTER  46166   FL-19591        +79015631665    201910  2019-06-07      29257   2019
10110   MASTER  7824    FL-21921        +79015975925    201904  2019-11-06      719     2019
10900   MASTER  1384    FL-7027 +79016766329    202006  2019-10-08      37629   2019
10300   MASTER  6348    FL-36202        +79013642056    201705  2019-08-07      41549   2019
10310   MIR     46577   FL-4579 +79010725256    202009  2019-07-16      7210    2019
11190   VISA    24811   FL-17853        +79016982207    201712  2019-03-04      45002   2019
11160   MIR     11939   FL-14432        +79017781556    201912  2019-12-10      38492   2019
10860   MIR     39096   FL-21829        +79015895404    202102  2019-09-29      6384    2019
*/

SELECT * FROM gosipenkov.ods_traffic LIMIT 25;

/*
11190   1560626551      d007    45.21.88.79     45546   13866   2019
11060   1552504951      d008    129.187.54.190  26273   28334   2019
10270   1549826551      d005    160.158.184.156 37879   22044   2019
10510   1557084151      d002    141.125.155.31  44814   21300   2019
10040   1553282551      d006    174.49.143.49   16968   24992   2019
10970   1549048951      d001    60.67.51.34     8566    18707   2019
10720   1570044151      d009    96.94.44.65     36564   27331   2019
10210   1570389751      d006    189.174.39.157  5378    2138    2019
10070   1576351351      d003    21.60.130.50    7212    7137    2019
11200   1574450551      d009    64.46.77.57     3889    13340   2019
10280   1549135351      d002    177.22.187.100  40236   18261   2019
11110   1550085751      d003    96.45.106.103   20236   16376   2019
10270   1573932151      d009    144.37.141.18   17819   26545   2019
11180   1575660151      d008    19.129.80.178   20378   36076   2019
10900   1557688951      d004    168.156.13.139  16980   12728   2019
10190   1569698551      d003    108.88.43.170   18199   1610    2019
11010   1559503351      d002    133.74.37.177   25239   14432   2019
10710   1555788151      d001    173.105.36.20   32588   49616   2019
10070   1575487351      d008    70.132.87.8     3001    2737    2019
11070   1570044151      d001    173.82.76.39    48596   9634    2019
10690   1548789751      d003    63.130.179.66   23919   15487   2019
11180   1552850551      d001    89.147.173.10   12310   38228   2019
10480   1567970551      d008    19.78.179.141   2090    3512    2019
11050   1552936951      d004    119.192.68.44   19253   3368    2019
10190   1548876151      d009    39.149.170.34   3920    27661   2019
*/

```

## DM

С помощью Hive из слоя ODS необходимо в схеме dm построить витрину
с распределением по скачанному трафику (`bytes_received`) и группировке
по пользователю. Для каждого пользователя необходимо вывести максимальное,
минимальное и среднее по количеству скачанных байт за год.

```sql

CREATE EXTERNAL TABLE gosipenkov.dm_traffic_stats (
    user_id INT,
    bytes_received_min INT,
    bytes_received_avg INT,
    bytes_received_max INT
)
PARTITIONED BY (year INT)
STORED AS PARQUET
    LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-gosipenkov/data_lake/dm/traffic_stats';

INSERT OVERWRITE TABLE gosipenkov.dm_traffic_stats
PARTITION (year = 2019)
SELECT
    user_id,
    MIN(bytes_received) AS bytes_received_min,
    AVG(bytes_received) AS bytes_received_avg,
    MAX(bytes_received) AS bytes_received_max
FROM gosipenkov.ods_traffic
WHERE year = 2019
GROUP BY user_id;

SELECT * FROM gosipenkov.dm_traffic_stats LIMIT 25;

```

## DM DATA

```sql

SELECT * FROM gosipenkov.dm_traffic_stats LIMIT 25;

/*
10010   411     18695   43271   2019
10020   5169    32708   47308   2019
10030   439     26170   48518   2019
10040   1539    22363   40883   2019
10050   11871   21569   35312   2019
10060   1286    28977   49152   2019
10070   2737    28113   49525   2019
10080   4101    20989   44593   2019
10090   4703    20935   49850   2019
10100   5154    19875   36717   2019
10110   299     24694   45243   2019
10120   660     25914   45485   2019
10130   2005    18983   43591   2019
10140   3309    22274   48638   2019
10150   5202    25112   48286   2019
10160   658     23176   46200   2019
10170   803     28602   47622   2019
10180   126     14236   27473   2019
10190   277     20969   48643   2019
10200   346     24758   43689   2019
10210   1371    16187   43665   2019
10220   12      14905   34838   2019
10230   8640    23396   40808   2019
10240   3104    28314   48374   2019
10250   2901    22695   44061   2019
*/

```
