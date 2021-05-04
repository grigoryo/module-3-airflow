CREATE EXTERNAL TABLE gosipenkov.stg_issue (
    user_id INT,
    start_date DATE,
    end_date DATE,
    title TEXT,
    description TEXT,
    service TEXT
)
LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-gosipenkov/data_lake/ods/issue/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

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
