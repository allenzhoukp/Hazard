libname fulldata "G:\Ö¤È¯Êý¾Ý\full_data";

/* First run Clean_New.sas by ZHAO.  */

data fulldata.fulldata;
set Network802  Network803  Network804  Network805  Network806  Network807  Network808  Network809  Network810  Network811  Network812  Network813  Network815  Network816  Network817  Network818  Network819  Network820  Network821  Network822  Network823  Network824  Network825  Network826  Network827  Network828  Network829  Network831  Network832  Network833  Network834  Network835  Network836  Network837  Network838  Network850  Network851  Network852  Network853  Network855  Network856  Network858  Network859  Network862  Network868  Network878  Network879;
run;

proc sql;
    create table fulldata.securities as
        select distinct SecurityCode from fulldata.fulldata;
quit;


/* calc average WARRANT trading volume among investors */
data fulldata.avgwarrant_1;
    set warrant.warrant_date;
    absTradeSize = amount * price;
    if absTradeSize < 0 then
        absTradeSize = -absTradeSize;
    if amount > 0 then
        absAmount = amount;
    else
        absAmount = -amount;
run;

proc sort data=fulldata.avgwarrant_1;
    by fundacctnum date time;
run;

proc sql;
    create table fulldata.avgwarrant_2 as
        select distinct date, fundacctnum, sum(absTradeSize) as dailyTradeSize, sum(absAmount) as dailyTradeVol
            from fulldata.avgwarrant_1
        group by date, fundacctnum
        order by date, fundacctnum;
quit;

proc sql;
    create table fulldata.avgwarrant_3 as
        select distinct date, mean(dailyTradeSize) as avgTradeSize, mean(dailyTradeVol) as avgTradeVol
            from fulldata.avgwarrant_2
        group by date
        order by date;
quit;


/* calc average STOCK trading volume among investors */
data fulldata.avgstock_1;
    set fulldata.fulldata;
    absTradeSize = Settle + Cost;
    if absTradeSize < 0 then
        absTradeSize = -absTradeSize;
    if amount > 0 then
        absAmount = amount;
    else
        absAmount = -amount;
run;

proc sort data=fulldata.avgstock_1;
    by fundacctnum date time;
run;

proc sql;
    create table fulldata.avgstock_2 as
        select distinct date, fundacctnum, sum(absTradeSize) as dailyTradeSize, sum(absAmount) as dailyTradeVol
            from fulldata.avgstock_1
        group by date, fundacctnum
        order by date, fundacctnum;
quit;

/* All investors */
proc sql;
    create table fulldata.avgstock_3 as
        select distinct date, mean(dailyTradeSize) as avgTradeSize, mean(dailyTradeVol) as avgTradeVol
            from fulldata.avgstock_2
        group by date
        order by date;
quit;

/* Only those who also trades warrants */
proc sql;
    create table fulldata.avgstock_wrnt_1 as
        select distinct * from fulldata.avgstock_2 where
            fundacctnum in (select distinct fundacctnum from warrant.warrant_date)
            order by date, fundacctnum;
quit;
proc sql;
    create table fulldata.avgstock_wrnt_2 as
        select distinct date, mean(dailyTradeSize) as avgTradeSize, mean(dailyTradeVol) as avgTradeVol
            from fulldata.avgstock_wrnt_1
        group by date
        order by date;
quit;

proc sql;
    create table fulldata.cmp as
        select distinct a.date, a.avgTradeSize as avgStockTrdsize, a.avgTradeVol as avgStockTrdvol,
                                b.avgTradeSize as avgWrntTrdsize, b.avgTradeVol as avgWrntTrdvol
            from fulldata.avgstock_3 a left join fulldata.avgwarrant_3 b
                on a.date = b.date
            order by a.date;
    create table fulldata.cmp_onlywrnt as
        select distinct a.date, a.avgTradeSize as avgStockTrdsize, a.avgTradeVol as avgStockTrdvol,
                                b.avgTradeSize as avgWrntTrdsize, b.avgTradeVol as avgWrntTrdvol
            from fulldata.avgstock_wrnt_2 a left join fulldata.avgwarrant_3 b
                on a.date = b.date
            order by a.date;
quit;

data fulldata.cmp;
    set fulldata.cmp;
    if avgWrntTrdsize ^= .;
run;
data fulldata.cmp_onlywrnt;
    set fulldata.cmp_onlywrnt;
    if avgWrntTrdsize ^= .;
run;

symbol i=join;
proc gplot data=fulldata.cmp;
    plot avgStockTrdsize*date avgWrntTrdsize*date / overlay legend;
run;
proc gplot data=fulldata.cmp_onlywrnt;
    plot avgStockTrdsize*date avgWrntTrdsize*date / overlay legend;
run;
quit;
