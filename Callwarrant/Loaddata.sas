libname wrcall "G:\证券数据\wrcall";

proc import datafile="G:\证券数据\wrcall\warrantinfo.csv"
    out=wrcall.basicInfo
    dbms=csv;
run;

proc import datafile="G:\证券数据\wrcall\warrant_dailyInfo.csv"
    out=wrcall.dailyInfo
    dbms=csv;
run;

/* Grab exprice, exrate, security type (call/put), last trading date from basicInfo */
proc sql;
    create table wrcall.allDailyInfo as
        select distinct a.exprice, a.CpType, a.exRate, a.LastTrdDt, b.*
            from wrcall.dailyInfo b left join wrcall.basicInfo a
                on a.securitycode = b.securitycode
            order by securitycode, date;
quit;

/* Drop non-trading days, put warrants, and unidentified warrant code */
data wrcall.callDailyInfo;
    set wrcall.allDailyInfo;
    if CpType = 1 and securitycode ^= . and DtState = 1;
    drop CpType DtState;
run;

/* Generate:
   return;
   intrinsic value as difference of stock price and strike price;
. */
data wrcall.callDailyInfo_1;
    set wrcall.callDailyInfo;
    dailyReturn = price / lag(price) - 1;
    differenceValue = stockprice - exprice;
run;

/* Plot */
symbol i=join;
proc gplot data=wrcall.callDailyInfo;
	by securitycode;
	plot dailyReturn*date / overlay legend;
	plot price*date differenceValue*date / overlay legend;
run;
quit;
