libname wrcomiss "G:\Ö¤È¯Êý¾Ý\warrant_comissionfee_calc\";

data wrcomiss.warrant_date_1;
    set warrant.warrant_date;
    cost = 5;
    if amount > 0 then
        sum = amount * price;
    else
        sum = -amount * price;
run;

/* proc sql;
    create table wrcomiss.sumAndCost_1 as
        select distinct securitycode, date, fundacctnum, sum(sum) as totalSum, sum(cost) as totalCost
            from wrcomiss.warrant_date_1
        group by fundacctnum, securitycode, Date
        order by fundacctnum, securitycode, Date;
quit; */

/* avg directly across fundacctnums, regardless of warrants and dates  */
proc sql;
    create table wrcomiss.sumAndCost_2 as
        select distinct fundacctnum, mean(sum) as avgSize, mean(cost) as avgCost
            from wrcomiss.warrant_date_1
        group by fundacctnum
        order by fundacctnum;
quit;

/* avg across fundacctnums for each warrant */
proc sql;
    create table wrcomiss.sumAndCost_3 as
        select distinct fundacctnum, securitycode, mean(sum) as avgSize, mean(cost) as avgCost
            from wrcomiss.warrant_date_1
        group by securitycode, fundacctnum
        order by securitycode, fundacctnum;
quit;

proc univariate data=wrcomiss.sumAndCost_2 noprint;
    var avgSize;
    output out=wrcomiss.percentiles_2 p5=p5 p25=p25 median=median p75=p75 p95=p95 mean=mean;
run;

data wrcomiss.percentiles_2;
    set wrcomiss.percentiles_2;
	p5ratio = 5 / p5;
	p25ratio = 5 / p25;
	medianratio = 5 / median;
	p75ratio = 5 / p75;
	p95ratio = 5 / p95;
    meanratio = 5 / mean;
run;

proc univariate data=wrcomiss.sumAndCost_3 noprint;
    by securitycode;
    var avgSize;
    output out=wrcomiss.percentiles_3 p5=p5 p25=p25 median=median p75=p75 p95=p95 mean=mean;
run;

data wrcomiss.percentiles_3;
    set wrcomiss.percentiles_3;
	p5ratio = 5 / p5;
	p25ratio = 5 / p25;
	medianratio = 5 / median;
	p75ratio = 5 / p75;
	p95ratio = 5 / p95;
    meanratio = 5 / mean;
run;

/* proc sql;
    create table wrcomiss.sumAndCost_2 as
        select distinct a.securitycode, a.date, b.totalSum, b.totalCost
            from warrant.warranttrans a left join wrcomiss.sumAndCost_1 b
                on a.securitycode = b.securitycode and a.date = b.date
        order by securitycode, date;
quit;

data wrcomiss.sumAndCost;
    set wrcomiss.sumAndCost_2;
    ratio = totalCost / totalSum;
run;

symbol i=join;
proc gplot data=wrcomiss.sumAndCost;
	by securitycode;
	plot ratio*date / overlay legend;
run;
quit; */
