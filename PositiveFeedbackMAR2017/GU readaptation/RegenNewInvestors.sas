/* Double check how expand_new is generated. */
/* ZHOU Kunpeng, 16 Jul 2018 */

/* NOTE the result has issues: psudocycle1 is incorrect. Some values are missing.
   Use the original data instead. */

libname newinvs "G:\Ö¤È¯Êý¾Ý\warrant_newinvestor_regen";

/* 1. Generate expanded data (copied from Shi's code) */
/* ========================================================================*/
proc sort data=warrant.warrant_date out=newinvs.warrant_date;
    by fundacctnum securitycode mark date time;
run;

/* Get cycles */
data newinvs.warrant_date;
    set newinvs.warrant_date;
    lagfund = lag(fundacctnum);
    lagsecurity = lag(securitycode);
    lagmark = lag(mark);
run;

data newinvs.warrant_date;
    set newinvs.warrant_date;
    if lagfund ^= fundacctnum or lagsecurity ^= securitycode then cycle = 0;
    if lagmark ^= mark then cycle + 1;
run;

/* Cycle start and cycle end. This will be replaced by psudocycles after. */

data newinvs.warrant_start;
    set newinvs.warrant_date;
    by fundacctnum securitycode cycle date time;
    if first.cycle;
    rename date = start;
run;

data newinvs.warrant_end;
    set newinvs.warrant_date;
    by fundacctnum securitycode cycle date time;
    if last.cycle;
    rename date = end;
run;

data newinvs.warrant_cycle;
    merge newinvs.warrant_start newinvs.warrant_end;
    by fundacctnum securitycode cycle;
    lagstart = lag(start);
    lagend = lag(end);
    lagfund = lag(fundacctnum);
    lagsecurity = lag(securitycode);
    format lagend date.;
run;

/* Generate psudocycle as merging consecutive two cycles
    if cycle1.start = cycle1.end = cycle2.start = cycle2.end. */

data newinvs.warrant_cycle;
    set newinvs.warrant_cycle;
    if lagfund ^= fundacctnum or lagsecurity ^= securitycode then
        psudocycle = 0;
    if lagfund ^= fundacctnum or lagsecurity ^= securitycode or
       lagstart ^= lagend or start ^= end or start ^= lagend then
        psudocycle + 1;
run;

/* Get pseudocycle */

proc sql;
create table newinvs.warrant_date as
    select distinct a.*, b.psudocycle
        from newinvs.warrant_date a left join newinvs.warrant_cycle b
            on a.mark = b.mark;
quit;

/* Descending amount is to ensure the order when there're multiple transactions at the same TIME. */

proc sort data = newinvs.warrant_date out = newinvs.warrant_date;
    by fundacctnum securitycode mark date time descending amount;
run;

/* Calc return for each psudocycle */
data newinvs.warrant_date;
    set newinvs.warrant_date;
    by fundacctnum securitycode psudocycle date time;
    if first.psudocycle then invest=0;
    if amount>0 then invest+amount*price;
    if first.psudocycle then return=0;
    if amount<0 then return+amount*price;
    returnrate=-1*return/invest-1;
run;

/* Generate newinvs.warrant_psudocycle. */

data newinvs.warrant_psudocycle;
    set newinvs.warrant_date;
    by fundacctnum securitycode psudocycle date time;
    if last.psudocycle;
    keep branch fundacctnum securitycode psudocycle returnrate;
    rename returnrate=return;
run;

/* Grab start and end for psudocycles in newinvs.warrant_psudocycle. */
data newinvs.warrant_start;
    set newinvs.warrant_date;
    by fundacctnum securitycode psudocycle date time;
    if first.psudocycle;
    keep fundacctnum securitycode psudocycle date;
    rename date=start;
run;

data newinvs.warrant_end;
    set newinvs.warrant_date;
    by fundacctnum securitycode psudocycle date time;
    if last.psudocycle;
    keep fundacctnum securitycode psudocycle date;
    rename date=end;
run;

data newinvs.warrant_psudocycle;
    merge newinvs.warrant_psudocycle newinvs.warrant_start newinvs.warrant_end;
    by fundacctnum securitycode psudocycle;
run;

/* drop return and those temporary lags again - it's already stored in newinvs.warrant_psudocycle. */
data newinvs.warrant_date;
    set newinvs.warrant_date;
    drop invest return returnrate lagfund lagsecurity lagmark;
run;




/* Format Securitycode in warranttrans to character */
data newinvs.warranttrans;
    set warrant.warranttrans;
    a = put(securitycode, 6.);
    drop securitycode;
    rename a = securitycode;
run;

/* Grab maturity n_day (DO NOT USE WARRANTTRANS_1 !)
   n_day = count(all subsequent days) + 1, and also counts 29 Feb 2008.*/
proc sort data = newinvs.warranttrans;
    by Securitycode date;
run;
proc sql;
    create table newinvs.warranttrans as
        select distinct a.*, count(distinct b.Date) + 1 as n_day
            from newinvs.warranttrans a left join newinvs.warranttrans b
                on a.Securitycode = b.securityCode and
                   a.Date < b.Date
            group by a.Securitycode, a.Date;
quit;

/* Expand to Cartesian product */

proc sql;
    create table newinvs.warrant_expand as
        select distinct b.fundacctnum,b.securitycode,a.date
            from newinvs.warranttrans a,
                (select distinct fundacctnum, securitycode from newinvs.warrant_date) b
            where a.securitycode = b.securitycode;
quit;

/* Left join newinvs.warrant_psudocycle. See all starts of Psudocycles, and create a dummy for psudo cycle starts. */
proc sql;
    create table newinvs.warrant_expand as
        select distinct a.*, b.start
            from newinvs.warrant_expand a left join newinvs.warrant_psudocycle b
                on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date=b.start;
quit;

/* Rewrite start as dummy  */
data newinvs.warrant_expand;
    set newinvs.warrant_expand;
    if start=. then start=0;
    else start=1;
    format start best.;
run;


/* Psudocycle1 as the last psudocycle previously.
   For a day within one psudocycle, the first day keeps the last psudocycle and the following days keep the present one. */
proc sql;
    create table newinvs.warrant_expand1 as
        select distinct a.*, max(b.psudocycle) as psudocycle1
            from newinvs.warrant_expand a left join newinvs.warrant_psudocycle b
                on a.fundacctnum=b.fundacctnum and
                   a.securitycode=b.securitycode and
                   a.date > b.end
            group by a.fundacctnum,a.securitycode,a.date;
quit;

/* According to psudocycle1, get return from the last psudocycle previously as lag1_return. */
proc sql;
    create table newinvs.warrant_expand1 as
        select distinct a.*, b.return as lag1_return
        from newinvs.warrant_expand1 a left join newinvs.warrant_psudocycle b
        on a.fundacctnum=b.fundacctnum and
           a.securitycode=b.securitycode and
           a.psudocycle1=b.psudocycle;
quit;

/* Similarly, we have lag2_return for the second previous psudocycle. */
proc sql;
create table newinvs.warrant_expand1 as
    select distinct a.*, max(b.psudocycle)-1 as psudocycle2
        from newinvs.warrant_expand1 a left join newinvs.warrant_psudocycle b
            on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date>b.end
        group by a.fundacctnum,a.securitycode,a.date;
quit;

proc sql;
create table newinvs.warrant_expand1 as
    select distinct a.*,b.return as lag2_return
        from newinvs.warrant_expand1 a left join newinvs.warrant_psudocycle b
            on a.fundacctnum=b.fundacctnum and
               a.securitycode=b.securitycode and
               a.psudocycle2=b.psudocycle;
quit;



/* Create a date lag called lagdate in TRADING date */
data newinvs.warrant_expand1;
    set newinvs.warrant_expand1;
    by fundacctnum securitycode date;
    lagdate=lag(date);
    if first.securitycode then lagdate = .;
    format lagdate date.;
run;


/* Get lagged information via warranttrans. */
proc sql;
    create table newinvs.warrant_expand1 as select distinct a.*,
        b.wrprice as lag1_wrprice, /* wrprice is newinvs.warrant_price */
        b.changepercent as lag1_market_ret,
        b.changeratio as lag1_turnover,
        (1-b.exprice/b.stockprice)/(b.end_date-a.date) as lag1_adjfundamental
    from newinvs.warrant_expand1 a left join newinvs.warranttrans b
    on a.securitycode=b.securitycode and a.lagdate=b.date;
quit;


/* Grab start_date of psudocycle and... */
proc sql;
    create table newinvs.warrant_expand1 as
        select distinct a.*, b.start as start_date
            from newinvs.warrant_expand1 a left join newinvs.warrant_psudocycle b
                on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode
                and a.date>=b.start and a.date<=end;
quit;

/* ...Delete all lines within a pseudocycle that's NOT start_date.  */
data newinvs.warrant_expand1;
    set newinvs.warrant_expand1;
    if start_date^=. and start=0 then delete;
    drop start_date;
run;

/* Delete all redunant rows. */
proc sql;
create table newinvs.warrant_expand1 as select distinct * from newinvs.warrant_expand1;
quit;


/* Grab (begin, end) accordingly.
   begin counts from the first day after a pseudocycle as 0,1,...
   and end = begin + 1. */

data newinvs.warrant_expand1;
    set newinvs.warrant_expand1;
    lagstart=lag(start);
run;


data newinvs.warrant_expand1;
    set newinvs.warrant_expand1;
    by fundacctnum securitycode date;
    if first.securitycode then begin=0;
    else if lagstart=1 then begin=0;
    else begin+1;
    end=begin+1;
run;


/* Generate
   I(lagi_return > 0) */
data newinvs.warrant_expand1;
set newinvs.warrant_expand1;
if lag1_return>0 then D1=1;
else D1=0;
if lag2_return>0 then D2=1;
else D2=0;
run;


/* 2. Get psudocycle_expand to generate hold/last/other dummies.  */
/* ========================================================================== */

/* Expand to Cartesian product, but for those dates within a psudocycle. */
proc sql;
    create table newinvs.warrant_psudocycle_expand_0 as
        select distinct a.*, b.date, b.wrprice
            from newinvs.warrant_psudocycle a left join newinvs.warranttrans b
                on a.securitycode = b.securitycode and
                   b.date >= a.start and b.date <= a.end
            order by fundacctnum, securitycode, psudocycle, date;
quit;

/* Generate obs number. */
data newinvs.warrant_date;
    set newinvs.warrant_date;
    obs = _n_;
run;

/* Get amount, price, obs from warrant_date. */
proc sql;
    create table newinvs.warrant_psudocycle_expand_1 as
        select distinct a.*, b.amount, b.price, b.obs
            from newinvs.warrant_psudocycle_expand_0 a left join newinvs.warrant_date b
                on a.fundacctnum = b.fundacctnum and a.securitycode = b.securitycode
                and a.psudocycle = b.psudocycle and a.date = b.date
            order by fundacctnum, securitycode, psudocycle, date, obs;
quit;

/* then, generate position, invest/return, papervalue and return rate.  */
data newinvs.warrant_psudocycle_expand_2;
    set newinvs.warrant_psudocycle_expand_1;
    by fundacctnum securitycode psudocycle;

    if first.psudocycle then position = 0;
    position + amount;

    if first.psudocycle then invest = 0;
    if amount > 0 then invest + amount * price;

    if first.psudocycle then return = 0;
    if amount < 0 then return + (-1) * amount * price;

    papervalue = position * wrprice;
    returnrate = (papervalue + return) / invest - 1;
run;

/* warrant_psudocycle_date as last transaction within a date.
   (So, all variables are the 'closing' values for a day. )
   ind_close : 1 if it is close of a pseudo */
proc sort data=newinvs.warrant_psudocycle_expand_2 out=newinvs.warrant_psudocycle_date;
    by fundacctnum securitycode psudocycle date obs;
run;

data newinvs.warrant_psudocycle_date;
    set newinvs.warrant_psudocycle_date;
    by fundacctnum securitycode psudocycle date obs;
    if last.date;
run;

data newinvs.warrant_psudocycle_date;
    set newinvs.warrant_psudocycle_date;
    if date = end then ind_close=1;
    if date ^= end then ind_close=0;
run;

/* 3. Generate hold/last/other dummies.  */
/* ========================================================================== */

/* hold_num is the other warrants that the investor is holding.
   hold_return is the accumulative return within psudocycle of those warrants. */
proc sql;
    create table newinvs.warrant_expand1_adj as
        select distinct a.*, count(b.date) as hold_num,
               (sum(b.return) + sum(b.papervalue)) / sum(b.invest) - 1 as hold_return
               from newinvs.warrant_expand1 a left join newinvs.warrant_psudocycle_date b
                    on a.fundacctnum = b.fundacctnum
                       and a.securitycode ^= b.securitycode
                       and a.date = b.date
                       and b.ind_close = 0
        group by a.fundacctnum,a.securitycode,a.date
        order by a.fundacctnum,a.securitycode,a.date;
quit;

/* psudocycle return is the final return of a psudocycle. */
data newinvs.warrant_psudocycle_return;
    set newinvs.warrant_psudocycle_date;
    if date=end;
run;

/* ind_date: psudocycle ended within the same date as next one
   (there is a 'descending psudocycle' when sorting. )*/
proc sort data=newinvs.warrant_psudocycle_return;
    by fundacctnum securitycode descending psudocycle;
run;
data newinvs.warrant_psudocycle_return;
    set newinvs.warrant_psudocycle_return;
    if (lag(fundacctnum) = fundacctnum and lag(securitycode) = securitycode
       and lag(end) = end) then
       ind_date = 1;
    else ind_date = 0;
run;


/* temp is the one that previously generated hold_return and so.  */
/* last_end: the most recent date of a psudocycle end (except for this warrant). */
proc sql;
create table newinvs.warrant_expand1_adj as
    select distinct a.*, max(b.end) as lastend
        from newinvs.warrant_expand1_adj a left join newinvs.warrant_psudocycle_return b
            on a.fundacctnum = b.fundacctnum and a.securitycode ^= b.securitycode
               and a.date > b.end
        group by a.fundacctnum, a.securitycode, a.date
        order by a.fundacctnum, a.securitycode, a.date;
quit;

/* Grab last_return accordingly.
   NOTE This calculation method may involve multiple pseudocycles for different warrants
        if they end on the same day.
        Choose not to change the formula to keep it aligned with one/two cycle regressions for now.
        */
proc sql;
create table newinvs.warrant_expand1_adj as
    select distinct a.*, count(b.date) as last_num,
           (sum(b.return) + sum(b.papervalue))/sum(b.invest) - 1 as last_return
        from newinvs.warrant_expand1_adj a left join newinvs.warrant_psudocycle_return b
            on a.fundacctnum = b.fundacctnum and a.securitycode ^= b.securitycode
            and a.lastend = b.date and b.ind_date = 0
        group by a.fundacctnum, a.securitycode, a.date
        order by a.fundacctnum, a.securitycode, a.date;
quit;

/* Other_num and other_return as count and return from
   all other pseudocycles except for the most recent one
   (Only OTHER warrants count). */
proc sql;
create table newinvs.warrant_expand1_adj as
    select distinct a.*, count(b.date) as other_num,
           (sum(b.return)+sum(b.papervalue))/sum(b.invest)-1 as other_return
        from newinvs.warrant_expand1_adj a left join newinvs.warrant_psudocycle_return b
            on a.fundacctnum = b.fundacctnum and a.securitycode ^= b.securitycode and
            (a.lastend > b.date or (a.lastend = b.date and b.ind_date = 1))
        group by a.fundacctnum, a.securitycode, a.date
        order by a.fundacctnum, a.securitycode, a.date;
quit;

/* XXX_num converted to dummies: 1 if no such records before.
   For them, returns are zero. */
data newinvs.warrant_expand1_adj;
    set temp2;

    if hold_num>=1 then hold_dummy=0;
    else hold_dummy = 1;
    if last_num>=1 then last_dummy=0;
    else last_dummy = 1;
    if other_num>=1 then other_dummy=0;
    else other_dummy = 1;

    if hold_return > 0 then isHoldReturnPositive = 1;
    else isHoldReturnPositive = 0;
    if last_return > 0 then isLastReturnPositive = 1;
    else isLastReturnPositive = 0;
    if other_return > 0 then isOtherReturnPositive = 1;
    else isOtherReturnPositive = 0;

    if hold_return=. then hold_return=0;
    if last_return=. then last_return=0;
    if other_return=. then other_return=0;
run;


/* 4. Generate market return and turnover lags (same code from RegenOnecycleAndTwo.sas) */
/* ===================================================================== */

proc sort data=newinvs.warranttrans;
    by securitycode date;
run;

data newinvs.warranttrans_1a;
    set newinvs.warranttrans;
    by securitycode date;
    if first.securitycode then
        n = 0;
    n + 1;
run;

/* mktrpx as return from xth previous trading day */
data newinvs.warranttrans_1b;
    set newinvs.warranttrans_1a;
    mktrp4 = wrprice / lag4(wrprice) - 1;
    mktrp15 = wrprice / lag15(wrprice) - 1;
    if n<=4 then mktrp4=.;
    if n<=15 then mktrp15=.;
run;

/* mktrp1 as mktrp4 of the-day-before-yesterday (in trading day)
   mktrp2 as mktrp15 of lag6
   (so, it is return of 1day / 2day, 2day / 6day, 6day / 21day,
    which is daily, weekly, and monthly.) */
data newinvs.warranttrans_1c;
    set newinvs.warranttrans_1b;
    mktrp1=lag2(mktrp4);
    mktrp2=lag6(mktrp15);
    if n<=2 then mktrp1=.;
    if n<=6 then mktrp2=.;
run;

data newinvs.warranttrans_1;
    set newinvs.warranttrans_1c;
run;

/* NOTE changeratio is turnover.
   Generate average turnover in [-2, -5], [-6, -21] lags (in trading day - n rather than date). */
proc sql;
    create table newinvs.warranttrans_2a as
        select distinct a.*, mean(b.changeratio) as turnoverLagWeek
            from newinvs.warranttrans_1 a left join newinvs.warranttrans_1 b
                on a.securityCode = b.securitycode
                and b.n <= (a.n - 2) and b.n >= (a.n - 5)
            group by a.securitycode, a.date
            order by a.securitycode, a.date;
    create table newinvs.warranttrans_2b as
        select distinct a.*, mean(b.changeratio) as turnoverLagMonth
            from newinvs.warranttrans_2a a left join newinvs.warranttrans_1 b
                on a.securityCode = b.securityCode
                and b.n <= (a.n - 6) and b.n >= (a.n - 21)
            group by a.securitycode, a.date
            order by a.securitycode, a.date;
quit;

data newinvs.warranttrans_2;
    set newinvs.warranttrans_2b;
    if n<=2 then turnoverLagWeek = .;
    if n<=6 then turnoverLagMonth = .;
    keep securitycode date mktrp1 mktrp2 turnoverLagWeek turnoverLagMonth;
run;


/* Merge the generated variables into expand1_adj. */
proc sql;
create table newinvs.warrant_expand1_adj as
    select distinct x.*, y.*
        from newinvs.warrant_expand1_adj x left join newinvs.warranttrans_2 y
            on x.securitycode=y.securitycode and x.date=y.date
        order by fundacctnum,securitycode,date;
quit;


/* 5. Split */
/* ========================================================================= */

data newinvs.warrant_expand_newbee_adj;
    set newinvs.warrant_expand1_adj;
    if lag1_return = . and
    (hold_dummy = 1 and last_dummy = 1 and other_dummy = 1);
run;
data newinvs.warrant_expand_newinvs_adj;
    set newinvs.warrant_expand1_adj;
    if lag1_return = . and
    (hold_dummy = 0 or last_dummy = 0 or other_dummy = 0);
run;
data newinvs.warrant_expand_1cycle_adj;
    set newinvs.warrant_expand1_adj;
    if lag2_return = . and lag1_return ^= .;
run;
data newinvs.warrant_expand_2cycle_adj;
    set newinvs.warrant_expand1_adj;
    if lag2_return ^= . ;
run;
