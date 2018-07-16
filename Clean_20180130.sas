/* Last Edited 2018-06-30 RECENT. */
/* This code is to generate expand1_x_adj dataset,
which contains hold_return, last_return, other_return, hold_num, last_num, other_num
and sort of things like that.
As for 2018-7-13, looks like it is decided to use as regression.  */

/* Also, looks like it contains a full set of preprocessing! */

proc sort data=warrant.warrant_date out=warrant_date;
    by fundacctnum securitycode mark date time;
run;

/* Get cycles */
data warrant_date;
    set warrant_date;
    lagfund = lag(fundacctnum);
    lagsecurity = lag(securitycode);
    lagmark = lag(mark);
run;

data warrant_date;
    set warrant_date;
    if lagfund ^= fundacctnum or lagsecurity ^= securitycode then cycle = 0;
    if lagmark ^= mark then cycle + 1;
run;

/* Cycle start and cycle end. This will be replaced by psudocycles after. */

data warrant_start;
    set warrant_date;
    by fundacctnum securitycode cycle date time;
    if first.cycle;
    rename date = start;
run;

data warrant_end;
    set warrant_date;
    by fundacctnum securitycode cycle date time;
    if last.cycle;
    rename date = end;
run;

data warrant_cycle;
    merge warrant_start warrant_end;
    by fundacctnum securitycode cycle;
    lagstart = lag(start);
    lagend = lag(end);
    lagfund = lag(fundacctnum);
    lagsecurity = lag(securitycode);
    format lagend date.;
run;

/* Generate psudocycle as merging consecutive two cycles
    if cycle1.start = cycle1.end = cycle2.start = cycle2.end. */

data warrant_cycle;
    set warrant_cycle;
    if lagfund ^= fundacctnum or lagsecurity ^= securitycode then
        psudocycle = 0;
    if lagfund ^= fundacctnum or lagsecurity ^= securitycode or
       lagstart ^= lagend or start ^= end or start ^= lagend then
        psudocycle + 1;
run;

/* Mark?
   It is the same in Clean_New.sas though. */

proc sql;
create table warrant_date as
    select distinct a.*, b.psudocycle
        from warrant_date a left join warrant_cycle b
            on a.mark = b.mark;
quit;

/* Descending amount is to ensure the order when there're multiple transactions at the same TIME. */

proc sort data = warrant_date out = warrant_date;
    by fundacctnum securitycode mark date time descending amount;
run;


/* Now, re-generate datasets for psudocycles, rather than cycles.
   Warrant_start and warrant_end will be replaced accordingly.
   Note that they are both for ALL transactions,
     so it doesnt matter whether it is _new, 1_1, 1_2 or any others.*/

/* Here I think first or last does not matter. All vars kept are only relevant to psudocycle code.  */
data warrant_psudocycle;
    set warrant_date;
    by fundacctnum securitycode psudocycle date time;
    if last.psudocycle;
    keep branch fundacctnum securitycode psudocycle;
run;

data warrant_start;
    set warrant_date;
    by fundacctnum securitycode psudocycle date time;
    if first.psudocycle;
    keep fundacctnum securitycode psudocycle date;
    rename date = start;
run;

data warrant_end;
    set warrant_date;
    by fundacctnum securitycode psudocycle date time;
    if last.psudocycle;
    keep fundacctnum securitycode psudocycle date;
    rename date = end;
run;

data warrant_psudocycle;
    merge warrant_psudocycle warrant_start warrant_end;
    by fundacctnum securitycode psudocycle;
run;



/* Expand to Cartesian product, but for those dates within a psudocycle. */
proc sql;
    create table warrant_psudocycle_expand as
        select distinct a.*, b.date
            from warrant_psudocycle a left join warrant.warranttrans b
                on a.securitycode = b.securitycode and
                   b.date >= a.start and b.date <= a.end
            order by fundacctnum, securitycode, psudocycle, date;
quit;

/* Generate obs number. */
data warrant_date;
    set warrant_date;
    obs = _n_;
run;

/* warrant_psudocycle_expand: Get amount, price, obs from warrant_date. */
proc sql;
    create table warrant_psudocycle_expand as
        select distinct a.*, b.amount, b.price, b.obs
            from warrant_psudocycle_expand a left join warrant_date b
                on a.fundacctnum = b.fundacctnum and a.securitycode = b.securitycode
                and a.psudocycle = b.psudocycle and a.date = b.date
            order by fundacctnum, securitycode, psudocycle, date, obs;
quit;

/* then, generate position, invest/return.  */
data warrant_psudocycle_expand;
    set warrant_psudocycle_expand;
    by fundacctnum securitycode psudocycle;
    if first.psudocycle then position = 0;
    position + amount;
    if first.psudocycle then invest = 0;
    if amount > 0 then invest + amount * price;
    if first.psudocycle then return = 0;
    if amount < 0 then return + (-1) * amount * price;
run;

/* Grab wrprice again.
   (......) */
proc sql;
create table warrant_psudocycle_expand as
    select distinct a.*, b.wrprice
        from warrant_psudocycle_expand a left join warrant.warranttrans b
            on a.securitycode = b.securitycode and a.date = b.date
        order by fundacctnum, securitycode, psudocycle, date, obs;
quit;

/* Calc papervalue and returnrate. */
data warrant_psudocycle_expand;
    set warrant_psudocycle_expand;
    papervalue = position * wrprice;
    returnrate = (papervalue + return) / invest - 1;
run;

/* warrant_psudocycle_date as last transaction within a date.
   (So, all variables are the 'closing' values for a day. ) */
proc sort data=warrant_psudocycle_expand out=warrant_psudocycle_date;
    by fundacctnum securitycode psudocycle date obs;
run;

data warrant_psudocycle_date;
    set warrant_psudocycle_date;
    by fundacctnum securitycode psudocycle date obs;
    if last.date;
run;

data warrant_psudocycle_date;
    set warrant_psudocycle_date;
    format a best12.;
    a = securitycode;
    drop securitycode;
    rename a = securitycode;
run;

/* ind_close = (date == (psudocycle) end) */
data warrant_psudocycle_date;
    set warrant_psudocycle_date;
    if date = end then ind_close=1;
    if date ^= end then ind_close=0;
run;




/* Directly get expand1_1. */
data warrant_expand1_1;
    set warrant.warrant_expand1_1;
    drop maturity lagdate lag1_wrprice lag2date lag3date
         lag1_bs lag2_bs lag3_bs lag1_fundamental lag2_fundamental lag3_fundamental
         lag3_market_ret lag3_turnover lag1_large_ret lag2_large_ret lag3_adjfundamental;
run;

/* hold_num is the other warrants that the investor is holding.
   hold_return is the accumulative return within psudocycle of those warrants. */
proc sql;
    create table temp as
        select distinct a.*, count(b.date) as hold_num,
               (sum(b.return)+sum(b.papervalue))/sum(b.invest)-1 as hold_return
               from warrant_expand1_1 a left join warrant_psudocycle_date b
                    on a.fundacctnum = b.fundacctnum and a.securitycode ^= b.securitycode
                       and a.date = b.date
                       and b.ind_close = 0
        group by a.fundacctnum,a.securitycode,a.date
        order by a.fundacctnum,a.securitycode,a.date;
quit;

/* psudocycle return is the final return of a psudocycle. */
data warrant_psudocycle_return;
set warrant_psudocycle_date;
if date=end;
run;

/* ind_date: psudocycle ended within the same date as next one
   (there is a 'descending psudocycle' when sorting. )*/
proc sort data=warrant_psudocycle_return;
    by fundacctnum securitycode descending psudocycle;
run;
data warrant_psudocycle_return;
    set warrant_psudocycle_return;
    if (lag(fundacctnum) = fundacctnum and lag(securitycode) = securitycode
       and lag(end) = end) then
       ind_date = 1;
run;

data warrant_psudocycle_return;
    set warrant_psudocycle_return;
    if ind_date = . then ind_date = 0;
run;

/* temp is the one that previously generated hold_return and so.  */
/* last_end: the most recent date of a psudocycle end (except for THIS warrant). */
proc sql;
create table temp1 as
    select distinct a.*, max(b.end) as lastend
        from temp a left join warrant_psudocycle_return b
            on a.fundacctnum = b.fundacctnum and a.securitycode ^= b.securitycode
               and a.date > b.end
        group by a.fundacctnum, a.securitycode, a.date
        order by a.fundacctnum, a.securitycode, a.date;
quit;

/* Grab last_return accordingly. */
proc sql;
create table temp1 as
    select distinct a.*, count(b.date) as last_num,
           (sum(b.return) + sum(b.papervalue))/sum(b.invest) - 1 as last_return
        from temp1 a left join warrant_psudocycle_return b
            on a.fundacctnum = b.fundacctnum and a.securitycode ^= b.securitycode
            and a.lastend = b.date and b.ind_date = 0
        group by a.fundacctnum, a.securitycode, a.date
        order by a.fundacctnum, a.securitycode, a.date;
quit;

/* Other_num and other_return as count and return from
   all other pseudocycles except for the most recent one
   (Only OTHER warrants count). */
proc sql;
create table temp2 as
    select distinct a.*, count(b.date) as other_num,
           (sum(b.return)+sum(b.papervalue))/sum(b.invest)-1 as other_return
        from temp1 a left join warrant_psudocycle_return b
            on a.fundacctnum = b.fundacctnum and a.securitycode ^= b.securitycode and
            (a.lastend > b.date or (a.lastend = b.date and b.ind_date = 1))
        group by a.fundacctnum, a.securitycode, a.date
        order by a.fundacctnum, a.securitycode, a.date;
quit;

/* XXX_num as dummies.
   All NA values set as zero (no such trading records before). */
data warrant_expand1_1_adj;
    set temp2;
    if hold_num>1 then hold_num=1;
    if last_num>1 then last_num=1;
    if other_num>1 then other_num=1;
    if hold_return=. then hold_return=0;
    if last_return=. then last_return=0;
    if other_return=. then other_return=0;
run;

/* Count dates as n = 1,2,... for each security. */
proc sort data=warrant.warranttrans_1 out=warranttrans_1;
    by securitycode date;
run;
data warranttrans_1;
    set warranttrans_1;
    by securitycode date;
    if first.securitycode then
        n = 0;
    n + 1;
run;

/* mktrpx as return from xth previous trading day */
data warranttrans_1;
set warranttrans_1;
    mktrp4 = wrprice / lag4(wrprice) - 1;
    mktrp15 = wrprice / lag15(wrprice) - 1;
run;
/* NA set for meaningless returns */
data warranttrans_1;
    set warranttrans_1;
    if n<=4 then mktrp4=.;
    if n<=15 then mktrp15=.;
run;

/* mktrp1 as mktrp4 of the-day-before-yesterday (in trading day)
   mktrp2 as mktrp15 of lag6
   (...reason for that?) */
data warranttrans_1;
    set warranttrans_1;
    mktrp1=lag2(mktrp4);
    mktrp2=lag6(mktrp15);
run;
/* NA set for meaningless returns */
data warranttrans_1;
    set warranttrans_1;
    if n<=2 then mktrp1=.;
    if n<=6 then mktrp2=.;
run;

/* Merge the returns into warrant_expand1_1 as _adj. */
proc sql;
create table warrant_expand1_1_adj as
    select distinct a.*, b.mktrp1 as mktrp1, b.mktrp2 as mktrp2
        from warrant_expand1_1_adj a left join warranttrans_1 b
            on a.securitycode=b.securitycode and a.date=b.date
        order by fundacctnum,securitycode,date;
quit;
/* ...and output that. */
data warrant.warrant_expand1_1_adj;
    set warrant_expand1_1_adj;
run;

/* Things are similar for 1_2.  */
data warrant_expand1_2;
set warrant.warrant_expand1_2;
drop maturity lagdate lag1_wrprice lag2date lag3date lag1_bs lag2_bs lag3_bs lag1_fundamental lag2_fundamental lag3_fundamental lag3_market_ret lag3_turnover lag1_large_ret lag2_large_ret lag3_adjfundamental;
run;

proc sql;
create table temp as select distinct a.*, count(b.date) as hold_num, (sum(b.return)+sum(b.papervalue))/sum(b.invest)-1 as hold_return
from warrant_expand1_2 a left join warrant_psudocycle_date b
on a.fundacctnum=b.fundacctnum and a.securitycode^=b.securitycode and a.date=b.date and b.ind_close=0
group by a.fundacctnum,a.securitycode,a.date
order by a.fundacctnum,a.securitycode,a.date;
quit;

proc sql;
create table temp1 as select distinct a.*, max(b.end) as lastend
from temp a left join warrant_psudocycle_return b
on a.fundacctnum=b.fundacctnum and a.securitycode^=b.securitycode and a.date>b.end
group by a.fundacctnum,a.securitycode,a.date
order by a.fundacctnum,a.securitycode,a.date;
quit;

proc sql;
create table temp1 as select distinct a.*, count(b.date) as last_num, (sum(b.return)+sum(b.papervalue))/sum(b.invest)-1 as last_return
from temp1 a left join warrant_psudocycle_return b
on a.fundacctnum=b.fundacctnum and a.securitycode^=b.securitycode and a.lastend=b.date and b.ind_date=0
group by a.fundacctnum,a.securitycode,a.date
order by a.fundacctnum,a.securitycode,a.date;
quit;

proc sql;
create table temp2 as select distinct a.*, count(b.date) as other_num, (sum(b.return)+sum(b.papervalue))/sum(b.invest)-1 as other_return
from temp1 a left join warrant_psudocycle_return b
on a.fundacctnum=b.fundacctnum and a.securitycode^=b.securitycode and (a.lastend>b.date or (a.lastend=b.date and b.ind_date=1))
group by a.fundacctnum,a.securitycode,a.date
order by a.fundacctnum,a.securitycode,a.date;
quit;

data warrant_expand1_2_adj;
set temp2;
if hold_num>1 then hold_num=1;
if last_num>1 then last_num=1;
if other_num>1 then other_num=1;
if hold_return=. then hold_return=0;
if last_return=. then last_return=0;
if other_return=. then other_return=0;
run;

proc sql;
create table warrant_expand1_2_adj as select distinct a.*, b.mktrp1 as mktrp1, b.mktrp2 as mktrp2
from warrant_expand1_2_adj a left join warranttrans_1 b
on a.securitycode=b.securitycode and a.date=b.date
order by fundacctnum,securitycode,date;
quit;

data warrant.warrant_expand1_2_adj;
set warrant_expand1_2_adj;
run;


/* REGRESSSSSSSSSSSS! */

proc phreg data=warrant.warrant_expand1_1;
class securitycode;
model (begin,end)*start(0) = lag1_return  D1   lag1_market_ret  lag1_turnover lag1_adjfundamental
    mktrp1 mktrp2
    hold_num hold_return last_num last_return other_num other_return
    securitycode
        / ties=efron rl;
baseline out=baseline survival=survival cumhaz=cumhaz xbeta=xbeta;
run;
