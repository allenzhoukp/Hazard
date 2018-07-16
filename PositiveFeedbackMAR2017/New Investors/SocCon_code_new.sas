/* Generate psudocycle */

proc sort data=warrant.warrant_date out=warrant_date;
by fundacctnum securitycode mark date time;
run;

data warrant_date;
set warrant_date;
lagfund=lag(fundacctnum);
lagsecurity=lag(securitycode);
lagmark=lag(mark);
run;

data warrant_date;
set warrant_date;
if lagfund^=fundacctnum or lagsecurity^=securitycode then cycle=0;
if lagmark^=mark then cycle+1;
run;

data warrant_start;
set warrant_date;
by fundacctnum securitycode cycle date time;
if first.cycle;
rename date=start;
run;

data warrant_end;
set warrant_date;
by fundacctnum securitycode cycle date time;
if last.cycle;
rename date=end;
run;

data warrant_cycle;
merge warrant_start warrant_end;
by fundacctnum securitycode cycle;
lagstart=lag(start);
lagend=lag(end);
lagfund=lag(fundacctnum);
lagsecurity=lag(securitycode);
format lagend date.;
run;

data warrant_cycle;
set warrant_cycle;
if lagfund^=fundacctnum or lagsecurity^=securitycode then psudocycle=0;
if lagfund^=fundacctnum or lagsecurity^=securitycode or lagstart^=lagend or start^=end or start^=lagend then psudocycle+1;
run;

proc sql;
create table warrant_date as select distinct a.*,b.psudocycle from warrant_date a left join warrant_cycle b
on a.mark=b.mark;
quit;

proc sort data=warrant_date out=warrant_date;
by  fundacctnum securitycode mark date time descending amount;
run;

/* Calculate return */

data warrant_date;
set warrant_date;
by fundacctnum securitycode psudocycle date time;
if first.psudocycle then invest=0;
if amount>0 then invest+amount*price;
if first.psudocycle then return=0;
if amount<0 then return+amount*price;
returnrate=-1*return/invest-1;
run;

/* Re-generate warrant_psudocycle */

data warrant_psudocycle;
set warrant_date;
by fundacctnum securitycode psudocycle date time;
if last.psudocycle;
keep branch fundacctnum securitycode begin_date end_date psudocycle returnrate;
rename returnrate=return;
run;

data warrant_start;
set warrant_date;
by fundacctnum securitycode psudocycle date time;
if first.psudocycle;
keep fundacctnum securitycode psudocycle date;
rename date=start;
run;

data warrant_end;
set warrant_date;
by fundacctnum securitycode psudocycle date time;
if last.psudocycle;
keep fundacctnum securitycode psudocycle date;
rename date=end;
run;

data warrant_psudocycle;
merge warrant_psudocycle warrant_start warrant_end;
by fundacctnum securitycode psudocycle;
run;

/* warrant_date return to its original form */

data warrant_date;
set warrant_date;
drop invest return returnrate lagfund lagsecurity lagmark;
run;


/* expand */
data warranttrans;
set warrant.warranttrans;
keep date securitycode;
run;

proc sql;
create table temp1 as select distinct fundacctnum,securitycode from warrant_date;
quit;

proc sql;
create table warrant_expand as select distinct b.fundacctnum,b.securitycode,a.date from warranttrans a, temp1 b
where a.securitycode=b.securitycode;
quit;

/*Ìí¼Ó*/
/* The first pseudocycle for each investor, each security */

data warrant_psudocycle_1;
set warrant_psudocycle;
if psudocycle=1;
run;

/*½áÊø*/

/* MAIN SELECTION CODE:
   21,417,368 rows ending up. */
proc sql;
create table warrant_expand as select distinct a.* from warrant_expand a, warrant_psudocycle_1 b
where a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date<=b.start
order by fundacctnum,securitycode,date;
quit;

/* Generate start dummy */

data warrant_expand;
set warrant_expand;
by fundacctnum securitycode date;
if last.securitycode then start=1;
run;

data warrant_expand;
set warrant_expand;
if start=. then start=0;
run;

/* lag dates (past trading days) */

data warrant_expand;
set warrant_expand;
by fundacctnum securitycode date;
lagdate=lag(date);
if not first.securitycode;
format lagdate date.;
run;

data warrant_expand;
set warrant_expand;
by fundacctnum securitycode date;
lag2date=lag(lagdate);
if not first.securitycode;
format lagdate date.;
run;

data warrant_expand;
set warrant_expand;
by fundacctnum securitycode date;
lag3date=lag(lag2date);
if not first.securitycode;
format lagdate date.;
run;

/* Variables for lag1date, lag2date, ... */

/* NOTE this part gets rid of the first 3 days. Lower number of obs.  */

proc sql;
create table warrant_expand1 as
    select distinct a.*,b.wrprice as lag1_wrprice,
                        b.changepercent as lag1_market_ret,
                        b.changeratio as lag1_turnover,
                        b.wrprice-b.p2 as lag1_bs,
                        1-b.exprice/b.stockprice as lag1_fundamental
        from warrant_expand a left join warrant.warranttrans b
            on a.securitycode=b.securitycode and a.lagdate=b.date;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,b.changepercent as lag2_market_ret,b.changeratio as lag2_turnover,b.wrprice-b.p2 as lag2_bs,1-b.exprice/b.stockprice as lag2_fundamental from warrant_expand1 a left join warrant.warranttrans b
on a.securitycode=b.securitycode and a.lag2date=b.date;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,b.changepercent as lag3_market_ret,b.changeratio as lag3_turnover,b.wrprice-b.p2 as lag3_bs,1-b.exprice/b.stockprice as lag3_fundamental from warrant_expand1 a left join warrant.warranttrans b
on a.securitycode=b.securitycode and a.lag3date=b.date;
quit;

/* Grab start_date for every row */
proc sql;
create table warrant_expand1 as
    select distinct a.*,b.start as start_date
        from warrant_expand1 a left join warrant_psudocycle b
            on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode
                and a.date>=b.start and a.date<=b.end;
quit;

proc sql;
create table warrant_expand1 as select distinct * from warrant_expand1;
quit;

/* begin, end */

data warrant_expand1;
set warrant_expand1;
by fundacctnum securitycode date;
if first.securitycode then begin=0;
else begin+1;
run;

data warrant_expand1;
set warrant_expand1;
end=begin+1;
run;

data warrant_expand1;
set warrant_expand1;
format b best12.;
b=securitycode;
drop securitycode;
rename b=securitycode;
run;

/* n_day is from warranttrans_1 */

proc sql;
create table warrant_expand1 as select distinct a.*, b.n_day as n_1 from warrant_expand1 a left join
warrant.warranttrans_1 b
on a.securitycode=b.securitycode and a.date=b.date
order by fundacctnum, securitycode, date;
run;

/* Similarly, grab adj_fundametal */

proc sql;
create table warrant_expand1 as select distinct a.*,
    (1-b.exprice/b.stockprice)/b.n_day as lag1_adjfundamental
    from warrant_expand1 a left join warrant.warranttrans_1 b
        on a.securitycode=b.securitycode and a.lagdate=b.date;
quit;



/* Next, grab warrant_psudocycle_expand.
   THese codes must be copied from somewhere else. They just do things once again.  */

/**/

proc sort data=warrant.warrant_date out=warrant_date;
by fundacctnum securitycode mark date time;
run;

data warrant_date;
set warrant_date;
lagfund=lag(fundacctnum);
lagsecurity=lag(securitycode);
lagmark=lag(mark);
run;

data warrant_date;
set warrant_date;
if lagfund^=fundacctnum or lagsecurity^=securitycode then cycle=0;
if lagmark^=mark then cycle+1;
run;

data warrant_start;
set warrant_date;
by fundacctnum securitycode cycle date time;
if first.cycle;
rename date=start;
run;

data warrant_end;
set warrant_date;
by fundacctnum securitycode cycle date time;
if last.cycle;
rename date=end;
run;

data warrant_cycle;
merge warrant_start warrant_end;
by fundacctnum securitycode cycle;
lagstart=lag(start);
lagend=lag(end);
lagfund=lag(fundacctnum);
lagsecurity=lag(securitycode);
format lagend date.;
run;

data warrant_cycle;
set warrant_cycle;
if lagfund^=fundacctnum or lagsecurity^=securitycode then psudocycle=0;
if lagfund^=fundacctnum or lagsecurity^=securitycode or lagstart^=lagend or start^=end or start^=lagend then psudocycle+1;
run;

proc sql;
create table warrant_date as select distinct a.*,b.psudocycle from warrant_date a left join warrant_cycle b
on a.mark=b.mark;
quit;

proc sort data=warrant_date out=warrant_date;
by  fundacctnum securitycode mark date time descending amount;
run;

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
rename date=start;
run;

data warrant_end;
set warrant_date;
by fundacctnum securitycode psudocycle date time;
if last.psudocycle;
keep fundacctnum securitycode psudocycle date;
rename date=end;
run;

data warrant_psudocycle;
merge warrant_psudocycle warrant_start warrant_end;
by fundacctnum securitycode psudocycle;
run;

/* Here: psudocycle_expand */

proc sql;
create table warrant_psudocycle_expand as select distinct a.*, b.date from warrant_psudocycle a left join warrant.warranttrans b
on a.securitycode=b.securitycode and b.date>=a.start and b.date<=a.end
order by fundacctnum, securitycode, psudocycle ,date;
run;

data warrant_date;
set warrant_date;
obs=_n_;
run;

/* Getting return
   The following are mostly identical to Clean_20180130.sas. Just look at that. */

proc sql;
create table warrant_psudocycle_expand as
    select distinct a.*, b.amount, b.price, b.obs
        from warrant_psudocycle_expand a left join warrant_date b
        on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode
           and a.psudocycle=b.psudocycle and a.date=b.date
        order by fundacctnum, securitycode, psudocycle ,date, obs;
run;

data warrant_psudocycle_expand;
set warrant_psudocycle_expand;
by fundacctnum securitycode psudocycle;
if first.psudocycle then position=0;
position+amount;
if first.psudocycle then invest=0;
if amount>0 then invest+amount*price;
if first.psudocycle then return=0;
if amount<0 then return+(-1)*amount*price;
run;

proc sql;
create table warrant_psudocycle_expand as
    select distinct a.*, b.wrprice
    from warrant_psudocycle_expand a left join warrant.warranttrans b
        on a.securitycode=b.securitycode and a.date=b.date
    order by fundacctnum, securitycode, psudocycle ,date, obs;
run;

data warrant_psudocycle_expand;
set warrant_psudocycle_expand;
papervalue=position*wrprice;
returnrate=(papervalue+return)/invest-1;
run;

/* warrant_psudocycle_date keeps last observation within a day  */

proc sort data=warrant_psudocycle_expand out=warrant_psudocycle_date;
by fundacctnum securitycode psudocycle date obs;
run;

/*Holding warrant*/
data warrant_psudocycle_date;
set warrant_psudocycle_date;
by fundacctnum securitycode psudocycle date obs;
if last.date;
run;

data warrant_psudocycle_date;
set warrant_psudocycle_date;
format a best12.;
a=securitycode;
drop securitycode;
rename a=securitycode;
run;

/* ind_close = (date == (psudocycle) end) */
data warrant_psudocycle_date;
set warrant_psudocycle_date;
if date=end then ind_close=1;
if date^=end then ind_close=0;
run;

/* hold_num and hold_return */

proc sql;
create table temp as select distinct a.*,
    count(b.date) as hold_num,
    (sum(b.return)+sum(b.papervalue))/sum(b.invest)-1 as hold_return
from warrant_expand1 a left join warrant_psudocycle_date b
on a.fundacctnum=b.fundacctnum and a.securitycode^=b.securitycode and a.date=b.date and b.ind_close=0
group by a.fundacctnum,a.securitycode,a.date
order by a.fundacctnum,a.securitycode,a.date;
run;


/*most recent cycle for other warrant*/

data warrant_psudocycle_return;
set warrant_psudocycle_date;
if date=end;
run;

proc sort data=warrant_psudocycle_return;
by fundacctnum securitycode descending psudocycle;
run;

data warrant_psudocycle_return;
set warrant_psudocycle_return;
if (lag(fundacctnum)=fundacctnum and lag(securitycode)=securitycode and lag(end)=end) then ind_date=1;
run;

data warrant_psudocycle_return;
set warrant_psudocycle_return;
if ind_date=. then ind_date=0;
run;

proc sql;
create table temp1 as select distinct a.*, max(b.end) as lastend
from temp a left join warrant_psudocycle_return b
on a.fundacctnum=b.fundacctnum and a.securitycode^=b.securitycode and a.date>b.end
group by a.fundacctnum,a.securitycode,a.date
order by a.fundacctnum,a.securitycode,a.date;
run;

proc sql;
create table temp1 as select distinct a.*, count(b.date) as last_num, (sum(b.return)+sum(b.papervalue))/sum(b.invest)-1 as last_return
from temp1 a left join warrant_psudocycle_return b
on a.fundacctnum=b.fundacctnum and a.securitycode^=b.securitycode and a.lastend=b.date and b.ind_date=0
group by a.fundacctnum,a.securitycode,a.date
order by a.fundacctnum,a.securitycode,a.date;
run;

proc sql;
create table temp2 as select distinct a.*, count(b.date) as other_num, (sum(b.return)+sum(b.papervalue))/sum(b.invest)-1 as other_return
from temp1 a left join warrant_psudocycle_return b
on a.fundacctnum=b.fundacctnum and a.securitycode^=b.securitycode and (a.lastend>b.date or (a.lastend=b.date and b.ind_date=1))
group by a.fundacctnum,a.securitycode,a.date
order by a.fundacctnum,a.securitycode,a.date;
run;

data temp2;
set temp2;
if hold_num>1 then hold_num=1;
if last_num>1 then last_num=1;
if other_num>1 then other_num=1;
if hold_return=. then hold_return=0;
if last_return=. then last_return=0;
if other_return=. then other_return=0;
run;

/* Delete all type-4 (totally new investors) */

data temp3;
set temp2;
if (hold_num=0 and last_num=0 and other_num=0) then delete;
run;

data warrant.warrant_expand_new;
set temp3;
run;
