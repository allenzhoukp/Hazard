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

proc sql;
create table warrant_psudocycle_expand as select distinct a.*, b.date from warrant_psudocycle a left join warrant.warranttrans b 
on a.securitycode=b.securitycode and b.date>=a.start and b.date<=a.end
order by fundacctnum, securitycode, psudocycle ,date;
run;

data warrant_date;
set warrant_date;
obs=_n_;
run;

proc sql;
create table warrant_psudocycle_expand as select distinct a.*, b.amount, b.price, b.obs from warrant_psudocycle_expand a left join warrant_date b on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.psudocycle=b.psudocycle and a.date=b.date
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
create table warrant_psudocycle_expand as select distinct a.*, b.wrprice from warrant_psudocycle_expand a left join warrant.warranttrans b
on a.securitycode=b.securitycode and a.date=b.date
order by fundacctnum, securitycode, psudocycle ,date, obs;
run;

data warrant_psudocycle_expand;
set warrant_psudocycle_expand;
papervalue=position*wrprice;
returnrate=(papervalue+return)/invest-1;
run;

proc sort data=warrant_psudocycle_expand out=warrant_psudocycle_date;
by fundacctnum securitycode date psudocycle obs;
run;

data warrant_psudocycle_date;
set warrant_psudocycle_date;
by fundacctnum securitycode date psudocycle obs;
if last.date;
run;

data warrant_psudocycle_date;
set warrant_psudocycle_date;
format a best12.;
a=securitycode;
drop securitycode;
rename a=securitycode;
run;

data warrant_expand1_1;
set warrant.warrant_expand1_1;
drop maturity lagdate lag1_wrprice lag2date lag3date lag1_bs lag2_bs lag3_bs lag1_fundamental lag2_fundamental lag3_fundamental lag3_market_ret lag3_turnover lag1_large_ret lag2_large_ret lag3_adjfundamental;
run;

proc sql;
create table temp as select distinct a.*, count(b.date) as hold_num, sum(b.invest) as hold_invest, sum(b.return) as hold_return, sum(b.papervalue) as hold_papervalue 
from warrant_expand1_1 a left join warrant_psudocycle_date b
on a.fundacctnum=b.fundacctnum and a.securitycode^=b.securitycode and a.date=b.date
group by a.fundacctnum,a.securitycode,a.date
order by a.fundacctnum,a.securitycode,a.date;
run;

proc sql;
create table temp as select distinct a.*, count(b.date) as hold_num, (sum(b.return)+sum(b.papervalue))/sum(b.invest)-1 as hold_return 
from warrant_expand1_1 a left join warrant_psudocycle_date b
on a.fundacctnum=b.fundacctnum and a.securitycode^=b.securitycode and a.date=b.date
group by a.fundacctnum,a.securitycode,a.date
order by a.fundacctnum,a.securitycode,a.date;
run;

proc sort data=warrant_psudocycle_expand;
by fundacctnum securitycode psudocycle date obs;
run;

data warrant_psudocycle_return;
set warrant_psudocycle_expand;
by fundacctnum securitycode psudocycle date obs;
if last.psudocycle;
run;

proc sort data=warrant_psudocycle_return;
by fundacctnum securitycode descending date descending psudocycle descending obs;
run;

data warrant_psudocycle_return;
set warrant_psudocycle_return;
by fundacctnum securitycode descending date descending psudocycle descending obs;
if first.date then ind=0;
ind+1;
run;

proc sort data=warrant_psudocycle_return;
by fundacctnum securitycode  psudocycle date obs;
run;

data warrant_psudocycle_return;
set warrant_psudocycle_return;
format a best12.;
a=securitycode;
drop securitycode;
rename a=securitycode;
run;

proc sql;
create table temp as select distinct a.*, (sum(b.return)+sum(b.papervalue))/sum(b.invest)-1 as prev_return 
from temp a left join warrant_psudocycle_return b
on a.fundacctnum=b.fundacctnum and a.securitycode^=b.securitycode and (a.date>b.date or (a.date=b.date and b.ind>1))
group by a.fundacctnum,a.securitycode,a.date
order by a.fundacctnum,a.securitycode,a.date;
run;
