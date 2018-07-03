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

data warrant_date;
set warrant_date;
by fundacctnum securitycode psudocycle date time;
if first.psudocycle then invest=0;
if amount>0 then invest+amount*price;
if first.psudocycle then return=0;
if amount<0 then return+amount*price;
returnrate=-1*return/invest-1;
run;

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

data warrant_date;
set warrant_date;
drop invest return returnrate lagfund lagsecurity lagmark;
run;

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

proc sql;
create table warrant_expand as select distinct a.*,b.start from warrant_expand a left join warrant_psudocycle b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date=b.start;
quit;

data warrant_expand;
set warrant_expand;
if start=. then start=0;
else start=1;
format start best.;
run;

proc sql;
create table warrant_daily as select distinct fundacctnum,securitycode,date,sum(amount) as amount_daily from warrant.warrant_date
group by fundacctnum,securitycode,date;
quit;

/*
proc sql;
create table warrant_expand as select distinct a.*,b.amount_daily from warrant_expand a left join warrant_daily b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date=b.date;
quit;

data warrant_expand;
set warrant_expand;
if start=0 and amount_daily>0 then start=1;
drop amount_daily;
run;
*/

proc sql;
create table warrant_expand1 as select distinct a.*,max(b.psudocycle) as psudocycle1 from warrant_expand a left join warrant_psudocycle b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date>b.end
group by a.fundacctnum,a.securitycode,a.date;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,b.return as lag1_return from warrant_expand1 a left join warrant_psudocycle b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.psudocycle1=b.psudocycle;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,max(b.psudocycle)-1 as psudocycle2 from warrant_expand1 a left join warrant_psudocycle b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date>b.end
group by a.fundacctnum,a.securitycode,a.date;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,b.return as lag2_return from warrant_expand1 a left join warrant_psudocycle b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.psudocycle2=b.psudocycle;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,max(b.psudocycle)-2 as psudocycle3 from warrant_expand1 a left join warrant_psudocycle b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date>b.end
group by a.fundacctnum,a.securitycode,a.date;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,mean(b.return) as lag3_return from warrant_expand1 a left join warrant_psudocycle b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.psudocycle2>b.psudocycle
group by a.fundacctnum,a.securitycode,a.psudocycle3;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,mean(b.return) as lag4_return from warrant_expand1 a left join warrant_psudocycle b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.psudocycle3>b.psudocycle
group by a.fundacctnum,a.securitycode,a.psudocycle3;
quit;

proc sql;
create table end_date as select distinct securitycode,begin_date,end_date from warrant.warrant_date;
quit;

proc sql;
create table warranttrans as select distinct a.*,count(distinct b.date) as maturity_trading,a.end_date-a.date as maturity_calendar from warrant.warranttrans a left join warrant.warranttrans b
on a.securitycode=b.securitycode and b.date>a.date
group by a.securitycode,a.date;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,b.end_date-a.date as maturity from warrant_expand1 a left join end_date b
on a.securitycode=b.securitycode;
quit;

data warrant_expand1;
set warrant_expand1;
by fundacctnum securitycode date;
lagdate=lag(date);
if not first.securitycode;
format lagdate date.;
run;

data warrant_expand1;
set warrant_expand1;
by fundacctnum securitycode date;
lag2date=lag(lagdate);
if not first.securitycode;
format lagdate date.;
run;

data warrant_expand1;
set warrant_expand1;
by fundacctnum securitycode date;
lag3date=lag(lag2date);
if not first.securitycode;
format lagdate date.;
run;

proc sql;
create table warrant_expand1 as select distinct a.*,b.wrprice as lag1_wrprice,b.changepercent as lag1_market_ret,b.changeratio as lag1_turnover,b.wrprice-b.p2 as lag1_bs,1-b.exprice/b.stockprice as lag1_fundamental,(1-b.exprice/b.stockprice)/b.maturity as lag1_adjfundamental from warrant_expand1 a left join warrant.warranttrans b
on a.securitycode=b.securitycode and a.lagdate=b.date;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,b.changepercent as lag2_market_ret,b.changeratio as lag2_turnover,b.wrprice-b.p2 as lag2_bs,1-b.exprice/b.stockprice as lag2_fundamental,(1-b.exprice/b.stockprice)/b.maturity as lag2_adjfundamental from warrant_expand1 a left join warrant.warranttrans b
on a.securitycode=b.securitycode and a.lag2date=b.date;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,b.changepercent as lag3_market_ret,b.changeratio as lag3_turnover,b.wrprice-b.p2 as lag3_bs,1-b.exprice/b.stockprice as lag3_fundamental,(1-b.exprice/b.stockprice)/b.maturity as lag3_adjfundamental from warrant_expand1 a left join warrant.warranttrans b
on a.securitycode=b.securitycode and a.lag3date=b.date;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,b.start as start_date from warrant_expand1 a left join warrant_psudocycle b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date>=b.start and a.date<=end;
quit;

data warrant_expand1;
set warrant_expand1;
if start_date^=. and start=0 then delete;
drop start_date;
run;

proc sql;
create table warrant_expand1 as select distinct * from warrant_expand1;
quit;

data warrant_expand1;
set warrant_expand1;
if maturity<=20 then m20=1;
else m20=0;
if maturity<=5 then m5=1;
else m5=0;
run;

data warrant_expand1;
set warrant_expand1;
lag1_return_m20=lag1_return*m20;
lag2_return_m20=lag2_return*m20;
lag3_return_m20=lag3_return*m20;
lag1_return_m5=lag1_return*m5;
lag2_return_m5=lag2_return*m5;
lag3_return_m5=lag3_return*m5;
lag1_adjfundamental_m20=lag1_adjfundamental*m20;
lag2_adjfundamental_m20=lag2_adjfundamental*m20;
lag3_adjfundamental_m20=lag3_adjfundamental*m20;
lag1_adjfundamental_m5=lag1_adjfundamental*m5;
lag2_adjfundamental_m5=lag2_adjfundamental*m5;
lag3_adjfundamental_m5=lag3_adjfundamental*m5;
run;

data warrant_start;
set warrant_date;
by fundacctnum securitycode psudocycle date time;
if first.psudocycle;
wealth=amount*price;
keep fundacctnum securitycode psudocycle date amount price wealth;
run;

data warrant_end;
set warrant_date;
by fundacctnum securitycode psudocycle date time;
if last.psudocycle;
wealth=amount*price;
keep fundacctnum securitycode psudocycle date amount price wealth;
run;

proc sql;
create table warrant_expand1 as select distinct a.*,b.price as last_end_price from warrant_expand1 a left join warrant_end b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.psudocycle1=b.psudocycle;
quit;

data warrant_expand1;
set warrant_expand1;
lag_multi_ret=lag1_wrprice/last_end_price-1;
run;

data warrant_expand2;
set warrant_expand1;
if start=1;
run;

proc sql;
create table warrant_expand2 as select distinct a.*,b.amount as amount,b.price from warrant_expand2 a left join warrant_start b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date=b.date;
quit;

proc sql;
create table warrant_expand2 as select distinct a.*,b.price as last_start_price,b.amount as last_start_amount from warrant_expand2 a left join warrant_start b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.psudocycle1=b.psudocycle;
quit;

proc sql;
create table warrant_expand2 as select distinct a.*,mean(b.amount) as previous_start_amount from warrant_expand2 a left join warrant_start b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.psudocycle1>=b.psudocycle
group by a.fundacctnum,a.securitycode,a.psudocycle1;
quit;

proc sql;
create table warrant_expand2 as select distinct a.*,b.wrprice as lag1_price from warrant_expand2 a left join warrant.warranttrans b
on a.securitycode=b.securitycode and a.lagdate=b.date;
quit;

data warrant_expand2;
set warrant_expand2;
amount_ratio=log(amount/previous_start_amount);
price_ratio=price/last_start_price;
log_price=log(price/last_start_price)-lag1_return;
wealth_ratio=log(amount*price/(last_start_amount*last_start_price));
run;

data warrant_expand1;
set warrant_expand1;
lagstart=lag(start);
run;

data warrant_expand1;
set warrant_expand1;
by fundacctnum securitycode date;
if first.securitycode then begin=0;
else if lagstart=1 then begin=0;
else begin+1;
run;

data warrant_expand1;
set warrant_expand1;
end=begin+1;
run;

data warrant_expand1;
set warrant_expand1;
if lag1_return>0 then D1=1;
else D1=0;
if lag2_return>0 then D2=1;
else D2=0;
if lag3_return>0 then D3=1;
else D3=0;
if lag1_return>0.10 then lag1_large_ret=lag1_return-0.10;
else lag1_large_ret=0;
if lag2_return>0.10 then lag2_large_ret=lag2_return-0.10;
else lag2_large_ret=0;
if lag3_return>0.10 then lag3_large_ret=lag3_return-0.10;
else lag3_large_ret=0;
run;

data warrant_expand1_3;
set warrant_expand1;
if lag3_return^=.;
run;

data warrant_expand1_2;
set warrant_expand1;
if lag3_return=. and lag2_return^=.;
run;

data warrant_expand1_1;
set warrant_expand1;
if lag2_return=. and lag1_return^=.;
run;

data warrant_expand2_3;
set warrant_expand2;
if lag3_return^=.;
run;

data warrant_expand2_2;
set warrant_expand2;
if lag3_return=. and lag2_return^=.;
run;

data warrant_expand2_1;
set warrant_expand2;
if lag2_return=. and lag1_return^=.;
run;

proc glm data=warrant_expand2_1;
absorb securitycode;
model wealth_ratio=lag1_return m20 m5 lag1_return_m20 lag1_return_m5 lag1_market_ret lag2_market_ret lag3_market_ret lag1_turnover lag2_turnover lag3_turnover lag1_adjfundamental lag1_adjfundamental_m20 lag1_adjfundamental_m5/solution noint;
run;
quit;

proc phreg data=warrant_expand1_3;
class securitycode;
model (begin,end)*start(0)=lag1_return lag2_return lag3_return D1 D2 D3 lag1_large_ret lag2_large_ret lag3_large_ret m20 m5 lag1_return_m20 lag1_return_m5 lag2_return_m20 lag2_return_m5 lag3_return_m20 lag3_return_m5 lag1_market_ret lag2_market_ret lag3_market_ret lag1_turnover lag2_turnover lag3_turnover lag1_adjfundamental lag1_adjfundamental_m20 lag1_adjfundamental_m5 securitycode/ties=efron rl;
run; 

data baseline;
set baseline;
sum_lambda=log(survival)*(-1)+xbeta;
run;

data baseline;
set baseline;
lambda=sum_lambda-lag(sum_lambda);
run;

proc sql;
create table prediction as select distinct a.*,b.lambda,1-1/exp(b.lambda*exp(a.xbeta)) as prob from prediction a left join baseline b
on a.end=b.end;
quit;

proc sql;
create table prediction_18warrant as select distinct a.*,mean(b.amount) as previous_start_amount from prediction_18warrant a left join warrant_start b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.psudocycle1>=b.psudocycle
group by a.fundacctnum,a.securitycode,a.psudocycle1;
quit;

proc sql;
create table second_stage as select distinct securitycode,date,sum(prob*previous_start_amount) as predicted_demand,lagdate,max(lag2date) as lag2date,max(lag3date) as lag3date,lag1_market_ret/100 as lag1_market_ret,lag1_turnover,lag1_adjfundamental,m20,m5,lag1_adjfundamental_m20,lag1_adjfundamental_m5 from prediction_18warrant
group by securitycode,date;
quit;

proc sql;
create table second_stage as select distinct a.*,b.changepercent/100 as lag2_market_ret,b.changeratio as lag2_turnover from second_stage a left join warrant.warranttrans b
on a.securitycode=b.securitycode and a.lag2date=b.date;
quit;

proc sql;
create table second_stage as select distinct a.*,b.changepercent/100 as lag3_market_ret,b.changeratio as lag3_turnover from second_stage a left join warrant.warranttrans b
on a.securitycode=b.securitycode and a.lag3date=b.date;
quit;

proc sql;
create table second_stage as select distinct a.*,b.changepercent/100 as return,b.tshare,a.predicted_demand/b.tshare as predicted_demand_adj from second_stage a left join warrant.warranttrans b
on a.securitycode=b.securitycode and a.date=b.date;
quit;
























