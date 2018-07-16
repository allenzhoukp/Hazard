/* Last edited 2018-06-30
   Perhaps it is the most beginning one? */

/* The outcome dataset is most perhaps warrant_expand_social. */

/* Mark keeps the same value within a cycle (position from 0 to 0) */
data warrant_use_2;
set warrant.warrant;
lag1position=lag1(position);
retain mark 0;
by fundacctnum securitycode date time;
if first.fundacctnum or first.securitycode or lag1position=0 then mark+1;
run;

/* Within a cycle, count 1,2,...*/
data warrant_use_2;
set warrant_use_2;
by mark;
if first.mark then seq=0;
seq+1;
run;

/* Left_join begin_date and end_date from warrant.warrant_date */
proc sql;
create table warrant_begindate as select distinct securitycode, begin_date,end_date from warrant.warrant_date;
run;

proc sql;
create table warrant_use_2 as select distinct a.*, b.begin_date, b.end_date from warrant_use_2 a left join warrant_begindate b
on a.securitycode=b.securitycode
order by fundacctnum, securitycode, date, time , mark, seq;
run;

/* Transaction before begin_date are set as begin_date */
data warrant_use_2;
set warrant_use_2;
if date<begin_date then date=begin_date;
run;

/* TODO what is type?
   From the following code, 3 and 4 may indicate "missing". */
data warrant_use_2;
set warrant_use_2;
if (type=3 or type=4) then price=.;
if (type=3 or type=4) then value=.;
run;

data warrant_use_2;
set warrant_use_2;
drop lag1position;
run;

/* sort according to date, time, mark and seq (same mark, increament seq.) */
proc sort data=warrant_use_2;
by fundacctnum securitycode date time mark seq;
run;

/* First transaction of each investor's each security. Name that b_date. */
data warrant_start;
set warrant_use_2;
by fundacctnum securitycode date time mark seq;
if first.securitycode;
rename date=b_date;
keep fundacctnum securitycode date;
run;

/* Last transaction of each investor's each security. Name that e_date. */
data warrant_end;
set warrant_use_2;
by fundacctnum securitycode date time mark seq;
if last.securitycode;
rename date=e_date;
if position^=0 then date=end_date;
keep fundacctnum securitycode date;
run;

/* Merge d_date and e_date into warrant_daterange. */
data warrant_daterange;
merge warrant_start warrant_end;
by fundacctnum securitycode;
run;

/* Left join warranttrans to get all dates' b&e_date.
   In warranttrans there is no fundacctnum.
   So each trading date will coexist in multiple investors, if within their time range between first buy and last sell.
   For sure, it does not hold much information... */
proc sql;
create table warrant_expand as select distinct a.*, b.date from warrant_daterange a left join  warrant.warranttrans b
on a.securitycode=b.securitycode and a.b_date<=b.date and a.e_date >=b.date
order by fundacctnum,securitycode,date;
quit;
run;

/* ...so we get time, amount, position, price, mark, seq
   for all trading dates as long as they are within someone's range.

   Theoretically there shouldn't be rows in b but not in a.
   The increase in number of rows ahould be due to multiple transactions within a day. */
proc sql;
create table warrant_expand_1 as select distinct a.*, b.time,b.amount,b.position,b.price,b.mark,b.seq
from warrant_expand a left join warrant_use_2 b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date=b.date
order by fundacctnum, securitycode,date,time,mark,seq;
run;

/* Assign all NA positions to previous closest non-NA value. */
data warrant_expand_1;
set warrant_expand_1;
by fundacctnum securitycode date time mark seq;
retain  position_1;
if position^=. then position_1=position;
rename position_1=position;
drop position;
run;

/* Warrant_Investor:
   Delete trading dates between cycles.
   And, if containing multiple transactions within a day, only last trading within a day remains. */
data warrant_investor;
set warrant_expand_1;
if (position=0 and amount=.) then delete;
run;

data warrant_investor;
set warrant_investor;
by fundacctnum securitycode date time mark seq;
if last.date;
run;

/* Warrant_expand_2:
   Amount_1 sets all NA transaction as 0 amount transaction.
   Amount_buy and amount_sell keeps the absolute value.
   Invest, Pay is money involved in buy/sell.  */
data warrant_expand_2;
set warrant_expand_1;
if amount=. then amount_1=0;
if amount^=. then amount_1=amount;
if amount_1>=0 then amount_buy=amount_1;
if amount_1<0 then amount_buy=0;
if amount_1>=0 then amount_sell=0;
if amount_1<0 then amount_sell=-amount_1;
drop amount;
invest=amount_buy*price;
pay=amount_sell*price;
if invest=. then invest=0;
if pay=. then pay=0;
run;

/* sum up invest and pay. */
proc sql;
create table warrant_expand_2 as select distinct fundacctnum, securitycode, date, sum(invest) as invest, sum(pay) as pay
from warrant_expand_2
group by fundacctnum, securitycode, date
order by fundacctnum, securitycode, date;
run;


/* warrant_position keeps the final position within a day. */
data warrant_position;
set warrant_expand_1;
by fundacctnum securitycode date time mark seq;
if last.date;
run;

/* Get the last position within a day. (Warrant_expand_2 is also one-row-one-day. )*/
proc sql;
create table warrant_expand_2 as select distinct a.*, b.position  from warrant_expand_2 a left join warrant_position b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date=b.date
order by fundacctnum, securitycode, date;
run;

/* Get the closing price from warranttrans. */
proc sql;
create table warrant_expand_2 as select distinct a.*,b.wrprice as close_price from warrant_expand_2 a left join warrant.warranttrans b
on a.securitycode=b.securitycode and a.date=b.date
order by fundacctnum, securitycode, date;
run;

/* paper_pay is total warrant value one investor had.  */
data warrant_expand_2;
set warrant_expand_2;
paper_pay=position*close_price;
run;

/* warrant_missing keeps transaction w/o price and value (?).
   Set them as ind_missing=1, which affects the entire day it is involved in expand_2.*/
data warrant_missing;
set warrant_use_2;
if type=3 or type=4;
run;

proc sql;
create table warrant_missing as select distinct fundacctnum, securitycode, date from warrant_missing;
run;

data warrant_missing;
set warrant_missing;
ind=1;
run;

proc sql;
create table warrant_expand_2 as select distinct a.*, b.ind as ind_missing from warrant_expand_2 a left join warrant_missing b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date=b.date
order by fundacctnum, securitycode, date;
run;

data warrant_expand_2;
set warrant_expand_2;
if ind_missing=. then ind_missing=0;
run;

proc sort data=warrant_expand_2 out=warrant_expand_3;
by fundacctnum securitycode date;
run;


/* Copy expand_2 to expand_3.
   For each investor's each security, count n (days) in 1,2,... */

data warrant_expand_3;
set warrant_expand_3;
by fundacctnum securitycode date;
if first.securitycode then n=0;
n+1;
run;

/* Create lags */
data warrant_expand_3;
set warrant_expand_3;
lag1paper_pay=lag1(paper_pay);
lag2paper_pay=lag2(paper_pay);
lag3paper_pay=lag3(paper_pay);
lag4paper_pay=lag4(paper_pay);
lag5paper_pay=lag5(paper_pay);
lag1invest=lag1(invest);
lag2invest=lag2(invest);
lag3invest=lag3(invest);
lag4invest=lag4(invest);
lag1pay=lag1(pay);
lag2pay=lag2(pay);
lag3pay=lag3(pay);
lag4pay=lag4(pay);
lag1ind_missing=lag1(ind_missing);
lag2ind_missing=lag2(ind_missing);
lag3ind_missing=lag3(ind_missing);
lag4ind_missing=lag4(ind_missing);
run;


/* fill in lag values that's meaningless
   0 rather than NA */

data warrant_expand_3;
set warrant_expand_3;

if n=1 then do
lag1paper_pay=0;
lag2paper_pay=0;
lag3paper_pay=0;
lag4paper_pay=0;
lag5paper_pay=0;
lag1invest=0;
lag2invest=0;
lag3invest=0;
lag4invest=0;
lag1pay=0;
lag2pay=0;
lag3pay=0;
lag4pay=0;
lag1ind_missing=0;
lag2ind_missing=0;
lag3ind_missing=0;
lag4ind_missing=0;
end;

if n=2 then do
lag2paper_pay=0;
lag3paper_pay=0;
lag4paper_pay=0;
lag5paper_pay=0;
lag2invest=0;
lag3invest=0;
lag4invest=0;
lag2pay=0;
lag3pay=0;
lag4pay=0;
lag2ind_missing=0;
lag3ind_missing=0;
lag4ind_missing=0;
end;

if n=3 then do
lag3paper_pay=0;
lag4paper_pay=0;
lag5paper_pay=0;
lag3invest=0;
lag4invest=0;
lag3pay=0;
lag4pay=0;
lag3ind_missing=0;
lag4ind_missing=0;
end;

if n=4 then do
lag4paper_pay=0;
lag5paper_pay=0;
lag4invest=0;
lag4pay=0;
lag4ind_missing=0;
end;

if n=5 then do
lag5paper_pay=0;
end;

run;


/* Warrant_return:
   return's coming.
   If any missing date involved, return is set to NA.

   Drop all irrelevant variables. Keep only investor, warrant, date and returns.*/

data warrant_return;
set warrant_expand_3;

return_1day= (paper_pay+pay)/(invest+lag1paper_pay)-1;
if ind_missing=1 then return_1day=.;

return_2day= (paper_pay+pay+lag1pay)/(invest+ lag1invest+lag2paper_pay)-1;
if (ind_missing=1 or lag1ind_missing=1) then return_2day=.;

return_3day= (paper_pay+pay+lag1pay+lag2pay)/(invest+ lag1invest+ lag2invest+lag3paper_pay)-1;
if (ind_missing=1 or lag1ind_missing=1 or lag2ind_missing=1) then return_3day=.;

return_4day= (paper_pay+pay+lag1pay+lag2pay+lag3pay)/(invest+ lag1invest+ lag2invest+lag3invest+lag4paper_pay)-1;
if (ind_missing=1 or lag1ind_missing=1 or lag2ind_missing=1 or lag3ind_missing=1) then return_4day=.;

return_5day= (paper_pay+pay+lag1pay+lag2pay+lag3pay+lag4pay)/(invest+ lag1invest+ lag2invest+lag3invest++lag4invest+lag5paper_pay)-1;
if (ind_missing=1 or lag1ind_missing=1 or lag2ind_missing=1 or lag3ind_missing=1 or lag4ind_missing=1) then return_5day=.;

keep fundacctnum securitycode date return_1day return_2day return_3day return_4day return_5day;
run;


/* warrant_investor is the one without dates between cycles */
data warrant_social;
set warrant_investor;
keep fundacctnum securitycode date;
run;

/* Grab returns. No new rows created. */
proc sql;
create table warrant_social as select distinct a.*, b.return_1day, b.return_2day, b.return_3day, b.return_4day, b.return_5day
from warrant_social a left join warrant_return b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date=b.date
order by fundacctnum, securitycode, date;
run;

/* Grab branches. So you have the branch data, right?
   Notice that one investor only trades with one branch. */
proc sql;
create table warrant_branch as select distinct branch, fundacctnum from warrant_use_2;
run;

proc sql;
create table warrant_social as select distinct a.*, b.branch
from warrant_social a left join warrant_branch b
on a.fundacctnum=b.fundacctnum
order by fundacctnum, securitycode, date;
run;

/* return_iday_max = max{0, return_iday}
   return_iday_min = min{0, return_iday} */
data warrant_social;
set warrant_social;
if return_1day<=0 then return_1day_max=0;
if return_1day>0 then return_1day_max=return_1day;
if return_1day=. then return_1day_max=.;

if return_2day<=0 then return_2day_max=0;
if return_2day>0 then return_2day_max=return_2day;
if return_2day=. then return_2day_max=.;

if return_3day<=0 then return_3day_max=0;
if return_3day>0 then return_3day_max=return_3day;
if return_3day=. then return_3day_max=.;

if return_4day<=0 then return_4day_max=0;
if return_4day>0 then return_4day_max=return_4day;
if return_4day=. then return_4day_max=.;

if return_5day<=0 then return_5day_max=0;
if return_5day>0 then return_5day_max=return_5day;
if return_5day=. then return_5day_max=.;

if return_1day>0 then return_1day_min=0;
if return_1day<=0 then return_1day_min=return_1day;
if return_1day=. then return_1day_min=.;

run;


/* group by branch and date: MEAN of returns */
proc sql;
create table warrant_social_day as select distinct securitycode, branch, date,
mean(return_1day) as return_1day, mean(return_1day_max) as return_1day_max, mean(return_1day_min) as return_1day_min,
mean(return_2day) as return_2day, mean(return_2day_max) as return_2day_max,
mean(return_3day) as return_3day, mean(return_3day_max) as return_3day_max,
mean(return_4day) as return_4day, mean(return_4day_max) as return_4day_max,
mean(return_5day) as return_5day, mean(return_5day_max) as return_5day_max,
mean(return_1day**2) as squ_return_1day, mean(return_1day_max**2) as squ_return_1day_max, mean(return_1day_min**2) as squ_return_1day_min,
mean(exp(return_1day)) as exp_return_1day, mean(exp(return_1day_max)) as exp_return_1day_max, mean(exp(return_1day_min)) as exp_return_1day_min
from warrant_social
group by securitycode, branch, date
order by securitycode, branch, date;
run;



/* warrant_investor grab ind_missing annd branch as well*/
proc sql;
create table warrant_investor as select distinct a.*, b.ind as ind_missing
from warrant_investor a left join warrant_missing b
on a.fundacctnum=b.fundacctnum and a.securitycode=b.securitycode and a.date=b.date
order by fundacctnum, securitycode, date;
run;

proc sql;
create table warrant_investor as select distinct a.*, b.branch
from warrant_investor a left join warrant_branch b
on a.fundacctnum=b.fundacctnum
order by fundacctnum, securitycode, date;
run;

/* new: keep only first non_type-3/4 transaction
   this "new" means new investors. */
data warrant_investor_new ;
set warrant_investor;
by fundacctnum securitycode date;
if first.securitycode;
if ind_missing=1 then delete;
run;

/* count the number of new investor every day
   for: within each branch; within each brokerage. */
proc sql;
create table warrant_investor_new_branch as select distinct securitycode, branch, date, count(date) as branch_new_investor from warrant_investor_new
group by securitycode, branch, date
order by securitycode, branch, date;
run;

proc sql;
create table warrant_investor_new_brokerage as select distinct securitycode, date, count(date) as brokerage_new_investor from warrant_investor_new
group by securitycode, date
order by securitycode, date;
run;

/* count the number of total investor every day
   for: within each branch; within each brokerage. */
proc sql;
create table warrant_investor_branch as select distinct securitycode, branch, date, count(date) as branch_investor from warrant_investor
group by securitycode, branch, date
order by securitycode, branch, date;
run;

proc sql;
create table warrant_investor_brokerage as select distinct securitycode, date, count(date) as brokerage_investor from warrant_investor
group by securitycode, date
order by securitycode, date;
run;


/* Warrant_expand_social:
   Expand transaction data into full set of
   <branch, securitycode, security-trading-dates> set.
 */
data warranttrans;
set warrant.warranttrans;
keep date securitycode;
run;

proc sql;
create table temp1 as select distinct branch,securitycode from warrant_use_2;
quit;

proc sql;
create table warrant_expand_social as select distinct b.branch,b.securitycode,a.date
from warranttrans a, temp1 b
where a.securitycode=b.securitycode;
quit;

/* Get MEAN returns (warrant_social_day). */
proc sql;
create table warrant_expand_social as select distinct a.*,
b.return_1day, b.return_2day, b.return_3day, b.return_4day, b.return_5day,
b.return_1day_max,b.return_2day_max,b.return_3day_max,b.return_4day_max,b.return_5day_max,b.squ_return_1day,b.squ_return_1day_max,b.exp_return_1day,b.exp_return_1day_max,
b.return_1day_min,b.squ_return_1day_min,b.exp_return_1day_min

from warrant_expand_social a left join warrant_social_day b
on a.securitycode=b.securitycode and a.branch=b.branch and a.date=b.date
order by securitycode, branch, date;
run;

/* Get no. of new/all investors within branch/brokerage.
   Set their NAs to 0. */
proc sql;
create table warrant_expand_social as select distinct a.*, b.branch_new_investor
from warrant_expand_social a left join warrant_investor_new_branch b
on a.securitycode=b.securitycode and a.branch=b.branch and a.date=b.date
order by securitycode, branch, date;
run;

proc sql;
create table warrant_expand_social as select distinct a.*, b.brokerage_new_investor
from warrant_expand_social a left join warrant_investor_new_brokerage b
on a.securitycode=b.securitycode and a.date=b.date
order by securitycode, branch, date;
run;

proc sql;
create table warrant_expand_social as select distinct a.*, b.branch_investor
from warrant_expand_social a left join warrant_investor_branch b
on a.securitycode=b.securitycode and a.branch=b.branch and a.date=b.date
order by securitycode, branch, date;
run;

proc sql;
create table warrant_expand_social as select distinct a.*, b.brokerage_investor
from warrant_expand_social a left join warrant_investor_brokerage b
on a.securitycode=b.securitycode and a.date=b.date
order by securitycode, branch, date;
run;

data warrant_expand_social;
set warrant_expand_social;
if branch_new_investor=. then branch_new_investor=0;
if brokerage_new_investor=. then brokerage_new_investor=0;
if branch_investor=. then branch_investor=0;
if brokerage_investor=. then brokerage_investor=0;
run;

/* id is for branch-security pair. */
data warrant_expand_social;
set warrant_expand_social;
by securitycode branch date;
retain id 0;
if first.branch then id+1;
run;

/* switch security code into group variable type */
data warrant_expand_social;
set warrant_expand_social;
format a best12.;
a=securitycode;
drop securitycode;
rename a=securitycode;
run;

/* n_1 should be something in Black-Scholes formula, perhaps.
   TODO Where do twarrant_1 come from? */
proc sql;
create table warrant_expand_social as select distinct a.*, b.n_1
from warrant_expand_social a left join  warrant.twarrant_1 b
on a.securitycode=b.securitycode and a.date=b.date
order by securitycode, branch, date;
run;

/* Adjusted: brokerage - branch
   TODO ? */
data warrant_expand_social;
set warrant_expand_social;
brokerage_new_investor_adj=brokerage_new_investor-branch_new_investor;
brokerage_investor_adj=brokerage_investor-branch_investor;
run;

/* Get warranttrans Again. Replace securitycode into group variable again. */
data warranttrans;
set warrant.warranttrans;
format a best12.;
a=securitycode;
drop securitycode;
rename a=securitycode;
run;

/* Grab market return and turnover
   Comment: then why change to group type that early? */
proc sql;
create table warrant_expand_social as select distinct a.*, b.changepercent as market_return, b.changeratio as turnover
from warrant_expand_social a left join  warranttrans b
on a.securitycode=b.securitycode and a.date=b.date
order by securitycode, branch, date;
run;

/* ...and float (shares outstanding). */
proc sql;
create table warrant_expand_social as select distinct a.*, b.tshare as float
from warrant_expand_social a left join  warranttrans b
on a.securitycode=b.securitycode and a.date=b.date
order by securitycode, branch, date;
run;

/* warrant_expand_social_3: count time for each security, each branch as 1,2,... */
data warrant_expand_social_3;
set warrant_expand_social;
by securitycode branch date;
if first.branch then time=0;
time+1;
run;

/* Grab n_day (TODO what is that then?) */
proc sql;
create table warrant_expand_social_3 as select distinct a.*, b.n_day
from warrant_expand_social_3 a left join warrant.warranttrans_1 b
on a.securitycode=b.securitycode and a.date=b.date
order by securitycode, branch ,date;
run;

/* Truncated: drop the first three days for each security, each branch*/
data warrant_expand_social_3_trun;
set warrant_expand_social_3;
if time<=3 then delete;
run;
