proc sql;
create table warrant_code as select distinct stockcode, securitycode from warrant.twarrant_1
order by securitycode;
run;

data warrant_code;
set warrant_code;
a=strip(put(stockcode,6.));
b=strip(put(securitycode,6.));
run;

data warrant_code;
set warrant_code;
if a='629' then a='000629';
if a='2' then a='000002';
if a='932' then a='000932';
if a='858' then a='000858';
if a='27' then a='000027';
if a='39' then a='000039';
if a='792' then a='000792';
if b='38001' then b='038001';
if b='38002' then b='038002';
if b='38003' then b='038003';
if b='38004' then b='038004';
if b='38005' then b='038005';
if b='38006' then b='038006';
if b='38008' then b='038008';
run;

data warrant_code;
set warrant_code;
drop stockcode securitycode;
rename a=stockcode b=securitycode;
run;

data raw_2;
set minzu.raw_2;
Date1=mdy(substr(date,5,2),substr(date,7,2),substr(date,1,4));
format date1 yymmdd10.;
drop date;
rename date1=date;
run;

proc sql;
create table raw_stock as select * from raw_2 
where code in (select distinct stockcode from warrant_code);
run;

data raw_stock;
set raw_stock;
if ((BS="买入" and StockType="股票" and Business="证券买入" )
or (BS="卖出" and StockType="股票" and Business="证券卖出" )
or (BS="买入" and StockType="股票" and Business="新股入帐" )
or (BS="买入" and StockType="配售申购" and Business="配售确认" )
or (BS="买入" and StockType="股票" and Business="配售确认" )
or (BS="买入" and StockType="股票" and Business="托管转入" )
or (BS="买入" and StockType="创业板" and Business="证券买入" )
or (BS="卖出" and StockType="创业板" and Business="证券卖出" )
or (BS="买入" and StockType="创业板" and Business="新股入帐" )
or (BS="买入" and StockType="创业板" and Business="转托转入" )
or (BS="卖出" and StockType="创业板" and Business="转托转出" )
or (BS="买入" and StockType="股票" and Business="红股入帐" )
or (BS="买入" and StockType="股票" and Business="配股入帐" )
or (BS="买入" and StockType="股票" and Business="托管转入" )
or (BS="买入" and StockType="股票" and Business="转托转出" )
or (BS="卖出" and StockType="股票" and Business="托管转出" )
or (BS="卖出" and StockType="股票" and Business="转托转入" )
or (BS="买入" and StockType="股票" and Business="余额入账" )
or (BS="买入" and StockType="股票" and Business="余额更新" )
or (BS="卖出" and StockType="股票" and Business="余额入账" )
or (BS="卖出" and StockType="股票" and Business="余额更新" ));
run;

data raw_stock;
set raw_stock;
if length(TIME)=1 then var3=HMS(substr(TIME,1,1),0,0);
	else if length(TIME)=2 then var3=HMS(substr(TIME,1,2),0,0);
	else if length(TIME)=5 then var3=HMS(substr(TIME,1,1),substr(TIME,2,2),substr(TIME,4,2));
	else var3=HMS(substr(TIME,1,2),substr(TIME,3,2),substr(TIME,5,2));
format var3 time.;
drop time;
rename var3=time;
run;

data raw_stock;
set raw_stock;
obs=_n_;
run;

data temp;
set raw_stock;
if date<'30MAY2007'd;
run;

proc sort data=temp;
by id code date time obs;
run;

data temp;
set temp;
by id code;
if last.code;
run;

data temp;
set temp;
format a best12.;
a=position;
drop position;
rename a=position;
run;

data temp_before;
set temp;
if position>=100;
run;

proc sql;
create table temp_before as select distinct id,code from temp_before
order by id,code;
run;

data temp;
set raw_stock;
if date>='30MAY2007'd;
run;

proc sql;
create table temp_after as select distinct id,code from temp 
order by id,code;
run;

proc sql;
create table warrant_trade as select distinct fundacctnum, securitycode from warrant.warrant
where date>='30MAY2007'd
order by fundacctnum, securitycode;
run;

proc sql;
create table warrant_trade as select distinct a.*, b.stockcode from warrant_trade a left join warrant_code b
on a.securitycode=b.securitycode
order by fundacctnum,securitycode;
run;

data stock_trade;
set temp_before temp_after;
run;

proc sql;
create table stock_trade as select distinct * from stock_trade
order by id,code;
run;

proc sql;
create table warrant_both as select distinct a.* from warrant_trade a, stock_trade b
where a.fundacctnum=b.id and a.stockcode=b.code
order by fundacctnum, stockcode;
run;

proc sql;
create table stat1 as select distinct securitycode, count(securitycode) as warrant_num from warrant_trade
group by securitycode
order by securitycode;
run;

proc sql;
create table stat2 as select distinct securitycode, count(securitycode) as stock_num from warrant_both
group by securitycode
order by securitycode;
run;

data stat;
merge stat1 stat2;
by securitycode;
run;
