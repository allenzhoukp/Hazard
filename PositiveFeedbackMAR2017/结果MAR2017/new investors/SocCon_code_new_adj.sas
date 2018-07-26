proc sort data=warrant.warranttrans_1 out=warranttrans_1;
by securitycode date;
run;

data warranttrans_1;
set warranttrans_1;
by securitycode date;
if first.securitycode then n=0;
n+1;
run;

data warranttrans_1;
set warranttrans_1;
mktrp4=wrprice/lag4(wrprice)-1;
mktrp15=wrprice/lag15(wrprice)-1;
run;

data warranttrans_1;
set warranttrans_1;
if n<=4 then mktrp4=.; 
if n<=15 then mktrp15=.; 
run;

data warranttrans_1;
set warranttrans_1;
mktrp1=lag2(mktrp4);
mktrp2=lag6(mktrp15);
run;

data warranttrans_1;
set warranttrans_1;
if n<=2 then mktrp1=.; 
if n<=6 then mktrp2=.; 
run;

data warranttrans_1;
set warranttrans_1; 
lagmkt=lag(ChangePercent);
if n=1 then lagmkt=.;
run;

data warranttrans_1;
set warranttrans_1; 
fundamental=(1-exprice/stockprice)/n_day;
lagfundamental=lag(fundamental);
if n=1 then lagfundamental=.;
run;

proc sql;
create table warranttrans_1 as select distinct a.*, mean(b.changeratio) as turnover1 from warranttrans_1 a left join warranttrans_1 b
on a.securitycode=b.securitycode and a.n-1=b.n
group by a.securitycode, a.n
order by a.securitycode, a.n;
run;

proc sql;
create table warranttrans_1 as select distinct a.*, mean(b.changeratio) as turnover2 from warranttrans_1 a left join warranttrans_1 b
on a.securitycode=b.securitycode and a.n-2>=b.n and b.n>=a.n-5
group by a.securitycode, a.n
order by a.securitycode, a.n;
run;

proc sql;
create table warranttrans_1 as select distinct a.*, mean(b.changeratio) as turnover3 from warranttrans_1 a left join warranttrans_1 b
on a.securitycode=b.securitycode and a.n-6>=b.n and b.n>=a.n-20
group by a.securitycode, a.n
order by a.securitycode, a.n;
run;

proc sql;
create table warrant.warrant_expand_new as select distinct a.*, b.lagmkt as lagmkt, b.mktrp1 as mktrp1, b.mktrp2 as mktrp2, b.turnover1 as turnover1, b.turnover2 as turnover2, b.turnover3 as turnover3, b.lagfundamental as lagfundamental, b.n_day as n_day
from warrant.warrant_expand_new a left join warranttrans_1 b
on a.securitycode=b.securitycode and a.date=b.date
order by fundacctnum,securitycode,date;
run;
