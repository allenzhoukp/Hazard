data warrant_expand1;
set temp1.warrant_expand1;
drop lag1_adjfundamental lag2_adjfundamental lag3_adjfundamental;
run;


proc sql;
create table warrant_expand1 as select distinct a.*,(1-b.exprice/b.stockprice)/b.n_day as lag1_adjfundamental from warrant_expand1 a left join warrant.warranttrans_1 b
on a.securitycode=b.securitycode and a.lagdate=b.date;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,(1-b.exprice/b.stockprice)/b.n_day as lag2_adjfundamental from warrant_expand1 a left join warrant.warranttrans_1 b
on a.securitycode=b.securitycode and a.lag2date=b.date;
quit;

proc sql;
create table warrant_expand1 as select distinct a.*,(1-b.exprice/b.stockprice)/b.n_day as lag3_adjfundamental from warrant_expand1 a left join warrant.warranttrans_1 b
on a.securitycode=b.securitycode and a.lag3date=b.date;
quit;



data warrant_expand1_2;
set warrant_expand1;
if lag2_return^=.;
run;

data warrant_expand1_1;
set warrant_expand1;
if lag2_return=. and lag1_return^=.;
run;

proc sort data=tempsave.warrant_expand1_1;
by fundacctnum securitycode date;
run;

proc sort data=tempsave.warrant_expand1_2;
by fundacctnum securitycode date;
run;