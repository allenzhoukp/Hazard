proc sql;
create table warrant_diff as select distinct securitycode, date, sum(diff) as pre_diff from warrant.expand_social_5
group by securitycode, date
order by securitycode, date;
run;

proc sql;
create table warrant_diff as select distinct a.*, b.amount as amount_new from warrant_diff a left join  warrant.warrant_start_new b
on a.securitycode=b.securitycode
order by securitycode,date;
run;

data warrant_diff;
set warrant_diff;
pre_volume_diff=pre_diff*amount_new;
run;

data warrant_diff;
set warrant_diff;
if securitycode=38003 or securitycode=38004 or securitycode=38006 or securitycode=38008 or securitycode=580997;
keep securitycode date pre_volume_diff;
rename pre_volume_diff=volume_soccont;
run;

data use_date1;
set warrant.use_date1;
if securitycode=38003 or securitycode=38004 or securitycode=38006 or securitycode=38008 or securitycode=580997;
keep securitycode date diff;
rename diff=volume_onecycle;
run;

data use_date2;
set warrant.use_date2;
if securitycode=38003 or securitycode=38004 or securitycode=38006 or securitycode=38008 or securitycode=580997;
keep securitycode date diff;
rename diff=volume_twocycle;
run;

data warrant_volume;
merge use_date1 use_date2 warrant_diff;
by securitycode date;
run; 

data warrant_volume;
set warrant_volume;
if date='29FEB2008'd then delete;
volume_sum=volume_soccont+volume_onecycle+volume_twocycle;
run;

proc sql;
create table warrant_volume as select distinct a.*, b.wrprice from warrant_volume a left join warrant.warranttrans_1 b
on a.securitycode=b.securitycode and a.date=b.date
order by securitycode, date;
run;