proc sql;
create table warrant_prenew as select distinct securitycode, date, sum(diff) as pre_new from warrant.expand_social_5
group by securitycode, date
order by securitycode, date;
run;

proc sql;
create table warrant_prenew as select distinct a.*, b.amount as amount_new from warrant_prenew a left join warrant.warrant_start_new b
on a.securitycode=b.securitycode
order by securitycode, date;
run;

data warrant_prenew;
set warrant_prenew;
pre_new_volume=pre_new*amount_new;
run;

proc sql;
create table Xiong_shock_3 as select distinct a.*, b.pre_new_volume as pre_new_2 from warrant.Xiong_shock_2 a left join warrant_prenew b
on a.securitycode = b.securitycode and a.date=b.date 
order by securitycode, date;
run; 

data warrant.Xiong_shock_3_revised2;
set Xiong_shock_3;
pre_new_2_scale=pre_new_2/gta_tshare*10000;
run;