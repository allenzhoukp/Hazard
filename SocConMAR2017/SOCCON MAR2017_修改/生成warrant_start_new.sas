data warrant_start;
set warrant.warrant_start;
if psudocycle=1;
run;

proc sql;
create table warrant.warrant_start_new as select distinct securitycode, mean(amount) as amount from warrant_start
group by securitycode
order by securitycode;
run;

data warrant.warrant_start_new;
set warrant.warrant_start_new;
format a best12.;
a=securitycode;
drop securitycode;
rename a=securitycode;
run;