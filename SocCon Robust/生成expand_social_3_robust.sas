proc sql;
create table warrant_robust as select distinct securitycode,branch from warrant.social_dayrobust
order by securitycode, branch;
run;

data warrant_robust;
set warrant_robust;
ind_city=1;
run;

data warrant_robust;
set warrant_robust;
format a best12.;
a=securitycode;
drop securitycode;
rename a=securitycode;
run;

proc sql;
create table expand_social_3 as select distinct a.*, b.ind_city from expand_social_3 a left join warrant_robust b
on a.securitycode=b.securitycode and a.branch=b.branch
order by securitycode, branch ,date;	
run;

data expand_social_3;
set expand_social_3;
if ind_city=. then delete;
run;

data expand_social_3_robust;
set expand_social_3;
drop ind_city;
run;
